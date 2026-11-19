# Vélib' Data Pipeline

Un pipeline de données de bout en bout sur un flux public en temps réel : la
disponibilité des 1 526 stations Vélib' de la métropole parisienne. Ingestion
continue, lac de données en couches, modèles analytiques testés, orchestration
et supervision, avec les outils qu'on retrouve dans la plupart des plateformes
data : Kafka, Spark, Delta Lake, dbt, Airflow, Prometheus et Grafana, plus
l'équivalent AWS en Terraform.

Toute la pile tourne en local avec Docker Compose (8 Go de RAM suffisent).

![Architecture](docs/architecture.svg)

Le document complet est dans [docs/velib-data-pipeline.pdf](docs/velib-data-pipeline.pdf).

## Le parcours de la donnée

| Étape | Outil | Ce qui s'y passe |
|---|---|---|
| Collecte | Python | interroge le flux GBFS chaque minute, ne publie que les stations qui ont un nouveau relevé |
| Transport | Kafka (KRaft) | un topic par flux, clé = station, producteur idempotent |
| Bronze | Spark Structured Streaming, Delta Lake | chaque message brut avec sa position Kafka, partitionné par jour |
| Silver | Spark, Delta `MERGE` | relevés typés et dédoublonnés, référentiel des stations à jour |
| Gold | dbt + DuckDB | disponibilité par heure, stations saturées, état du réseau, par zone |
| Orchestration | Airflow | contrôle de fraîcheur, `dbt build`, métriques, export Parquet |
| Supervision | Prometheus, Pushgateway, Grafana | débit, erreurs de collecte, retard, état du réseau |
| Stockage objet | Garage (API S3) | le lac ; remplaçable par S3 sans toucher au code |

## Choix qui comptent

- **Un seul écrivain par table Delta.** La compaction et le nettoyage sont
  faits par le job Spark lui-même, pas par un second processus qui pourrait
  entrer en conflit avec ses écritures.
- **Idempotence à chaque étage.** Producteur Kafka idempotent, bronze en ajout
  avec point de reprise, silver en `MERGE` sur (station, horodatage du relevé) :
  rejouer un topic ne crée aucun doublon.
- **Le lac ne dépend que de l'API S3.** En local, Garage ; sur AWS, S3. MinIO,
  longtemps le choix par défaut, ne publie plus d'images ; la bascule n'a
  touché que le bloc de stockage du `docker-compose.yml`.
- **Des tests à chaque couche** : transformations Spark testées sans Kafka ni
  S3, 23 tests dbt, DAG Airflow chargé en CI, `terraform validate`.

## Ce que le vrai flux a appris

- Le serveur GBFS répond 403 à un client qui ne s'identifie pas : il faut un
  User-Agent explicite, en ASCII (un accent le rendait invalide et faisait
  échouer chaque collecte — un test le garde désormais).
- Le flux renvoie par intermittence un instantané plus ancien pour une centaine
  de stations. Le producteur ne peut pas le voir ; la couche silver
  dédoublonne : sur les 70 premières minutes, 5 794 messages pour 3 212 relevés
  uniques.
- 21 stations n'ont rien émis depuis plus de 7 jours, l'une depuis 2021. Elles
  restent dans le référentiel mais faussent les moyennes si on ne les filtre pas.
- Avec Garage, le connecteur S3A de Hadoop doit garder ses marqueurs de
  répertoire (`fs.s3a.directory.marker.retention=keep`) : sinon il réessaie
  indéfiniment des suppressions que le serveur refuse.

## Démarrage

```bash
cp .env.example .env       # renseigner les secrets (voir les commentaires)
docker compose up -d --build
```

| Interface | Adresse |
|---|---|
| Airflow | http://localhost:8080 (utilisateur `admin`) |
| Grafana | http://localhost:3000 |
| Kafka UI | `docker compose --profile debug up -d kafka-ui`, puis http://localhost:8081 |

Les premières tables Delta apparaissent quelques minutes après le démarrage ;
le DAG `velib_hourly` tourne à chaque heure.

## Structure

```
producer/        collecte GBFS -> Kafka (Python, confluent-kafka)
spark/jobs/      Kafka -> bronze -> silver (Structured Streaming, Delta)
dbt/             modèles staging et marts, tests
airflow/dags/    DAG horaire et ses tâches
monitoring/      Prometheus, tableau de bord Grafana provisionné
infra/local/     configuration et initialisation de Garage
infra/aws/       équivalent AWS : S3, MSK Serverless, EMR Serverless, Glue
```

## Tests

La CI exécute cinq jobs indépendants : tests du producteur, tests Spark
(PySpark local), `dbt build` sur un échantillon réel versionné, chargement des
DAG Airflow, `terraform validate`.

## Licence

Code sous licence MIT. Données : Vélib' Métropole, flux GBFS en open data.
