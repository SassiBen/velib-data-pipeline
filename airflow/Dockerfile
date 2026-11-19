FROM apache/airflow:2.10.5-python3.12

USER root
# dbt dans son propre environnement : ses dépendances (protobuf, jinja2...) ne
# doivent pas se battre avec les contraintes d'Airflow. DuckDB est épinglé à la
# même version des deux côtés : un fichier écrit par une version récente ne se
# relit pas avec une plus ancienne.
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-duckdb==1.9.1" "duckdb==2.0.0" \
    && chown -R airflow: /opt/dbt-venv
# Le volume de l'entrepôt hérite de ce propriétaire à sa création ; sans cette
# ligne il appartient à root et dbt ne peut pas y écrire.
RUN mkdir -p /data/warehouse && chown airflow: /data/warehouse
USER airflow
RUN pip install --no-cache-dir "deltalake==0.22.3" "duckdb==2.0.0" "prometheus-client==0.21.1"
