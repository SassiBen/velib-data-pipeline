-- Les vingt arrondissements de Paris ont tous des stations : s'il en manque,
-- c'est la règle de découpage par code qui est fausse.
select count(distinct zone) as arrondissements
from {{ ref('dim_stations') }}
where zone like 'Paris %'
having count(distinct zone) <> 20
