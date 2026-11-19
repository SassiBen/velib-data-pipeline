-- Le code d'une station parisienne commence par son arrondissement, suivi de
-- trois chiffres : 9020 est dans le 9e, 16107 dans le 16e. Au-delà de 20, ce
-- sont des communes de la métropole. Une première version exigeait cinq
-- chiffres et rangeait tout le centre de Paris (1er au 9e) en « Métropole » :
-- d'où le test assert_twenty_arrondissements.
with codes as (
    select *, try_cast(station_code as integer) // 1000 as prefix
    from {{ ref('stg_stations') }}
)
select
    station_id,
    station_code,
    name,
    lat,
    lon,
    capacity,
    case
        when prefix between 1 and 20 then 'Paris ' || cast(prefix as varchar)
        else 'Métropole'
    end as zone
from codes
