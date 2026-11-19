-- Disponibilité par station et par heure.
select
    station_id,
    date_trunc('hour', reported_at)                       as hour,
    count(*)                                              as reports,
    round(avg(bikes), 2)                                  as avg_bikes,
    min(bikes)                                            as min_bikes,
    round(avg(ebike), 2)                                  as avg_ebikes,
    round(avg(case when bikes = 0 then 1.0 else 0 end), 3) as empty_ratio,
    round(avg(case when docks = 0 then 1.0 else 0 end), 3) as full_ratio
from {{ ref('stg_station_status') }}
group by all
