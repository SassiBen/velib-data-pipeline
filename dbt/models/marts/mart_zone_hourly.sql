-- Vélos disponibles par zone et par heure.
select
    s.zone,
    h.hour,
    count(distinct h.station_id)   as stations,
    round(sum(h.avg_bikes), 0)     as bikes,
    round(sum(h.avg_ebikes), 0)    as ebikes,
    round(avg(h.empty_ratio), 3)   as empty_ratio
from {{ ref('fct_station_hourly') }} h
join {{ ref('dim_stations') }} s using (station_id)
group by all
