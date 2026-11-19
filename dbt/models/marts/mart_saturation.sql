-- Stations les plus souvent vides ou pleines : là où la régulation doit passer.
select
    h.station_id,
    s.name,
    s.zone,
    count(*)                         as hours_observed,
    round(avg(h.empty_ratio), 3)     as empty_ratio,
    round(avg(h.full_ratio), 3)      as full_ratio
from {{ ref('fct_station_hourly') }} h
left join {{ ref('dim_stations') }} s using (station_id)
group by all
having count(*) >= 2
