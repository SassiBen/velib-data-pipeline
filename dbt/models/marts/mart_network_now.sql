-- État le plus récent de chaque station, pour le tableau de bord.
with latest as (
    select *,
        row_number() over (partition by station_id order by reported_at desc) as rn
    from {{ ref('stg_station_status') }}
)
select
    l.station_id,
    s.name,
    s.zone,
    s.capacity,
    l.bikes,
    l.mechanical,
    l.ebike,
    l.docks,
    l.reported_at,
    l.bikes = 0 as is_empty,
    l.docks = 0 as is_full
from latest l
left join {{ ref('dim_stations') }} s using (station_id)
where l.rn = 1
