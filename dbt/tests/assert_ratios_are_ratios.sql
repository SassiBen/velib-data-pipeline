select *
from {{ ref('fct_station_hourly') }}
where empty_ratio not between 0 and 1 or full_ratio not between 0 and 1
