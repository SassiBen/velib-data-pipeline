select
    station_id,
    station_code,
    name,
    lat,
    lon,
    capacity,
    polled_at as updated_at
from {{ silver('stations') }}
