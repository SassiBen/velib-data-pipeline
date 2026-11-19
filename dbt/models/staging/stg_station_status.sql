select
    station_id,
    station_code,
    bikes,
    mechanical,
    ebike,
    docks,
    is_renting,
    is_returning,
    reported_at,
    polled_at
from {{ silver('station_status') }}
where is_installed
