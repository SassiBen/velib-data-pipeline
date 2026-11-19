-- Une station ne peut pas annoncer nettement plus de vélos que de bornes.
-- Une petite marge est tolérée : certaines stations acceptent des vélos en surnombre.
select n.station_id, n.bikes, n.capacity
from {{ ref('mart_network_now') }} n
where n.capacity > 0 and n.bikes > n.capacity + 10
