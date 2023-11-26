select 
 traffic_channel,
 priority
from {{ ref('traffic_channel_priority_seed') }}