select 
    team_id,
    team_name,
    team_code,
    country,
    founded,
    venue_name,
    venue_city,
from {{ ref('stg_teams') }}