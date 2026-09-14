select
    league_id,
    league_name,
    league_type,
    logo,
    country
from {{ ref('stg_leagues') }}