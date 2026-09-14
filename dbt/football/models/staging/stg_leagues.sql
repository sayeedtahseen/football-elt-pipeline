with parsed as (
    select
        ingested_at,
        parse_json(payload) as j
    from {{ source('raw', 'raw_leagues') }}
)

select 
  int64(j.league.id)     as league_id,
  string(j.league.name)  as league_name,
  string(j.league.type)  as league_type,
  string(j.league.logo)  as logo,
  string(j.country.name) as country,
  ingested_at
from parsed
qualify row_number() over (partition by int64(j.league.id) order by ingested_at desc) = 1 