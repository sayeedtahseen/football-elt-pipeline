with parsed as (
    select 
        ingested_at,
        parse_json(payload) as j,
    from {{ source('raw', 'raw_teams') }}
)

select 
    int64(j.team.id)      as team_id,
    string(j.team.name)   as team_name,
    string(j.team.code)   as team_code,
    string(j.team.country) as country,
    int64(j.team.founded) as founded,
    string(j.venue.name)  as venue_name,
    string(j.venue.city)  as venue_city,
    ingested_at
from parsed
qualify row_number() over (partition by int64(j.team.id) order by ingested_at desc) = 1