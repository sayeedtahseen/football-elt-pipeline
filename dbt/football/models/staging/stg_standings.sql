with parsed as (
    select 
        ingested_at,
        parse_json(payload) as j
    from {{ source('raw', 'raw_standings') }}
),

stage_groups as (
    select 
        ingested_at, j, grp 
    from parsed, unnest(json_query_array(j, '$.league.standings')) as grp 
),

entries as (
    select 
        ingested_at,
        j,
        entry
    from stage_groups, unnest(json_query_array(grp)) as entry 
)

select
  int64(j.league.id)             as league_id,
  int64(j.league.season)         as season,
  int64(entry.team.id)           as team_id,
  string(entry.team.name)        as team_name,
  int64(entry.rank)              as rank,
  int64(entry.points)            as points,
  int64(entry.goalsDiff)         as goal_diff,
  int64(entry.all.played)        as played,
  int64(entry.all.win)           as win,
  int64(entry.all.draw)          as draw,
  int64(entry.all.lose)          as lose,
  int64(entry.all.goals.for)     as goals_for,
  int64(entry.all.goals.against) as goals_against,
  string(entry.group)            as group_name,
  ingested_at
from entries
qualify row_number() over (
  partition by int64(j.league.id), int64(j.league.season), int64(entry.team.id), string(entry.group)
  order by ingested_at desc 
) = 1