select 
    team_id,
    team_name,
    rank,
    points,
    goal_diff,
    played,
    win,
    draw,
    lose,
    goals_for,
    goals_against,
    league_id,
    season,
    group_name
from {{ ref('stg_standings') }}
order by rank asc 