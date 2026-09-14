with base as (
    select
        kickoff_utc,
        fixture_id,
        status_long,
        status_short,
        venue_id,
        home_team_id,
        home_team_name,
        home_goals,
        home_halftime_goals,
        home_fulltime_goals,
        home_extratime_goals,
        home_penalty_goals,
        away_team_id,
        away_team_name,
        away_goals,
        away_halftime_goals,
        away_fulltime_goals,
        away_extratime_goals,
        away_penalty_goals,
        league_id,
        league_season,
        league_round,
        status_short in ('FT', 'AET', 'PEN', 'AWD', 'WO') as is_finished
    from {{ ref('stg_fixtures') }}
),

final as (
    select
        *,
        case
            when not is_finished then null
            when home_goals > away_goals then 'HOME_WIN'
            when away_goals > home_goals then 'AWAY_WIN'
            else 'DRAW'
        end as result
    from base
)

select
    *,
    case
        when result = 'HOME_WIN' then 3
        when result = 'DRAW' then 1
        when result = 'AWAY_WIN' then 0
        else null
    end as home_points,
    case
        when result = 'AWAY_WIN' then 3
        when result = 'DRAW' then 1
        when result = 'HOME_WIN' then 0
        else null
    end as away_points
from final
