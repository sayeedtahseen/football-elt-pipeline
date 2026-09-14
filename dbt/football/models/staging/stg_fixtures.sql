with parsed as (
    select 
        ingested_at,
        parse_json(payload) as j
    from {{ source('raw', 'raw_fixtures') }}
)

select 
    ingested_at,
    timestamp(string(j.fixture.date)) as kickoff_utc,
    int64(j.fixture.id) as fixture_id,
    string(j.fixture.status.long) as status_long,
    string(j.fixture.status.short) as status_short,
    int64(j.fixture.venue.id) as venue_id,
    
    int64(j.teams.home.id) as home_team_id,
    string(j.teams.home.name) as home_team_name,
    int64(j.goals.home) as home_goals,
    safe.int64(j.score.halftime.home) as home_halftime_goals,
    safe.int64(j.score.fulltime.home) as home_fulltime_goals,
    safe.int64(j.score.extratime.home) as home_extratime_goals,
    safe.int64(j.score.penalty.home) as home_penalty_goals,

    int64(j.teams.away.id) as away_team_id,
    string(j.teams.away.name) as away_team_name,
    int64(j.goals.away) as away_goals,
    safe.int64(j.score.halftime.away) as away_halftime_goals,
    safe.int64(j.score.fulltime.away) as away_fulltime_goals,
    safe.int64(j.score.extratime.away) as away_extratime_goals,
    safe.int64(j.score.penalty.away) as away_penalty_goals,

    int64(j.league.id) as league_id,
    int64(j.league.season) as league_season,
    string(j.league.round) as league_round

from parsed
qualify row_number() over (
    partition by int64(j.fixture.id) 
    order by ingested_at desc
) = 1