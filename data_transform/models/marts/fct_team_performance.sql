-- Fact table: Team performance metrics by season and competition
-- Aggregates match results to team-level statistics

with match_results as (
    
    select * from {{ ref('int_match_results') }}

),

home_games as (
    
    select
        season,
        competition,
        home_team as team,
        count(*) as matches_played,
        sum(case when result = 'home_win' then 1 else 0 end) as wins,
        sum(case when result = 'draw' then 1 else 0 end) as draws,
        sum(case when result = 'away_win' then 1 else 0 end) as losses,
        sum(home_score) as goals_scored,
        sum(away_score) as goals_conceded,
        sum(home_points) as points
    from match_results
    group by season, competition, home_team

),

away_games as (
    
    select
        season,
        competition,
        away_team as team,
        count(*) as matches_played,
        sum(case when result = 'away_win' then 1 else 0 end) as wins,
        sum(case when result = 'draw' then 1 else 0 end) as draws,
        sum(case when result = 'home_win' then 1 else 0 end) as losses,
        sum(away_score) as goals_scored,
        sum(home_score) as goals_conceded,
        sum(away_points) as points
    from match_results
    group by season, competition, away_team

),

combined as (

    select
        season,
        competition,
        team,
        sum(matches_played) as matches_played,
        sum(wins) as wins,
        sum(draws) as draws,
        sum(losses) as losses,
        sum(goals_scored) as goals_scored,
        sum(goals_conceded) as goals_conceded,
        sum(points) as points
    from (
        select * from home_games
        union all
        select * from away_games
    )
    group by season, competition, team

),

final as (

    select
        *,
        goals_scored - goals_conceded as goal_difference,
        round(cast(points as numeric) / nullif(matches_played, 0), 2) as points_per_match,
        round(cast(goals_scored as numeric) / nullif(matches_played, 0), 2) as goals_per_match,
        round(cast(goals_conceded as numeric) / nullif(matches_played, 0), 2) as goals_conceded_per_match
    from combined

)

select * from final
order by season desc, competition, points desc, goal_difference desc
