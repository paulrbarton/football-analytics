-- Intermediate model to calculate match results and points
-- Adds derived fields like winner, points, goal difference

with matches as (
    
    select * from {{ ref('stg_matches') }}

),

enriched as (

    select
        *,
        
        -- Match result
        case
            when home_score > away_score then 'home_win'
            when away_score > home_score then 'away_win'
            else 'draw'
        end as result,
        
        -- Points (3-1-0 system)
        case
            when home_score > away_score then 3
            when home_score = away_score then 1
            else 0
        end as home_points,
        
        case
            when away_score > home_score then 3
            when away_score = home_score then 1
            else 0
        end as away_points,
        
        -- Goal difference
        home_score - away_score as home_goal_difference,
        away_score - home_score as away_goal_difference,
        
        -- Total goals
        home_score + away_score as total_goals

    from matches

)

select * from enriched
