-- Staging model for raw match data
-- Clean and standardize match information

with source as (
    
    select * from {{ source('raw', 'matches') }}

),

cleaned as (

    select
        -- IDs
        match_id,
        
        -- Match details
        match_date,
        competition,
        season,
        
        -- Teams
        trim(home_team) as home_team,
        trim(away_team) as away_team,
        
        -- Scores
        home_score,
        away_score,
        
        -- Metadata
        scraped_at,
        current_timestamp as dbt_updated_at

    from source
    
    where match_date is not null
      and home_team is not null
      and away_team is not null

)

select * from cleaned
