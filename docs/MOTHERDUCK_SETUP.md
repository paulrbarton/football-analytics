# MotherDuck Upload Guide

## Setup

1. **Get your MotherDuck token:**
   - Visit https://app.motherduck.com/
   - Sign in or create an account
   - Go to Settings → Access Tokens
   - Copy your token

2. **Configure environment:**
   ```bash
   cp .env.example .env
   ```
   
   Edit `.env` and set:
   ```bash
   MOTHERDUCK_TOKEN=your_actual_token_here
   DATA_DESTINATION=motherduck
   DATABASE_NAME=football_analytics
   SCHEMA_NAME=raw
   ```

## Usage

### Option 1: Using environment variables (recommended)

Set in `.env` file:
```bash
DATA_DESTINATION=motherduck  # or 'local' or 'duckdb'
```

Then run the scraper normally:
```bash
cd data_collection/scrapers
python fbref_scraper.py
```

### Option 2: Programmatic control

```python
from scrapers.fbref_scraper import FBRefScraper

scraper = FBRefScraper()
df = scraper.scrape_team_season('e4a775cb', '2025-2026', 'Nottingham-Forest')

# Save to local file
scraper.save_data(df, 'nottingham_forest', destination='local', format='csv')

# Save to MotherDuck
scraper.save_data(
    df, 
    'nottingham_forest_matches',
    destination='motherduck',
    database='football_analytics',
    schema='raw',
    if_exists='replace'  # or 'append' or 'fail'
)

# Save to local DuckDB file
scraper.save_data(
    df,
    'nottingham_forest_matches', 
    destination='duckdb',
    database='football_analytics',
    schema='raw'
)
```

## Destinations

- **`local`**: Save as CSV/JSON/Parquet to `data/raw/` directory
- **`motherduck`**: Upload to MotherDuck cloud database
- **`duckdb`**: Save to local DuckDB file in `data/` directory

## Query your data in MotherDuck

After uploading, query in MotherDuck:

```sql
-- View your data
SELECT * FROM raw.nottingham_forest_matches LIMIT 10;

-- Count matches
SELECT COUNT(*) FROM raw.nottingham_forest_matches;

-- Top scorers
SELECT 
    team,
    COUNT(*) as matches,
    SUM(GF) as goals_scored
FROM raw.premier_league_matches
GROUP BY team
ORDER BY goals_scored DESC;
```
