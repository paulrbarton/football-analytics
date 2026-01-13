# Football Analytics Project

A data pipeline for collecting and analyzing football data.

## Project Structure

```
football-analytics/
├── .github/workflows/   # GitHub Actions for automated scraping
├── data_collection/     # Python scripts for web scraping
├── data_transform/      # dbt models for data transformation
└── data/               # Local data storage (gitignored)
```

## Quick Start

### Option 1: GitHub Actions (Automated - Recommended)

The easiest way to scrape and upload data automatically:

1. **Set up:** Add your `MOTHERDUCK_TOKEN` to GitHub Secrets
2. **Run:** Trigger workflow manually or let it run on schedule
3. **Query:** Access data in MotherDuck

See [docs/GITHUB_ACTIONS_SETUP.md](docs/GITHUB_ACTIONS_SETUP.md) for detailed instructions.

### Option 2: Run Locally

```bash
# Setup
cd data_collection
pip install -r requirements.txt

# Configure (optional)
cp ../.env.example ../.env
# Edit .env with your MOTHERDUCK_TOKEN

# Run scraper
python scrapers/fbref_scraper.py
```

## Data Collection

Python-based web scrapers to collect football data from various sources.

### Setup
```bash
cd data_collection
pip install -r requirements.txt

# Optional: Configure data destination
cp ../.env.example ../.env
# Edit .env to set MOTHERDUCK_TOKEN and DATA_DESTINATION
```

### Usage
```bash
python scrapers/fbref_scraper.py
```

### Data Destinations

The scraper supports multiple output destinations:

- **Local files** (default): Saves to `data/raw/` as CSV/JSON/Parquet
- **MotherDuck**: Uploads to your MotherDuck cloud database
- **DuckDB**: Saves to local DuckDB file

Configure via environment variables in `.env`:
```bash
DATA_DESTINATION=motherduck  # or 'local' or 'duckdb'
MOTHERDUCK_TOKEN=your_token
DATABASE_NAME=football_analytics
SCHEMA_NAME=raw
```

See [docs/MOTHERDUCK_SETUP.md](docs/MOTHERDUCK_SETUP.md) for detailed setup instructions.

## Data Transformation

dbt models to transform raw data into analytical datasets.

### Setup
1. Copy `profiles.yml.example` to `~/.dbt/profiles.yml`
2. Configure your database connection
3. Run dbt models

### Usage
```bash
cd data_transform
dbt run
dbt test
```

## Development Status

**To Be Decided:**
- Deployment strategy for data collection scripts
- Database platform for dbt (Postgres, Snowflake, BigQuery, etc.)
- Orchestration for dbt runs (Airflow, Prefect, dbt Cloud, etc.)
