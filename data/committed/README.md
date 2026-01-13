# Committed Data for MotherDuck Upload

This directory contains data files that are committed to the repository for automated upload to MotherDuck via GitHub Actions.

## Workflow

1. **Run scraper locally** (avoids FBRef 403 errors):
   ```bash
   cd data_collection/scrapers
   python scrape_all_teams.py
   ```

2. **Copy data here** after scraping:
   ```bash
   cp data/raw/premier_league_2025_2026.csv data/committed/
   # or
   cp data/export/premier_league_2025_2026.parquet data/committed/
   ```

3. **Commit and push**:
   ```bash
   git add data/committed/
   git commit -m "Update Premier League data"
   git push
   ```

4. **GitHub Action automatically uploads** to MotherDuck

## File Naming

Use descriptive names that will become table names in MotherDuck:
- `premier_league_2025_2026.csv` → `raw.premier_league_2025_2026`
- `nottingham_forest_2025_2026.csv` → `raw.nottingham_forest_2025_2026`

## Manual Trigger

You can also manually trigger upload via GitHub Actions UI:
- Go to Actions → "Upload Data to MotherDuck"
- Click "Run workflow"
- Optionally specify a file pattern (e.g., `nottingham_forest_*`)
