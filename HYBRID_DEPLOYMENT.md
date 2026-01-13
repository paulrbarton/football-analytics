# Hybrid Deployment Strategy (Option D)

This setup allows you to **run scrapers locally** (avoiding FBRef bot detection) and **automatically upload to MotherDuck** via GitHub Actions.

## 📋 Quick Start

### 1. Run Scraper Locally

```bash
cd data_collection
./run_local_scraper.sh
```

Or manually:
```bash
cd data_collection/scrapers
python scrape_all_teams.py  # All 20 teams
# OR
python fbref_scraper.py     # Single team (edit script for team name)
```

### 2. Copy Data to Committed Folder

```bash
# From project root
cp data/raw/premier_league_2025_2026.csv data/committed/
# OR use parquet
cp data/export/premier_league_2025_2026.parquet data/committed/
```

### 3. Commit and Push

```bash
git add data/committed/
git commit -m "Update Premier League data for 2025-2026"
git push
```

### 4. Automatic Upload ✨

GitHub Actions automatically uploads to MotherDuck:
- Triggered by push to `data/committed/` folder
- Uploads all CSV/Parquet files
- Creates tables in `football_analytics.raw` schema

## 🔧 Manual Upload Trigger

Via GitHub UI:
1. Go to **Actions** → **Upload Data to MotherDuck**
2. Click **Run workflow**
3. (Optional) Specify file pattern, e.g., `nottingham_forest_*`

## 📁 File Naming Convention

File names become table names in MotherDuck:

| File | MotherDuck Table |
|------|------------------|
| `premier_league_2025_2026.csv` | `raw.premier_league_2025_2026` |
| `nottingham_forest_2025_2026.csv` | `raw.nottingham_forest_2025_2026` |
| `understat_api_premier_league_2024.csv` | `raw.understat_api_premier_league_2024` |

## 🧪 Test Upload Locally (Optional)

```bash
cd data_collection
python test_motherduck_upload.py [file_pattern]
```

**Note:** May fail due to corporate SSL interception. That's fine - GitHub Actions uses different network and will work.

## 🤖 Alternative Scrapers

### FBRef (comprehensive stats)
- Run locally (403 errors in GitHub Actions)
- 242 columns, 9 stat categories
- Uses: `scrape_all_teams.py` or `fbref_scraper.py`

### Understat (xG data)
- Can run in GitHub Actions (API-based, no bot detection)
- Already automated via `scrape-understat-api.yml`
- Uses: `understat_api_scraper.py`

## 📊 Workflow Comparison

| Aspect | FBRef (Local→Upload) | Understat (Fully Automated) |
|--------|---------------------|----------------------------|
| **Scraping** | Local only | GitHub Actions |
| **Data Richness** | 242 columns | ~12 columns (xG focus) |
| **Automation** | Semi-automated | Fully automated |
| **Upload** | GitHub Actions | GitHub Actions |
| **Reliability** | ✅ Always works | ✅ Always works |

## 🔄 Recommended Schedule

### Weekly Updates
1. **Monday morning:** Run FBRef scraper locally for weekend matches
2. **Copy & commit** data files
3. **GitHub uploads** automatically to MotherDuck
4. **(Optional)** Understat API scraper runs daily automatically

### Ad-hoc Updates
- Manual workflow trigger for immediate updates
- Test new scrapers or data sources
- One-off historical data loads

## 🚨 Troubleshooting

### Local MotherDuck connection fails
- **Expected** due to corporate SSL interception
- GitHub Actions will work (different network)
- Workaround: Test with local DuckDB instead

### GitHub Action fails to upload
- Check `MOTHERDUCK_TOKEN` secret is set
- Verify files exist in `data/committed/`
- Check workflow logs for specific errors

### Scraper gets 403 errors
- Run locally, not in GitHub Actions
- Check rate limiting delays (5-8 seconds)
- Verify headers match real browser

## 📈 Next Steps

Once data is in MotherDuck:
1. Run dbt transformations (see `data_transform/`)
2. Create analytics dashboards
3. Set up alerts for team performance
4. Export aggregated data for reporting
