# GitHub Actions Setup

## Overview

This repository includes a GitHub Action that automatically scrapes football data from FBRef and uploads it to MotherDuck.

## Setup Instructions

### 1. Add MotherDuck Token to GitHub Secrets

1. Go to your repository on GitHub
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Add the following secret:
   - **Name:** `MOTHERDUCK_TOKEN`
   - **Value:** Your MotherDuck token from https://app.motherduck.com/

### 2. Workflow Triggers

The workflow runs:

#### Automatically (Scheduled)
- Daily at 6:00 AM UTC
- Scrapes Nottingham Forest by default

#### Manually (On-Demand)
1. Go to **Actions** tab in your repository
2. Select "Scrape Football Data and Upload to MotherDuck"
3. Click **Run workflow**
4. Options:
   - **teams:** Team names (comma-separated) or "all" for all Premier League teams
   - **season:** e.g., "2025-2026"

### 3. Configure the Workflow

Edit [.github/workflows/scrape-football-data.yml](.github/workflows/scrape-football-data.yml):

#### Change Schedule
```yaml
schedule:
  - cron: '0 6 * * *'  # Daily at 6 AM UTC
  # - cron: '0 */6 * * *'  # Every 6 hours
  # - cron: '0 18 * * 0'  # Sundays at 6 PM UTC
```

#### Change Default Team
```yaml
inputs:
  teams:
    default: 'Nottingham-Forest'  # Change to your team
```

#### Change Destination
The workflow uses these environment variables:
- `DATA_DESTINATION=motherduck` (can be 'local', 'motherduck', or 'duckdb')
- `DATABASE_NAME=football_analytics`
- `SCHEMA_NAME=raw`

### 4. View Results

#### Check Workflow Runs
- Go to **Actions** tab
- View logs and status of each run

#### Download Backup Data
Each run creates artifacts (available for 7 days):
1. Go to completed workflow run
2. Scroll to **Artifacts** section
3. Download `scraped-data-{run-number}.zip`

#### Query in MotherDuck
After successful upload:
```sql
-- View all matches
SELECT * FROM raw.premier_league_matches LIMIT 10;

-- Latest scraping run
SELECT MAX(Date) as latest_match FROM raw.premier_league_matches;

-- Team statistics
SELECT 
    team,
    COUNT(*) as matches,
    AVG(GF) as avg_goals_for
FROM raw.premier_league_matches
GROUP BY team
ORDER BY avg_goals_for DESC;
```

## Workflow Behavior

1. **Checkout code** from repository
2. **Install Python** and dependencies
3. **Run scraper** (respects rate limits)
4. **Upload to MotherDuck** (cloud storage)
5. **Save artifacts** (CSV/Parquet backups)
6. **Report status** (success/failure)

## Advantages of GitHub Actions

✅ **No corporate SSL issues** - Runs in GitHub's cloud
✅ **Automatic scheduling** - Set and forget
✅ **Free for public repos** - 2,000 minutes/month for private
✅ **Artifact storage** - Automatic backups
✅ **Version controlled** - Workflow is in git

## Troubleshooting

### Workflow fails with "MOTHERDUCK_TOKEN not set"
- Make sure you added the secret in repository settings
- Secret name must be exactly `MOTHERDUCK_TOKEN`

### Rate limiting from FBRef
- Adjust `rate_limit` in scraper (default: 3 seconds)
- Don't run workflow too frequently

### MotherDuck connection fails
- Verify token is valid at https://app.motherduck.com/
- Check workflow logs for specific error

### Need to scrape different teams
- Run workflow manually with custom inputs
- Or modify default teams in workflow file

## Example Manual Runs

**Single team:**
- teams: `Arsenal`
- season: `2025-2026`

**Multiple teams:**
- teams: `Arsenal,Liverpool,Manchester-City`
- season: `2025-2026`

**All Premier League teams:**
- teams: `all`
- season: `2025-2026`

## Cost Considerations

- GitHub Actions: Free for public repos (2,000 min/month private)
- MotherDuck: Check your plan's storage limits
- FBRef: Be respectful with rate limiting (3+ seconds between requests)
