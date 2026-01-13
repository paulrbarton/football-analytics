# Handling 403 Errors from FBRef

## What Changed

Updated the scraper to handle FBRef blocking automated requests:

### 1. **Better Browser Headers**
- More realistic User-Agent (Windows/Chrome)
- Full browser header set (Accept, Accept-Language, etc.)
- Connection management headers

### 2. **Retry Logic**
- Automatic retry on 403 Forbidden (up to 3 attempts)
- Exponential backoff (5s, 10s, 15s)
- Rate limiting retry for 429 errors

### 3. **Slower Rate Limiting**
- Default: 5 seconds between requests (was 3s)
- GitHub Actions: 8 seconds (configurable via `SCRAPER_RATE_LIMIT`)

## If You Still Get 403 Errors

### Option 1: Increase Rate Limit (Recommended)

In GitHub Actions workflow inputs or environment:
```yaml
env:
  SCRAPER_RATE_LIMIT: '10'  # 10 seconds between requests
```

Or locally:
```bash
export SCRAPER_RATE_LIMIT=10
python scrapers/fbref_scraper.py
```

### Option 2: Use Rotating Proxy

If FBRef is blocking GitHub Actions IPs completely, you might need a proxy service:

1. **Free option:** Run locally on your home network
2. **Paid option:** Use proxy service like ScraperAPI, Bright Data, etc.

Add to scraper:
```python
# In base_scraper.py __init__
proxies = {
    'http': 'http://your-proxy:port',
    'https': 'http://your-proxy:port'
}
self.session.proxies.update(proxies)
```

### Option 3: Schedule at Off-Peak Times

FBRef may be less strict at certain times:
```yaml
schedule:
  - cron: '0 3 * * *'  # 3 AM UTC (late night EU time)
```

### Option 4: Manual Scraping + Upload

If automation keeps failing:
1. Run scraper locally (where it works)
2. Commit data files to repo
3. Or upload Parquet to MotherDuck manually

## Testing Changes

Test locally first:
```bash
# Set high rate limit for testing
export SCRAPER_RATE_LIMIT=10
cd data_collection/scrapers
python fbref_scraper.py
```

## Current Configuration

- **Local:** 5 seconds between requests
- **GitHub Actions:** 8 seconds between requests
- **Retries:** 3 attempts with backoff on 403 errors
- **Headers:** Full browser-like headers

## Best Practices

✅ **DO:**
- Use rate limits of 5+ seconds
- Run during off-peak hours
- Monitor for 403 errors and adjust
- Keep local backups of data

❌ **DON'T:**
- Scrape too frequently (max once per day)
- Use default Python User-Agent
- Ignore 403 errors without backoff
- Scrape all 20 teams without delays

## Alternative Data Sources

If FBRef continues blocking:
- **Understat** (xG data)
- **Football-Data.co.uk** (CSV downloads)
- **Official Premier League API** (if available)
- **Kaggle datasets** (historical data)
