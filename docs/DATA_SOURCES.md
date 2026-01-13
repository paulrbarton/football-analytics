# Data Sources

## Overview

This project supports scraping football data from multiple sources:

1. **FBRef.com** - Comprehensive match statistics
2. **Understat.com** - xG (Expected Goals) data and advanced metrics

## FBRef Scraper

### Data Collected
- Scores & Fixtures (48 matches for Nottingham Forest)
- Shooting statistics (goals, shots, shots on target)
- Goalkeeping (saves, clean sheets)
- Passing (completions, key passes, progressive passes)
- Pass Types (crosses, corners, throw-ins)
- Goal & Shot Creation
- Defensive Actions (tackles, blocks, interceptions)
- Possession (touches, dribbles, carries)
- Miscellaneous (cards, fouls, offsides)

**Total:** ~242 columns per match

### Status
✅ **Works locally** with corporate network
⚠️ **Limited in GitHub Actions** - May get 403 errors due to IP blocking

### Usage
```bash
# Local
cd data_collection/scrapers
python fbref_scraper.py

# GitHub Actions
# Go to Actions → "Scrape Football Data and Upload to MotherDuck"
```

## Understat Scraper

### Data Collected
- Match-level xG (Expected Goals)
- Match-level xGA (Expected Goals Against)
- Deep progressions
- PPDA (Passes Per Defensive Action)
- Match results and points
- Player-level xG contributions

### Status
✅ **More reliable in CI environments** - Less aggressive bot detection
✅ **Focus on advanced metrics** - xG, xGA, npxG, npxGA

### Usage
```bash
# Local
cd data_collection/scrapers  
python understat_scraper.py

# GitHub Actions
# Go to Actions → "Scrape Understat Data and Upload to MotherDuck"
```

### Note
The Understat scraper extracts data from embedded JavaScript variables on the page. 
Season format: Use year only (e.g., '2024' for 2024-25 season)

## Comparison

| Feature | FBRef | Understat |
|---------|-------|-----------|
| **Data Breadth** | Very comprehensive (242 cols) | Focused on xG/advanced metrics |
| **CI Reliability** | ⚠️ May be blocked | ✅ More reliable |
| **Rate Limiting** | 8s recommended | 3s sufficient |
| **Season Format** | `2025-2026` | `2024` (year only) |
| **Best For** | Detailed match analysis | Expected goals analysis |
| **Free** | Yes (be respectful) | Yes (be respectful) |

## Recommended Workflow

**For local development:**
- Use FBRef for comprehensive data (works reliably)

**For GitHub Actions:**
- Use Understat (more reliable in CI)
- Or scrape FBRef locally and commit files

**For production:**
- Run both scrapers to get complementary data
- FBRef for traditional stats
- Understat for xG insights

## Alternative Sources

If both face issues:

- **Football-Data.co.uk** - CSV downloads (historical data)
- **StatsBomb** - Open data (requires account)
- **API Football** - Paid API ($0-$10/month)
- **Official Premier League** - Official stats (if API available)

## Rate Limiting Guidelines

**FBRef:**
- Local: 5+ seconds
- GitHub Actions: 8+ seconds
- Maximum: Once per day

**Understat:**
- Local: 3+ seconds
- GitHub Actions: 3+ seconds  
- Maximum: Once per day

**Be respectful:** These sites provide free data. Don't abuse it.
