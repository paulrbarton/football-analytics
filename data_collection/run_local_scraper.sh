#!/bin/bash
# Quick script to run FBRef scraper locally and prepare for commit

set -e

cd "$(dirname "$0")"

echo "🏈 Football Analytics - Local Scraper Runner"
echo "=========================================="
echo ""

# Check if virtual environment is activated
if [ -z "$VIRTUAL_ENV" ]; then
    echo "Activating virtual environment..."
    source ../.venv/bin/activate
fi

# Navigate to scrapers directory
cd scrapers

# Scrape options
echo "What would you like to scrape?"
echo "1) Single team"
echo "2) All Premier League teams"
read -p "Choice (1 or 2): " choice

if [ "$choice" = "1" ]; then
    read -p "Team name (e.g., Nottingham Forest): " team_name
    read -p "Season (default: 2025-2026): " season
    season=${season:-2025-2026}
    
    echo ""
    echo "📥 Scraping $team_name for season $season..."
    python fbref_scraper.py
    
elif [ "$choice" = "2" ]; then
    read -p "Season (default: 2025-2026): " season
    season=${season:-2025-2026}
    
    echo ""
    echo "📥 Scraping all Premier League teams for season $season..."
    python scrape_all_teams.py
    
else
    echo "Invalid choice"
    exit 1
fi

echo ""
echo "✓ Scraping complete!"
echo ""
echo "📦 Next steps:"
echo "1. Review data in data/raw/ or data/export/"
echo "2. Copy to data/committed/ for GitHub upload:"
echo "   cp ../data/raw/premier_league_*.csv ../data/committed/"
echo "3. Commit and push:"
echo "   git add data/committed/"
echo "   git commit -m 'Update football data'"
echo "   git push"
