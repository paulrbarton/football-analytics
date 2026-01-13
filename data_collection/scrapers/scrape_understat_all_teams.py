"""
Script to scrape all Premier League teams from Understat.
Used by GitHub Actions workflow.
"""

import os
import sys
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.understat_scraper import UnderstatScraper

load_dotenv()


def main():
    """Scrape all Premier League teams from Understat and upload to configured destination."""
    
    # Configuration
    DESTINATION = os.getenv('DATA_DESTINATION', 'local')
    DATABASE = os.getenv('DATABASE_NAME', 'football_analytics')
    SCHEMA = os.getenv('SCHEMA_NAME', 'raw')
    SEASON = os.getenv('SEASON', '2024')  # 2024 = 2024-25 season
    RATE_LIMIT = float(os.getenv('SCRAPER_RATE_LIMIT', '3.0'))
    
    print(f"{'='*60}")
    print(f"Scraping all Premier League teams from Understat - {SEASON}")
    print(f"Destination: {DESTINATION}")
    print(f"Rate limit: {RATE_LIMIT}s between requests")
    print(f"{'='*60}\n")
    
    scraper = UnderstatScraper(rate_limit=RATE_LIMIT)
    
    # Scrape all teams
    all_df = scraper.scrape_premier_league_season(SEASON)
    
    if not all_df.empty:
        print(f"\n{'='*60}")
        print(f"Total scraped: {len(all_df)} matches from {all_df['team'].nunique()} teams")
        print(f"{'='*60}\n")
        
        # Save locally (always as backup)
        scraper.save_data(
            all_df,
            f'understat_premier_league_{SEASON}_all_teams',
            format='csv',
            destination='local'
        )
        
        # Upload to configured destination if not local
        if DESTINATION != 'local':
            print(f"\nUploading to {DESTINATION}...")
            try:
                scraper.save_data(
                    all_df,
                    'understat_premier_league_matches',
                    destination=DESTINATION,
                    database=DATABASE,
                    schema=SCHEMA,
                    if_exists='replace'
                )
                print("✓ Upload successful!")
            except Exception as e:
                print(f"✗ Upload failed: {str(e)}")
                print("Data saved locally only")
                sys.exit(1)
        
        print("\n✅ Scraping completed successfully!")
    else:
        print("\n❌ No data scraped")
        sys.exit(1)


if __name__ == "__main__":
    main()
