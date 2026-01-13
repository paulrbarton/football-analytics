"""FBRef.com scraper for Premier League match data."""

import pandas as pd
import time
from typing import Dict, List, Optional
from base_scraper import BaseScraper


class FBRefScraper(BaseScraper):
    """Scraper for football statistics from FBRef.com."""

    # Mapping of category names to URL fragments
    STAT_CATEGORIES = {
        'scores_fixtures': 'schedule',
        'shooting': 'shooting',
        'goalkeeping': 'keeper',
        'passing': 'passing',
        'pass_types': 'passing_types',
        'goal_shot_creation': 'gca',
        'defensive_actions': 'defense',
        'possession': 'possession',
        'miscellaneous': 'misc'
    }

    def __init__(self, rate_limit: float = 5.0, verify_ssl: bool = False):
        """
        Initialize FBRef scraper.
        
        Args:
            rate_limit: Seconds between requests (default 5.0 for politeness, higher in CI)
            verify_ssl: Whether to verify SSL certificates (default False for compatibility)
        """
        super().__init__(base_url="https://fbref.com", rate_limit=rate_limit, verify_ssl=verify_ssl)

    def get_match_logs_url(self, team_id: str, season: str, category: str, team_name: str) -> str:
        """
        Construct URL for a specific stat category.
        
        Args:
            team_id: FBRef team ID (e.g., 'e4a775cb')
            season: Season string (e.g., '2025-2026')
            category: Stat category key from STAT_CATEGORIES
            team_name: URL-formatted team name (e.g., 'Nottingham-Forest')
            
        Returns:
            Full URL for the stat category
        """
        url_fragment = self.STAT_CATEGORIES[category]
        
        if category == 'scores_fixtures':
            return (f"{self.base_url}/en/squads/{team_id}/{season}/matchlogs/all_comps/"
                   f"{url_fragment}/{team_name}-Scores-and-Fixtures-All-Competitions")
        else:
            return (f"{self.base_url}/en/squads/{team_id}/{season}/matchlogs/all_comps/"
                   f"{url_fragment}/{team_name}-Match-Logs-All-Competitions")

    def scrape_category(self, team_id: str, season: str, category: str, 
                       team_name: str) -> Optional[pd.DataFrame]:
        """
        Scrape data for a single stat category.
        
        Args:
            team_id: FBRef team ID
            season: Season string
            category: Stat category key
            team_name: URL-formatted team name
            
        Returns:
            DataFrame with stats or None if error
        """
        url = self.get_match_logs_url(team_id, season, category, team_name)
        print(f"Scraping {category}...")
        
        try:
            # Fetch the page
            response = self.session.get(url, verify=self.verify_ssl, timeout=30)
            response.raise_for_status()
            
            # Parse tables using pandas - FBRef has tables with specific IDs
            tables = pd.read_html(response.text)
            
            if not tables:
                print(f"  ✗ No table found for {category}")
                return None
            
            # Usually the first table is what we want
            df = tables[0]
            
            # Clean up multi-level column headers
            if isinstance(df.columns, pd.MultiIndex):
                # Flatten multi-level columns
                df.columns = ['_'.join(str(col).strip() for col in cols if col != '').strip('_') 
                             for cols in df.columns.values]
            
            # Convert all column names to strings and clean them
            df.columns = [str(col).strip() for col in df.columns]
            
            # Remove team name prefixes from multi-level headers 
            # (e.g., "For Nottingham Forest_Date" -> "Date")
            import re
            df.columns = [re.sub(r'^(For|Against) .+?_', '', col) for col in df.columns]
            
            # Add prefix to column names to avoid conflicts when merging
            # Keep common keys unprefixed for merging
            common_keys = ['Date', 'Comp', 'Round', 'Venue', 'Result', 'Opponent', 'Day', 'Time', 'GF', 'GA']
            prefix = category
            df.columns = [f"{prefix}_{col}" if col not in common_keys else col 
                         for col in df.columns]
            
            # Find the Date column (might have prefix)
            date_col = next((col for col in df.columns if 'Date' in col), None)
            
            if date_col:
                # Remove rows that are actually headers (FBRef repeats headers in tables)
                df = df[df[date_col] != 'Date']
                df = df[df[date_col].notna()]
            
            print(f"  ✓ Scraped {len(df)} rows, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            print(f"  ✗ Error scraping {category}: {str(e)}")
            return None

    def scrape_team_season(self, team_id: str, season: str, team_name: str,
                          categories: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Scrape all stat categories for a team's season.
        
        Args:
            team_id: FBRef team ID
            season: Season string (e.g., '2025-2026')
            team_name: URL-formatted team name (e.g., 'Nottingham-Forest')
            categories: List of categories to scrape (default: all)
            
        Returns:
            Wide DataFrame with all statistics combined
        """
        if categories is None:
            categories = list(self.STAT_CATEGORIES.keys())
        
        dfs = {}
        
        # Scrape each category
        for category in categories:
            df = self.scrape_category(team_id, season, category, team_name)
            if df is not None:
                dfs[category] = df
        
        if not dfs:
            print("No data scraped for any category")
            return pd.DataFrame()
        
        print(f"\n{'='*60}")
        print("Merging data from all categories...")
        print(f"{'='*60}")
        
        # Start with scores_fixtures as the base
        if 'scores_fixtures' in dfs:
            combined_df = dfs['scores_fixtures'].copy()
            merge_keys = ['Date', 'Opponent']  # Simplified merge keys
            print(f"Base: scores_fixtures with {len(combined_df)} rows, {len(combined_df.columns)} columns")
        else:
            # If no scores_fixtures, start with first available
            first_cat = list(dfs.keys())[0]
            combined_df = dfs[first_cat].copy()
            merge_keys = ['Date', 'Opponent']
        
        # Merge other categories
        for category, df in dfs.items():
            if category == 'scores_fixtures':
                continue
            
            # Identify merge keys that exist in this dataframe
            available_keys = [k for k in merge_keys if k in df.columns and k in combined_df.columns]
            
            if available_keys:
                before_cols = len(combined_df.columns)
                combined_df = combined_df.merge(
                    df, 
                    on=available_keys, 
                    how='left',  # Changed from outer to left to keep only matches in base
                    suffixes=('', f'_{category}_dup')
                )
                after_cols = len(combined_df.columns)
                print(f"Merged {category}: added {after_cols - before_cols} columns (total: {after_cols})")
            else:
                print(f"Skipped {category}: no common merge keys")
        
        # Add team information
        combined_df['team'] = team_name.replace('-', ' ')
        combined_df['team_id'] = team_id
        combined_df['season'] = season
        
        return combined_df

    def scrape_premier_league_season(self, season: str, 
                                     teams: Optional[Dict[str, str]] = None) -> pd.DataFrame:
        """
        Scrape all Premier League teams for a season.
        
        Args:
            season: Season string (e.g., '2025-2026')
            teams: Dict of {team_name: team_id}. If None, uses default PL teams
            
        Returns:
            Combined DataFrame with all teams' data
        """
        if teams is None:
            teams = self.get_premier_league_teams()
        
        all_data = []
        
        for team_name, team_id in teams.items():
            print(f"\n{'='*60}")
            print(f"Scraping {team_name} ({season})")
            print(f"{'='*60}")
            
            team_df = self.scrape_team_season(team_id, season, team_name)
            
            if not team_df.empty:
                all_data.append(team_df)
                print(f"✓ Scraped {len(team_df)} matches for {team_name}")
            else:
                print(f"✗ No data for {team_name}")
            
            # Be polite with requests
            time.sleep(self.rate_limit)
        
        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            print(f"\n{'='*60}")
            print(f"Total matches scraped: {len(combined)}")
            print(f"{'='*60}")
            return combined
        else:
            return pd.DataFrame()

    def get_premier_league_teams(self) -> Dict[str, str]:
        """
        Get Premier League team names and IDs for 2025-26 season.
        
        Returns:
            Dict of {team_name: team_id}
        """
        # These would need to be collected or provided
        # This is a starter set - you'll need to add/update team IDs
        return {
            'Nottingham-Forest': 'e4a775cb',
            'Arsenal': '18bb7c10',
            'Liverpool': '822bd0ba',
            'Manchester-City': 'b8fd03ef',
            'Chelsea': 'cff3d9bb',
            'Manchester-United': '19538871',
            'Tottenham-Hotspur': '361ca564',
            'Newcastle-United': 'b2b47a98',
            'Aston-Villa': '8602292d',
            'Brighton-and-Hove-Albion': 'd07537b9',
            'Bournemouth': '4ba7cbea',
            'Fulham': 'fd962109',
            'West-Ham-United': '7c21e445',
            'Brentford': 'cd051869',
            'Crystal-Palace': '47c64c55',
            'Everton': 'd3fd31cc',
            'Leicester-City': 'a2d435b3',
            'Ipswich-Town': 'b74092de',
            'Southampton': '33c895d4',
            'Wolverhampton-Wanderers': '8cec06e1',
        }

    def scrape(self) -> pd.DataFrame:
        """
        Main scraping method.
        
        Returns:
            DataFrame with all scraped data
        """
        # Default: scrape current season (2025-2026)
        return self.scrape_premier_league_season('2025-2026')


def main():
    """Run the scraper."""
    import os
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    # Get rate limit from environment (higher in CI to avoid blocking)
    rate_limit = float(os.getenv('SCRAPER_RATE_LIMIT', '5.0'))
    
    scraper = FBRefScraper(rate_limit=rate_limit)
    
    # Configure destination: 'local', 'motherduck', or 'duckdb'
    DESTINATION = os.getenv('DATA_DESTINATION', 'local')  # default to local
    DATABASE = os.getenv('DATABASE_NAME', 'football_analytics')
    SCHEMA = os.getenv('SCHEMA_NAME', 'raw')
    
    # Example 1: Scrape single team
    print("Scraping Nottingham Forest 2025-2026 season...")
    df = scraper.scrape_team_season(
        team_id='e4a775cb',
        season='2025-2026',
        team_name='Nottingham-Forest'
    )
    
    if not df.empty:
        print(f"\nScraped {len(df)} matches with {len(df.columns)} columns")
        
        # Save locally (always)
        scraper.save_data(
            df, 
            'nottingham_forest_2025_2026', 
            format='csv',
            destination='local'
        )
        
        # Also save to configured destination if not local
        if DESTINATION != 'local':
            print(f"\nUploading to {DESTINATION}...")
            scraper.save_data(
                df,
                'nottingham_forest_2025_2026',
                destination=DESTINATION,
                database=DATABASE,
                schema=SCHEMA,
                if_exists='replace'
            )
    
    # Example 2: Scrape all Premier League teams (uncomment to use)
    # print("\n\nScraping all Premier League teams...")
    # all_df = scraper.scrape_premier_league_season('2025-2026')
    # 
    # if not all_df.empty:
    #     scraper.save_data(
    #         all_df, 
    #         'premier_league_2025_2026_all_teams', 
    #         format='csv',
    #         destination='local'
    #     )
    #     
    #     # Upload to MotherDuck/DuckDB if configured
    #     if DESTINATION != 'local':
    #         scraper.save_data(
    #             all_df,
    #             'premier_league_matches',
    #             destination=DESTINATION,
    #             database=DATABASE,
    #             schema=SCHEMA,
    #             if_exists='replace'
    #         )


if __name__ == "__main__":
    main()
