"""
Alternative Understat scraper using the understat Python package.

This uses the official understat async library which is more reliable
than scraping the website directly.

Installation:
    pip install understat aiohttp

Usage:
    python understat_api_scraper.py
"""

import asyncio
import pandas as pd
import os
from dotenv import load_dotenv

try:
    from understat import Understat
except ImportError:
    print("Error: understat package not installed")
    print("Install with: pip install understat aiohttp")
    exit(1)


class UnderstatAPIScraper:
    """Scraper using Understat's Python API wrapper."""
    
    PREMIER_LEAGUE_TEAMS = [
        'Arsenal', 'Aston Villa', 'Bournemouth', 'Brentford', 'Brighton',
        'Chelsea', 'Crystal Palace', 'Everton', 'Fulham', 'Ipswich',
        'Leicester', 'Liverpool', 'Manchester City', 'Manchester United',
        'Newcastle United', 'Nottingham Forest', 'Southampton', 'Tottenham',
        'West Ham', 'Wolves'
    ]
    
    def __init__(self):
        """Initialize the API scraper."""
        import aiohttp
        self.session = aiohttp.ClientSession()
        self.understat = Understat(self.session)
    
    async def get_team_matches(self, team_name: str, season: str) -> pd.DataFrame:
        """
        Get match data for a specific team and season.
        
        Args:
            team_name: Team name (e.g., 'Arsenal')
            season: Season year (e.g., '2024' for 2024-25)
            
        Returns:
            DataFrame with match data
        """
        try:
            # Get team results
            results = await self.understat.get_team_results(
                team_name,
                int(season)
            )
            
            df = pd.DataFrame(results)
            df['team'] = team_name
            df['season'] = season
            
            return df
        except Exception as e:
            print(f"Error fetching {team_name}: {e}")
            return pd.DataFrame()
    
    async def get_league_matches(self, season: str) -> pd.DataFrame:
        """
        Get all Premier League matches for a season.
        
        Args:
            season: Season year
            
        Returns:
            DataFrame with all matches
        """
        try:
            matches = await self.understat.get_league_results(
                'EPL',
                int(season)
            )
            
            df = pd.DataFrame(matches)
            df['season'] = season
            df['league'] = 'Premier League'
            
            return df
        except Exception as e:
            print(f"Error fetching league data: {e}")
            return pd.DataFrame()
    
    async def get_all_teams_data(self, season: str) -> pd.DataFrame:
        """
        Get match data for all Premier League teams.
        
        Args:
            season: Season year
            
        Returns:
            Combined DataFrame
        """
        print(f"Fetching data for all teams (season {season})...")
        
        tasks = [
            self.get_team_matches(team, season)
            for team in self.PREMIER_LEAGUE_TEAMS
        ]
        
        results = await asyncio.gather(*tasks)
        
        # Combine all dataframes
        valid_dfs = [df for df in results if not df.empty]
        
        if valid_dfs:
            combined = pd.concat(valid_dfs, ignore_index=True)
            print(f"✓ Fetched {len(combined)} matches from {len(valid_dfs)} teams")
            return combined
        else:
            return pd.DataFrame()
    
    async def close(self):
        """Close the session."""
        await self.session.close()


def save_data(df: pd.DataFrame, filename: str, destination: str = 'local', **kwargs):
    """Save data to specified destination."""
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from fbref_scraper import FBRefScraper
    
    # Use FBRef scraper's save method
    scraper = FBRefScraper()
    scraper.save_data(df, filename, destination=destination, **kwargs)


async def main():
    """Main execution function."""
    load_dotenv()
    
    # Configuration
    DESTINATION = os.getenv('DATA_DESTINATION', 'local')
    DATABASE = os.getenv('DATABASE_NAME', 'football_analytics')
    SCHEMA = os.getenv('SCHEMA_NAME', 'raw')
    SEASON = os.getenv('SEASON', '2024')
    
    scraper = UnderstatAPIScraper()
    
    try:
        # Example 1: Get single team
        print(f"Fetching Nottingham Forest data for {SEASON}...")
        df = await scraper.get_team_matches('Nottingham Forest', SEASON)
        
        if not df.empty:
            print(f"✓ Got {len(df)} matches")
            print(f"Columns: {list(df.columns)}")
            
            save_data(
                df,
                f'understat_api_nottingham_forest_{SEASON}',
                destination='local',
                format='csv'
            )
            
            if DESTINATION != 'local':
                save_data(
                    df,
                    f'understat_api_nottingham_forest_{SEASON}',
                    destination=DESTINATION,
                    database=DATABASE,
                    schema=SCHEMA,
                    if_exists='replace'
                )
        
        # Example 2: Get all teams (uncomment to use)
        # print(f"\nFetching all Premier League teams...")
        # all_df = await scraper.get_all_teams_data(SEASON)
        # 
        # if not all_df.empty:
        #     save_data(
        #         all_df,
        #         f'understat_api_premier_league_{SEASON}',
        #         destination='local',
        #         format='csv'
        #     )
        #     
        #     if DESTINATION != 'local':
        #         save_data(
        #             all_df,
        #             'understat_api_premier_league_matches',
        #             destination=DESTINATION,
        #             database=DATABASE,
        #             schema=SCHEMA,
        #             if_exists='replace'
        #         )
        
    finally:
        await scraper.close()
    
    print("\n✓ Scraping completed!")


if __name__ == "__main__":
    asyncio.run(main())
