"""Understat.com scraper for Premier League match data."""

import pandas as pd
import json
import re
import time
from typing import Dict, List, Optional
from base_scraper import BaseScraper


class UnderstatScraper(BaseScraper):
    """
    Scraper for football statistics from Understat.com.

    Note: Understat renders data via JavaScript, making it challenging to scrape
    without a browser automation tool like Selenium or Playwright.
    This scraper attempts to extract data from embedded JSON, but may require
    updates as the site structure changes.

    Alternative: Consider using understat Python package (pip install understat)
    which provides an async API wrapper.
    """

    # Premier League teams with their Understat IDs/names
    PREMIER_LEAGUE_TEAMS = {
        "Arsenal": "Arsenal",
        "Aston Villa": "Aston_Villa",
        "Bournemouth": "Bournemouth",
        "Brentford": "Brentford",
        "Brighton": "Brighton",
        "Chelsea": "Chelsea",
        "Crystal Palace": "Crystal_Palace",
        "Everton": "Everton",
        "Fulham": "Fulham",
        "Ipswich Town": "Ipswich",
        "Leicester": "Leicester",
        "Liverpool": "Liverpool",
        "Manchester City": "Manchester_City",
        "Manchester United": "Manchester_United",
        "Newcastle United": "Newcastle_United",
        "Nottingham Forest": "Nottingham_Forest",
        "Southampton": "Southampton",
        "Tottenham": "Tottenham",
        "West Ham": "West_Ham",
        "Wolves": "Wolverhampton_Wanderers",
    }

    def __init__(self, rate_limit: float = 3.0):
        """
        Initialize Understat scraper.

        Args:
            rate_limit: Seconds between requests (default 3.0)
        """
        super().__init__(
            base_url="https://understat.com", rate_limit=rate_limit, verify_ssl=False
        )

    def extract_json_from_script(self, soup, variable_name: str) -> Optional[List]:
        """
        Extract JSON data from JavaScript variable in page source.

        Understat embeds data in JavaScript variables that need to be parsed.

        Args:
            soup: BeautifulSoup object
            variable_name: Name of JS variable to extract

        Returns:
            Parsed JSON data or None
        """
        scripts = soup.find_all("script")

        for script in scripts:
            if script.string and variable_name in script.string:
                # Extract JSON data from JavaScript variable
                pattern = rf"var {variable_name}\s*=\s*JSON\.parse\('(.+?)'\);"
                match = re.search(pattern, script.string)

                if match:
                    json_str = match.group(1)
                    # Unescape the string
                    json_str = json_str.encode().decode("unicode_escape")
                    try:
                        return json.loads(json_str)
                    except json.JSONDecodeError as e:
                        print(f"Error parsing JSON for {variable_name}: {e}")
                        return None

        return None

    def scrape_team_season(self, team_name: str, season: str) -> pd.DataFrame:
        """
        Scrape match data for a team's season.

        Args:
            team_name: Team name (display name like 'Arsenal')
            season: Season string (e.g., '2025')

        Returns:
            DataFrame with match statistics
        """
        # Get URL-friendly team name
        url_team_name = self.PREMIER_LEAGUE_TEAMS.get(team_name)
        if not url_team_name:
            print(f"Team '{team_name}' not found in Premier League teams list")
            return pd.DataFrame()

        url = f"{self.base_url}/team/{url_team_name}/{season}"
        print(f"Scraping {team_name} ({season})...")

        try:
            soup = self.fetch_page(url)

            # Extract match data from JavaScript variable
            matches_data = self.extract_json_from_script(soup, "datesData")

            if not matches_data:
                print(f"  ✗ No match data found for {team_name}")
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(matches_data)

            # Add team information
            df["team"] = team_name
            df["season"] = season

            # Clean up data types
            numeric_cols = [
                "xG",
                "xGA",
                "npxG",
                "npxGA",
                "deep",
                "deep_allowed",
                "scored",
                "missed",
                "xpts",
                "wins",
                "draws",
                "loses",
                "pts",
            ]

            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            print(f"  ✓ Scraped {len(df)} matches")
            return df

        except Exception as e:
            print(f"  ✗ Error scraping {team_name}: {str(e)}")
            return pd.DataFrame()

    def scrape_team_players(self, team_name: str, season: str) -> pd.DataFrame:
        """
        Scrape player statistics for a team's season.

        Args:
            team_name: Team name
            season: Season string

        Returns:
            DataFrame with player statistics
        """
        url_team_name = self.PREMIER_LEAGUE_TEAMS.get(team_name)
        if not url_team_name:
            return pd.DataFrame()

        url = f"{self.base_url}/team/{url_team_name}/{season}"

        try:
            soup = self.fetch_page(url)

            # Extract player data
            players_data = self.extract_json_from_script(soup, "playersData")

            if not players_data:
                return pd.DataFrame()

            df = pd.DataFrame(players_data)
            df["team"] = team_name
            df["season"] = season

            return df

        except Exception as e:
            print(f"  ✗ Error scraping player data for {team_name}: {str(e)}")
            return pd.DataFrame()

    def scrape_league_matches(self, season: str) -> pd.DataFrame:
        """
        Scrape all Premier League matches for a season.

        Args:
            season: Season string (e.g., '2025')

        Returns:
            DataFrame with all match data
        """
        url = f"{self.base_url}/league/EPL/{season}"
        print(f"Scraping Premier League {season} season overview...")

        try:
            soup = self.fetch_page(url)

            # Extract fixtures data
            fixtures_data = self.extract_json_from_script(soup, "datesData")

            if not fixtures_data:
                print("  ✗ No fixtures data found")
                return pd.DataFrame()

            df = pd.DataFrame(fixtures_data)
            df["season"] = season
            df["league"] = "Premier League"

            print(f"  ✓ Scraped {len(df)} matches")
            return df

        except Exception as e:
            print(f"  ✗ Error scraping league data: {str(e)}")
            return pd.DataFrame()

    def scrape_premier_league_season(self, season: str) -> pd.DataFrame:
        """
        Scrape all Premier League teams for a season.

        Args:
            season: Season string (e.g., '2025')

        Returns:
            Combined DataFrame with all teams' data
        """
        all_data = []

        print(f"\n{'='*60}")
        print(f"Scraping all Premier League teams - {season} season")
        print(f"{'='*60}\n")

        for team_name in self.PREMIER_LEAGUE_TEAMS.keys():
            team_df = self.scrape_team_season(team_name, season)

            if not team_df.empty:
                all_data.append(team_df)

            # Be polite with requests
            time.sleep(self.rate_limit)

        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            print(f"\n{'='*60}")
            print(f"Total matches scraped: {len(combined)}")
            print(f"Teams: {combined['team'].nunique()}")
            print(f"{'='*60}")
            return combined
        else:
            return pd.DataFrame()

    def scrape(self) -> pd.DataFrame:
        """
        Main scraping method.

        Returns:
            DataFrame with all scraped data
        """
        # Default: scrape current season (2025)
        return self.scrape_premier_league_season("2025")


def main():
    """Run the scraper."""
    import os
    from dotenv import load_dotenv

    # Load environment variables
    load_dotenv()

    # Get configuration
    rate_limit = float(os.getenv("SCRAPER_RATE_LIMIT", "3.0"))
    DESTINATION = os.getenv("DATA_DESTINATION", "local")
    DATABASE = os.getenv("DATABASE_NAME", "football_analytics")
    SCHEMA = os.getenv("SCHEMA_NAME", "raw")
    # Note: Understat uses year only, and data is rendered via JavaScript
    # Current season 2025-26 would be '2025'
    SEASON = os.getenv("SEASON", "2024")  # Use 2024 for 2024-25 season

    scraper = UnderstatScraper(rate_limit=rate_limit)

    # Example 1: Scrape single team
    print("Scraping Nottingham Forest from Understat...")
    df = scraper.scrape_team_season("Nottingham Forest", SEASON)

    if not df.empty:
        print(f"\nScraped {len(df)} matches with {len(df.columns)} columns")

        # Save locally (always as backup)
        scraper.save_data(
            df,
            f"understat_nottingham_forest_{SEASON}",
            format="csv",
            destination="local",
        )

        # Upload to configured destination if not local
        if DESTINATION != "local":
            print(f"\nUploading to {DESTINATION}...")
            try:
                scraper.save_data(
                    df,
                    f"understat_nottingham_forest_{SEASON}",
                    destination=DESTINATION,
                    database=DATABASE,
                    schema=SCHEMA,
                    if_exists="replace",
                )
                print("✓ Upload successful!")
            except Exception as e:
                print(f"✗ Upload failed: {str(e)}")
                print("Data saved locally only")

    # Example 2: Scrape all teams (uncomment to use)
    # print("\n\nScraping all Premier League teams...")
    # all_df = scraper.scrape_premier_league_season(SEASON)
    #
    # if not all_df.empty:
    #     scraper.save_data(
    #         all_df,
    #         f'understat_premier_league_{SEASON}_all_teams',
    #         format='csv',
    #         destination='local'
    #     )
    #
    #     if DESTINATION != 'local':
    #         scraper.save_data(
    #             all_df,
    #             'understat_premier_league_matches',
    #             destination=DESTINATION,
    #             database=DATABASE,
    #             schema=SCHEMA,
    #             if_exists='replace'
    #         )


if __name__ == "__main__":
    main()
