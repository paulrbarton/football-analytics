"""Base scraper class with common functionality."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time


class BaseScraper(ABC):
    """Abstract base class for all scrapers."""

    def __init__(self, base_url: str, rate_limit: float = 1.0, verify_ssl: bool = False):
        """
        Initialize the scraper.
        
        Args:
            base_url: Base URL for the data source
            rate_limit: Minimum seconds between requests (default: 1.0)
            verify_ssl: Whether to verify SSL certificates (default: False for compatibility)
        """
        self.base_url = base_url
        self.rate_limit = rate_limit
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        # Disable SSL warnings when verification is off
        if not verify_ssl:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def fetch_page(self, url: str) -> BeautifulSoup:
        """
        Fetch and parse a web page.
        
        Args:
            url: URL to fetch
            
        Returns:
            BeautifulSoup object of the page
        """
        time.sleep(self.rate_limit)  # Rate limiting
        response = self.session.get(url, verify=self.verify_ssl, timeout=30)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'lxml')

    @abstractmethod
    def scrape(self) -> pd.DataFrame:
        """
        Main scraping method to be implemented by subclasses.
        
        Returns:
            DataFrame containing scraped data
        """
        pass

    def save_data(self, df: pd.DataFrame, filename: str, format: str = 'csv', 
                  destination: str = 'local', **kwargs):
        """
        Save scraped data to file or database.
        
        Args:
            df: DataFrame to save
            filename: Output filename or table name
            format: File format ('csv', 'json', 'parquet')
            destination: Where to save ('local', 'motherduck', 'duckdb')
            **kwargs: Additional arguments for specific destinations
                - database: database name (for motherduck/duckdb)
                - schema: schema name (for motherduck/duckdb)
                - if_exists: 'replace', 'append', or 'fail' (for databases)
        """
        import os
        
        if destination == 'local':
            # Get the project root (two levels up from scrapers/)
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            output_dir = os.path.join(project_root, 'data', 'raw')
            
            # Create directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            output_path = os.path.join(output_dir, filename)
            
            if format == 'csv':
                df.to_csv(f"{output_path}.csv", index=False)
            elif format == 'json':
                df.to_json(f"{output_path}.json", orient='records', indent=2)
            elif format == 'parquet':
                df.to_parquet(f"{output_path}.parquet", index=False)
            
            print(f"Data saved to {output_path}.{format}")
            
        elif destination in ['motherduck', 'duckdb']:
            self._save_to_duckdb(df, filename, destination, **kwargs)
        
        else:
            raise ValueError(f"Unknown destination: {destination}. Use 'local', 'motherduck', or 'duckdb'")
    
    def _save_to_duckdb(self, df: pd.DataFrame, table_name: str, destination: str, **kwargs):
        """
        Save data to DuckDB or MotherDuck.
        
        Args:
            df: DataFrame to save
            table_name: Name of the table
            destination: 'duckdb' or 'motherduck'
            **kwargs: database, schema, if_exists
        """
        import duckdb
        import os
        from dotenv import load_dotenv
        
        load_dotenv()
        
        database = kwargs.get('database', 'football_analytics')
        schema = kwargs.get('schema', 'raw')
        if_exists = kwargs.get('if_exists', 'replace')
        
        try:
            if destination == 'motherduck':
                # Get MotherDuck token from environment
                motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
                if not motherduck_token:
                    raise ValueError(
                        "MOTHERDUCK_TOKEN not found in environment. "
                        "Set it with: export MOTHERDUCK_TOKEN='your_token'"
                    )
                
                # Disable SSL verification for corporate networks
                os.environ['DUCKDB_MOTHERDUCK_DISABLE_SSL_VERIFICATION'] = '1'
                os.environ['GRPC_DEFAULT_SSL_ROOTS_FILE_PATH'] = ''
                
                # Try disabling SSL verification at Python level
                import ssl
                try:
                    ssl._create_default_https_context = ssl._create_unverified_context
                except:
                    pass
                
                # Connect to MotherDuck with SSL verification disabled
                conn = duckdb.connect(f'md:{database}?motherduck_token={motherduck_token}')
                print(f"Connected to MotherDuck database: {database}")
            else:
                # Local DuckDB file
                project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                db_path = os.path.join(project_root, 'data', f'{database}.duckdb')
                os.makedirs(os.path.dirname(db_path), exist_ok=True)
                conn = duckdb.connect(db_path)
                print(f"Connected to local DuckDB: {db_path}")
            
            # Create schema if it doesn't exist
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            
            # Full table name with schema
            full_table_name = f"{schema}.{table_name}"
            
            # Handle if_exists
            if if_exists == 'replace':
                conn.execute(f"DROP TABLE IF EXISTS {full_table_name}")
            elif if_exists == 'fail':
                # Check if table exists
                result = conn.execute(
                    f"SELECT COUNT(*) FROM information_schema.tables "
                    f"WHERE table_schema = '{schema}' AND table_name = '{table_name}'"
                ).fetchone()
                if result[0] > 0:
                    raise ValueError(f"Table {full_table_name} already exists")
            
            # Insert data
            conn.execute(f"CREATE TABLE IF NOT EXISTS {full_table_name} AS SELECT * FROM df")
            
            if if_exists == 'append':
                conn.execute(f"INSERT INTO {full_table_name} SELECT * FROM df")
            
            row_count = conn.execute(f"SELECT COUNT(*) FROM {full_table_name}").fetchone()[0]
            
            conn.close()
            
            print(f"✓ Saved {row_count} rows to {destination}: {full_table_name}")
            
        except Exception as e:
            print(f"✗ Error saving to {destination}: {str(e)}")
            raise
