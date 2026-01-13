"""Configuration for data destinations."""

import os
from typing import Literal
from dotenv import load_dotenv

load_dotenv()


class DestinationConfig:
    """Configuration for where to save scraped data."""
    
    # Destination type: 'local', 'motherduck', or 'duckdb'
    DESTINATION: Literal['local', 'motherduck', 'duckdb'] = os.getenv('DATA_DESTINATION', 'local')
    
    # Database/schema names
    DATABASE = os.getenv('DATABASE_NAME', 'football_analytics')
    SCHEMA = os.getenv('SCHEMA_NAME', 'raw')
    
    # MotherDuck token
    MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN')
    
    @classmethod
    def get_save_kwargs(cls) -> dict:
        """Get keyword arguments for save_data method."""
        return {
            'destination': cls.DESTINATION,
            'database': cls.DATABASE,
            'schema': cls.SCHEMA,
            'if_exists': 'replace'
        }
    
    @classmethod
    def validate(cls):
        """Validate configuration."""
        if cls.DESTINATION == 'motherduck' and not cls.MOTHERDUCK_TOKEN:
            raise ValueError(
                "MOTHERDUCK_TOKEN must be set in .env file when using MotherDuck. "
                "Get your token from https://app.motherduck.com/"
            )
        return True
