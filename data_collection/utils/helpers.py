"""Helper functions for data collection."""

import re
from datetime import datetime
from typing import Optional


def parse_date(date_str: str, format: str = "%Y-%m-%d") -> Optional[datetime]:
    """
    Parse a date string into a datetime object.
    
    Args:
        date_str: Date string to parse
        format: Expected date format
        
    Returns:
        datetime object or None if parsing fails
    """
    try:
        return datetime.strptime(date_str.strip(), format)
    except (ValueError, AttributeError):
        return None


def clean_team_name(name: str) -> str:
    """
    Clean and standardize team names.
    
    Args:
        name: Raw team name
        
    Returns:
        Cleaned team name
    """
    if not name:
        return ""
    
    # Remove extra whitespace
    name = " ".join(name.split())
    
    # Remove special characters but keep accented letters
    name = re.sub(r'[^\w\s\-]', '', name, flags=re.UNICODE)
    
    return name.strip()


def parse_score(score_str: str) -> tuple[Optional[int], Optional[int]]:
    """
    Parse a score string into home and away scores.
    
    Args:
        score_str: Score string (e.g., "2-1", "3:0")
        
    Returns:
        Tuple of (home_score, away_score)
    """
    if not score_str:
        return None, None
    
    # Match various score formats
    match = re.search(r'(\d+)\s*[-:]\s*(\d+)', score_str)
    if match:
        return int(match.group(1)), int(match.group(2))
    
    return None, None


def validate_url(url: str) -> bool:
    """
    Validate if a string is a proper URL.
    
    Args:
        url: URL string to validate
        
    Returns:
        True if valid URL, False otherwise
    """
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain
        r'localhost|'  # localhost
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # or IP
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    
    return url_pattern.match(url) is not None
