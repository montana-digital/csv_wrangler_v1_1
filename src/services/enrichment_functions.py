"""
Enrichment functions for CSV Wrangler v1.1.

Provides functions to enrich and validate data columns:
- Phone numbers
- Web domains/URLs
- Email addresses
- Date-only fields
- DateTime fields

All functions use best-effort parsing and validation.
"""
import re
from datetime import datetime
from typing import Optional

import pandas as pd

from src.utils.logging_config import get_logger
from src.utils.package_check import has_dateutil

logger = get_logger(__name__)

# Lazy import dateutil - only import if available
_dateutil_available = False
date_parser = None

try:
    if has_dateutil():
        from dateutil import parser as date_parser
        _dateutil_available = True
except ImportError:
    pass

# Phone number patterns (international formats)
PHONE_PATTERNS = [
    r"\+?\d{1,4}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}",  # International
    r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}",  # US format
    r"\d{10,15}",  # Simple numeric
]

# URL/Domain patterns
URL_PATTERN = re.compile(
    r"https?://(?:[-\w.])+(?:[:\d]+)?(?:/(?:[\w/_.])*)?(?:\?(?:[\w&=%.])*)?(?:#(?:[\w.])*)?",
    re.IGNORECASE,
)
DOMAIN_PATTERN = re.compile(
    r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}", re.IGNORECASE
)

# Email pattern
EMAIL_PATTERN = re.compile(
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", re.IGNORECASE
)


def enrich_phone_numbers(series: pd.Series) -> pd.Series:
    """
    Enrich and validate phone numbers.
    
    Extracts and formats phone numbers from text, handling various formats.
    Invalid or unparseable values return None.
    
    Args:
        series: Pandas Series containing phone number data
        
    Returns:
        Series with enriched/validated phone numbers (or None for invalid)
    """
    result = pd.Series(index=series.index, dtype="object")
    
    for idx, value in series.items():
        if pd.isna(value):
            result[idx] = None
            continue
        
        value_str = str(value).strip()
        if not value_str:
            result[idx] = None
            continue
        
        # Try to extract phone number using patterns
        phone = None
        for pattern in PHONE_PATTERNS:
            match = re.search(pattern, value_str)
            if match:
                phone = re.sub(r"[^\d+]", "", match.group())  # Keep only digits and +
                # Normalize: ensure it starts with + for international
                if phone and not phone.startswith("+"):
                    # If 10+ digits, assume international without country code
                    if len(phone) >= 10:
                        phone = "+" + phone
                break
        
        # If no pattern match, try to extract all digits
        if not phone:
            digits = re.sub(r"[^\d]", "", value_str)
            if len(digits) >= 10:  # Minimum valid phone length
                phone = "+" + digits if len(digits) > 10 else digits
        
        result[idx] = phone if phone and len(phone) >= 10 else None
    
    return result


def enrich_web_domains(series: pd.Series) -> pd.Series:
    """
    Enrich and validate web domains/URLs.
    
    Extracts domains or URLs from text and normalizes them.
    Normalizes protocols: http:// and https:// are treated the same (normalized to https://).
    Returns cleaned domain with normalized protocol.
    
    Args:
        series: Pandas Series containing URL/domain data
        
    Returns:
        Series with enriched domains/URLs normalized to https:// (or None for invalid)
    """
    result = pd.Series(index=series.index, dtype="object")
    
    for idx, value in series.items():
        if pd.isna(value):
            result[idx] = None
            continue
        
        value_str = str(value).strip()
        if not value_str:
            result[idx] = None
            continue
        
        # Normalize: extract domain and use https:// protocol
        domain = None
        
        # Try to find URL first
        url_match = URL_PATTERN.search(value_str)
        if url_match:
            url = url_match.group().lower()
            # Extract domain from URL
            # Remove protocol
            domain_part = url.replace("http://", "").replace("https://", "")
            # Remove path, query, fragment (keep only domain)
            domain_part = domain_part.split("/")[0].split("?")[0].split("#")[0]
            # Re-add normalized protocol
            domain = "https://" + domain_part
            result[idx] = domain
            continue
        
        # Try to find domain pattern
        domain_match = DOMAIN_PATTERN.search(value_str)
        if domain_match:
            domain_part = domain_match.group().lower()
            # Remove any existing protocol
            domain_part = domain_part.replace("http://", "").replace("https://", "")
            # Remove path if present (everything after first /)
            domain_part = domain_part.split("/")[0].split("?")[0].split("#")[0]
            # Normalize to https://
            domain = "https://" + domain_part
            result[idx] = domain
            continue
        
        # If no pattern match, check if it looks like a domain
        if "." in value_str and not " " in value_str:
            parts = value_str.split(".")
            if len(parts) >= 2 and all(len(p) > 0 for p in parts):
                # Remove any existing protocol
                domain_part = value_str.lower().replace("http://", "").replace("https://", "")
                # Remove path if present
                domain_part = domain_part.split("/")[0].split("?")[0].split("#")[0]
                # Normalize to https://
                domain = "https://" + domain_part
                result[idx] = domain
            else:
                result[idx] = None
        else:
            result[idx] = None
    
    return result


def enrich_emails(series: pd.Series) -> pd.Series:
    """
    Enrich and validate email addresses.
    
    Extracts and validates email addresses from text.
    
    Args:
        series: Pandas Series containing email data
        
    Returns:
        Series with validated email addresses (or None for invalid)
    """
    result = pd.Series(index=series.index, dtype="object")
    
    for idx, value in series.items():
        if pd.isna(value):
            result[idx] = None
            continue
        
        value_str = str(value).strip()
        if not value_str:
            result[idx] = None
            continue
        
        # Try to find email pattern
        email_match = EMAIL_PATTERN.search(value_str)
        if email_match:
            email = email_match.group().lower()
            # Basic validation: must have @ and .
            if "@" in email and "." in email.split("@")[1]:
                result[idx] = email
            else:
                result[idx] = None
        else:
            result[idx] = None
    
    return result


def enrich_date_only(series: pd.Series) -> pd.Series:
    """
    Enrich and parse date-only fields.
    
    Parses various date formats and returns date strings in YYYY-MM-DD format.
    
    Args:
        series: Pandas Series containing date data
        
    Returns:
        Series with parsed dates in YYYY-MM-DD format (or None for invalid)
    """
    result = pd.Series(index=series.index, dtype="object")
    
    for idx, value in series.items():
        if pd.isna(value):
            result[idx] = None
            continue
        
        value_str = str(value).strip()
        if not value_str:
            result[idx] = None
            continue
        
        try:
            # Try parsing with dateutil if available (handles many formats)
            if _dateutil_available and date_parser:
                parsed_date = date_parser.parse(value_str, default=datetime(1900, 1, 1))
                # Format as date-only (YYYY-MM-DD)
                result[idx] = parsed_date.strftime("%Y-%m-%d")
            else:
                # Fallback to manual parsing without dateutil
                raise ValueError("dateutil not available, using manual parsing")
        except (ValueError, TypeError, OverflowError):
            # Try common date formats manually
            date_formats = [
                "%Y-%m-%d",
                "%m/%d/%Y",
                "%d/%m/%Y",
                "%Y/%m/%d",
                "%d-%m-%Y",
                "%m-%d-%Y",
            ]
            parsed = None
            for fmt in date_formats:
                try:
                    parsed = datetime.strptime(value_str, fmt)
                    break
                except ValueError:
                    continue
            
            if parsed:
                result[idx] = parsed.strftime("%Y-%m-%d")
            else:
                result[idx] = None
    
    return result


def enrich_datetime(series: pd.Series) -> pd.Series:
    """
    Enrich and parse datetime fields.
    
    Parses various datetime formats and returns ISO format datetime strings.
    
    Args:
        series: Pandas Series containing datetime data
        
    Returns:
        Series with parsed datetimes in ISO format (or None for invalid)
    """
    result = pd.Series(index=series.index, dtype="object")
    
    for idx, value in series.items():
        if pd.isna(value):
            result[idx] = None
            continue
        
        value_str = str(value).strip()
        if not value_str:
            result[idx] = None
            continue
        
        try:
            # Try parsing with dateutil if available (handles many formats)
            if _dateutil_available and date_parser:
                parsed_datetime = date_parser.parse(value_str)
                # Format as ISO datetime
                result[idx] = parsed_datetime.isoformat()
            else:
                # Fallback to manual parsing without dateutil
                raise ValueError("dateutil not available, using manual parsing")
        except (ValueError, TypeError, OverflowError):
            # Try common datetime formats manually
            datetime_formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%m/%d/%Y %H:%M:%S",
                "%d/%m/%Y %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%m/%d/%Y %H:%M",
            ]
            parsed = None
            for fmt in datetime_formats:
                try:
                    parsed = datetime.strptime(value_str, fmt)
                    break
                except ValueError:
                    continue
            
            if parsed:
                result[idx] = parsed.isoformat()
            else:
                result[idx] = None
    
    return result


# Mapping of function names to functions
ENRICHMENT_FUNCTIONS = {
    "phone_numbers": enrich_phone_numbers,
    "web_domains": enrich_web_domains,
    "emails": enrich_emails,
    "date_only": enrich_date_only,
    "datetime": enrich_datetime,
}


def get_enrichment_function(function_name: str):
    """
    Get enrichment function by name.
    
    Args:
        function_name: Name of enrichment function
        
    Returns:
        Function callable
        
    Raises:
        ValueError: If function name is not found
    """
    if function_name not in ENRICHMENT_FUNCTIONS:
        raise ValueError(
            f"Unknown enrichment function: {function_name}. "
            f"Available: {list(ENRICHMENT_FUNCTIONS.keys())}"
        )
    return ENRICHMENT_FUNCTIONS[function_name]

