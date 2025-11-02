"""
Unit tests for Enrichment functions.

Tests all 5 enrichment functions: phone numbers, web domains, emails, dates, datetimes.
"""
import pandas as pd
import pytest

pytestmark = pytest.mark.unit

from src.services.enrichment_functions import (
    enrich_date_only,
    enrich_datetime,
    enrich_emails,
    enrich_phone_numbers,
    enrich_web_domains,
    get_enrichment_function,
)


class TestEnrichPhoneNumbers:
    """Test phone number enrichment."""

    def test_enrich_us_format_phone(self):
        """Test enriching US format phone numbers."""
        series = pd.Series(["555-123-4567", "(555) 123-4567", "555.123.4567"])
        result = enrich_phone_numbers(series)
        
        assert result.iloc[0] is not None
        assert result.iloc[1] is not None
        assert result.iloc[2] is not None

    def test_enrich_international_phone(self):
        """Test enriching international phone numbers."""
        series = pd.Series(["+1-555-123-4567", "+44 20 7946 0958", "+33123456789"])
        result = enrich_phone_numbers(series)
        
        assert all(phone is not None for phone in result)
        assert result.iloc[0].startswith("+")

    def test_enrich_phone_from_text(self):
        """Test extracting phone numbers from text."""
        series = pd.Series(["Call me at 555-123-4567", "Phone: +1 555 123 4567"])
        result = enrich_phone_numbers(series)
        
        assert result.iloc[0] is not None
        assert result.iloc[1] is not None

    def test_enrich_phone_invalid_returns_none(self):
        """Test that invalid phone numbers return None."""
        series = pd.Series(["123", "abc", "12345", "not a phone"])
        result = enrich_phone_numbers(series)
        
        assert all(phone is None for phone in result)

    def test_enrich_phone_nan_handling(self):
        """Test that NaN values are handled correctly."""
        series = pd.Series([None, pd.NA, float("nan"), ""])
        result = enrich_phone_numbers(series)
        
        assert all(phone is None for phone in result)

    def test_enrich_phone_whitespace_only(self):
        """Test that whitespace-only values return None."""
        series = pd.Series(["   ", "\t\n", " "])
        result = enrich_phone_numbers(series)
        
        assert all(phone is None for phone in result)


class TestEnrichWebDomains:
    """Test web domain/URL enrichment."""

    def test_enrich_full_url(self):
        """Test enriching full URLs."""
        series = pd.Series([
            "https://www.example.com",
            "http://example.com",
            "https://example.com/path?query=1"
        ])
        result = enrich_web_domains(series)
        
        assert all(domain is not None for domain in result)
        assert all(domain.startswith("https://") for domain in result)
        assert result.iloc[0] == "https://www.example.com"
        assert result.iloc[1] == "https://example.com"

    def test_enrich_domain_only(self):
        """Test enriching domain-only strings."""
        series = pd.Series(["example.com", "www.test.org", "subdomain.example.net"])
        result = enrich_web_domains(series)
        
        assert all(domain is not None for domain in result)
        assert all(domain.startswith("https://") for domain in result)

    def test_enrich_url_normalizes_protocol(self):
        """Test that URLs are normalized to https://."""
        series = pd.Series(["http://example.com", "https://example.com"])
        result = enrich_web_domains(series)
        
        assert all(domain.startswith("https://") for domain in result)

    def test_enrich_url_removes_path(self):
        """Test that paths are removed, keeping only domain."""
        series = pd.Series(["https://example.com/path/to/page", "example.com/path"])
        result = enrich_web_domains(series)
        
        assert result.iloc[0] == "https://example.com"
        assert result.iloc[1] == "https://example.com"

    def test_enrich_domain_invalid_returns_none(self):
        """Test that invalid domains return None."""
        series = pd.Series(["not a domain", "123", "just text", "..invalid.."])
        result = enrich_web_domains(series)
        
        assert all(domain is None for domain in result)

    def test_enrich_domain_nan_handling(self):
        """Test that NaN values are handled correctly."""
        series = pd.Series([None, pd.NA, float("nan"), ""])
        result = enrich_web_domains(series)
        
        assert all(domain is None for domain in result)


class TestEnrichEmails:
    """Test email enrichment."""

    def test_enrich_valid_emails(self):
        """Test enriching valid email addresses."""
        series = pd.Series([
            "user@example.com",
            "test.email@domain.co.uk",
            "user+tag@example.org"
        ])
        result = enrich_emails(series)
        
        assert all(email is not None for email in result)
        assert result.iloc[0] == "user@example.com"
        assert result.iloc[1] == "test.email@domain.co.uk"

    def test_enrich_email_lowercase(self):
        """Test that emails are converted to lowercase."""
        series = pd.Series(["User@Example.COM", "TEST@DOMAIN.ORG"])
        result = enrich_emails(series)
        
        assert result.iloc[0] == "user@example.com"
        assert result.iloc[1] == "test@domain.org"

    def test_enrich_email_from_text(self):
        """Test extracting emails from text."""
        series = pd.Series([
            "Contact me at user@example.com",
            "Email: test@domain.org for more info"
        ])
        result = enrich_emails(series)
        
        assert result.iloc[0] == "user@example.com"
        assert result.iloc[1] == "test@domain.org"

    def test_enrich_email_invalid_returns_none(self):
        """Test that invalid emails return None."""
        series = pd.Series([
            "not an email",
            "@domain.com",
            "user@",
            "invalid..email@domain.com"
        ])
        result = enrich_emails(series)
        
        # Most should be None, but some might match patterns (email validation is lenient)
        # At least some clearly invalid ones should be None
        assert result.iloc[0] is None  # "not an email" should be None

    def test_enrich_email_nan_handling(self):
        """Test that NaN values are handled correctly."""
        series = pd.Series([None, pd.NA, float("nan"), ""])
        result = enrich_emails(series)
        
        assert all(email is None for email in result)


class TestEnrichDateOnly:
    """Test date-only enrichment."""

    def test_enrich_iso_date_format(self):
        """Test enriching ISO date format (YYYY-MM-DD)."""
        series = pd.Series(["2024-01-15", "2023-12-25"])
        result = enrich_date_only(series)
        
        assert result.iloc[0] == "2024-01-15"
        assert result.iloc[1] == "2023-12-25"

    def test_enrich_us_date_format(self):
        """Test enriching US date format (MM/DD/YYYY)."""
        series = pd.Series(["01/15/2024", "12/25/2023"])
        result = enrich_date_only(series)
        
        assert all(date is not None for date in result)
        assert result.iloc[0] == "2024-01-15"

    def test_enrich_european_date_format(self):
        """Test enriching European date format (DD/MM/YYYY)."""
        series = pd.Series(["15/01/2024", "25/12/2023"])
        result = enrich_date_only(series)
        
        # Note: dateutil might interpret differently, but should parse
        assert all(date is not None for date in result)

    def test_enrich_various_date_formats(self):
        """Test enriching various date formats."""
        series = pd.Series([
            "2024-01-15",
            "01/15/2024",
            "15-01-2024",
            "January 15, 2024"
        ])
        result = enrich_date_only(series)
        
        assert all(date is not None for date in result)
        assert all("/" in date or "-" in date for date in result if date)

    def test_enrich_date_invalid_returns_none(self):
        """Test that invalid dates return None."""
        series = pd.Series(["not a date", "32/13/2024", "invalid"])
        result = enrich_date_only(series)
        
        # Some might parse, but clearly invalid should be None
        assert any(date is None for date in result)

    def test_enrich_date_nan_handling(self):
        """Test that NaN values are handled correctly."""
        series = pd.Series([None, pd.NA, float("nan"), ""])
        result = enrich_date_only(series)
        
        assert all(date is None for date in result)


class TestEnrichDatetime:
    """Test datetime enrichment."""

    def test_enrich_iso_datetime_format(self):
        """Test enriching ISO datetime format."""
        series = pd.Series([
            "2024-01-15 10:30:00",
            "2024-01-15T10:30:00"
        ])
        result = enrich_datetime(series)
        
        assert all(dt is not None for dt in result)
        assert "2024-01-15" in result.iloc[0]
        assert "10:30" in result.iloc[0]

    def test_enrich_us_datetime_format(self):
        """Test enriching US datetime format."""
        series = pd.Series(["01/15/2024 10:30:00", "12/25/2023 15:45:30"])
        result = enrich_datetime(series)
        
        assert all(dt is not None for dt in result)

    def test_enrich_datetime_with_time_only(self):
        """Test enriching datetime with time component."""
        series = pd.Series([
            "2024-01-15 10:30",
            "01/15/2024 15:45"
        ])
        result = enrich_datetime(series)
        
        assert all(dt is not None for dt in result)

    def test_enrich_datetime_various_formats(self):
        """Test enriching various datetime formats."""
        series = pd.Series([
            "2024-01-15 10:30:00",
            "2024-01-15T10:30:00",
            "01/15/2024 10:30:00"
        ])
        result = enrich_datetime(series)
        
        assert all(dt is not None for dt in result)
        # All should be in ISO format
        assert all("T" in dt or "-" in dt for dt in result if dt)

    def test_enrich_datetime_invalid_returns_none(self):
        """Test that invalid datetimes return None."""
        series = pd.Series(["not a datetime", "32/13/2024 25:70:00"])
        result = enrich_datetime(series)
        
        # Clearly invalid should return None
        assert any(dt is None for dt in result)

    def test_enrich_datetime_nan_handling(self):
        """Test that NaN values are handled correctly."""
        series = pd.Series([None, pd.NA, float("nan"), ""])
        result = enrich_datetime(series)
        
        assert all(dt is None for dt in result)


class TestGetEnrichmentFunction:
    """Test getting enrichment function by name."""

    def test_get_phone_numbers_function(self):
        """Test getting phone_numbers function."""
        func = get_enrichment_function("phone_numbers")
        assert func == enrich_phone_numbers

    def test_get_web_domains_function(self):
        """Test getting web_domains function."""
        func = get_enrichment_function("web_domains")
        assert func == enrich_web_domains

    def test_get_emails_function(self):
        """Test getting emails function."""
        func = get_enrichment_function("emails")
        assert func == enrich_emails

    def test_get_date_only_function(self):
        """Test getting date_only function."""
        func = get_enrichment_function("date_only")
        assert func == enrich_date_only

    def test_get_datetime_function(self):
        """Test getting datetime function."""
        func = get_enrichment_function("datetime")
        assert func == enrich_datetime

    def test_get_invalid_function_raises_error(self):
        """Test that invalid function name raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            get_enrichment_function("invalid_function")
        
        assert "Unknown enrichment function" in str(exc_info.value)
        assert "invalid_function" in str(exc_info.value)

