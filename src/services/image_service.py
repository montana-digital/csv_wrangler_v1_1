"""
Image service for CSV Wrangler.

Provides utilities for finding tables with image columns and retrieving
Knowledge Table associations for rows containing phone numbers or web domains.
"""
from typing import Any

import pandas as pd
from sqlalchemy.orm import Session

from src.database.models import DatasetConfig, EnrichedDataset
from src.services.search_service import search_knowledge_base
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def get_tables_with_image_columns(session: Session) -> list[dict[str, Any]]:
    """
    Get all tables (datasets and enriched datasets) that contain image columns.
    
    Args:
        session: Database session
        
    Returns:
        List of dictionaries with table information:
        - type: "dataset" or "enriched_dataset"
        - id: dataset_id or enriched_dataset_id
        - name: Display name for the table
        - table_name: Database table name
        - image_columns: List of image column names
    """
    tables = []
    
    # Get all datasets with image columns
    datasets = session.query(DatasetConfig).all()
    for dataset in datasets:
        if dataset.image_columns and len(dataset.image_columns) > 0:
            tables.append({
                "type": "dataset",
                "id": dataset.id,
                "name": f"{dataset.name} (Dataset {dataset.slot_number})",
                "table_name": dataset.table_name,
                "image_columns": dataset.image_columns,
            })
    
    # Get all enriched datasets with image columns (from source dataset)
    enriched_datasets = session.query(EnrichedDataset).all()
    for enriched_dataset in enriched_datasets:
        # Check source dataset for image columns
        source_dataset = session.get(DatasetConfig, enriched_dataset.source_dataset_id)
        if source_dataset and source_dataset.image_columns and len(source_dataset.image_columns) > 0:
            tables.append({
                "type": "enriched_dataset",
                "id": enriched_dataset.id,
                "name": f"{enriched_dataset.name} (Enriched Dataset)",
                "table_name": enriched_dataset.enriched_table_name,
                "image_columns": source_dataset.image_columns,  # Inherited from source
            })
    
    logger.info(f"Found {len(tables)} tables with image columns")
    return tables


def extract_enriched_columns_from_row(row_data: dict[str, Any]) -> dict[str, list[str]]:
    """
    Extract phone number and web domain enriched columns from a row.
    
    Looks for columns matching patterns:
    - Phone: *_enriched_phone_numbers
    - Web domains: *_enriched_web_domains
    
    Args:
        row_data: Dictionary of row data (column_name -> value)
        
    Returns:
        Dictionary with:
        - phone_numbers: List of non-null phone values
        - web_domains: List of non-null domain values
    """
    phone_numbers = []
    web_domains = []
    
    for column_name, value in row_data.items():
        if pd.isna(value) or value is None or value == "":
            continue
            
        # Check for phone number enriched columns
        if column_name.endswith("_enriched_phone_numbers"):
            phone_numbers.append(str(value))
        
        # Check for web domain enriched columns
        elif column_name.endswith("_enriched_web_domains"):
            web_domains.append(str(value))
    
    # Remove duplicates while preserving order
    phone_numbers = list(dict.fromkeys(phone_numbers))
    web_domains = list(dict.fromkeys(web_domains))
    
    return {
        "phone_numbers": phone_numbers,
        "web_domains": web_domains,
    }


def get_knowledge_associations_for_row(
    session: Session,
    row_data: dict[str, Any],
    table_type: str,
    table_id: int,
) -> dict[str, Any]:
    """
    Get Knowledge Table associations for phone numbers and web domains in a row.
    
    Searches all Knowledge Tables and enriched datasets for each found phone/domain value.
    
    Args:
        session: Database session
        row_data: Dictionary of row data (column_name -> value)
        table_type: "dataset" or "enriched_dataset"
        table_id: ID of the dataset or enriched dataset
        
    Returns:
        Dictionary with:
        - phone_numbers: [
            {
                "value": str,
                "search_results": dict (from search_knowledge_base)
            }
          ]
        - web_domains: [
            {
                "value": str,
                "search_results": dict (from search_knowledge_base)
            }
          ]
    """
    # Extract enriched columns from row
    extracted = extract_enriched_columns_from_row(row_data)
    
    results = {
        "phone_numbers": [],
        "web_domains": [],
    }
    
    # Search for each phone number
    for phone_value in extracted["phone_numbers"]:
        try:
            search_results = search_knowledge_base(
                session=session,
                search_value=phone_value,
                data_type="phone_numbers",
                source_filters=None,  # Search all sources
            )
            results["phone_numbers"].append({
                "value": phone_value,
                "search_results": search_results,
            })
        except Exception as e:
            logger.warning(f"Failed to search Knowledge Base for phone {phone_value}: {e}")
            # Still include the value, but with empty results
            results["phone_numbers"].append({
                "value": phone_value,
                "search_results": {
                    "presence": {
                        "knowledge_tables": [],
                        "enriched_datasets": [],
                    }
                },
            })
    
    # Search for each web domain
    for domain_value in extracted["web_domains"]:
        try:
            search_results = search_knowledge_base(
                session=session,
                search_value=domain_value,
                data_type="web_domains",
                source_filters=None,  # Search all sources
            )
            results["web_domains"].append({
                "value": domain_value,
                "search_results": search_results,
            })
        except Exception as e:
            logger.warning(f"Failed to search Knowledge Base for domain {domain_value}: {e}")
            # Still include the value, but with empty results
            results["web_domains"].append({
                "value": domain_value,
                "search_results": {
                    "presence": {
                        "knowledge_tables": [],
                        "enriched_datasets": [],
                    }
                },
            })
    
    return results

