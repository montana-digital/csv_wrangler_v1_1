"""
Database integrity checking and cleanup utilities.

Provides functions to detect and clean up orphaned tables, records, and other data integrity issues.
"""
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from src.database.models import DatasetConfig, EnrichedDataset, KnowledgeTable
from src.utils.errors import DatabaseError
from src.utils.logging_config import get_logger
from src.utils.validation import quote_identifier, table_exists

logger = get_logger(__name__)


def check_database_integrity(session: Session) -> dict[str, Any]:
    """
    Check database integrity and detect orphaned data.
    
    Checks for:
    - Orphaned enriched tables (table exists but no EnrichedDataset record)
    - Orphaned EnrichedDataset records (record exists but table missing)
    - Orphaned Knowledge Tables (table exists but no KnowledgeTable record)
    - EnrichedDataset records with invalid source_dataset_id
    - Tables without corresponding DatasetConfig records
    
    Args:
        session: Database session
        
    Returns:
        Dictionary with integrity check results:
        {
            "orphaned_enriched_tables": [...],
            "orphaned_enriched_records": [...],
            "orphaned_knowledge_tables": [...],
            "invalid_enriched_references": [...],
            "orphaned_dataset_tables": [...],
            "total_issues": int
        }
    """
    results = {
        "orphaned_enriched_tables": [],
        "orphaned_enriched_records": [],
        "orphaned_knowledge_tables": [],
        "invalid_enriched_references": [],
        "orphaned_dataset_tables": [],
        "total_issues": 0,
    }
    
    try:
        inspector = inspect(session.bind)
        all_tables = set(inspector.get_table_names())
        
        # Get all metadata tables (exclude system tables)
        metadata_tables = {
            "dataset_config",
            "upload_log",
            "user_profile",
            "enriched_dataset",
            "note",
            "knowledge_table",
            "data_analysis",
        }
        data_tables = all_tables - metadata_tables
        
        # Check enriched tables
        enriched_datasets = session.query(EnrichedDataset).all()
        enriched_table_names = {ed.enriched_table_name for ed in enriched_datasets}
        
        # Find orphaned enriched tables (table exists but no record)
        enriched_tables_in_db = {t for t in data_tables if t.startswith("enriched_")}
        orphaned_enriched_tables = enriched_tables_in_db - enriched_table_names
        results["orphaned_enriched_tables"] = list(orphaned_enriched_tables)
        
        # Find orphaned enriched records (record exists but table missing)
        for enriched_dataset in enriched_datasets:
            if not table_exists(session, enriched_dataset.enriched_table_name):
                results["orphaned_enriched_records"].append({
                    "id": enriched_dataset.id,
                    "name": enriched_dataset.name,
                    "enriched_table_name": enriched_dataset.enriched_table_name,
                    "source_dataset_id": enriched_dataset.source_dataset_id,
                })
        
        # Check for invalid source_dataset_id references
        for enriched_dataset in enriched_datasets:
            source_dataset = session.get(DatasetConfig, enriched_dataset.source_dataset_id)
            if not source_dataset:
                results["invalid_enriched_references"].append({
                    "id": enriched_dataset.id,
                    "name": enriched_dataset.name,
                    "source_dataset_id": enriched_dataset.source_dataset_id,
                })
        
        # Check Knowledge Tables
        knowledge_tables = session.query(KnowledgeTable).all()
        knowledge_table_names = {kt.table_name for kt in knowledge_tables}
        
        # Find orphaned knowledge tables (table exists but no record)
        knowledge_tables_in_db = {t for t in data_tables if t.startswith("knowledge_")}
        orphaned_knowledge_tables = knowledge_tables_in_db - knowledge_table_names
        results["orphaned_knowledge_tables"] = list(orphaned_knowledge_tables)
        
        # Check dataset tables
        datasets = session.query(DatasetConfig).all()
        dataset_table_names = {d.table_name for d in datasets}
        
        # Find orphaned dataset tables (table exists but no DatasetConfig)
        dataset_tables_in_db = {t for t in data_tables if t.startswith("dataset_")}
        orphaned_dataset_tables = dataset_tables_in_db - dataset_table_names
        results["orphaned_dataset_tables"] = list(orphaned_dataset_tables)
        
        # Calculate total issues
        results["total_issues"] = (
            len(results["orphaned_enriched_tables"])
            + len(results["orphaned_enriched_records"])
            + len(results["orphaned_knowledge_tables"])
            + len(results["invalid_enriched_references"])
            + len(results["orphaned_dataset_tables"])
        )
        
        logger.info(f"Database integrity check completed: {results['total_issues']} issues found")
        
    except Exception as e:
        logger.error(f"Failed to check database integrity: {e}", exc_info=True)
        raise DatabaseError(
            f"Failed to check database integrity: {e}",
            operation="check_database_integrity"
        ) from e
    
    return results


def cleanup_orphaned_data(session: Session, dry_run: bool = True) -> dict[str, Any]:
    """
    Clean up orphaned tables and records.
    
    Args:
        session: Database session
        dry_run: If True, only report what would be cleaned (default True)
        
    Returns:
        Dictionary with cleanup results:
        {
            "tables_dropped": [...],
            "records_deleted": [...],
            "dry_run": bool
        }
    """
    results = {
        "tables_dropped": [],
        "records_deleted": [],
        "dry_run": dry_run,
    }
    
    try:
        integrity_results = check_database_integrity(session)
        
        # Drop orphaned enriched tables
        for table_name in integrity_results["orphaned_enriched_tables"]:
            if not dry_run:
                try:
                    quoted_table = quote_identifier(table_name)
                    session.execute(text(f"DROP TABLE IF EXISTS {quoted_table}"))
                    session.commit()
                    results["tables_dropped"].append(table_name)
                    logger.info(f"Dropped orphaned enriched table: {table_name}")
                except Exception as e:
                    logger.warning(f"Failed to drop orphaned table {table_name}: {e}")
                    session.rollback()
            else:
                results["tables_dropped"].append(table_name)
        
        # Delete orphaned enriched records
        for record_info in integrity_results["orphaned_enriched_records"]:
            if not dry_run:
                try:
                    enriched_dataset = session.get(EnrichedDataset, record_info["id"])
                    if enriched_dataset:
                        session.delete(enriched_dataset)
                        session.commit()
                        results["records_deleted"].append({
                            "type": "EnrichedDataset",
                            "id": record_info["id"],
                            "name": record_info["name"],
                        })
                        logger.info(f"Deleted orphaned enriched dataset record: {record_info['name']}")
                except Exception as e:
                    logger.warning(f"Failed to delete orphaned record {record_info['id']}: {e}")
                    session.rollback()
            else:
                results["records_deleted"].append({
                    "type": "EnrichedDataset",
                    "id": record_info["id"],
                    "name": record_info["name"],
                })
        
        # Drop orphaned knowledge tables
        for table_name in integrity_results["orphaned_knowledge_tables"]:
            if not dry_run:
                try:
                    quoted_table = quote_identifier(table_name)
                    session.execute(text(f"DROP TABLE IF EXISTS {quoted_table}"))
                    session.commit()
                    results["tables_dropped"].append(table_name)
                    logger.info(f"Dropped orphaned knowledge table: {table_name}")
                except Exception as e:
                    logger.warning(f"Failed to drop orphaned table {table_name}: {e}")
                    session.rollback()
            else:
                results["tables_dropped"].append(table_name)
        
        # Drop orphaned dataset tables (be careful - these might be legitimate)
        for table_name in integrity_results["orphaned_dataset_tables"]:
            if not dry_run:
                try:
                    quoted_table = quote_identifier(table_name)
                    session.execute(text(f"DROP TABLE IF EXISTS {quoted_table}"))
                    session.commit()
                    results["tables_dropped"].append(table_name)
                    logger.warning(f"Dropped orphaned dataset table: {table_name} (verify this is correct)")
                except Exception as e:
                    logger.warning(f"Failed to drop orphaned table {table_name}: {e}")
                    session.rollback()
            else:
                results["tables_dropped"].append(table_name)
        
        logger.info(
            f"Cleanup completed (dry_run={dry_run}): "
            f"{len(results['tables_dropped'])} tables, "
            f"{len(results['records_deleted'])} records"
        )
        
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to cleanup orphaned data: {e}", exc_info=True)
        raise DatabaseError(
            f"Failed to cleanup orphaned data: {e}",
            operation="cleanup_orphaned_data"
        ) from e
    
    return results

