"""
Database connection and session management for CSV Wrangler.

SQLAlchemy 2.0 setup with SQLite engine, connection pooling, and session management.
"""
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.config.settings import (
    SQLITE_CHECK_SAME_THREAD,
    SQLITE_TIMEOUT,
    get_database_url,
)
from src.utils.errors import DatabaseError
from src.utils.logging_config import get_logger
from src.utils.validation import quote_identifier

logger = get_logger(__name__)

# Global engine instance
_engine: Engine | None = None
_SessionLocal: sessionmaker[Session] | None = None


def get_engine() -> Engine:
    """
    Get or create SQLAlchemy engine.
    
    Returns:
        SQLAlchemy engine instance
        
    Raises:
        DatabaseError: If engine creation fails
    """
    global _engine

    if _engine is None:
        try:
            database_url = get_database_url()

            # Create engine with SQLite-specific configuration
            # Use StaticPool for Windows to avoid connection issues
            # StaticPool maintains a single connection per thread, better for Windows file locking
            from sqlalchemy.pool import StaticPool
            
            _engine = create_engine(
                database_url,
                connect_args={
                    "check_same_thread": SQLITE_CHECK_SAME_THREAD,
                    "timeout": SQLITE_TIMEOUT,
                    # Windows-specific: explicitly set isolation level for better locking
                    "isolation_level": None,  # Let SQLite handle transactions
                },
                poolclass=StaticPool,  # Single connection per thread for Windows
                pool_pre_ping=False,  # Not needed for SQLite
                echo=False,  # Set to True for SQL debugging
            )

            # Enable foreign key constraints and optimize SQLite settings
            @event.listens_for(_engine, "connect")
            def set_sqlite_pragma(dbapi_conn, connection_record):
                cursor = dbapi_conn.cursor()
                # Enable foreign key constraints
                cursor.execute("PRAGMA foreign_keys=ON")
                # Enable WAL mode for better concurrent access
                # Note: WAL mode can have issues on Windows with network-synced paths (OneDrive)
                # but it's still better than DELETE mode for concurrency
                cursor.execute("PRAGMA journal_mode=WAL")
                # Optimize synchronous mode for better performance with WAL
                # NORMAL is safe with WAL and provides good performance
                cursor.execute("PRAGMA synchronous=NORMAL")
                # Use memory for temporary tables (improves performance)
                cursor.execute("PRAGMA temp_store=MEMORY")
                # Increase page cache size for better performance (default is -2000, set to -64000 = 64MB)
                cursor.execute("PRAGMA cache_size=-64000")
                # Windows-specific: Set busy timeout to handle concurrent access better
                # This helps when multiple operations happen quickly
                cursor.execute(f"PRAGMA busy_timeout={int(SQLITE_TIMEOUT * 1000)}")  # Convert to milliseconds
                cursor.close()

            logger.info("Database engine created successfully")

        except Exception as e:
            logger.error(f"Failed to create database engine: {e}", exc_info=True)
            raise DatabaseError(f"Failed to create database engine: {e}") from e

    return _engine


def get_session_factory() -> sessionmaker[Session]:
    """
    Get or create session factory.
    
    Returns:
        Session factory instance
    """
    global _SessionLocal

    if _SessionLocal is None:
        engine = get_engine()
        _SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=engine,
            expire_on_commit=False,
        )

    return _SessionLocal


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """
    Get database session with automatic cleanup.
    
    Usage:
        with get_session() as session:
            # Use session
            pass
    
    Yields:
        Database session
    """
    session_factory = get_session_factory()
    session = session_factory()

    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Database session error: {e}", exc_info=True)
        raise
    finally:
        # Explicitly close session and ensure connection is returned to pool
        session.close()
        # Note: StaticPool will handle connection cleanup automatically
        # No need to dispose the engine here as it's shared


def migrate_database() -> None:
    """
    Migrate database schema - add missing columns and update structure.
    
    This function handles schema migrations for existing databases.
    """
    try:
        from src.config.settings import UNIQUE_ID_COLUMN_NAME
        
        engine = get_engine()
        with engine.connect() as conn:
            # Migration 1: Add logo_path column to user_profile table
            result = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='user_profile'")
            )
            if result.fetchone():
                # Check if logo_path column exists
                result = conn.execute(text("PRAGMA table_info(user_profile)"))
                columns = [row[1] for row in result.fetchall()]
                
                if "logo_path" not in columns:
                    logger.info("Adding logo_path column to user_profile table")
                    conn.execute(text("ALTER TABLE user_profile ADD COLUMN logo_path VARCHAR(500)"))
                    conn.commit()
                    logger.info("Successfully added logo_path column")
            
            # Migration 2: Rename unique_id column to uuid_value in all dataset tables
            # Get all dataset tables (dataset_* pattern) and enriched tables (enriched_* pattern)
            result = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE 'dataset_%' OR name LIKE 'enriched_%')")
            )
            tables = [row[0] for row in result.fetchall()]
            
            for table_name in tables:
                # Check if unique_id column exists in this table
                # Quote table name for PRAGMA (though PRAGMA doesn't strictly require it, it's safer)
                quoted_table = quote_identifier(table_name)
                result = conn.execute(text(f"PRAGMA table_info({quoted_table})"))
                columns = {row[1]: row[0] for row in result.fetchall()}  # name: cid
                
                if "unique_id" in columns and UNIQUE_ID_COLUMN_NAME not in columns:
                    logger.info(f"Renaming unique_id to {UNIQUE_ID_COLUMN_NAME} in table {table_name}")
                    # SQLite 3.25.0+ supports ALTER TABLE RENAME COLUMN
                    # Quote identifiers to handle any edge cases
                    quoted_unique_id = quote_identifier("unique_id")
                    quoted_uuid_value = quote_identifier(UNIQUE_ID_COLUMN_NAME)
                    conn.execute(text(f"ALTER TABLE {quoted_table} RENAME COLUMN {quoted_unique_id} TO {quoted_uuid_value}"))
                    conn.commit()
                    logger.info(f"Successfully renamed column in table {table_name}")
            
            # Migration 3: Create note table if it doesn't exist
            result = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='note'")
            )
            if not result.fetchone():
                logger.info("Creating note table")
                from src.database.models import Note
                Note.__table__.create(bind=conn, checkfirst=True)
                conn.commit()
                logger.info("Successfully created note table")
            
            # Migration 4: Create data_analysis table if it doesn't exist
            result = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='data_analysis'")
            )
            if not result.fetchone():
                logger.info("Creating data_analysis table")
                from src.database.models import DataAnalysis
                DataAnalysis.__table__.create(bind=conn, checkfirst=True)
                conn.commit()
                logger.info("Successfully created data_analysis table")
            
            # Migration 5: Create knowledge_table table if it doesn't exist
            result = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='knowledge_table'")
            )
            if not result.fetchone():
                logger.info("Creating knowledge_table table")
                from src.database.models import KnowledgeTable
                KnowledgeTable.__table__.create(bind=conn, checkfirst=True)
                
                # Create indexes for performance
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_knowledge_table_data_type ON knowledge_table(data_type)"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_knowledge_table_table_name ON knowledge_table(table_name)"))
                
                conn.commit()
                logger.info("Successfully created knowledge_table table with indexes")
            
            # Migration 6: Index existing enriched columns for search performance
            # This must be inside the connection context to ensure proper transaction handling
            try:
                from src.database.models import EnrichedDataset
                
                # Check if enriched_dataset table exists before querying
                result = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name='enriched_dataset'")
                )
                if not result.fetchone():
                    logger.info("Migration 6: enriched_dataset table does not exist, skipping index creation")
                else:
                    enriched_datasets = conn.execute(
                        text("SELECT id, enriched_table_name, columns_added FROM enriched_dataset")
                    ).fetchall()
                    
                    indexed_count = 0
                    failed_count = 0
                    for enriched_dataset_row in enriched_datasets:
                        enriched_id = enriched_dataset_row[0]
                        enriched_table_name = enriched_dataset_row[1]
                        columns_added_json = enriched_dataset_row[2]
                        
                        if not columns_added_json:
                            logger.debug(f"Migration 6: EnrichedDataset ID {enriched_id} has no columns_added, skipping")
                            continue
                        
                        # Parse columns_added (JSON array of strings)
                        import json
                        try:
                            columns_added = json.loads(columns_added_json) if isinstance(columns_added_json, str) else columns_added_json
                        except (json.JSONDecodeError, TypeError) as e:
                            logger.warning(f"Migration 6: Failed to parse columns_added JSON for EnrichedDataset ID {enriched_id}: {e}")
                            continue
                        
                        if not isinstance(columns_added, list):
                            logger.warning(f"Migration 6: columns_added is not a list for EnrichedDataset ID {enriched_id}, skipping")
                            continue
                        
                        # Create index for each enriched column
                        for enriched_col_name in columns_added:
                            if not isinstance(enriched_col_name, str):
                                logger.warning(
                                    f"Migration 6: Invalid column name type for EnrichedDataset ID {enriched_id}, "
                                    f"column: {enriched_col_name}, type: {type(enriched_col_name)}"
                                )
                                continue
                            
                            # Check if index already exists
                            safe_table = enriched_table_name.replace(".", "_").replace("-", "_")
                            safe_column = enriched_col_name.replace(".", "_").replace("-", "_")
                            index_name = f"idx_{safe_table}_{safe_column}_not_null"
                            
                            try:
                                existing_index = conn.execute(
                                    text("SELECT name FROM sqlite_master WHERE type='index' AND name=?"),
                                    (index_name,)
                                ).fetchone()
                                
                                if not existing_index:
                                    # Create index directly using SQL
                                    # Quote identifiers to handle spaces/special characters
                                    quoted_table = quote_identifier(enriched_table_name)
                                    quoted_col = quote_identifier(enriched_col_name)
                                    conn.execute(text(
                                        f"CREATE INDEX IF NOT EXISTS {index_name} "
                                        f"ON {quoted_table}({quoted_col}) "
                                        f"WHERE {quoted_col} IS NOT NULL"
                                    ))
                                    indexed_count += 1
                                    logger.debug(
                                        f"Migration 6: Created index {index_name} on {enriched_table_name}.{enriched_col_name}"
                                    )
                                else:
                                    logger.debug(
                                        f"Migration 6: Index {index_name} already exists for {enriched_table_name}.{enriched_col_name}"
                                    )
                            except Exception as e:
                                failed_count += 1
                                logger.warning(
                                    f"Migration 6: Failed to create index on {enriched_table_name}.{enriched_col_name} "
                                    f"for EnrichedDataset ID {enriched_id}: {e}"
                                )
                                continue
                    
                    if indexed_count > 0:
                        conn.commit()
                        logger.info(
                            f"Migration 6: Created {indexed_count} indexes on existing enriched columns "
                            f"({failed_count} failures)"
                        )
                    elif failed_count > 0:
                        logger.warning(f"Migration 6: Failed to create {failed_count} indexes, no indexes created")
                    else:
                        logger.info("Migration 6: No new indexes needed for enriched columns")
                        
            except Exception as e:
                logger.error(f"Migration 6 (index enriched columns) failed: {e}", exc_info=True)
                # Don't raise - allow migration to continue even if this fails
                # The indexes are for performance, not critical for functionality
            
            # Migration 7: Add theme_mode and wide_mode columns to user_profile table
            result = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='user_profile'")
            )
            if result.fetchone():
                # Check existing columns
                result = conn.execute(text("PRAGMA table_info(user_profile)"))
                columns = [row[1] for row in result.fetchall()]
                
                if "theme_mode" not in columns:
                    logger.info("Migration 7: Adding theme_mode column to user_profile table")
                    conn.execute(text("ALTER TABLE user_profile ADD COLUMN theme_mode VARCHAR(20) DEFAULT 'dark'"))
                    conn.commit()
                    logger.info("Migration 7: Successfully added theme_mode column")
                
                if "wide_mode" not in columns:
                    logger.info("Migration 7: Adding wide_mode column to user_profile table")
                    conn.execute(text("ALTER TABLE user_profile ADD COLUMN wide_mode BOOLEAN DEFAULT 1"))
                    conn.commit()
                    logger.info("Migration 7: Successfully added wide_mode column")
            
            # Migration 8: Add indexes for enriched_dataset table
            try:
                result = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name='enriched_dataset'")
                )
                if result.fetchone():
                    # Check if indexes already exist
                    result = conn.execute(
                        text("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_enriched_dataset_source_dataset_id'")
                    )
                    if not result.fetchone():
                        logger.info("Migration 8: Creating index on enriched_dataset.source_dataset_id")
                        conn.execute(
                            text("CREATE INDEX IF NOT EXISTS idx_enriched_dataset_source_dataset_id ON enriched_dataset(source_dataset_id)")
                        )
                        conn.commit()
                        logger.info("Migration 8: Created index idx_enriched_dataset_source_dataset_id")
                    else:
                        logger.debug("Migration 8: Index idx_enriched_dataset_source_dataset_id already exists")
                    
                    # Note: enriched_table_name already has a unique constraint which acts as an index
                    # but we can still create an explicit index if needed for clarity
                    # For now, we skip it since unique constraints create indexes automatically
                    
                    conn.commit()
                    logger.info("Migration 8: Added indexes for enriched_dataset table")
                else:
                    logger.debug("Migration 8: enriched_dataset table does not exist, skipping index creation")
            except Exception as e:
                logger.warning(f"Migration 8 (add enriched_dataset indexes) failed: {e}. Continuing...")
                    
    except Exception as e:
        logger.error(f"Failed to migrate database: {e}", exc_info=True)
        raise DatabaseError(f"Failed to migrate database: {e}") from e


def init_database() -> None:
    """
    Initialize database - create all tables and run migrations.
    
    Raises:
        DatabaseError: If initialization fails
    """
    try:
        from src.database.models import Base

        engine = get_engine()
        Base.metadata.create_all(bind=engine)
        
        # Run migrations for existing databases
        migrate_database()
        
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}", exc_info=True)
        raise DatabaseError(f"Failed to initialize database: {e}") from e


def check_schema_version() -> int:
    """
    Check database schema version.
    
    Returns:
        Schema version number (0 if not set)
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
            )
            if result.fetchone():
                result = conn.execute("SELECT version FROM schema_version LIMIT 1")
                row = result.fetchone()
                return row[0] if row else 0
            return 0
    except Exception as e:
        logger.warning(f"Failed to check schema version: {e}")
        return 0

