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
            _engine = create_engine(
                database_url,
                connect_args={
                    "check_same_thread": SQLITE_CHECK_SAME_THREAD,
                    "timeout": SQLITE_TIMEOUT,
                },
                echo=False,  # Set to True for SQL debugging
                pool_pre_ping=True,  # Verify connections before using
            )

            # Enable foreign key constraints for SQLite
            @event.listens_for(_engine, "connect")
            def set_sqlite_pragma(dbapi_conn, connection_record):
                cursor = dbapi_conn.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
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
        session.close()


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
                result = conn.execute(text(f"PRAGMA table_info({table_name})"))
                columns = {row[1]: row[0] for row in result.fetchall()}  # name: cid
                
                if "unique_id" in columns and UNIQUE_ID_COLUMN_NAME not in columns:
                    logger.info(f"Renaming unique_id to {UNIQUE_ID_COLUMN_NAME} in table {table_name}")
                    # SQLite 3.25.0+ supports ALTER TABLE RENAME COLUMN
                    conn.execute(text(f"ALTER TABLE {table_name} RENAME COLUMN unique_id TO {UNIQUE_ID_COLUMN_NAME}"))
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
        try:
            from src.database.models import EnrichedDataset
            from src.services.table_service import create_index_on_column
            
            enriched_datasets = conn.execute(
                text("SELECT id, enriched_table_name, columns_added FROM enriched_dataset")
            ).fetchall()
            
            indexed_count = 0
            for enriched_dataset_row in enriched_datasets:
                enriched_table_name = enriched_dataset_row[1]
                columns_added_json = enriched_dataset_row[2]
                
                if not columns_added_json:
                    continue
                
                # Parse columns_added (JSON array of strings)
                import json
                try:
                    columns_added = json.loads(columns_added_json) if isinstance(columns_added_json, str) else columns_added_json
                except (json.JSONDecodeError, TypeError):
                    continue
                
                # Create index for each enriched column
                for enriched_col_name in columns_added:
                    if not isinstance(enriched_col_name, str):
                        continue
                    
                    # Check if index already exists
                    safe_table = enriched_table_name.replace(".", "_").replace("-", "_")
                    safe_column = enriched_col_name.replace(".", "_").replace("-", "_")
                    index_name = f"idx_{safe_table}_{safe_column}_not_null"
                    
                    existing_index = conn.execute(
                        text("SELECT name FROM sqlite_master WHERE type='index' AND name=?"),
                        (index_name,)
                    ).fetchone()
                    
                    if not existing_index:
                        try:
                            # Create index directly using SQL (simpler than creating session)
                            conn.execute(text(
                                f"CREATE INDEX IF NOT EXISTS {index_name} "
                                f"ON {enriched_table_name}({enriched_col_name}) "
                                f"WHERE {enriched_col_name} IS NOT NULL"
                            ))
                            indexed_count += 1
                        except Exception as e:
                            logger.warning(f"Failed to create index on {enriched_table_name}.{enriched_col_name}: {e}")
                            continue
            
            if indexed_count > 0:
                conn.commit()
                logger.info(f"Migration 6: Created {indexed_count} indexes on existing enriched columns")
            else:
                logger.info("Migration 6: No new indexes needed for enriched columns")
                
        except Exception as e:
            logger.warning(f"Migration 6 (index enriched columns) failed: {e}. Continuing...")
                    
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

