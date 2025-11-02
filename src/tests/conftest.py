"""
Pytest configuration and fixtures for CSV Wrangler tests.
"""
import tempfile
from pathlib import Path
from typing import Generator

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from src.database.connection import get_session
from src.database.models import Base


@pytest.fixture
def temp_db_path() -> Generator[Path, None, None]:
    """Create a temporary database file."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = Path(tmp.name)
        yield db_path
        # Cleanup - try to remove, ignore errors on Windows
        try:
            if db_path.exists():
                db_path.unlink(missing_ok=True)
        except (PermissionError, OSError):
            # Windows file locking - ignore cleanup errors
            pass


@pytest.fixture
def test_engine(temp_db_path: Path):
    """Create a test database engine."""
    database_url = f"sqlite:///{temp_db_path}"
    engine = create_engine(
        database_url,
        connect_args={"check_same_thread": False},
        echo=False,
        pool_pre_ping=True,
    )
    
    # Enable foreign key constraints
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()
    
    # Create tables
    Base.metadata.create_all(bind=engine)
    
    yield engine
    
    # Cleanup - properly close all connections
    Base.metadata.drop_all(bind=engine)
    engine.dispose()
    
    # Wait a bit for Windows to release file lock
    import time
    time.sleep(0.1)


@pytest.fixture
def test_session(test_engine) -> Generator[Session, None, None]:
    """Create a test database session."""
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)
    session = SessionLocal()
    
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@pytest.fixture
def sample_csv_content() -> str:
    """Sample CSV content for testing."""
    return """name,age,email,image_data
John Doe,30,john@example.com,data:image/png;base64,iVBORw0KGgoAAAANS
Jane Smith,25,jane@example.com,data:image/jpeg;base64,/9j/4AAQSkZJRg
Bob Johnson,35,bob@example.com,no_image_here
"""


@pytest.fixture
def sample_csv_file(tmp_path: Path, sample_csv_content: str) -> Path:
    """Create a temporary CSV file."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(sample_csv_content, encoding="utf-8")
    return csv_file

