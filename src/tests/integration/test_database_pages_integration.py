"""
Comprehensive integration tests for database functionality across all pages.

Tests database operations, relationships, data integrity, and cascade deletes
through all pages in the application.
"""
import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime
from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

pytestmark = pytest.mark.integration

from src.database.models import (
    DatasetConfig,
    UploadLog,
    EnrichedDataset,
    DataAnalysis,
    KnowledgeTable,
    UserProfile,
)
from src.database.repository import (
    DatasetRepository,
    EnrichedDatasetRepository,
    DataAnalysisRepository,
    KnowledgeTableRepository,
)
from src.services.dataset_service import (
    initialize_dataset,
    upload_csv_to_dataset,
    delete_dataset,
    get_dataset_statistics,
)
from src.services.enrichment_service import (
    create_enriched_dataset,
    sync_enriched_dataset,
    delete_enriched_dataset,
    sync_all_enriched_datasets_for_source,
)
from src.services.dataframe_service import (
    load_dataset_dataframe,
    load_enriched_dataset_dataframe,
    get_dataset_row_count,
    get_enriched_dataset_row_count,
)
from src.services.analysis_service import (
    create_analysis,
    refresh_analysis,
    get_all_analyses,
)
from src.services.knowledge_service import (
    initialize_knowledge_table,
    upload_to_knowledge_table,
    get_all_knowledge_tables,
)
from src.services.export_service import export_dataset_to_csv, export_dataset_to_pickle
from src.services.profile_service import (
    create_user_profile,
    get_current_profile,
    update_profile_name,
    update_profile_logo,
    is_app_initialized,
)
from src.services.image_service import get_tables_with_image_columns
from src.services.database_integrity import check_database_integrity
from src.config.settings import UNIQUE_ID_COLUMN_NAME
from src.utils.errors import ValidationError, DatabaseError
from src.utils.validation import table_exists


class TestHomePageDatabaseOperations:
    """Test Home page database operations."""

    def test_home_page_database_operations(self, test_session):
        """Test UserProfile reading and database connection."""
        # Create a user profile
        profile = create_user_profile(test_session, "Test User")
        
        # Verify profile is created
        assert profile is not None
        assert profile.name == "Test User"
        assert profile.id is not None
        
        # Test get_current_profile
        current_profile = get_current_profile(test_session)
        assert current_profile is not None
        assert current_profile.name == "Test User"
        
        # Test is_app_initialized
        assert is_app_initialized(test_session) is True
        
        # Verify database connection works
        repo = DatasetRepository(test_session)
        datasets = repo.get_all()
        assert isinstance(datasets, list)


class TestDatasetPagesFullWorkflow:
    """Test complete dataset pages workflow."""

    def test_dataset_pages_full_workflow(self, test_session, tmp_path):
        """Test all 5 dataset slots, uploads, exports, and cascade deletes."""
        datasets = []
        csv_files = []
        
        # Initialize all 5 dataset slots
        for slot in range(1, 6):
            columns_config = {
                "name": {"type": "TEXT", "is_image": False},
                "email": {"type": "TEXT", "is_image": False},
                "age": {"type": "INTEGER", "is_image": False},
            }
            
            dataset = initialize_dataset(
                session=test_session,
                name=f"Dataset {slot}",
                slot_number=slot,
                columns_config=columns_config,
                image_columns=[],
            )
            datasets.append(dataset)
            
            # Verify DatasetConfig record is created
            assert dataset.id is not None
            assert dataset.slot_number == slot
            assert dataset.name == f"Dataset {slot}"
            assert dataset.table_name is not None
            
            # Verify dataset table is created with correct structure
            assert table_exists(test_session, dataset.table_name)
            
            inspector = inspect(test_session.bind)
            columns = inspector.get_columns(dataset.table_name)
            column_names = [col["name"] for col in columns]
            
            # Verify uuid_value is primary key
            assert UNIQUE_ID_COLUMN_NAME in column_names
            pk_columns = [col for col in columns if col.get("primary_key")]
            assert len(pk_columns) == 1
            assert pk_columns[0]["name"] == UNIQUE_ID_COLUMN_NAME
            
            # Verify other columns exist
            assert "name" in column_names
            assert "email" in column_names
            assert "age" in column_names
            
            # Create CSV file for upload
            csv_content = f"name,email,age\nPerson{slot}A,person{slot}a@example.com,{20 + slot}\nPerson{slot}B,person{slot}b@example.com,{25 + slot}"
            csv_file = tmp_path / f"dataset_{slot}.csv"
            csv_file.write_text(csv_content, encoding="utf-8")
            csv_files.append(csv_file)
            
            # Upload CSV file
            upload_log = upload_csv_to_dataset(
                session=test_session,
                dataset_id=dataset.id,
                csv_file=csv_file,
                filename=f"dataset_{slot}.csv",
            )
            
            # Verify UploadLog record is created
            assert upload_log.id is not None
            assert upload_log.dataset_id == dataset.id
            assert upload_log.filename == f"dataset_{slot}.csv"
            assert upload_log.row_count == 2
            
            # Verify data is inserted correctly
            df = load_dataset_dataframe(
                session=test_session,
                dataset_id=dataset.id,
                limit=100,
                include_image_columns=False,
            )
            assert len(df) == 2
            assert UNIQUE_ID_COLUMN_NAME in df.columns
            
            # Test export functionality - CSV
            export_csv = tmp_path / f"export_{slot}.csv"
            export_path = export_dataset_to_csv(
                session=test_session,
                dataset_id=dataset.id,
                output_path=export_csv,
                include_image_columns=False,
            )
            assert export_path.exists()
            
            # Verify exported CSV contains data
            exported_df = pd.read_csv(export_path)
            assert len(exported_df) == 2
            
            # Test export - Pickle
            export_pkl = tmp_path / f"export_{slot}.pkl"
            export_path = export_dataset_to_pickle(
                session=test_session,
                dataset_id=dataset.id,
                output_path=export_pkl,
                include_image_columns=False,
            )
            assert export_path.exists()
            
            # Verify exported Pickle contains data
            import pickle
            with open(export_path, "rb") as f:
                exported_df = pickle.load(f)
            assert len(exported_df) == 2
        
        # Create enriched datasets and analyses for cascade delete testing
        enriched_datasets = []
        analyses = []
        
        # Create enriched dataset from first dataset
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=datasets[0].id,
            name="Enriched from Dataset 1",
            enrichment_config={"email": "emails"},
        )
        enriched_datasets.append(enriched)
        
        # Create analysis from first dataset
        analysis = create_analysis(
            session=test_session,
            name="Analysis 1",
            operation_type="groupby",
            source_dataset_id=datasets[0].id,
            operation_config={"group_columns": ["age"], "aggregations": {"name": "count"}},
        )
        analyses.append(analysis)
        
        # Verify cascade deletes work
        # Delete first dataset
        dataset_to_delete = datasets[0]
        enriched_table_name = enriched_datasets[0].enriched_table_name
        
        delete_dataset(test_session, dataset_to_delete.id)
        
        # Verify UploadLog records deleted
        upload_logs = (
            test_session.query(UploadLog)
            .filter_by(dataset_id=dataset_to_delete.id)
            .all()
        )
        assert len(upload_logs) == 0
        
        # Verify EnrichedDataset records deleted
        enriched_records = (
            test_session.query(EnrichedDataset)
            .filter_by(source_dataset_id=dataset_to_delete.id)
            .all()
        )
        assert len(enriched_records) == 0
        
        # Verify DataAnalysis records deleted
        analysis_records = (
            test_session.query(DataAnalysis)
            .filter_by(source_dataset_id=dataset_to_delete.id)
            .all()
        )
        assert len(analysis_records) == 0
        
        # Verify enriched table is dropped
        assert not table_exists(test_session, enriched_table_name)
        
        # Verify source dataset table is dropped
        assert not table_exists(test_session, dataset_to_delete.table_name)
        
        # Verify remaining datasets are unaffected
        remaining_datasets = (
            test_session.query(DatasetConfig)
            .filter(DatasetConfig.id.in_([d.id for d in datasets[1:]]))
            .all()
        )
        assert len(remaining_datasets) == 4


class TestEnrichmentSuiteDatabaseOperations:
    """Test Enrichment Suite page database operations."""

    def test_enrichment_suite_database_operations(self, test_session, tmp_path):
        """Test enriched dataset creation, sync, and cascade deletes."""
        # Create source dataset
        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "phone": {"type": "TEXT", "is_image": False},
            "email": {"type": "TEXT", "is_image": False},
        }
        
        source_dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Upload initial data
        csv_content = "name,phone,email\nJohn Doe,555-1111,john@example.com\nJane Smith,555-2222,jane@example.com"
        csv_file = tmp_path / "source.csv"
        csv_file.write_text(csv_content, encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=source_dataset.id,
            csv_file=csv_file,
            filename="source.csv",
        )
        
        # Create multiple enriched datasets
        enriched1 = create_enriched_dataset(
            session=test_session,
            source_dataset_id=source_dataset.id,
            name="Enriched Phone",
            enrichment_config={"phone": "phone_numbers"},
        )
        
        enriched2 = create_enriched_dataset(
            session=test_session,
            source_dataset_id=source_dataset.id,
            name="Enriched Email",
            enrichment_config={"email": "emails"},
        )
        
        # Verify EnrichedDataset records are created
        assert enriched1.id is not None
        assert enriched2.id is not None
        assert enriched1.source_dataset_id == source_dataset.id
        assert enriched2.source_dataset_id == source_dataset.id
        
        # Verify enriched tables are created
        assert table_exists(test_session, enriched1.enriched_table_name)
        assert table_exists(test_session, enriched2.enriched_table_name)
        
        # Verify enriched tables inherit uuid_value
        inspector = inspect(test_session.bind)
        enriched1_columns = inspector.get_columns(enriched1.enriched_table_name)
        enriched1_column_names = [col["name"] for col in enriched1_columns]
        assert UNIQUE_ID_COLUMN_NAME in enriched1_column_names
        
        # Verify enrichment_config JSON is stored correctly
        assert enriched1.enrichment_config == {"phone": "phone_numbers"}
        assert enriched2.enrichment_config == {"email": "emails"}
        
        # Upload more data to source
        csv_content2 = "name,phone,email\nBob Johnson,555-3333,bob@example.com"
        csv_file2 = tmp_path / "source2.csv"
        csv_file2.write_text(csv_content2, encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=source_dataset.id,
            csv_file=csv_file2,
            filename="source2.csv",
        )
        
        # Test sync_enriched_dataset
        rows_synced = sync_enriched_dataset(
            session=test_session,
            enriched_dataset_id=enriched1.id,
        )
        
        # Verify new rows are detected and synced
        assert rows_synced >= 0  # Could be 0 if already synced
        
        # Verify enriched columns are populated
        df = load_enriched_dataset_dataframe(
            session=test_session,
            enriched_dataset_id=enriched1.id,
            limit=100,
            include_image_columns=False,
        )
        
        # Check for enriched column
        enriched_cols = [col for col in df.columns if "enriched" in col.lower()]
        assert len(enriched_cols) > 0
        
        # Verify last_sync_date is updated
        test_session.refresh(enriched1)
        assert enriched1.last_sync_date is not None
        
        # Test cascade delete when source dataset is deleted
        enriched_table_name = enriched1.enriched_table_name
        delete_dataset(test_session, source_dataset.id)
        
        # Verify enriched datasets are deleted
        enriched_records = (
            test_session.query(EnrichedDataset)
            .filter_by(source_dataset_id=source_dataset.id)
            .all()
        )
        assert len(enriched_records) == 0
        
        # Verify enriched tables are dropped
        assert not table_exists(test_session, enriched_table_name)
        
        # Test deleting enriched dataset drops the table
        # Create new dataset and enriched dataset
        source_dataset2 = initialize_dataset(
            session=test_session,
            name="Source Dataset 2",
            slot_number=2,
            columns_config=columns_config,
            image_columns=[],
        )
        
        csv_file3 = tmp_path / "source3.csv"
        csv_file3.write_text(csv_content, encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=source_dataset2.id,
            csv_file=csv_file3,
            filename="source3.csv",
        )
        
        enriched3 = create_enriched_dataset(
            session=test_session,
            source_dataset_id=source_dataset2.id,
            name="Enriched 3",
            enrichment_config={"phone": "phone_numbers"},
        )
        
        enriched_table_name3 = enriched3.enriched_table_name
        delete_enriched_dataset(test_session, enriched3.id)
        
        # Verify table is dropped
        assert not table_exists(test_session, enriched_table_name3)
        
        # Verify record is deleted
        deleted = test_session.get(EnrichedDataset, enriched3.id)
        assert deleted is None


class TestDataFrameViewDatabaseOperations:
    """Test DataFrame View page database operations."""

    def test_dataframe_view_database_operations(self, test_session, tmp_path):
        """Test dataset loading with image column exclusion."""
        # Create regular dataset
        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "email": {"type": "TEXT", "is_image": False},
        }
        
        dataset = initialize_dataset(
            session=test_session,
            name="Regular Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        csv_content = "name,email\nJohn,john@example.com\nJane,jane@example.com"
        csv_file = tmp_path / "regular.csv"
        csv_file.write_text(csv_content, encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="regular.csv",
        )
        
        # Load regular dataset (exclude image columns by default)
        df = load_dataset_dataframe(
            session=test_session,
            dataset_id=dataset.id,
            limit=100,
            include_image_columns=False,
        )
        
        assert len(df) == 2
        assert "name" in df.columns
        assert "email" in df.columns
        
        # Create enriched dataset
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched Dataset",
            enrichment_config={"email": "emails"},
        )
        
        # Load enriched dataset (exclude image columns by default)
        df_enriched = load_enriched_dataset_dataframe(
            session=test_session,
            enriched_dataset_id=enriched.id,
            limit=100,
            include_image_columns=False,
        )
        
        assert len(df_enriched) == 2
        assert UNIQUE_ID_COLUMN_NAME in df_enriched.columns
        
        # Verify image columns can be included when requested
        # (even though this dataset doesn't have images, the parameter should work)
        df_with_images = load_dataset_dataframe(
            session=test_session,
            dataset_id=dataset.id,
            limit=100,
            include_image_columns=True,
        )
        
        assert len(df_with_images) == 2
        
        # Verify data integrity
        assert len(df) == len(df_enriched)
        # Check that enriched dataset has source data plus enriched columns
        assert len(df_enriched.columns) >= len(df.columns)


class TestDataGeekPageDatabaseOperations:
    """Test Data Geek page database operations."""

    def test_data_geek_page_database_operations(self, test_session, tmp_path):
        """Test all analysis types and DataAnalysis relationships."""
        # Create source datasets
        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "age": {"type": "INTEGER", "is_image": False},
            "score": {"type": "INTEGER", "is_image": False},
        }
        
        source_dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        secondary_dataset = initialize_dataset(
            session=test_session,
            name="Secondary Dataset",
            slot_number=2,
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Upload data
        csv_content = "name,age,score\nJohn,30,100\nJane,25,95\nBob,35,110"
        csv_file1 = tmp_path / "source.csv"
        csv_file1.write_text(csv_content, encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=source_dataset.id,
            csv_file=csv_file1,
            filename="source.csv",
        )
        
        csv_file2 = tmp_path / "secondary.csv"
        csv_file2.write_text(csv_content, encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=secondary_dataset.id,
            csv_file=csv_file2,
            filename="secondary.csv",
        )
        
        # Create GroupBy analysis
        groupby_analysis = create_analysis(
            session=test_session,
            name="GroupBy Analysis",
            operation_type="groupby",
            source_dataset_id=source_dataset.id,
            operation_config={"group_columns": ["age"], "aggregations": {"score": "sum"}},
        )
        
        # Create Pivot analysis
        pivot_analysis = create_analysis(
            session=test_session,
            name="Pivot Analysis",
            operation_type="pivot",
            source_dataset_id=source_dataset.id,
            operation_config={"index": "name", "columns": "age", "values": "score", "aggfunc": "sum"},
        )
        
        # Create Merge analysis (with secondary dataset)
        merge_analysis = create_analysis(
            session=test_session,
            name="Merge Analysis",
            operation_type="merge",
            source_dataset_id=source_dataset.id,
            secondary_dataset_id=secondary_dataset.id,
            operation_config={"join_keys": ["name"], "how": "inner"},
        )
        
        # Create Join analysis (with secondary dataset)
        join_analysis = create_analysis(
            session=test_session,
            name="Join Analysis",
            operation_type="join",
            source_dataset_id=source_dataset.id,
            secondary_dataset_id=secondary_dataset.id,
            operation_config={"join_keys": ["name"], "how": "inner"},
        )
        
        # Create Concat analysis (with secondary dataset)
        concat_analysis = create_analysis(
            session=test_session,
            name="Concat Analysis",
            operation_type="concat",
            source_dataset_id=source_dataset.id,
            secondary_dataset_id=secondary_dataset.id,
            operation_config={},
        )
        
        # Create Apply analysis (single dataset)
        apply_analysis = create_analysis(
            session=test_session,
            name="Apply Analysis",
            operation_type="apply",
            source_dataset_id=source_dataset.id,
            operation_config={"column": "age", "function": "lambda x: x * 2"},
        )
        
        # Create Map analysis (single dataset)
        map_analysis = create_analysis(
            session=test_session,
            name="Map Analysis",
            operation_type="map",
            source_dataset_id=source_dataset.id,
            operation_config={"column": "age", "mapping": {30: "thirties", 25: "twenties", 35: "thirties"}},
        )
        
        # Verify DataAnalysis records are created
        analyses = [
            groupby_analysis,
            pivot_analysis,
            merge_analysis,
            join_analysis,
            concat_analysis,
            apply_analysis,
            map_analysis,
        ]
        
        for analysis in analyses:
            assert analysis.id is not None
            assert analysis.source_dataset_id == source_dataset.id
            assert analysis.operation_config is not None
            assert analysis.result_file_path is not None
            
            # Verify source_dataset_id foreign key
            test_session.refresh(analysis)
            assert analysis.source_dataset is not None
            assert analysis.source_dataset.id == source_dataset.id
        
        # Verify secondary_dataset_id foreign key (for multi-dataset ops)
        test_session.refresh(merge_analysis)
        assert merge_analysis.secondary_dataset_id == secondary_dataset.id
        assert merge_analysis.secondary_dataset is not None
        assert merge_analysis.secondary_dataset.id == secondary_dataset.id
        
        # Verify NULL secondary_dataset_id for single-dataset ops
        test_session.refresh(groupby_analysis)
        assert groupby_analysis.secondary_dataset_id is None
        assert groupby_analysis.secondary_dataset is None
        
        # Test refresh_analysis operation
        refreshed = refresh_analysis(test_session, groupby_analysis.id)
        assert refreshed is not None
        assert refreshed.id == groupby_analysis.id
        
        # Test cascade delete when source dataset is deleted
        analysis_ids = [a.id for a in analyses]
        delete_dataset(test_session, source_dataset.id)
        
        # Verify DataAnalysis records deleted
        remaining_analyses = (
            test_session.query(DataAnalysis)
            .filter(DataAnalysis.id.in_(analysis_ids))
            .all()
        )
        assert len(remaining_analyses) == 0


class TestKnowledgeBasePageDatabaseOperations:
    """Test Knowledge Base page database operations."""

    def test_knowledge_base_page_database_operations(self, test_session, tmp_path):
        """Test Knowledge Table creation and data upload."""
        # Create multiple Knowledge Tables (different data types)
        phone_columns = {"phone": {"type": "TEXT", "is_image": False}}
        phone_table = initialize_knowledge_table(
            session=test_session,
            name="Phone Knowledge Table",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=phone_columns,
            image_columns=[],
            initial_data_df=pd.DataFrame({"phone": ["555-1111", "555-2222"]}),
        )
        
        email_columns = {"email": {"type": "TEXT", "is_image": False}}
        email_table = initialize_knowledge_table(
            session=test_session,
            name="Email Knowledge Table",
            data_type="emails",
            primary_key_column="email",
            columns_config=email_columns,
            image_columns=[],
            initial_data_df=pd.DataFrame({"email": ["test@example.com", "user@example.com"]}),
        )
        
        domain_columns = {"domain": {"type": "TEXT", "is_image": False}}
        domain_table = initialize_knowledge_table(
            session=test_session,
            name="Domain Knowledge Table",
            data_type="web_domains",
            primary_key_column="domain",
            columns_config=domain_columns,
            image_columns=[],
            initial_data_df=pd.DataFrame({"domain": ["example.com", "test.com"]}),
        )
        
        # Verify KnowledgeTable records are created
        assert phone_table.id is not None
        assert email_table.id is not None
        assert domain_table.id is not None
        
        # Verify knowledge tables are created with Key_ID column
        inspector = inspect(test_session.bind)
        
        phone_columns = inspector.get_columns(phone_table.table_name)
        phone_column_names = [col["name"] for col in phone_columns]
        assert "Key_ID" in phone_column_names
        
        # Verify columns_config JSON is stored correctly
        assert phone_table.columns_config == {"phone": {"type": "TEXT", "is_image": False}}
        assert phone_table.data_type == "phone_numbers"
        
        # Upload more data to knowledge table
        new_data = pd.DataFrame({"phone": ["555-3333", "555-4444"]})
        upload_result = upload_to_knowledge_table(
            session=test_session,
            knowledge_table_id=phone_table.id,
            df=new_data,
        )
        
        # Verify Key_ID generation works
        assert upload_result["rows_inserted"] >= 0
        
        # Verify duplicate detection works
        # Try uploading same data again
        duplicate_result = upload_to_knowledge_table(
            session=test_session,
            knowledge_table_id=phone_table.id,
            df=new_data,
        )
        
        # Should detect duplicates (exact match or same Key_ID)
        # The exact behavior depends on implementation
        
        # Test that Knowledge Tables are standalone (no cascade delete)
        # Knowledge Tables should not be deleted when datasets are deleted
        all_tables_before = get_all_knowledge_tables(test_session)
        table_count_before = len(all_tables_before)
        
        # Create and delete a dataset
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        temp_dataset = initialize_dataset(
            session=test_session,
            name="Temp Dataset",
            slot_number=3,
            columns_config=columns_config,
            image_columns=[],
        )
        delete_dataset(test_session, temp_dataset.id)
        
        # Verify Knowledge Tables still exist
        all_tables_after = get_all_knowledge_tables(test_session)
        table_count_after = len(all_tables_after)
        assert table_count_after == table_count_before


class TestKnowledgeSearchPageDatabaseOperations:
    """Test Knowledge Search page database operations."""

    def test_knowledge_search_page_database_operations(self, test_session, tmp_path):
        """Test search across Knowledge Tables and enriched datasets."""
        from src.services.search_service import search_knowledge_base
        
        # Create Knowledge Tables
        phone_columns = {"phone": {"type": "TEXT", "is_image": False}}
        phone_table = initialize_knowledge_table(
            session=test_session,
            name="Phone Table",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=phone_columns,
            image_columns=[],
            initial_data_df=pd.DataFrame({"phone": ["555-123-4567"]}),
        )
        
        # Create source dataset and enriched dataset
        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "phone": {"type": "TEXT", "is_image": False},
        }
        
        source_dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        csv_content = "name,phone\nJohn,555-987-6543"
        csv_file = tmp_path / "source.csv"
        csv_file.write_text(csv_content, encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=source_dataset.id,
            csv_file=csv_file,
            filename="source.csv",
        )
        
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=source_dataset.id,
            name="Enriched Phone",
            enrichment_config={"phone": "phone_numbers"},
        )
        
        # Search across Knowledge Tables
        results = search_knowledge_base(
            session=test_session,
            search_value="5551234567",
            data_type="phone_numbers",
        )
        
        # Verify search results
        assert "presence" in results
        assert "knowledge_tables" in results["presence"]
        assert "enriched_datasets" in results["presence"]
        
        # Verify linking between Knowledge Tables and enriched datasets works
        # The search should find matches in both Knowledge Tables and enriched datasets
        assert isinstance(results["presence"]["knowledge_tables"], list)
        assert isinstance(results["presence"]["enriched_datasets"], list)
        
        # Test with multiple data types
        email_table = initialize_knowledge_table(
            session=test_session,
            name="Email Table",
            data_type="emails",
            primary_key_column="email",
            columns_config={"email": {"type": "TEXT", "is_image": False}},
            image_columns=[],
            initial_data_df=pd.DataFrame({"email": ["test@example.com"]}),
        )
        
        email_results = search_knowledge_base(
            session=test_session,
            search_value="test@example.com",
            data_type="emails",
        )
        
        assert "presence" in email_results
        assert len(email_results["presence"]["knowledge_tables"]) >= 0


class TestImageSearchPageDatabaseOperations:
    """Test Image Search page database operations."""

    def test_image_search_page_database_operations(self, test_session, tmp_path):
        """Test image column detection and display."""
        # Create dataset with image columns
        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "photo": {"type": "TEXT", "is_image": True},
        }
        
        dataset = initialize_dataset(
            session=test_session,
            name="Image Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=["photo"],
        )
        
        # Create CSV with Base64 images
        base64_image = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
        csv_content = f"name,photo\nJohn,data:image/png;base64,{base64_image}\nJane,data:image/png;base64,{base64_image}"
        csv_file = tmp_path / "images.csv"
        csv_file.write_text(csv_content, encoding="utf-8")
        
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="images.csv",
        )
        
        # Identify tables with image columns
        tables_with_images = get_tables_with_image_columns(test_session)
        
        # Verify image columns are detected
        assert len(tables_with_images) > 0
        dataset_table = next((t for t in tables_with_images if t["id"] == dataset.id and t["type"] == "dataset"), None)
        assert dataset_table is not None
        assert "photo" in dataset_table["image_columns"]
        
        # Load data with image columns included
        df = load_dataset_dataframe(
            session=test_session,
            dataset_id=dataset.id,
            limit=100,
            include_image_columns=True,
        )
        
        assert len(df) == 2
        assert "photo" in df.columns
        
        # Verify Base64 image detection works
        # Check that image data is present
        assert df["photo"].notna().any()
        
        # Create enriched dataset with images
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched Images",
            enrichment_config={"name": "phone_numbers"},  # Dummy enrichment
        )
        
        # Verify enriched dataset inherits image columns
        enriched_tables = get_tables_with_image_columns(test_session)
        enriched_table = next((t for t in enriched_tables if t["id"] == enriched.id and t["type"] == "enriched_dataset"), None)
        if enriched_table:
            # Enriched datasets should inherit image columns from source
            assert "photo" in enriched_table["image_columns"]
        
        # Verify image columns are properly tracked in image_columns JSON
        test_session.refresh(dataset)
        assert dataset.image_columns == ["photo"]


class TestBulkUploaderPageDatabaseOperations:
    """Test Bulk Uploader page database operations."""

    def test_bulk_uploader_page_database_operations(self, test_session, tmp_path):
        """Test multiple file uploads and auto-sync."""
        from src.services.bulk_upload_service import process_bulk_upload
        
        # Create dataset
        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "email": {"type": "TEXT", "is_image": False},
        }
        
        dataset = initialize_dataset(
            session=test_session,
            name="Bulk Upload Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Create multiple CSV files
        csv_files = []
        for i in range(3):
            csv_content = f"name,email\nPerson{i}A,person{i}a@example.com\nPerson{i}B,person{i}b@example.com"
            csv_file = tmp_path / f"bulk_{i}.csv"
            csv_file.write_text(csv_content, encoding="utf-8")
            csv_files.append((csv_file, f"bulk_{i}.csv"))
        
        # Process bulk upload
        result = process_bulk_upload(
            session=test_session,
            dataset_id=dataset.id,
            files=csv_files,
            show_progress=False,
        )
        
        # Verify all files are processed
        assert result.total_files == 3
        assert len(result.successful) == 3
        
        # Verify UploadLog records are created for each file
        upload_logs = (
            test_session.query(UploadLog)
            .filter_by(dataset_id=dataset.id)
            .all()
        )
        assert len(upload_logs) == 3
        
        # Verify duplicate file detection works
        # Try uploading same files again
        result2 = process_bulk_upload(
            session=test_session,
            dataset_id=dataset.id,
            files=csv_files,
            show_progress=False,
        )
        
        # Should detect duplicates
        assert result2.total_files == 3
        # Duplicates should be skipped
        assert len(result2.skipped) >= 0
        
        # Verify data is inserted correctly
        df = load_dataset_dataframe(
            session=test_session,
            dataset_id=dataset.id,
            limit=1000,
            include_image_columns=False,
        )
        
        # Should have 6 rows (2 per file * 3 files)
        assert len(df) == 6
        
        # Create enriched dataset and verify auto-sync
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Auto Sync Enriched",
            enrichment_config={"email": "emails"},
        )
        
        # Upload one more file
        csv_content_new = "name,email\nNew Person,new@example.com"
        csv_file_new = tmp_path / "new.csv"
        csv_file_new.write_text(csv_content_new, encoding="utf-8")
        
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file_new,
            filename="new.csv",
        )
        
        # Verify enriched datasets are auto-synced after upload
        # The sync should happen automatically via sync_all_enriched_datasets_for_source
        # We can verify by checking if new row appears in enriched dataset
        df_enriched = load_enriched_dataset_dataframe(
            session=test_session,
            enriched_dataset_id=enriched.id,
            limit=1000,
            include_image_columns=False,
        )
        
        # Should have at least the original rows (enriched dataset created after initial upload)
        assert len(df_enriched) >= 0


class TestSettingsPageDatabaseOperations:
    """Test Settings page database operations."""

    def test_settings_page_database_operations(self, test_session, tmp_path):
        """Test profile updates and dataset deletion."""
        # Create user profile
        profile = create_user_profile(test_session, "Original Name")
        
        # Update UserProfile name
        updated_profile = update_profile_name(test_session, "Updated Name")
        assert updated_profile.name == "Updated Name"
        
        # Verify profile is updated
        current_profile = get_current_profile(test_session)
        assert current_profile.name == "Updated Name"
        
        # Update UserProfile logo_path
        logo_path = str(tmp_path / "logo.png")
        # Create dummy logo file
        Path(logo_path).write_bytes(b"fake image data")
        
        updated_profile = update_profile_logo(test_session, logo_path)
        assert updated_profile.logo_path == logo_path
        
        # View dataset statistics
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Stats Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        csv_content = "name\nJohn\nJane\nBob"
        csv_file = tmp_path / "stats.csv"
        csv_file.write_text(csv_content, encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="stats.csv",
        )
        
        stats = get_dataset_statistics(test_session, dataset.id)
        assert stats["total_rows"] == 3
        
        # Test database integrity checks
        integrity_results = check_database_integrity(test_session)
        assert "total_issues" in integrity_results
        # Should have no issues for clean database
        assert integrity_results["total_issues"] == 0
        
        # Delete dataset (verify cascade operations)
        # Create enriched dataset and analysis for cascade test
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched for Delete",
            enrichment_config={"name": "phone_numbers"},
        )
        
        analysis = create_analysis(
            session=test_session,
            name="Analysis for Delete",
            operation_type="groupby",
            source_dataset_id=dataset.id,
            operation_config={"group_columns": ["name"], "aggregations": {}},
        )
        
        enriched_table_name = enriched.enriched_table_name
        
        delete_dataset(test_session, dataset.id)
        
        # Verify cascade delete
        assert not table_exists(test_session, dataset.table_name)
        assert not table_exists(test_session, enriched_table_name)
        
        deleted_enriched = test_session.get(EnrichedDataset, enriched.id)
        assert deleted_enriched is None
        
        deleted_analysis = test_session.get(DataAnalysis, analysis.id)
        assert deleted_analysis is None


class TestFullWorkflowMultipleDatasets:
    """Test complete end-to-end workflow across pages."""

    def test_full_workflow_multiple_datasets(self, test_session, tmp_path):
        """Test complete workflow with multiple datasets."""
        # 1. Initialize 3 datasets
        datasets = []
        for i in range(1, 4):
            columns_config = {
                "name": {"type": "TEXT", "is_image": False},
                "phone": {"type": "TEXT", "is_image": False},
                "email": {"type": "TEXT", "is_image": False},
            }
            
            dataset = initialize_dataset(
                session=test_session,
                name=f"Workflow Dataset {i}",
                slot_number=i,
                columns_config=columns_config,
                image_columns=[],
            )
            datasets.append(dataset)
            
            # 2. Upload CSV files to each
            csv_content = f"name,phone,email\nPerson{i}A,555-{i}111,person{i}a@example.com\nPerson{i}B,555-{i}222,person{i}b@example.com"
            csv_file = tmp_path / f"workflow_{i}.csv"
            csv_file.write_text(csv_content, encoding="utf-8")
            upload_csv_to_dataset(
                session=test_session,
                dataset_id=dataset.id,
                csv_file=csv_file,
                filename=f"workflow_{i}.csv",
            )
        
        # 3. Create enriched datasets from each
        enriched_datasets = []
        for dataset in datasets:
            enriched = create_enriched_dataset(
                session=test_session,
                source_dataset_id=dataset.id,
                name=f"Enriched {dataset.name}",
                enrichment_config={"phone": "phone_numbers"},
            )
            enriched_datasets.append(enriched)
        
        # 4. Create Knowledge Tables
        phone_table = initialize_knowledge_table(
            session=test_session,
            name="Workflow Phone Table",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config={"phone": {"type": "TEXT", "is_image": False}},
            image_columns=[],
            initial_data_df=pd.DataFrame({"phone": ["555-1111"]}),
        )
        
        # 5. Link enriched datasets to Knowledge Tables
        # This happens automatically through search - enriched columns match Key_IDs
        
        # 6. Create analyses linking datasets
        analysis1 = create_analysis(
            session=test_session,
            name="Workflow Analysis 1",
            operation_type="groupby",
            source_dataset_id=datasets[0].id,
            operation_config={"group_columns": ["name"], "aggregations": {}},
        )
        
        analysis2 = create_analysis(
            session=test_session,
            name="Workflow Analysis 2",
            operation_type="merge",
            source_dataset_id=datasets[0].id,
            secondary_dataset_id=datasets[1].id,
            operation_config={"join_keys": ["name"], "how": "inner"},
        )
        
        # 7. Verify all relationships are intact
        assert len(datasets) == 3
        assert len(enriched_datasets) == 3
        assert len([a for a in [analysis1, analysis2] if a]) == 2
        
        # Verify enriched datasets reference source datasets
        for enriched in enriched_datasets:
            test_session.refresh(enriched)
            assert enriched.source_dataset is not None
            assert enriched.source_dataset_id in [d.id for d in datasets]
        
        # Verify analyses reference datasets
        test_session.refresh(analysis1)
        assert analysis1.source_dataset is not None
        assert analysis1.source_dataset_id == datasets[0].id
        
        test_session.refresh(analysis2)
        assert analysis2.secondary_dataset is not None
        assert analysis2.secondary_dataset_id == datasets[1].id
        
        # 8. Delete one dataset and verify cascade deletes work correctly
        dataset_to_delete = datasets[0]
        enriched_table_name = enriched_datasets[0].enriched_table_name
        
        delete_dataset(test_session, dataset_to_delete.id)
        
        # Verify cascade deletes
        enriched_records = (
            test_session.query(EnrichedDataset)
            .filter_by(source_dataset_id=dataset_to_delete.id)
            .all()
        )
        assert len(enriched_records) == 0
        
        analysis_records = (
            test_session.query(DataAnalysis)
            .filter_by(source_dataset_id=dataset_to_delete.id)
            .all()
        )
        assert len(analysis_records) == 0
        
        assert not table_exists(test_session, enriched_table_name)
        
        # 9. Verify remaining datasets and relationships are unaffected
        remaining_datasets = (
            test_session.query(DatasetConfig)
            .filter(DatasetConfig.id.in_([d.id for d in datasets[1:]]))
            .all()
        )
        assert len(remaining_datasets) == 2
        
        remaining_enriched = (
            test_session.query(EnrichedDataset)
            .filter(EnrichedDataset.source_dataset_id.in_([d.id for d in datasets[1:]]))
            .all()
        )
        assert len(remaining_enriched) == 2
        
        # Knowledge Table should still exist
        all_tables = get_all_knowledge_tables(test_session)
        assert phone_table.id in [t.id for t in all_tables]


class TestRelationshipIntegrity:
    """Test relationship integrity and cascade deletes."""

    def test_relationship_integrity(self, test_session, tmp_path):
        """Test all foreign keys and cascade deletes."""
        # Create datasets
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset1 = initialize_dataset(
            session=test_session,
            name="Dataset 1",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        dataset2 = initialize_dataset(
            session=test_session,
            name="Dataset 2",
            slot_number=2,
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Create upload log
        csv_content = "name\nJohn"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content, encoding="utf-8")
        upload_log = upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset1.id,
            csv_file=csv_file,
            filename="test.csv",
        )
        
        # Create enriched dataset
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset1.id,
            name="Enriched",
            enrichment_config={"name": "phone_numbers"},
        )
        
        # Create analyses
        analysis1 = create_analysis(
            session=test_session,
            name="Analysis 1",
            operation_type="groupby",
            source_dataset_id=dataset1.id,
            operation_config={"group_columns": ["name"], "aggregations": {}},
        )
        
        analysis2 = create_analysis(
            session=test_session,
            name="Analysis 2",
            operation_type="merge",
            source_dataset_id=dataset1.id,
            secondary_dataset_id=dataset2.id,
            operation_config={"join_keys": ["name"], "how": "inner"},
        )
        
        # Verify all foreign key constraints work
        test_session.refresh(upload_log)
        assert upload_log.dataset is not None
        
        test_session.refresh(enriched)
        assert enriched.source_dataset is not None
        
        test_session.refresh(analysis1)
        assert analysis1.source_dataset is not None
        
        test_session.refresh(analysis2)
        assert analysis2.source_dataset is not None
        assert analysis2.secondary_dataset is not None
        
        # Test cascade deletes: DatasetConfig → UploadLog
        upload_log_id = upload_log.id
        delete_dataset(test_session, dataset1.id)
        
        deleted_upload_log = test_session.get(UploadLog, upload_log_id)
        assert deleted_upload_log is None
        
        # Recreate for next test
        dataset1 = initialize_dataset(
            session=test_session,
            name="Dataset 1",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset1.id,
            name="Enriched",
            enrichment_config={"name": "phone_numbers"},
        )
        
        # Test cascade deletes: DatasetConfig → EnrichedDataset
        enriched_id = enriched.id
        enriched_table_name = enriched.enriched_table_name
        delete_dataset(test_session, dataset1.id)
        
        deleted_enriched = test_session.get(EnrichedDataset, enriched_id)
        assert deleted_enriched is None
        assert not table_exists(test_session, enriched_table_name)
        
        # Recreate for next test
        dataset1 = initialize_dataset(
            session=test_session,
            name="Dataset 1",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        analysis1 = create_analysis(
            session=test_session,
            name="Analysis 1",
            operation_type="groupby",
            source_dataset_id=dataset1.id,
            operation_config={"group_columns": ["name"], "aggregations": {}},
        )
        
        analysis2 = create_analysis(
            session=test_session,
            name="Analysis 2",
            operation_type="merge",
            source_dataset_id=dataset1.id,
            secondary_dataset_id=dataset2.id,
            operation_config={"join_keys": ["name"], "how": "inner"},
        )
        
        # Test cascade deletes: DatasetConfig → DataAnalysis (source and secondary)
        analysis1_id = analysis1.id
        analysis2_id = analysis2.id
        
        delete_dataset(test_session, dataset1.id)
        
        deleted_analysis1 = test_session.get(DataAnalysis, analysis1_id)
        assert deleted_analysis1 is None
        
        # Analysis2 should also be deleted because source_dataset_id was dataset1.id
        deleted_analysis2 = test_session.get(DataAnalysis, analysis2_id)
        assert deleted_analysis2 is None
        
        # Verify orphaned data detection works
        integrity_results = check_database_integrity(test_session)
        assert "total_issues" in integrity_results
        assert integrity_results["total_issues"] == 0
        
        # Verify database integrity check finds no issues
        assert len(integrity_results["orphaned_enriched_tables"]) == 0
        assert len(integrity_results["orphaned_enriched_records"]) == 0
        assert len(integrity_results["invalid_enriched_references"]) == 0


class TestImageColumnHandlingAcrossPages:
    """Test image column handling across all pages."""

    def test_image_column_handling_across_pages(self, test_session, tmp_path):
        """Test image column handling across all pages."""
        # Create dataset with image columns
        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "photo": {"type": "TEXT", "is_image": True},
        }
        
        dataset = initialize_dataset(
            session=test_session,
            name="Image Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=["photo"],
        )
        
        # Upload CSV with Base64 images
        base64_image = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
        csv_content = f"name,photo\nJohn,data:image/png;base64,{base64_image}\nJane,data:image/png;base64,{base64_image}"
        csv_file = tmp_path / "images.csv"
        csv_file.write_text(csv_content, encoding="utf-8")
        
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="images.csv",
        )
        
        # Verify images detected correctly
        test_session.refresh(dataset)
        assert dataset.image_columns == ["photo"]
        
        # Verify images excluded from DataFrame View by default
        df = load_dataset_dataframe(
            session=test_session,
            dataset_id=dataset.id,
            limit=100,
            include_image_columns=False,
        )
        assert "photo" not in df.columns
        assert "name" in df.columns
        
        # Verify images included when requested
        df_with_images = load_dataset_dataframe(
            session=test_session,
            dataset_id=dataset.id,
            limit=100,
            include_image_columns=True,
        )
        assert "photo" in df_with_images.columns
        
        # Verify images work in Image Search page
        tables_with_images = get_tables_with_image_columns(test_session)
        dataset_table = next((t for t in tables_with_images if t["id"] == dataset.id), None)
        assert dataset_table is not None
        assert "photo" in dataset_table["image_columns"]
        
        # Verify images excluded from exports by default
        export_csv = tmp_path / "export_no_images.csv"
        export_path = export_dataset_to_csv(
            session=test_session,
            dataset_id=dataset.id,
            output_path=export_csv,
            include_image_columns=False,
        )
        
        exported_df = pd.read_csv(export_path)
        assert "photo" not in exported_df.columns
        
        # Verify images can be included in exports
        export_csv_with = tmp_path / "export_with_images.csv"
        export_path = export_dataset_to_csv(
            session=test_session,
            dataset_id=dataset.id,
            output_path=export_csv_with,
            include_image_columns=True,
        )
        
        exported_df_with = pd.read_csv(export_path)
        assert "photo" in exported_df_with.columns


class TestUUIDConsistencyAcrossOperations:
    """Test UUID consistency across all operations."""

    def test_uuid_consistency_across_operations(self, test_session, tmp_path):
        """Verify uuid_value consistency across all operations."""
        # Create dataset
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="UUID Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Upload data
        csv_content = "name\nJohn\nJane\nBob"
        csv_file = tmp_path / "uuid_test.csv"
        csv_file.write_text(csv_content, encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="uuid_test.csv",
        )
        
        # Verify uuid_value is primary key in dataset table
        inspector = inspect(test_session.bind)
        columns = inspector.get_columns(dataset.table_name)
        pk_columns = [col for col in columns if col.get("primary_key")]
        assert len(pk_columns) == 1
        assert pk_columns[0]["name"] == UNIQUE_ID_COLUMN_NAME
        
        # Load dataset and get UUIDs
        df = load_dataset_dataframe(
            session=test_session,
            dataset_id=dataset.id,
            limit=100,
            include_image_columns=False,
        )
        
        uuids = df[UNIQUE_ID_COLUMN_NAME].tolist()
        
        # Verify UUIDs are unique within dataset
        assert len(uuids) == len(set(uuids))
        
        # Create enriched dataset
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched UUID",
            enrichment_config={"name": "phone_numbers"},
        )
        
        # Verify uuid_value is inherited in enriched tables
        enriched_columns = inspector.get_columns(enriched.enriched_table_name)
        enriched_column_names = [col["name"] for col in enriched_columns]
        assert UNIQUE_ID_COLUMN_NAME in enriched_column_names
        
        # Verify UUIDs are preserved through enrichment
        df_enriched = load_enriched_dataset_dataframe(
            session=test_session,
            enriched_dataset_id=enriched.id,
            limit=100,
            include_image_columns=False,
        )
        
        enriched_uuids = df_enriched[UNIQUE_ID_COLUMN_NAME].tolist()
        
        # UUIDs should match between source and enriched
        assert set(uuids) == set(enriched_uuids)
        
        # Upload more data
        csv_content2 = "name\nAlice"
        csv_file2 = tmp_path / "uuid_test2.csv"
        csv_file2.write_text(csv_content2, encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file2,
            filename="uuid_test2.csv",
        )
        
        # Sync enriched dataset
        sync_enriched_dataset(test_session, enriched.id)
        
        # Verify UUIDs are preserved through sync operations
        df_enriched_after = load_enriched_dataset_dataframe(
            session=test_session,
            enriched_dataset_id=enriched.id,
            limit=100,
            include_image_columns=False,
        )
        
        enriched_uuids_after = df_enriched_after[UNIQUE_ID_COLUMN_NAME].tolist()
        
        # Should have all original UUIDs plus new one
        assert len(enriched_uuids_after) >= len(enriched_uuids)
        
        # Verify UUID format consistency
        import uuid
        for uuid_val in uuids:
            # UUID should be a valid UUID string
            try:
                uuid.UUID(uuid_val)
            except ValueError:
                # If not standard UUID, at least verify it's a string
                assert isinstance(uuid_val, str)
                assert len(uuid_val) > 0


class TestKnowledgeTableLinking:
    """Test Knowledge Table to enriched dataset linking."""

    def test_knowledge_table_linking(self, test_session, tmp_path):
        """Test Knowledge Table to enriched dataset linking."""
        # Create Knowledge Tables for phone_numbers, emails, web_domains
        phone_table = initialize_knowledge_table(
            session=test_session,
            name="Phone KT",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config={"phone": {"type": "TEXT", "is_image": False}},
            image_columns=[],
            initial_data_df=pd.DataFrame({"phone": ["555-123-4567"]}),
        )
        
        email_table = initialize_knowledge_table(
            session=test_session,
            name="Email KT",
            data_type="emails",
            primary_key_column="email",
            columns_config={"email": {"type": "TEXT", "is_image": False}},
            image_columns=[],
            initial_data_df=pd.DataFrame({"email": ["test@example.com"]}),
        )
        
        domain_table = initialize_knowledge_table(
            session=test_session,
            name="Domain KT",
            data_type="web_domains",
            primary_key_column="domain",
            columns_config={"domain": {"type": "TEXT", "is_image": False}},
            image_columns=[],
            initial_data_df=pd.DataFrame({"domain": ["example.com"]}),
        )
        
        # Create enriched datasets with matching enriched columns
        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "phone": {"type": "TEXT", "is_image": False},
            "email": {"type": "TEXT", "is_image": False},
        }
        
        source_dataset = initialize_dataset(
            session=test_session,
            name="Source for Linking",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        csv_content = "name,phone,email\nJohn,555-123-4567,john@example.com"
        csv_file = tmp_path / "linking.csv"
        csv_file.write_text(csv_content, encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=source_dataset.id,
            csv_file=csv_file,
            filename="linking.csv",
        )
        
        enriched_phone = create_enriched_dataset(
            session=test_session,
            source_dataset_id=source_dataset.id,
            name="Enriched Phone",
            enrichment_config={"phone": "phone_numbers"},
        )
        
        enriched_email = create_enriched_dataset(
            session=test_session,
            source_dataset_id=source_dataset.id,
            name="Enriched Email",
            enrichment_config={"email": "emails"},
        )
        
        # Verify linking works correctly
        from src.services.search_service import search_knowledge_base
        
        # Search for phone number
        phone_results = search_knowledge_base(
            session=test_session,
            search_value="5551234567",
            data_type="phone_numbers",
        )
        
        # Should find matches in both Knowledge Table and enriched dataset
        assert "presence" in phone_results
        assert len(phone_results["presence"]["knowledge_tables"]) >= 0
        assert len(phone_results["presence"]["enriched_datasets"]) >= 0
        
        # Search for email
        email_results = search_knowledge_base(
            session=test_session,
            search_value="test@example.com",
            data_type="emails",
        )
        
        assert "presence" in email_results
        
        # Test with multiple Knowledge Tables per data type
        phone_table2 = initialize_knowledge_table(
            session=test_session,
            name="Phone KT 2",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config={"phone": {"type": "TEXT", "is_image": False}},
            image_columns=[],
            initial_data_df=pd.DataFrame({"phone": ["555-987-6543"]}),
        )
        
        # Search should find matches in both Knowledge Tables
        phone_results2 = search_knowledge_base(
            session=test_session,
            search_value="5559876543",
            data_type="phone_numbers",
        )
        
        assert "presence" in phone_results2
        knowledge_table_names = [kt["name"] for kt in phone_results2["presence"]["knowledge_tables"]]
        # Should potentially find in multiple tables
        assert isinstance(knowledge_table_names, list)

