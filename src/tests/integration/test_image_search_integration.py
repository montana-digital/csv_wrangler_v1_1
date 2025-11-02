"""
Integration tests for Image Search feature.

Tests the integration between image service, table loading, and Knowledge Table associations.
"""
import pandas as pd
import pytest
from sqlalchemy import text

from src.database.models import DatasetConfig, EnrichedDataset
from src.services.dataframe_service import (
    load_dataset_dataframe,
    load_enriched_dataset_dataframe,
)
from src.services.image_service import (
    get_knowledge_associations_for_row,
    get_tables_with_image_columns,
)
from src.services.knowledge_service import initialize_knowledge_table
from src.utils.errors import ValidationError


class TestImageTableDiscovery:
    """Test finding tables with image columns."""

    def test_find_dataset_with_images(self, test_session):
        """Test finding a dataset with image columns."""
        # Create dataset with image columns
        dataset = DatasetConfig(
            name="Test Images Dataset",
            slot_number=1,
            table_name="test_images_dataset_1",
            columns_config={
                "name": {"type": "TEXT", "is_image": False},
                "photo": {"type": "TEXT", "is_image": True},
            },
            image_columns=["photo"],
        )
        test_session.add(dataset)
        test_session.commit()

        # Create table and insert test data
        test_session.execute(text(
            f"CREATE TABLE IF NOT EXISTS {dataset.table_name} ("
            "uuid_value TEXT PRIMARY KEY, "
            "name TEXT, "
            "photo TEXT"
            ")"
        ))
        test_session.execute(text(
            f"INSERT INTO {dataset.table_name} (uuid_value, name, photo) "
            "VALUES ('uuid1', 'Test Name', 'data:image/png;base64,test123')"
        ))
        test_session.commit()

        # Get tables with images
        tables = get_tables_with_image_columns(test_session)

        # Should find our dataset
        found = [t for t in tables if t["id"] == dataset.id]
        assert len(found) == 1
        assert found[0]["image_columns"] == ["photo"]
        assert found[0]["table_name"] == dataset.table_name

    def test_find_enriched_dataset_with_images(
        self, test_session
    ):
        """Test finding enriched dataset with images from source."""
        # Create source dataset with images
        source = DatasetConfig(
            name="Source Dataset",
            slot_number=2,
            table_name="source_dataset_2",
            columns_config={
                "name": {"type": "TEXT", "is_image": False},
                "thumbnail": {"type": "TEXT", "is_image": True},
            },
            image_columns=["thumbnail"],
        )
        test_session.add(source)
        test_session.commit()

        # Create enriched dataset
        enriched = EnrichedDataset(
            name="Enriched Dataset",
            source_dataset_id=source.id,
            enriched_table_name="enriched_dataset_2",
            source_table_name=source.table_name,
            enrichment_config={"phone": "phone_numbers"},
            columns_added=["phone_enriched_phone_numbers"],
        )
        test_session.add(enriched)
        test_session.commit()

        # Create enriched table
        test_session.execute(text(
            f"CREATE TABLE IF NOT EXISTS {enriched.enriched_table_name} ("
            "uuid_value TEXT PRIMARY KEY, "
            "name TEXT, "
            "thumbnail TEXT, "
            "phone_enriched_phone_numbers TEXT"
            ")"
        ))
        test_session.commit()

        # Get tables with images
        tables = get_tables_with_image_columns(test_session)

        # Should find enriched dataset (inherits images from source)
        found = [t for t in tables if t["type"] == "enriched_dataset" and t["id"] == enriched.id]
        assert len(found) == 1
        assert found[0]["image_columns"] == ["thumbnail"]
        assert found[0]["table_name"] == enriched.enriched_table_name


class TestImageTableDataLoading:
    """Test loading data from tables with images."""

    def test_load_dataset_with_images(self, test_session):
        """Test loading dataset data including image columns."""
        # Create dataset with images
        dataset = DatasetConfig(
            name="Images Test",
            slot_number=3,
            table_name="images_test_3",
            columns_config={
                "name": {"type": "TEXT", "is_image": False},
                "photo": {"type": "TEXT", "is_image": True},
                "signature": {"type": "TEXT", "is_image": True},
            },
            image_columns=["photo", "signature"],
        )
        test_session.add(dataset)
        test_session.commit()

        # Create table and insert data
        test_session.execute(text(
            f"CREATE TABLE IF NOT EXISTS {dataset.table_name} ("
            "uuid_value TEXT PRIMARY KEY, "
            "name TEXT, "
            "photo TEXT, "
            "signature TEXT"
            ")"
        ))
        test_session.execute(text(
            f"INSERT INTO {dataset.table_name} (uuid_value, name, photo, signature) VALUES "
            "('uuid1', 'John', 'data:image/png;base64,photo1', 'data:image/png;base64,sig1'), "
            "('uuid2', 'Jane', 'data:image/jpeg;base64,photo2', NULL)"
        ))
        test_session.commit()

        # Load with images
        df = load_dataset_dataframe(
            session=test_session,
            dataset_id=dataset.id,
            limit=1000,
            include_image_columns=True,
        )

        assert len(df) == 2
        assert "photo" in df.columns
        assert "signature" in df.columns
        assert "name" in df.columns
        # Verify photo values exist (order may vary due to ORDER BY rowid DESC)
        photo_values = df["photo"].tolist()
        assert "data:image/png;base64,photo1" in photo_values
        assert "data:image/jpeg;base64,photo2" in photo_values
        # Verify one signature is NULL
        assert df["signature"].isna().any()

    def test_load_enriched_dataset_with_images(self, test_session):
        """Test loading enriched dataset data including images."""
        # Create source with images
        source = DatasetConfig(
            name="Source",
            slot_number=4,
            table_name="source_4",
            columns_config={
                "name": {"type": "TEXT", "is_image": False},
                "image": {"type": "TEXT", "is_image": True},
            },
            image_columns=["image"],
        )
        test_session.add(source)
        test_session.commit()

        # Create enriched
        enriched = EnrichedDataset(
            name="Enriched",
            source_dataset_id=source.id,
            enriched_table_name="enriched_4",
            source_table_name=source.table_name,
            enrichment_config={"phone": "phone_numbers"},
            columns_added=["phone_enriched_phone_numbers"],
        )
        test_session.add(enriched)
        test_session.commit()

        # Create table
        test_session.execute(text(
            f"CREATE TABLE IF NOT EXISTS {enriched.enriched_table_name} ("
            "uuid_value TEXT PRIMARY KEY, "
            "name TEXT, "
            "image TEXT, "
            "phone_enriched_phone_numbers TEXT"
            ")"
        ))
        test_session.execute(text(
            f"INSERT INTO {enriched.enriched_table_name} "
            "(uuid_value, name, image, phone_enriched_phone_numbers) VALUES "
            "('uuid1', 'Test', 'data:image/png;base64,img1', '+1234567890')"
        ))
        test_session.commit()

        # Load with images
        df = load_enriched_dataset_dataframe(
            session=test_session,
            enriched_dataset_id=enriched.id,
            limit=1000,
            include_image_columns=True,
        )

        assert len(df) == 1
        assert "image" in df.columns
        assert "phone_enriched_phone_numbers" in df.columns
        assert df.loc[0, "image"] == "data:image/png;base64,img1"


class TestKnowledgeTableAssociations:
    """Test Knowledge Table associations for rows with phone/domain data."""

    def test_associations_with_phone_number(
        self, test_session
    ):
        """Test getting associations for row with phone number."""
        # Create Knowledge Table
        kt_data = pd.DataFrame({
            "phone": ["+1234567890"],
            "carrier": ["Verizon"],
        })
        kt = initialize_knowledge_table(
            session=test_session,
            name="Carrier Info",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config={
                "phone": {"type": "TEXT", "is_image": False},
                "carrier": {"type": "TEXT", "is_image": False},
            },
            image_columns=[],
            initial_data_df=kt_data,
        )

        # Row with matching phone
        row_data = {
            "name": "Contact",
            "phone_enriched_phone_numbers": "+1234567890",
            "email": "test@example.com",
        }

        # Get associations
        associations = get_knowledge_associations_for_row(
            session=test_session,
            row_data=row_data,
            table_type="dataset",
            table_id=1,  # Dummy ID
        )

        # Should find phone associations
        assert len(associations["phone_numbers"]) == 1
        assert associations["phone_numbers"][0]["value"] == "+1234567890"
        search_results = associations["phone_numbers"][0]["search_results"]
        presence = search_results.get("presence", {})
        kt_results = presence.get("knowledge_tables", [])
        
        # Should find our Knowledge Table
        found_kt = [kt_r for kt_r in kt_results if kt_r["table_id"] == kt.id]
        assert len(found_kt) == 1
        assert found_kt[0]["has_data"] is True

    def test_associations_with_multiple_phones(self, test_session):
        """Test associations for row with multiple phone numbers."""
        # Create Knowledge Tables
        kt1_data = pd.DataFrame({
            "phone": ["+1111111111"],
            "info": ["Info1"],
        })
        kt1 = initialize_knowledge_table(
            session=test_session,
            name="List 1",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config={
                "phone": {"type": "TEXT", "is_image": False},
                "info": {"type": "TEXT", "is_image": False},
            },
            image_columns=[],
            initial_data_df=kt1_data,
        )

        kt2_data = pd.DataFrame({
            "phone": ["+2222222222"],
            "info": ["Info2"],
        })
        kt2 = initialize_knowledge_table(
            session=test_session,
            name="List 2",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config={
                "phone": {"type": "TEXT", "is_image": False},
                "info": {"type": "TEXT", "is_image": False},
            },
            image_columns=[],
            initial_data_df=kt2_data,
        )

        # Row with multiple phones
        row_data = {
            "name": "Multi Phone",
            "mobile_enriched_phone_numbers": "+1111111111",
            "work_enriched_phone_numbers": "+2222222222",
        }

        # Get associations
        associations = get_knowledge_associations_for_row(
            session=test_session,
            row_data=row_data,
            table_type="dataset",
            table_id=1,
        )

        # Should find both phones
        assert len(associations["phone_numbers"]) == 2
        phone_values = [p["value"] for p in associations["phone_numbers"]]
        assert "+1111111111" in phone_values
        assert "+2222222222" in phone_values

    def test_associations_with_domain(self, test_session):
        """Test associations for row with web domain."""
        # Create Knowledge Table for domains
        kt_data = pd.DataFrame({
            "domain": ["https://example.com"],
            "status": ["verified"],
        })
        kt = initialize_knowledge_table(
            session=test_session,
            name="Domain List",
            data_type="web_domains",
            primary_key_column="domain",
            columns_config={
                "domain": {"type": "TEXT", "is_image": False},
                "status": {"type": "TEXT", "is_image": False},
            },
            image_columns=[],
            initial_data_df=kt_data,
        )

        # Row with domain
        row_data = {
            "name": "Company",
            "website_enriched_web_domains": "https://example.com",
        }

        # Get associations
        associations = get_knowledge_associations_for_row(
            session=test_session,
            row_data=row_data,
            table_type="dataset",
            table_id=1,
        )

        # Should find domain associations
        assert len(associations["web_domains"]) == 1
        assert associations["web_domains"][0]["value"] == "https://example.com"


class TestImageSearchIntegration:
    """Test end-to-end Image Search integration."""

    def test_full_image_search_workflow(self, test_session):
        """Test complete workflow: find table, load data, get associations."""
        # Create dataset with images and enriched phone
        dataset = DatasetConfig(
            name="Full Test",
            slot_number=5,
            table_name="full_test_5",
            columns_config={
                "name": {"type": "TEXT", "is_image": False},
                "photo": {"type": "TEXT", "is_image": True},
                "phone": {"type": "TEXT", "is_image": False},
            },
            image_columns=["photo"],
        )
        test_session.add(dataset)
        test_session.commit()

        # Create table with data
        test_session.execute(text(
            f"CREATE TABLE IF NOT EXISTS {dataset.table_name} ("
            "uuid_value TEXT PRIMARY KEY, "
            "name TEXT, "
            "photo TEXT, "
            "phone TEXT"
            ")"
        ))
        test_session.execute(text(
            f"INSERT INTO {dataset.table_name} (uuid_value, name, photo, phone) VALUES "
            "('uuid1', 'John Doe', 'data:image/png;base64,photo123', '+1234567890')"
        ))
        test_session.commit()

        # Create Knowledge Table
        kt_data = pd.DataFrame({
            "phone": ["+1234567890"],
            "carrier": ["Verizon"],
        })
        kt = initialize_knowledge_table(
            session=test_session,
            name="Carrier Info",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config={
                "phone": {"type": "TEXT", "is_image": False},
                "carrier": {"type": "TEXT", "is_image": False},
            },
            image_columns=[],
            initial_data_df=kt_data,
        )

        # Step 1: Find table with images
        tables = get_tables_with_image_columns(test_session)
        found_table = [t for t in tables if t["id"] == dataset.id][0]
        assert found_table is not None
        assert "photo" in found_table["image_columns"]

        # Step 2: Load data with images
        df = load_dataset_dataframe(
            session=test_session,
            dataset_id=dataset.id,
            limit=1000,
            include_image_columns=True,
        )
        assert len(df) == 1
        assert "photo" in df.columns
        row_data = df.iloc[0].to_dict()

        # Step 3: Get Knowledge Table associations
        # Note: We'd need enriched phone column for this to work
        # For this test, we'll simulate it
        row_data["phone_enriched_phone_numbers"] = row_data["phone"]
        associations = get_knowledge_associations_for_row(
            session=test_session,
            row_data=row_data,
            table_type="dataset",
            table_id=dataset.id,
        )

        # Should find associations
        assert len(associations["phone_numbers"]) == 1

