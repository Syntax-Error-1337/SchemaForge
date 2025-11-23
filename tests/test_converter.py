"""
Unit tests for the converter module.
"""

import json
import pytest
import tempfile
import shutil
import pandas as pd
from pathlib import Path
from src.schema_reader import SchemaReader
from src.converter import Converter


@pytest.fixture
def temp_data_dir():
    """Create a temporary directory with test JSON files."""
    temp_dir = tempfile.mkdtemp()
    data_dir = Path(temp_dir) / "data"
    data_dir.mkdir()
    
    # Create test JSON file
    test_data = [
        {
            "id": 1,
            "name": "Alice",
            "age": 30,
            "active": True,
            "score": 95.5,
            "email": "alice@example.com",
            "address": {
                "street": "123 Main St",
                "city": "New York",
                "zip": "10001"
            }
        },
        {
            "id": 2,
            "name": "Bob",
            "age": 25,
            "active": False,
            "score": 87.0,
            "email": "bob@example.com",
            "address": {
                "street": "456 Oak Ave",
                "city": "Los Angeles",
                "zip": "90001"
            }
        }
    ]
    
    with open(data_dir / "test_data.json", 'w') as f:
        json.dump(test_data, f, indent=2)
    
    yield data_dir
    
    # Cleanup
    shutil.rmtree(temp_dir)


@pytest.fixture
def temp_output_dir():
    """Create a temporary output directory."""
    temp_dir = tempfile.mkdtemp()
    output_dir = Path(temp_dir) / "output"
    output_dir.mkdir()
    yield output_dir
    shutil.rmtree(temp_dir)


def test_converter_initialization(temp_data_dir, temp_output_dir):
    """Test Converter initialization."""
    converter = Converter(
        data_dir=str(temp_data_dir),
        output_dir=str(temp_output_dir)
    )
    
    assert converter.data_dir == Path(temp_data_dir)
    assert converter.output_dir == Path(temp_output_dir)


def test_flatten_dict():
    """Test dictionary flattening in converter."""
    converter = Converter()
    
    nested = {
        "user": {
            "name": "Alice",
            "address": {
                "city": "New York"
            }
        },
        "id": 1
    }
    
    flattened = converter._flatten_dict(nested)
    assert "user.name" in flattened
    assert "user.address.city" in flattened
    assert "id" in flattened


def test_coerce_type():
    """Test type coercion."""
    converter = Converter()
    
    assert converter._coerce_type("42", "integer") == 42
    assert converter._coerce_type("3.14", "float") == 3.14
    assert converter._coerce_type("true", "boolean") == True
    assert converter._coerce_type("false", "boolean") == False
    assert converter._coerce_type(42, "string") == "42"
    assert converter._coerce_type(None, "integer") is None


def test_convert_to_parquet(temp_data_dir, temp_output_dir):
    """Test conversion to Parquet format."""
    converter = Converter(
        data_dir=str(temp_data_dir),
        output_dir=str(temp_output_dir)
    )
    
    filepath = temp_data_dir / "test_data.json"
    success = converter.convert_to_parquet(filepath)
    
    assert success == True
    
    # Check output file exists
    output_file = temp_output_dir / "test_data.parquet"
    assert output_file.exists()
    
    # Verify Parquet file can be read
    df = pd.read_parquet(output_file)
    assert len(df) == 2
    assert "id" in df.columns
    assert "name" in df.columns
    assert "address.city" in df.columns  # Nested field flattened


def test_convert_to_csv(temp_data_dir, temp_output_dir):
    """Test conversion to CSV format."""
    converter = Converter(
        data_dir=str(temp_data_dir),
        output_dir=str(temp_output_dir)
    )
    
    filepath = temp_data_dir / "test_data.json"
    success = converter.convert_to_csv(filepath)
    
    assert success == True
    
    # Check output file exists
    output_file = temp_output_dir / "test_data.csv"
    assert output_file.exists()
    
    # Verify CSV file can be read
    df = pd.read_csv(output_file)
    assert len(df) == 2
    assert "id" in df.columns
    assert "name" in df.columns
    assert "address.city" in df.columns  # Nested field flattened


def test_convert_all_parquet(temp_data_dir, temp_output_dir):
    """Test converting all files to Parquet."""
    converter = Converter(
        data_dir=str(temp_data_dir),
        output_dir=str(temp_output_dir)
    )
    
    # Generate schema report first
    reader = SchemaReader(data_dir=str(temp_data_dir))
    reader.scan_directory()
    report_path = temp_output_dir / "schemas.md"
    reader.generate_report(str(report_path))
    json_report_path = report_path.with_suffix('.json')
    
    converter.schema_report_path = str(json_report_path)
    
    results = converter.convert_all("parquet")
    
    assert len(results) == 1
    assert results["test_data.json"] == True
    
    # Check output file
    output_file = temp_output_dir / "test_data.parquet"
    assert output_file.exists()


def test_convert_all_csv(temp_data_dir, temp_output_dir):
    """Test converting all files to CSV."""
    converter = Converter(
        data_dir=str(temp_data_dir),
        output_dir=str(temp_output_dir)
    )
    
    # Generate schema report first
    reader = SchemaReader(data_dir=str(temp_data_dir))
    reader.scan_directory()
    report_path = temp_output_dir / "schemas.md"
    reader.generate_report(str(report_path))
    json_report_path = report_path.with_suffix('.json')
    
    converter.schema_report_path = str(json_report_path)
    
    results = converter.convert_all("csv")
    
    assert len(results) == 1
    assert results["test_data.json"] == True
    
    # Check output file
    output_file = temp_output_dir / "test_data.csv"
    assert output_file.exists()


def test_nested_structure_handling(temp_data_dir, temp_output_dir):
    """Test that nested structures are properly flattened."""
    converter = Converter(
        data_dir=str(temp_data_dir),
        output_dir=str(temp_output_dir)
    )
    
    filepath = temp_data_dir / "test_data.json"
    converter.convert_to_parquet(filepath)
    
    # Read back and verify nested fields
    output_file = temp_output_dir / "test_data.parquet"
    df = pd.read_parquet(output_file)
    
    # Check flattened nested fields exist
    assert "address.street" in df.columns
    assert "address.city" in df.columns
    assert "address.zip" in df.columns
    
    # Check values
    assert df["address.city"].iloc[0] == "New York"
    assert df["address.city"].iloc[1] == "Los Angeles"

