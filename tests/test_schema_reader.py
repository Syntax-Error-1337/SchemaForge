"""
Unit tests for the schema_reader module.
"""

import json
import pytest
import tempfile
import shutil
from pathlib import Path
from src.schema_reader import SchemaReader, FileSchema


@pytest.fixture
def temp_data_dir():
    """Create a temporary directory with test JSON files."""
    temp_dir = tempfile.mkdtemp()
    data_dir = Path(temp_dir) / "data"
    data_dir.mkdir()
    
    # Create test JSON file 1: Array format
    test_data_1 = [
        {
            "id": 1,
            "name": "Alice",
            "age": 30,
            "active": True,
            "score": 95.5,
            "email": "alice@example.com",
            "created_at": "2023-01-01T10:00:00Z",
            "tags": ["python", "data"],
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
            "created_at": "2023-01-02T11:00:00Z",
            "tags": ["java", "web"],
            "address": {
                "street": "456 Oak Ave",
                "city": "Los Angeles",
                "zip": "90001"
            }
        },
        {
            "id": 3,
            "name": "Charlie",
            "age": None,  # Nullable field
            "active": True,
            "score": 92.3,
            "email": "charlie@example.com",
            "created_at": "2023-01-03T12:00:00Z",
            "tags": ["python", "ml"],
            "address": {
                "street": "789 Pine Rd",
                "city": "Chicago",
                "zip": "60601"
            }
        }
    ]
    
    with open(data_dir / "test_array.json", 'w') as f:
        json.dump(test_data_1, f, indent=2)
    
    # Create test JSON file 2: NDJSON format
    test_data_2 = [
        {"product": "Laptop", "price": 999.99, "in_stock": True},
        {"product": "Mouse", "price": 29.99, "in_stock": False},
        {"product": "Keyboard", "price": 79.99, "in_stock": True}
    ]
    
    with open(data_dir / "test_ndjson.json", 'w') as f:
        for record in test_data_2:
            f.write(json.dumps(record) + '\n')
    
    # Create test JSON file 3: Mixed types
    test_data_3 = [
        {"value": 42, "type": "number"},
        {"value": "hello", "type": "string"},
        {"value": 3.14, "type": "number"}
    ]
    
    with open(data_dir / "test_mixed.json", 'w') as f:
        json.dump(test_data_3, f, indent=2)
    
    yield data_dir
    
    # Cleanup
    shutil.rmtree(temp_dir)


def test_schema_reader_initialization():
    """Test SchemaReader initialization."""
    reader = SchemaReader(data_dir="data", max_sample_size=100)
    assert reader.data_dir == Path("data")
    assert reader.max_sample_size == 100
    assert reader.sampling_strategy == "first"


def test_infer_type():
    """Test type inference."""
    reader = SchemaReader()
    
    assert reader._infer_type(42) == "integer"
    assert reader._infer_type(3.14) == "float"
    assert reader._infer_type("hello") == "string"
    assert reader._infer_type(True) == "boolean"
    assert reader._infer_type(None) == "null"
    assert reader._infer_type([1, 2, 3]) == "array"
    assert reader._infer_type({"key": "value"}) == "object"


def test_looks_like_timestamp():
    """Test timestamp detection."""
    reader = SchemaReader()
    
    assert reader._looks_like_timestamp("2023-01-01") == True
    assert reader._looks_like_timestamp("2023-01-01T10:00:00Z") == True
    assert reader._looks_like_timestamp("1234567890") == True
    assert reader._looks_like_timestamp("1234567890123") == True
    assert reader._looks_like_timestamp("hello") == False
    assert reader._looks_like_timestamp("123") == False


def test_flatten_dict():
    """Test dictionary flattening."""
    reader = SchemaReader()
    
    nested = {
        "user": {
            "name": "Alice",
            "address": {
                "city": "New York"
            }
        },
        "id": 1
    }
    
    flattened = reader._flatten_dict(nested)
    assert "user.name" in flattened
    assert "user.address.city" in flattened
    assert "id" in flattened
    assert flattened["user.name"] == "Alice"
    assert flattened["user.address.city"] == "New York"
    assert flattened["id"] == 1


def test_infer_schema_array_format(temp_data_dir):
    """Test schema inference for array format JSON."""
    reader = SchemaReader(data_dir=str(temp_data_dir))
    schema = reader.infer_schema(temp_data_dir / "test_array.json")
    
    assert schema is not None
    assert schema.filename == "test_array.json"
    assert schema.record_count == 3
    assert len(schema.fields) > 0
    
    # Check specific fields
    assert "id" in schema.fields
    assert schema.fields["id"].field_type == "integer"
    assert "name" in schema.fields
    assert schema.fields["name"].field_type == "string"
    assert "age" in schema.fields
    assert schema.fields["age"].nullable == True  # Has None value
    assert "address.city" in schema.fields  # Nested field


def test_infer_schema_ndjson_format(temp_data_dir):
    """Test schema inference for NDJSON format."""
    reader = SchemaReader(data_dir=str(temp_data_dir))
    schema = reader.infer_schema(temp_data_dir / "test_ndjson.json")
    
    assert schema is not None
    assert schema.filename == "test_ndjson.json"
    assert schema.record_count == 3
    assert "product" in schema.fields
    assert "price" in schema.fields
    assert schema.fields["price"].field_type == "float"


def test_infer_schema_mixed_types(temp_data_dir):
    """Test schema inference with mixed types."""
    reader = SchemaReader(data_dir=str(temp_data_dir))
    schema = reader.infer_schema(temp_data_dir / "test_mixed.json")
    
    assert schema is not None
    assert "value" in schema.fields
    # Value field should have mixed types (integer, float, string)
    value_field = schema.fields["value"]
    assert isinstance(value_field.field_type, set) or "mixed" in str(value_field.field_type).lower()


def test_scan_directory(temp_data_dir):
    """Test scanning entire directory."""
    reader = SchemaReader(data_dir=str(temp_data_dir))
    schemas = reader.scan_directory()
    
    assert len(schemas) == 3
    assert "test_array.json" in schemas
    assert "test_ndjson.json" in schemas
    assert "test_mixed.json" in schemas


def test_generate_report(temp_data_dir):
    """Test schema report generation."""
    import tempfile
    
    reader = SchemaReader(data_dir=str(temp_data_dir))
    schemas = reader.scan_directory()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        report_path = Path(temp_dir) / "test_report.md"
        generated_path = reader.generate_report(str(report_path))
        
        assert Path(generated_path).exists()
        
        content = Path(generated_path).read_text()
        assert "# JSON Schema Report" in content
        assert "test_array.json" in content
        assert "test_ndjson.json" in content


def test_sampling_strategy(temp_data_dir):
    """Test sampling strategy."""
    reader = SchemaReader(data_dir=str(temp_data_dir), max_sample_size=2)
    schema = reader.infer_schema(temp_data_dir / "test_array.json")
    
    assert schema is not None
    # With streaming optimization, we only read up to max_sample_size
    assert schema.record_count == 2

