import pytest
import json
import tempfile
from pathlib import Path
from src.json_loader import load_json_file
from src.schema_reader import SchemaReader

@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmp:
        yield Path(tmp)

def test_load_list_of_lists(temp_dir):
    # Mimic rows.json structure (often array of arrays)
    data = [
        ["id", "name", "value"], # Header-like row, but we treat as data if no metadata
        [1, "A", 10],
        [2, "B", 20]
    ]
    filepath = temp_dir / "rows_simple.json"
    with open(filepath, "w") as f:
        json.dump(data, f)
    
    records = load_json_file(filepath)
    assert len(records) == 3
    # Should be normalized to dicts with column_N keys
    assert records[1]["column_0"] == 1
    assert records[1]["column_1"] == "A"

def test_load_list_of_primitives(temp_dir):
    data = [1, 2, 3, 4, 5]
    filepath = temp_dir / "primitives.json"
    with open(filepath, "w") as f:
        json.dump(data, f)
    
    records = load_json_file(filepath)
    assert len(records) == 5
    assert records[0]["value"] == 1

def test_schema_inference_list_of_lists(temp_dir):
    data = [
        [1, "A"],
        [2, "B"]
    ]
    filepath = temp_dir / "rows_schema.json"
    with open(filepath, "w") as f:
        json.dump(data, f)
    
    reader = SchemaReader(data_dir=str(temp_dir))
    schema = reader.infer_schema(filepath)
    
    assert schema is not None
    assert "column_0" in schema.fields
    assert schema.fields["column_0"].field_type == "integer"
    assert "column_1" in schema.fields
    assert schema.fields["column_1"].field_type == "string"

def test_streaming_fallback_on_error(temp_dir):
    # Create a file that might fail streaming (e.g. valid JSON but complex wrapper)
    # or just ensure fallback works.
    # Here we create a file that ijson might struggle with if we expect array but get object
    data = {"meta": {}, "data": [{"id": 1}]}
    filepath = temp_dir / "wrapper_fallback.json"
    with open(filepath, "w") as f:
        json.dump(data, f)
        
    # Force streaming
    records_iter = load_json_file(filepath, stream=True)
    records = list(records_iter)
    assert len(records) == 1
    assert records[0]["id"] == 1
