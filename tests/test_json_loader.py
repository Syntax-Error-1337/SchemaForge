import pytest
import json
import tempfile
from pathlib import Path
from src.json_loader import load_json_file

@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmp:
        yield Path(tmp)

def test_load_standard_json(temp_dir):
    data = [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]
    filepath = temp_dir / "test.json"
    with open(filepath, "w") as f:
        json.dump(data, f)
    
    records = load_json_file(filepath)
    assert len(records) == 2
    assert records[0]["id"] == 1

def test_load_ndjson(temp_dir):
    data = '{"id": 1, "name": "test"}\n{"id": 2, "name": "test2"}'
    filepath = temp_dir / "test.ndjson"
    with open(filepath, "w") as f:
        f.write(data)
    
    records = load_json_file(filepath)
    assert len(records) == 2
    assert records[1]["name"] == "test2"

def test_load_json5(temp_dir):
    data = """
    [
        {
            id: 1, // comment
            name: 'test',
        }
    ]
    """
    filepath = temp_dir / "test.json5"
    with open(filepath, "w") as f:
        f.write(data)
    
    records = load_json_file(filepath)
    assert len(records) == 1
    assert records[0]["id"] == 1

def test_streaming(temp_dir):
    data = [{"id": i} for i in range(100)]
    filepath = temp_dir / "large.json"
    with open(filepath, "w") as f:
        json.dump(data, f)
    
    # Test streaming
    records_iter = load_json_file(filepath, stream=True)
    records = list(records_iter)
    assert len(records) == 100
    assert records[99]["id"] == 99

def test_wrapper_object(temp_dir):
    data = {"data": [{"id": 1}, {"id": 2}], "meta": {"count": 2}}
    filepath = temp_dir / "wrapper.json"
    with open(filepath, "w") as f:
        json.dump(data, f)
    
    records = load_json_file(filepath)
    assert len(records) == 2
    assert records[0]["id"] == 1
