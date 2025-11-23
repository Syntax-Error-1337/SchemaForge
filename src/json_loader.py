"""
JSON Loader Module

This module provides functionality to load JSON files in various formats,
including standard JSON, NDJSON, and streaming for large files.
"""

import json
import logging
import ast
from pathlib import Path
from typing import List, Dict, Any, Generator, Union, Optional

import ijson
import json5

logger = logging.getLogger(__name__)

def load_json_file(filepath: Path, stream: bool = False) -> Union[List[Dict[str, Any]], Generator[Dict[str, Any], None, None]]:
    """
    Load JSON data from a file, handling multiple formats.
    
    Args:
        filepath: Path to the JSON file.
        stream: If True, returns a generator for streaming records (useful for large files).
                Note: Streaming is best effort and may fall back to loading all if structure allows.
    
    Returns:
        List of records or a generator yielding records.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    if stream:
        return _load_json_stream(filepath)
    else:
        return _load_json_memory(filepath)

def _load_json_stream(filepath: Path) -> Generator[Dict[str, Any], None, None]:
    """Stream records from a JSON file using ijson."""
    try:
        # Try to detect if it's an array or object
        with open(filepath, 'rb') as f:
            # Check first non-whitespace character
            while True:
                char = f.read(1)
                if not char or not char.isspace():
                    break
            
            f.seek(0)
            
            if char == b'[':
                # Array of objects
                # ijson.items(f, 'item') yields each item in the top-level array
                for item in ijson.items(f, 'item'):
                    if isinstance(item, dict):
                        yield item
                    elif isinstance(item, list):
                        yield {f"column_{i}": val for i, val in enumerate(item)}
                    else:
                        yield {"value": item}
            elif char == b'{':
                # Single object or wrapper
                # For simplicity in streaming, we might just yield the whole object if it's not a known wrapper
                # But let's try to find common wrapper keys
                # This is tricky with ijson without parsing the whole thing.
                # A common pattern is { "data": [...] }
                # We can try to stream specific paths
                
                # Strategy: Try to find a large array under common keys
                common_keys = ['data', 'results', 'items', 'records', 'rows', 'entries', 'features']
                found_wrapper = False
                
                for key in common_keys:
                    try:
                        # This will raise an error if the key doesn't exist or isn't an array immediately?
                        # ijson.items is lazy.
                        # We need a way to peek or try multiple.
                        # Re-opening file for each attempt is safe but slow.
                        f.seek(0)
                        # 'key.item' means: value of 'key' -> iterate over items in that array
                        for item in ijson.items(f, f'{key}.item'):
                            if isinstance(item, dict):
                                yield item
                            elif isinstance(item, list):
                                yield {f"column_{i}": val for i, val in enumerate(item)}
                            else:
                                yield {"value": item}
                        found_wrapper = True
                        break
                    except Exception:
                        continue
                
                if not found_wrapper:
                    # Treat as single record
                    f.seek(0)
                    data = json.load(f) # Fallback to standard load for single object
                    if isinstance(data, dict):
                        yield data
            else:
                # Maybe NDJSON?
                f.seek(0)
                # ijson doesn't natively support NDJSON well in the same way as 'items'
                # We can just read line by line
                for line in f:
                    line = line.strip()
                    if not line: continue
                    try:
                        item = json.loads(line)
                        if isinstance(item, dict):
                            yield item
                        elif isinstance(item, list):
                            yield {f"column_{i}": val for i, val in enumerate(item)}
                        else:
                            yield {"value": item}
                    except json.JSONDecodeError:
                        pass

    except Exception as e:
        # Log as warning instead of error since we have a fallback
        logger.warning(f"Streaming failed for {filepath}: {e}. Falling back to memory load.")
        yield from _load_json_memory(filepath)

def _load_json_memory(filepath: Path) -> List[Dict[str, Any]]:
    """Load JSON file completely into memory."""
    records = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            
            if not content:
                return []

            # 1. Try Standard JSON
            try:
                data = json.loads(content)
                return _normalize_data(data)
            except json.JSONDecodeError:
                pass
            
            # 2. Try JSON5 (relaxed JSON)
            try:
                data = json5.loads(content)
                return _normalize_data(data)
            except Exception:
                pass
            
            # 3. Try Python Literal
            try:
                data = ast.literal_eval(content)
                return _normalize_data(data)
            except Exception:
                pass
            
            # 4. Try NDJSON
            records = []
            for line in content.splitlines():
                line = line.strip()
                if not line: continue
                try:
                    record = json.loads(line)
                    if isinstance(record, dict):
                        records.append(record)
                except json.JSONDecodeError:
                    try:
                        record = ast.literal_eval(line)
                        if isinstance(record, dict):
                            records.append(record)
                    except Exception:
                        pass
            
            if records:
                return records
            
            logger.warning(f"Could not parse {filepath} with any known method.")
            return []

    except Exception as e:
        logger.error(f"Error loading file {filepath}: {e}")
        return []

def _normalize_data(data: Any) -> List[Dict[str, Any]]:
    """Normalize loaded data into a list of dictionaries."""
    if isinstance(data, list):
        if not data:
            return []
            
        # Check for array of arrays
        if isinstance(data[0], list):
             # Convert array-based rows to objects with generic keys
             normalized = []
             for row in data:
                 if isinstance(row, list):
                     normalized.append({f"column_{i}": val for i, val in enumerate(row)})
             return normalized
        
        # Check for array of primitives (not dicts)
        if not isinstance(data[0], dict):
            # Convert list of primitives to objects with a generic key
            return [{"value": item} for item in data]
        
        # Filter for dicts (standard case)
        return [item for item in data if isinstance(item, dict)]
    
    elif isinstance(data, dict):
        # Check for wrapper keys
        common_keys = ['data', 'results', 'items', 'records', 'rows', 'entries', 'features']
        for key in common_keys:
            if key in data and isinstance(data[key], list):
                # Check if it's GeoJSON features
                if key == 'features' and data.get('type') == 'FeatureCollection':
                     return [f.get('properties', {}) for f in data['features'] if isinstance(f, dict)]
                
                # Recursively normalize the inner list
                return _normalize_data(data[key])
        
        # Check for Socrata/OpenData format (meta + data array of arrays)
        if 'meta' in data and 'data' in data:
             # This might have been caught by the loop above if 'data' is a list
             # But if 'data' is a list of lists, the recursive call handles it.
             pass

        # Check for GeoJSON Feature
        if data.get('type') == 'Feature':
            return [data.get('properties', {})]
            
        # Single record
        return [data]
    
    return []
