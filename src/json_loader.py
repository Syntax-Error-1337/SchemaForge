"""
JSON Loader Module

This module provides functionality to load JSON files in various formats,
including standard JSON, NDJSON, and streaming for large files.
"""

import json
import logging
import ast
import warnings
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
                If False but file is > 50MB, will automatically use streaming.
    
    Returns:
        List of records or a generator yielding records.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    # Automatically use streaming for large files (> 50MB) even if stream=False
    file_size_mb = filepath.stat().st_size / (1024 * 1024)
    if not stream and file_size_mb > 50:
        logger.info(f"File {filepath.name} is {file_size_mb:.1f}MB. Using streaming for efficiency.")
        stream = True

    if stream:
        return _load_json_stream(filepath)
    else:
        return _load_json_memory(filepath)

def _load_json_stream(filepath: Path) -> Generator[Dict[str, Any], None, None]:
    """Stream records from a JSON file using ijson."""
    try:
        # First, try to detect NDJSON format by checking if there are multiple lines
        # with JSON objects (common for large datasets)
        is_ndjson = False
        with open(filepath, 'rb') as f:
            # Read first few lines to detect format
            first_lines = []
            for i, line in enumerate(f):
                if i >= 3:  # Check first 3 lines
                    break
                line = line.strip()
                if line:
                    first_lines.append(line)
            
            # If we have multiple non-empty lines, check if each is valid JSON
            if len(first_lines) >= 2:
                try:
                    # Try parsing each line as JSON
                    for line in first_lines:
                        json.loads(line.decode('utf-8'))
                    # If all lines are valid JSON, it's likely NDJSON
                    is_ndjson = True
                except (json.JSONDecodeError, UnicodeDecodeError):
                    pass
        
        # If detected as NDJSON, process line by line
        if is_ndjson:
            with open(filepath, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        item = json.loads(line)
                        if isinstance(item, dict):
                            yield item
                        elif isinstance(item, list):
                            yield {f"column_{i}": val for i, val in enumerate(item)}
                        else:
                            yield {"value": item}
                    except json.JSONDecodeError as e:
                        logger.debug(f"Skipping invalid JSON on line {line_num}: {e}")
                        continue
            return
        
        # Not NDJSON, try standard JSON formats
        with open(filepath, 'rb') as f:
            # Check first non-whitespace character
            first_char = None
            while True:
                char = f.read(1)
                if not char:
                    break
                if not char.isspace():
                    first_char = char
                    break
            
            if not first_char:
                logger.warning(f"File {filepath} appears to be empty")
                return
            
            f.seek(0)
            
            if first_char == b'[':
                # Array of objects
                # ijson.items(f, 'item') yields each item in the top-level array
                for item in ijson.items(f, 'item'):
                    if isinstance(item, dict):
                        yield item
                    elif isinstance(item, list):
                        yield {f"column_{i}": val for i, val in enumerate(item)}
                    else:
                        yield {"value": item}
            elif first_char == b'{':
                # Single object or wrapper
                # Strategy: Try to find a large array under common keys
                common_keys = ['data', 'results', 'items', 'records', 'rows', 'entries', 'features']
                found_wrapper = False
                
                for key in common_keys:
                    try:
                        f.seek(0)
                        # 'key.item' means: value of 'key' -> iterate over items in that array
                        item_count = 0
                        for item in ijson.items(f, f'{key}.item'):
                            item_count += 1
                            if isinstance(item, dict):
                                yield item
                            elif isinstance(item, list):
                                yield {f"column_{i}": val for i, val in enumerate(item)}
                            else:
                                yield {"value": item}
                        if item_count > 0:
                            found_wrapper = True
                            break
                    except (ijson.JSONError, StopIteration):
                        continue
                    except Exception:
                        continue
                
                if not found_wrapper:
                    # Could be a single object or NDJSON that starts with {
                    # Try NDJSON as fallback before attempting full load
                    f.seek(0)
                    try:
                        # Check if second line exists and is valid JSON
                        lines_checked = 0
                        for line in f:
                            lines_checked += 1
                            if lines_checked > 2:
                                # Multiple lines exist, treat as NDJSON
                                f.seek(0)
                                for line in f:
                                    line = line.strip()
                                    if not line:
                                        continue
                                    try:
                                        item = json.loads(line.decode('utf-8'))
                                        if isinstance(item, dict):
                                            yield item
                                        elif isinstance(item, list):
                                            yield {f"column_{i}": val for i, val in enumerate(item)}
                                        else:
                                            yield {"value": item}
                                    except (json.JSONDecodeError, UnicodeDecodeError):
                                        pass
                                return
                            if lines_checked == 1:
                                # Only one line, try as single JSON object
                                f.seek(0)
                                data = json.load(f)
                                if isinstance(data, dict):
                                    yield data
                                return
                    except Exception:
                        # If NDJSON detection fails, try single object
                        f.seek(0)
                        try:
                            data = json.load(f)
                            if isinstance(data, dict):
                                yield data
                        except json.JSONDecodeError:
                            raise ValueError(f"Could not parse {filepath} as JSON array, object, or NDJSON")
            else:
                # Maybe NDJSON starting with something else?
                f.seek(0)
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        item = json.loads(line.decode('utf-8'))
                        if isinstance(item, dict):
                            yield item
                        elif isinstance(item, list):
                            yield {f"column_{i}": val for i, val in enumerate(item)}
                        else:
                            yield {"value": item}
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        pass

    except Exception as e:
        # For very large files, don't fall back to memory load
        file_size_mb = filepath.stat().st_size / (1024 * 1024)
        if file_size_mb > 100:
            logger.error(f"Streaming failed for large file {filepath} ({file_size_mb:.1f}MB): {e}")
            logger.error("Cannot fall back to memory load for files > 100MB. Please ensure the file is in a supported format (JSON array, NDJSON, or wrapped JSON).")
            raise
        else:
            # Log as warning and try fallback for smaller files
            logger.warning(f"Streaming failed for {filepath}: {e}. Falling back to memory load.")
            # Convert list to generator for consistency
            records = _load_json_memory(filepath)
            for record in records:
                yield record

def _load_json_memory(filepath: Path) -> List[Dict[str, Any]]:
    """Load JSON file completely into memory."""
    records = []
    try:
        # For very large files, prefer streaming even in memory mode
        # But this function is only called for smaller files now
        with open(filepath, 'r', encoding='utf-8') as f:
            # Use read() but with a size hint for better memory management
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
            
            # 3. Try Python Literal (with warning suppression for escape sequences)
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=SyntaxWarning)
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
                        with warnings.catch_warnings():
                            warnings.filterwarnings("ignore", category=SyntaxWarning)
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
        import traceback
        logger.error(f"Error loading file {filepath}: {e}")
        logger.debug(f"Traceback: {traceback.format_exc()}")
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
