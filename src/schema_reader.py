"""
Schema Reader Module

This module provides functionality to scan JSON files, infer their schemas,
and generate human-readable schema reports.
"""

import json
import os
import ast
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Set
from collections import defaultdict
from datetime import datetime
import concurrent.futures
from src.json_loader import load_json_file

logger = logging.getLogger(__name__)


class SchemaField:
    """Represents a single field in a schema with its properties."""
    
    def __init__(self, name: str, field_type: Union[str, Set[str]], nullable: bool = False,
                 example_value: Any = None, is_nested: bool = False, nested_fields: Optional[Dict] = None,
                 distinct_values: Optional[Set[Any]] = None, min_value: Optional[Any] = None,
                 max_value: Optional[Any] = None, min_length: Optional[int] = None,
                 max_length: Optional[int] = None, avg_length: Optional[float] = None):
        self.name = name
        self.field_type = field_type
        self.nullable = nullable
        self.example_value = example_value
        self.is_nested = is_nested
        self.nested_fields = nested_fields or {}
        self.distinct_values = distinct_values or set()
        self.min_value = min_value
        self.max_value = max_value
        self.min_length = min_length
        self.max_length = max_length
        self.avg_length = avg_length
    
    def __repr__(self):
        type_str = self.field_type if isinstance(self.field_type, str) else f"mixed({', '.join(sorted(self.field_type))})"
        return f"SchemaField(name={self.name}, type={type_str}, nullable={self.nullable})"


class FileSchema:
    """Represents the schema of a single JSON file."""
    
    def __init__(self, filename: str, record_count: int, fields: Dict[str, SchemaField]):
        self.filename = filename
        self.record_count = record_count
        self.fields = fields
    
    def __repr__(self):
        return f"FileSchema(filename={self.filename}, records={self.record_count}, fields={len(self.fields)})"


class SchemaReader:
    """Main class for reading and inferring schemas from JSON files."""
    
    def __init__(self, data_dir: str = "data", max_sample_size: Optional[int] = None,
                 sampling_strategy: str = "first"):
        """
        Initialize the SchemaReader.
        
        Args:
            data_dir: Directory containing JSON files
            max_sample_size: Maximum number of records to sample per file (None = all)
            sampling_strategy: 'first' or 'random' sampling strategy
        """
        self.data_dir = Path(data_dir)
        self.max_sample_size = max_sample_size
        self.sampling_strategy = sampling_strategy
        self.schemas: Dict[str, FileSchema] = {}
    
    def _infer_type(self, value: Any) -> str:
        """Infer the type of a Python value."""
        if value is None:
            return "null"
        elif isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "integer"
        elif isinstance(value, float):
            return "float"
        elif isinstance(value, str):
            # Try to detect special string types
            if self._looks_like_timestamp(value):
                return "timestamp"
            elif self._looks_like_url(value):
                return "url"
            elif self._looks_like_email(value):
                return "email"
            elif self._looks_like_uuid(value):
                return "uuid"
            elif self._looks_like_ip_address(value):
                return "ip_address"
            elif self._looks_like_numeric_string(value):
                # Could be a numeric string that should be parsed
                return "numeric_string"
            elif self._looks_like_json_string(value):
                # Embedded JSON string
                return "json_string"
            return "string"
        elif isinstance(value, list):
            return "array"
        elif isinstance(value, dict):
            return "object"
        else:
            return "unknown"
    
    def _looks_like_timestamp(self, value: str) -> bool:
        """Check if a string looks like a timestamp."""
        if not isinstance(value, str) or len(value) < 10:
            return False
        
        import re
        # Common timestamp patterns
        patterns = [
            r'^\d{4}-\d{2}-\d{2}',  # ISO date (YYYY-MM-DD)
            r'^\d{4}-\d{2}-\d{2}T',  # ISO datetime (YYYY-MM-DDTHH:MM:SS)
            r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}',  # Date time with space
            r'^\d{4}/\d{2}/\d{2}',  # Date with slashes
            r'^\d{2}/\d{2}/\d{4}',  # US date format
            r'^\d{10}$',  # Unix timestamp (seconds)
            r'^\d{13}$',  # Unix timestamp (milliseconds)
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}',  # ISO with timezone
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z',  # ISO with Z
        ]
        
        for pattern in patterns:
            if re.match(pattern, value):
                return True
        return False
    
    def _looks_like_url(self, value: str) -> bool:
        """Check if a string looks like a URL."""
        if not isinstance(value, str):
            return False
        
        import re
        # Basic URL pattern
        url_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        return bool(re.match(url_pattern, value, re.IGNORECASE))
    
    def _looks_like_email(self, value: str) -> bool:
        """Check if a string looks like an email address."""
        if not isinstance(value, str):
            return False
        
        import re
        # Basic email pattern
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(email_pattern, value))
    
    def _looks_like_uuid(self, value: str) -> bool:
        """Check if a string looks like a UUID."""
        if not isinstance(value, str):
            return False
        
        import re
        # UUID pattern (with or without hyphens)
        uuid_pattern = r'^[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}$'
        return bool(re.match(uuid_pattern, value, re.IGNORECASE))
    
    def _looks_like_ip_address(self, value: str) -> bool:
        """Check if a string looks like an IP address."""
        if not isinstance(value, str):
            return False
        
        import re
        # IPv4 pattern
        ipv4_pattern = r'^(\d{1,3}\.){3}\d{1,3}$'
        if re.match(ipv4_pattern, value):
            # Validate each octet is 0-255
            try:
                parts = value.split('.')
                if all(0 <= int(part) <= 255 for part in parts):
                    return True
            except ValueError:
                pass
        
        # IPv6 pattern (simplified)
        ipv6_pattern = r'^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$|^::1$|^::$'
        return bool(re.match(ipv6_pattern, value))
    
    def _looks_like_numeric_string(self, value: str) -> bool:
        """Check if a string contains only numeric characters (could be parsed as number)."""
        if not isinstance(value, str) or not value.strip():
            return False
        
        import re
        # Matches integers, floats, scientific notation, with optional leading/trailing whitespace
        numeric_pattern = r'^\s*[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?\s*$'
        return bool(re.match(numeric_pattern, value))
    
    def _looks_like_json_string(self, value: str) -> bool:
        """Check if a string looks like embedded JSON."""
        if not isinstance(value, str) or len(value) < 2:
            return False
        
        value = value.strip()
        # Check if it starts and ends with JSON-like delimiters
        if (value.startswith('{') and value.endswith('}')) or \
           (value.startswith('[') and value.endswith(']')):
            # Try to parse it
            try:
                json.loads(value)
                return True
            except (json.JSONDecodeError, ValueError):
                pass
        
        return False
    
    def _flatten_dict(self, d: Dict, parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
        """Flatten a nested dictionary."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                # Array of objects - store as array type but note the structure
                items.append((new_key, v))
            else:
                items.append((new_key, v))
        return dict(items)
    
    def _analyze_field(self, values: List[Any], field_name: str) -> SchemaField:
        """Analyze a list of values for a field and infer its schema."""
        if not values:
            return SchemaField(field_name, "unknown", nullable=True)
        
        # Collect all non-null values
        non_null_values = [v for v in values if v is not None]
        nullable = len(non_null_values) < len(values)
        
        if not non_null_values:
            return SchemaField(field_name, "null", nullable=True)
        
        # Infer types for all values
        types = set()
        example_value = None
        
        # Statistics collection
        numeric_values = []
        string_lengths = []
        distinct_values_set = set()
        min_val = None
        max_val = None
        
        for value in non_null_values:
            inferred_type = self._infer_type(value)
            types.add(inferred_type)
            
            if example_value is None:
                example_value = value
            
            # Collect distinct values for enum detection
            try:
                # Use hashable representation for distinct values
                if isinstance(value, (str, int, float, bool)):
                    distinct_values_set.add(value)
                elif isinstance(value, list):
                    distinct_values_set.add(tuple(value) if len(value) <= 10 else str(value))
                elif isinstance(value, dict):
                    distinct_values_set.add(str(sorted(value.items())) if len(value) <= 10 else str(value))
                else:
                    distinct_values_set.add(str(value))
            except (TypeError, ValueError):
                pass
            
            # Collect numeric statistics
            if isinstance(value, (int, float)):
                numeric_values.append(value)
                if min_val is None or value < min_val:
                    min_val = value
                if max_val is None or value > max_val:
                    max_val = value
            elif isinstance(value, str) and self._looks_like_numeric_string(value):
                try:
                    num_val = float(value)
                    numeric_values.append(num_val)
                    if min_val is None or num_val < min_val:
                        min_val = num_val
                    if max_val is None or num_val > max_val:
                        max_val = num_val
                except (ValueError, TypeError):
                    pass
            
            # Collect string length statistics
            if isinstance(value, str):
                string_lengths.append(len(value))
            
            # Handle nested objects
            if isinstance(value, dict):
                nested_fields = {}
                for nested_key, nested_val in value.items():
                    nested_field = self._analyze_field([nested_val], f"{field_name}.{nested_key}")
                    nested_fields[nested_key] = nested_field
                return SchemaField(
                    field_name,
                    "object",
                    nullable=nullable,
                    example_value=example_value,
                    is_nested=True,
                    nested_fields=nested_fields
                )
            
            # Handle arrays
            if isinstance(value, list) and len(value) > 0:
                array_types = set()
                for item in value[:10]:  # Sample first 10 items
                    array_types.add(self._infer_type(item))
                
                if len(array_types) == 1:
                    array_type = list(array_types)[0]
                    return SchemaField(
                        field_name,
                        f"array<{array_type}>",
                        nullable=nullable,
                        example_value=example_value
                    )
                else:
                    return SchemaField(
                        field_name,
                        f"array<mixed>",
                        nullable=nullable,
                        example_value=example_value
                    )
        
        # Determine final type
        if len(types) == 1:
            final_type = list(types)[0]
        else:
            final_type = types  # Mixed types
        
        # Calculate statistics
        min_value = min_val if numeric_values else None
        max_value = max_val if numeric_values else None
        min_length = min(string_lengths) if string_lengths else None
        max_length = max(string_lengths) if string_lengths else None
        avg_length = sum(string_lengths) / len(string_lengths) if string_lengths else None
        
        # Detect enum (if distinct values are limited and represent a small percentage)
        # Consider it an enum if there are <= 20 distinct values and they represent > 50% of non-null values
        distinct_count = len(distinct_values_set)
        enum_threshold = min(20, len(non_null_values) // 2)
        distinct_values = distinct_values_set if distinct_count <= enum_threshold else set()
        
        return SchemaField(
            field_name,
            final_type,
            nullable=nullable,
            example_value=example_value,
            distinct_values=distinct_values,
            min_value=min_value,
            max_value=max_value,
            min_length=min_length,
            max_length=max_length,
            avg_length=avg_length
        )
    
    def _extract_columns_from_metadata(self, metadata: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """Extract column definitions from metadata (e.g., Socrata/OpenData format)."""
        # Try various metadata paths
        metadata_paths = [
            ['meta', 'view', 'columns'],
            ['view', 'columns'],
            ['columns'],
            ['schema', 'fields'],
            ['fields'],
            ['header']
        ]
        
        for path in metadata_paths:
            current = metadata
            found = True
            for key in path:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    found = False
                    break
            
            if found and isinstance(current, list) and len(current) > 0:
                # Check if it looks like column definitions
                if isinstance(current[0], dict) and ('name' in current[0] or 'fieldName' in current[0]):
                    logger.info(f"Found column definitions in metadata path: {' -> '.join(path)}")
                    return current
        
        return None
    
    def _convert_array_row_to_object(self, row: List[Any], columns: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Convert an array-based row to an object using column definitions."""
        record = {}
        
        for col_def in columns:
            # Get column name - try multiple field names
            col_name = None
            for name_field in ['fieldName', 'name', 'id', 'key']:
                if name_field in col_def:
                    col_name = str(col_def[name_field])
                    break
            
            if col_name is None:
                # Fallback to position-based name
                position = col_def.get('position', len(record))
                col_name = f"column_{position}"
            
            # Skip hidden/meta columns if flagged
            if col_def.get('flags') and 'hidden' in col_def.get('flags', []):
                continue
            
            # Skip meta_data type columns by default (can be overridden)
            if col_def.get('dataTypeName') == 'meta_data' and 'hidden' in col_def.get('flags', []):
                continue
            
            # Get value from row using position
            # Position in Socrata/OpenData format is 0-indexed and matches array index
            # But we need to account for hidden columns that might be skipped
            position = col_def.get('position', -1)
            
            # If position is valid and within row bounds
            if position >= 0 and position < len(row):
                value = row[position]
            else:
                # Fallback: try to use index in columns list
                col_idx = columns.index(col_def) if col_def in columns else -1
                if col_idx >= 0 and col_idx < len(row):
                    value = row[col_idx]
                else:
                    continue  # Skip if we can't map this column
            
            # Clean column name (remove : prefix if present)
            clean_name = col_name.lstrip(':')
            record[clean_name] = value
        
        return record
    
    # _load_json_file removed in favor of src.json_loader.load_json_file
    
    def _sample_records(self, records: List[Dict[str, Any]], sample_size: Optional[int] = None) -> List[Dict[str, Any]]:
        """Sample records based on the sampling strategy."""
        size = sample_size if sample_size is not None else self.max_sample_size
        
        if size is None or len(records) <= size:
            return records
        
        if self.sampling_strategy == "random":
            import random
            return random.sample(records, size)
        else:  # default to "first"
            return records[:size]
    
    def infer_schema(self, filepath: Path) -> Optional[FileSchema]:
        """Infer schema for a single JSON file."""
        logger.info(f"Processing file: {filepath.name}")
        
        # Default to a reasonable sample size if not specified to avoid "stuck" analysis on huge files
        # 100,000 records is usually plenty for schema inference
        effective_sample_size = self.max_sample_size
        if effective_sample_size is None:
            effective_sample_size = 10000
            logger.info(f"No max_sample_size set. Defaulting to {effective_sample_size} for performance.")

        try:
            # Use streaming for potentially large files if max_sample_size is set but small
            # or if we just want to be efficient.
            # For schema inference, we often need to see many records.
            # If sampling is 'first', streaming is great.
            # If sampling is 'random', we need to load more or reservoir sample.
            
            stream = self.sampling_strategy == "first"
            records_iter = load_json_file(filepath, stream=stream)
            
            if stream:
                # Consume iterator to get list if we need random access later or just to simplify
                # But if we only need first N, we can slice the iterator
                if effective_sample_size and self.sampling_strategy == "first":
                    import itertools
                    records = list(itertools.islice(records_iter, effective_sample_size))
                    # We don't know total count without reading all
                    actual_record_count = -1 # Unknown
                else:
                    records = list(records_iter)
                    actual_record_count = len(records)
            else:
                records = records_iter
                actual_record_count = len(records)
            
            if not records:
                logger.warning(f"No records found in {filepath.name}")
                return None
            
            if actual_record_count == -1:
                 # If we streamed a subset, we might not know the total. 
                 # For now, let's just say "at least X" or just use the sample count
                 actual_record_count = len(records)

            # Sample records if needed (if we didn't already limit during load)
            if self.sampling_strategy == "random" and effective_sample_size and len(records) > effective_sample_size:
                 sampled_records = self._sample_records(records, effective_sample_size)
            elif self.sampling_strategy == "first" and effective_sample_size and len(records) > effective_sample_size:
                 sampled_records = records[:effective_sample_size]
            else:
                 sampled_records = records
            
            sampled_count = len(sampled_records)
            logger.info(f"Analyzing {sampled_count} of {actual_record_count} records from {filepath.name}")
            
            # Collect all field values
            field_values: Dict[str, List[Any]] = defaultdict(list)
            
            for record in sampled_records:
                if not isinstance(record, dict):
                    # Should be handled by json_loader normalization, but extra safety
                    logger.warning(f"Skipping non-dict record in {filepath.name}: {type(record)}")
                    continue
                    
                # Flatten nested structures
                flat_record = self._flatten_dict(record)
                for key, value in flat_record.items():
                    # Try to parse embedded JSON strings
                    if isinstance(value, str) and self._looks_like_json_string(value):
                        try:
                            parsed = json.loads(value)
                            # If it's a dict or list, we might want to flatten it further
                            # For now, keep both the string and parsed version for analysis
                            field_values[key].append(value)  # Keep original
                            # Also add parsed version with a suffix
                            if isinstance(parsed, dict):
                                for nested_key, nested_val in parsed.items():
                                    field_values[f"{key}.parsed.{nested_key}"].append(nested_val)
                        except (json.JSONDecodeError, ValueError):
                            field_values[key].append(value)
                    else:
                        field_values[key].append(value)
            
            # Analyze each field
            fields = {}
            for field_name, values in field_values.items():
                field = self._analyze_field(values, field_name)
                fields[field_name] = field
            
            schema = FileSchema(filepath.name, actual_record_count, fields)
            logger.info(f"Successfully inferred schema for {filepath.name}: {len(fields)} fields")
            
            return schema
        
        except Exception as e:
            logger.error(f"Failed to infer schema for {filepath.name}: {e}")
            return None
    
    def scan_directory(self) -> Dict[str, FileSchema]:
        """Scan the data directory and infer schemas for all JSON files."""
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")
        
        json_files = list(self.data_dir.glob("*.json"))
        
        if not json_files:
            logger.warning(f"No JSON files found in {self.data_dir}")
            return {}
        
        logger.info(f"Found {len(json_files)} JSON file(s) in {self.data_dir}")
        
        # Use ProcessPoolExecutor for parallel processing
        max_workers = min(len(json_files), 4)  # Cap at 4 workers or number of files
        if max_workers < 1: max_workers = 1
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {executor.submit(self.infer_schema, json_file): json_file for json_file in json_files}
            
            for future in concurrent.futures.as_completed(future_to_file):
                json_file = future_to_file[future]
                try:
                    schema = future.result()
                    if schema:
                        self.schemas[json_file.name] = schema
                except Exception as e:
                    logger.error(f"File {json_file.name} generated an exception: {e}")
        
        return self.schemas
    
    def generate_report(self, output_path: str = "reports/schema_report.md") -> str:
        """Generate a human-readable schema report."""
        if not self.schemas:
            logger.warning("No schemas available. Run scan_directory() first.")
            return ""
        
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        lines = []
        lines.append("# JSON Schema Report")
        lines.append("")
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        lines.append("---")
        lines.append("")
        
        for filename, schema in self.schemas.items():
            lines.append(f"## File: {filename}")
            lines.append("")
            lines.append(f"- **Records Scanned:** {schema.record_count}")
            lines.append(f"- **Fields Detected:** {len(schema.fields)}")
            lines.append("")
            lines.append("### Field Details")
            lines.append("")
            lines.append("| Field Name | Type | Nullable | Example Value | Statistics | Notes |")
            lines.append("|------------|------|----------|---------------|------------|-------|")
            
            for field_name in sorted(schema.fields.keys()):
                field = schema.fields[field_name]
                
                # Format type
                if isinstance(field.field_type, set):
                    type_str = f"mixed({', '.join(sorted(field.field_type))})"
                else:
                    type_str = field.field_type
                
                # Format example value
                example_str = str(field.example_value)
                if len(example_str) > 50:
                    example_str = example_str[:47] + "..."
                example_str = example_str.replace("|", "\\|")  # Escape pipe for markdown
                
                # Format statistics
                stats = []
                if field.min_value is not None and field.max_value is not None:
                    stats.append(f"min: {field.min_value}, max: {field.max_value}")
                if field.min_length is not None and field.max_length is not None:
                    if field.avg_length is not None:
                        stats.append(f"len: {field.min_length}-{field.max_length} (avg: {field.avg_length:.1f})")
                    else:
                        stats.append(f"len: {field.min_length}-{field.max_length}")
                if field.distinct_values and len(field.distinct_values) <= 10:
                    # Show enum values if small set
                    enum_vals = sorted([str(v) for v in list(field.distinct_values)[:10]])
                    if len(enum_vals) <= 5:
                        stats.append(f"enum: {', '.join(enum_vals)}")
                    else:
                        stats.append(f"enum: {len(field.distinct_values)} values")
                stats_str = "; ".join(stats) if stats else "-"
                stats_str = stats_str.replace("|", "\\|")  # Escape pipe for markdown
                
                # Notes
                notes = []
                if field.nullable:
                    notes.append("nullable")
                if field.is_nested:
                    notes.append("nested")
                if isinstance(field.field_type, set):
                    notes.append("mixed types")
                if field.distinct_values and len(field.distinct_values) <= 20:
                    notes.append("enum-like")
                notes_str = ", ".join(notes) if notes else "-"
                
                lines.append(f"| `{field_name}` | {type_str} | {'Yes' if field.nullable else 'No'} | `{example_str}` | {stats_str} | {notes_str} |")
            
            lines.append("")
            lines.append("---")
            lines.append("")
        
        report_content = "\n".join(lines)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.info(f"Schema report written to {output_file}")
        
        # Also save schemas in JSON format for machine reading
        json_path = output_file.with_suffix('.json')
        self.save_schemas_to_json(str(json_path))
        
        return str(output_file)
    
    def save_schemas_to_json(self, output_path: str = "reports/schema_report.json") -> str:
        """Save schemas to JSON format for machine reading."""
        if not self.schemas:
            logger.warning("No schemas available. Run scan_directory() first.")
            return ""
        
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        schemas_dict = {}
        for filename, schema in self.schemas.items():
            schema_data = {
                "filename": schema.filename,
                "record_count": schema.record_count,
                "fields": {}
            }
            
            for field_name, field in schema.fields.items():
                # Convert field_type to serializable format
                if isinstance(field.field_type, set):
                    field_type_serialized = list(field.field_type)
                else:
                    field_type_serialized = field.field_type
                
                # Convert distinct_values to serializable format
                distinct_values_serialized = None
                if field.distinct_values:
                    distinct_values_serialized = [str(v) if not isinstance(v, (str, int, float, bool)) else v 
                                                   for v in list(field.distinct_values)[:100]]  # Limit to 100
                
                schema_data["fields"][field_name] = {
                    "name": field.name,
                    "field_type": field_type_serialized,
                    "nullable": field.nullable,
                    "example_value": str(field.example_value) if field.example_value is not None else None,
                    "is_nested": field.is_nested,
                    "nested_fields": field.nested_fields if field.nested_fields else {},
                    "distinct_values": distinct_values_serialized,
                    "min_value": field.min_value,
                    "max_value": field.max_value,
                    "min_length": field.min_length,
                    "max_length": field.max_length,
                    "avg_length": field.avg_length
                }
            
            schemas_dict[filename] = schema_data
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(schemas_dict, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Schemas saved to JSON: {output_file}")
        return str(output_file)
    
    @classmethod
    def load_schemas_from_json(cls, json_path: str) -> Dict[str, FileSchema]:
        """Load schemas from a JSON file."""
        json_file = Path(json_path)
        
        if not json_file.exists():
            raise FileNotFoundError(f"Schema report JSON not found: {json_path}")
        
        with open(json_file, 'r', encoding='utf-8') as f:
            schemas_dict = json.load(f)
        
        schemas = {}
        for filename, schema_data in schemas_dict.items():
            fields = {}
            
            for field_name, field_data in schema_data["fields"].items():
                # Convert field_type back from serialized format
                field_type = field_data["field_type"]
                if isinstance(field_type, list):
                    field_type = set(field_type)
                
                # Convert distinct_values back from serialized format
                distinct_values = None
                if field_data.get("distinct_values"):
                    distinct_values = set(field_data["distinct_values"])
                
                field = SchemaField(
                    name=field_data["name"],
                    field_type=field_type,
                    nullable=field_data["nullable"],
                    example_value=field_data.get("example_value"),
                    is_nested=field_data.get("is_nested", False),
                    nested_fields=field_data.get("nested_fields", {}),
                    distinct_values=distinct_values,
                    min_value=field_data.get("min_value"),
                    max_value=field_data.get("max_value"),
                    min_length=field_data.get("min_length"),
                    max_length=field_data.get("max_length"),
                    avg_length=field_data.get("avg_length")
                )
                fields[field_name] = field
            
            schema = FileSchema(
                filename=schema_data["filename"],
                record_count=schema_data["record_count"],
                fields=fields
            )
            schemas[filename] = schema
        
        logger.info(f"Loaded {len(schemas)} schema(s) from {json_path}")
        return schemas

