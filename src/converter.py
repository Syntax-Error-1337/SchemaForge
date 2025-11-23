"""
Converter Module

This module provides functionality to convert JSON files to Parquet and CSV formats
using the schemas inferred by the schema reader.
"""

import json
import ast
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.orc as orc
import fastavro
from pathlib import Path
from typing import Dict, List, Any, Optional
import concurrent.futures
from src.schema_reader import SchemaReader, FileSchema
from src.json_loader import load_json_file

logger = logging.getLogger(__name__)


class Converter:
    """Main class for converting JSON files to other formats."""
    
    def __init__(self, data_dir: str = "data", output_dir: str = "output",
                 schema_reader: Optional[SchemaReader] = None,
                 schema_report_path: Optional[str] = None):
        """
        Initialize the Converter.
        
        Args:
            data_dir: Directory containing JSON files
            output_dir: Directory for output files
            schema_reader: Optional SchemaReader instance (will create one if not provided)
            schema_report_path: Path to schema report JSON file (required for conversion)
        """
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.schema_report_path = schema_report_path
        
        if schema_reader is None:
            self.schema_reader = SchemaReader(data_dir=data_dir)
        else:
            self.schema_reader = schema_reader
    
    def _flatten_dict(self, d: Dict, parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
        """Flatten a nested dictionary."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Handle arrays
                if len(v) > 0 and isinstance(v[0], dict):
                    # Array of objects - convert to JSON string
                    items.append((new_key, json.dumps(v)))
                else:
                    # Simple array - keep as is or convert to string
                    items.append((new_key, v))
            else:
                items.append((new_key, v))
        return dict(items)
    
    def _coerce_type(self, value: Any, field_type: Any) -> Any:
        """Coerce a value to the expected type based on schema."""
        if value is None:
            return None
        
        # Handle mixed types
        if isinstance(field_type, set):
            # For mixed types, try to preserve the original value
            return value
        
        # Handle array types
        if isinstance(field_type, str) and field_type.startswith("array<"):
            return value  # Keep arrays as-is or as JSON strings
        
        # Type coercion
        type_str = field_type if isinstance(field_type, str) else str(field_type)
        
        try:
            if type_str == "integer":
                if isinstance(value, (int, float)):
                    return int(value)
                elif isinstance(value, str):
                    # Try to parse numeric strings
                    try:
                        return int(float(value))  # Use float first to handle "123.0"
                    except (ValueError, TypeError):
                        return value
                return value
            elif type_str == "float":
                if isinstance(value, (int, float)):
                    return float(value)
                elif isinstance(value, str):
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return value
                return value
            elif type_str == "numeric_string":
                # Keep as string but ensure it's numeric
                if isinstance(value, (int, float)):
                    return str(value)
                return str(value)
            elif type_str == "boolean":
                if isinstance(value, bool):
                    return value
                str_val = str(value).lower().strip()
                return str_val in ('true', '1', 'yes', 'on', 'y', 't')
            elif type_str in ("string", "timestamp", "url", "email", "uuid", "ip_address"):
                return str(value)
            elif type_str == "json_string":
                # If it's already a JSON string, keep it; otherwise stringify
                if isinstance(value, str):
                    return value
                elif isinstance(value, (dict, list)):
                    return json.dumps(value)
                return str(value)
            else:
                return value
        except (ValueError, TypeError) as e:
            logger.warning(f"Type coercion failed for value {value} to type {type_str}: {e}")
            return value
    
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
    
    def _prepare_dataframe(self, records: List[Dict[str, Any]], schema: FileSchema) -> pd.DataFrame:
        """Prepare a pandas DataFrame from records using the schema."""
        if not records:
            return pd.DataFrame()
        
        # Flatten all records
        flattened_records = []
        for record in records:
            flattened = self._flatten_dict(record)
            flattened_records.append(flattened)
        
        # Create DataFrame
        df = pd.DataFrame(flattened_records)
        
        # Ensure all schema fields are present (fill missing with None)
        for field_name in schema.fields.keys():
            if field_name not in df.columns:
                df[field_name] = None
        
        # Reorder columns to match schema order
        schema_fields = sorted(schema.fields.keys())
        existing_fields = [f for f in schema_fields if f in df.columns]
        df = df[existing_fields]
        
        # Apply type coercion based on schema
        for field_name, field in schema.fields.items():
            if field_name in df.columns:
                df[field_name] = df[field_name].apply(
                    lambda x: self._coerce_type(x, field.field_type)
                )
        
        return df
    
    def convert_to_parquet(self, filepath: Path, schema: Optional[FileSchema] = None) -> bool:
        """Convert a JSON file to Parquet format."""
        logger.info(f"Converting {filepath.name} to Parquet...")
        
        try:
            # Load schema if not provided
            if schema is None:
                schema = self.schema_reader.infer_schema(filepath)
                if schema is None:
                    logger.error(f"Failed to infer schema for {filepath.name}")
                    return False
            
            # Load JSON data
            records = load_json_file(filepath, stream=False)
            
            if not records:
                logger.warning(f"No records to convert in {filepath.name}")
                return False
            
            # Prepare DataFrame
            df = self._prepare_dataframe(records, schema)
            
            if df.empty:
                logger.warning(f"Empty DataFrame created for {filepath.name}")
                return False
            
            # Generate output filename
            output_filename = filepath.stem + ".parquet"
            output_path = self.output_dir / output_filename
            
            # Convert to PyArrow table and write
            table = pa.Table.from_pandas(df)
            pq.write_table(table, output_path)
            
            logger.info(f"Successfully converted {filepath.name} to {output_path}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to convert {filepath.name} to Parquet: {e}")
            return False
    
    def convert_to_csv(self, filepath: Path, schema: Optional[FileSchema] = None) -> bool:
        """Convert a JSON file to CSV format."""
        logger.info(f"Converting {filepath.name} to CSV...")
        
        try:
            # Load schema if not provided
            if schema is None:
                schema = self.schema_reader.infer_schema(filepath)
                if schema is None:
                    logger.error(f"Failed to infer schema for {filepath.name}")
                    return False
            
            # Load JSON data
            records = load_json_file(filepath, stream=False)
            
            if not records:
                logger.warning(f"No records to convert in {filepath.name}")
                return False
            
            # Prepare DataFrame
            df = self._prepare_dataframe(records, schema)
            
            if df.empty:
                logger.warning(f"Empty DataFrame created for {filepath.name}")
                return False
            
            # Generate output filename
            output_filename = filepath.stem + ".csv"
            output_path = self.output_dir / output_filename
            
            # Write to CSV
            df.to_csv(output_path, index=False, encoding='utf-8')
            
            logger.info(f"Successfully converted {filepath.name} to {output_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to convert {filepath.name} to CSV: {e}")
            return False
    
    def convert_to_avro(self, filepath: Path, schema: Optional[FileSchema] = None) -> bool:
        """Convert a JSON file to Avro format."""
        logger.info(f"Converting {filepath.name} to Avro...")
        
        try:
            # Load schema if not provided
            if schema is None:
                schema = self.schema_reader.infer_schema(filepath)
                if schema is None:
                    logger.error(f"Failed to infer schema for {filepath.name}")
                    return False
            
            # Load JSON data
            records = load_json_file(filepath, stream=False)
            
            if not records:
                logger.warning(f"No records to convert in {filepath.name}")
                return False
            
            # Prepare DataFrame
            df = self._prepare_dataframe(records, schema)
            
            if df.empty:
                logger.warning(f"Empty DataFrame created for {filepath.name}")
                return False
            
            # Generate output filename
            output_filename = filepath.stem + ".avro"
            output_path = self.output_dir / output_filename
            
            # Generate Avro schema from DataFrame
            avro_schema = {
                "doc": f"Schema for {filepath.name}",
                "name": "Record",
                "namespace": "schemaforge",
                "type": "record",
                "fields": []
            }
            
            for col_name, dtype in df.dtypes.items():
                field_type = ["null"]
                if pd.api.types.is_integer_dtype(dtype):
                    field_type.append("long")
                elif pd.api.types.is_float_dtype(dtype):
                    field_type.append("double")
                elif pd.api.types.is_bool_dtype(dtype):
                    field_type.append("boolean")
                else:
                    field_type.append("string")
                
                avro_schema["fields"].append({"name": col_name, "type": field_type})
            
            # Convert DataFrame to list of dicts
            records_list = df.to_dict('records')
            
            # Write Avro file
            with open(output_path, 'wb') as out:
                fastavro.writer(out, avro_schema, records_list)
            
            logger.info(f"Successfully converted {filepath.name} to {output_path}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to convert {filepath.name} to Avro: {e}")
            return False
    
    def convert_to_orc(self, filepath: Path, schema: Optional[FileSchema] = None) -> bool:
        """Convert a JSON file to ORC format."""
        logger.info(f"Converting {filepath.name} to ORC...")
        
        try:
            # Load schema if not provided
            if schema is None:
                schema = self.schema_reader.infer_schema(filepath)
                if schema is None:
                    logger.error(f"Failed to infer schema for {filepath.name}")
                    return False
            
            # Load JSON data
            records = load_json_file(filepath, stream=False)
            
            if not records:
                logger.warning(f"No records to convert in {filepath.name}")
                return False
            
            # Prepare DataFrame
            df = self._prepare_dataframe(records, schema)
            
            if df.empty:
                logger.warning(f"Empty DataFrame created for {filepath.name}")
                return False
            
            # Generate output filename
            output_filename = filepath.stem + ".orc"
            output_path = self.output_dir / output_filename
            
            # Convert to PyArrow table and handle null columns
            table = pa.Table.from_pandas(df)
            
            # Filter out columns with null type (PyArrow can't write them to ORC)
            schema_fields = []
            valid_columns = []
            for i, field in enumerate(table.schema):
                if field.type != pa.null():
                    schema_fields.append(field)
                    valid_columns.append(table.column(i))
                else:
                    logger.warning(f"Skipping null-type column '{field.name}' in {filepath.name} for ORC")
            
            if not valid_columns:
                logger.error(f"No valid columns for ORC conversion in {filepath.name}")
                return False
            
            # Create new table with only valid columns
            filtered_table = pa.Table.from_arrays(valid_columns, schema=pa.schema(schema_fields))
            orc.write_table(filtered_table, output_path)
            
            logger.info(f"Successfully converted {filepath.name} to {output_path}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to convert {filepath.name} to ORC: {e}")
            return False
    
    def convert_all(self, format_type: str) -> Dict[str, bool]:
        """Convert all JSON files in the data directory to the specified format.
        
        Requires a schema report to be generated first using scan-schemas command.
        """
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")
        
        json_files = list(self.data_dir.glob("*.json"))
        
        if not json_files:
            logger.warning(f"No JSON files found in {self.data_dir}")
            return {}
        
        # Load schemas - either from schema_reader (if already loaded) or from schema report file
        if self.schema_reader.schemas:
            # Use pre-loaded schemas from schema_reader
            logger.info(f"Using pre-loaded schemas from SchemaReader")
            schemas = self.schema_reader.schemas
        elif self.schema_report_path:
            # Load schemas from schema report JSON file
            logger.info(f"Loading schemas from schema report: {self.schema_report_path}")
            try:
                schemas = SchemaReader.load_schemas_from_json(self.schema_report_path)
            except FileNotFoundError as e:
                raise FileNotFoundError(
                    f"Schema report not found: {self.schema_report_path}. "
                    "Please run 'scan-schemas' command first to generate a schema report."
                ) from e
        else:
            raise ValueError(
                "Schema report path or pre-loaded schemas are required. "
                "Please run 'scan-schemas' command first to generate a schema report, "
                "then provide the path using --schema-report option."
            )
        
        if not schemas:
            raise ValueError("No schemas found. Please regenerate the schema report.")
        
        results = {}
        
        # Determine if we should use parallel processing
        # If schemas are pre-loaded (from schema_reader), use sequential to avoid serialization issues
        use_parallel = not bool(self.schema_reader.schemas)
        
        if use_parallel:
            # Use ProcessPoolExecutor for parallel processing (when loading from file)
            max_workers = min(len(json_files), 4)
            if max_workers < 1: max_workers = 1
            
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                future_to_file = {}
                for json_file in json_files:
                    schema = schemas.get(json_file.name)
                    if schema is None:
                        logger.warning(
                            f"No schema found for {json_file.name} in the schema report. "
                            "Skipping this file. Please regenerate the schema report."
                        )
                        results[json_file.name] = False
                        continue
                    
                    if format_type.lower() == "parquet":
                        future = executor.submit(self.convert_to_parquet, json_file, schema)
                    elif format_type.lower() == "csv":
                        future = executor.submit(self.convert_to_csv, json_file, schema)
                    elif format_type.lower() == "avro":
                        future = executor.submit(self.convert_to_avro, json_file, schema)
                    elif format_type.lower() == "orc":
                        future = executor.submit(self.convert_to_orc, json_file, schema)
                    else:
                        logger.error(f"Unsupported format: {format_type}")
                        results[json_file.name] = False
                        continue
                    
                    future_to_file[future] = json_file
                
                for future in concurrent.futures.as_completed(future_to_file):
                    json_file = future_to_file[future]
                    try:
                        success = future.result()
                        results[json_file.name] = success
                    except Exception as e:
                        logger.error(f"File {json_file.name} generated an exception: {e}")
                        results[json_file.name] = False
        else:
            # Sequential processing (when using pre-loaded schemas to avoid serialization issues)
            logger.info("Using sequential processing for pre-loaded schemas")
            for json_file in json_files:
                schema = schemas.get(json_file.name)
                if schema is None:
                    logger.warning(
                        f"No schema found for {json_file.name}. "
                        "Skipping this file."
                    )
                    results[json_file.name] = False
                    continue
                
                try:
                    if format_type.lower() == "parquet":
                        success = self.convert_to_parquet(json_file, schema)
                    elif format_type.lower() == "csv":
                        success = self.convert_to_csv(json_file, schema)
                    elif format_type.lower() == "avro":
                        success = self.convert_to_avro(json_file, schema)
                    elif format_type.lower() == "orc":
                        success = self.convert_to_orc(json_file, schema)
                    else:
                        logger.error(f"Unsupported format: {format_type}")
                        success = False
                    
                    results[json_file.name] = success
                except Exception as e:
                    logger.error(f"File {json_file.name} generated an exception: {e}")
                    results[json_file.name] = False
        
        return results

