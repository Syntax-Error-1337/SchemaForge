import logging
import json
import pandas as pd
import pyarrow as pa
import pyarrow.orc as orc
from pathlib import Path
from typing import Optional
from src.schema_reader import SchemaReader, FileSchema
from src.json_loader import load_json_file
from src.converter.utils import prepare_dataframe

logger = logging.getLogger(__name__)

def convert_to_orc(filepath: Path, output_dir: Path, schema_reader: SchemaReader, schema: Optional[FileSchema] = None) -> bool:
    """Convert a JSON file to ORC format."""
    logger.info(f"Converting {filepath.name} to ORC...")
    
    try:
        # Load schema if not provided
        if schema is None:
            schema = schema_reader.infer_schema(filepath)
            if schema is None:
                logger.error(f"Failed to infer schema for {filepath.name}")
                return False
        
        # Load JSON data
        records = load_json_file(filepath, stream=False)
        
        if not records:
            logger.warning(f"No records to convert in {filepath.name}")
            return False
        
        # Prepare DataFrame
        df = prepare_dataframe(records, schema)
        
        if df.empty:
            logger.warning(f"Empty DataFrame created for {filepath.name}")
            return False
        
        # Generate output filename
        output_filename = filepath.stem + ".orc"
        output_path = output_dir / output_filename
        
        # Filter out null-type fields before creating Arrow table
        # ORC/Arrow doesn't support pure null types
        valid_columns = []
        for col_name in df.columns:
            field = schema.fields.get(col_name)
            if field:
                field_type = field.field_type
                # Skip null-type fields
                if isinstance(field_type, str) and field_type == "null":
                    logger.warning(f"Skipping null-type column '{col_name}' in {filepath.name} for ORC")
                    continue
            valid_columns.append(col_name)
        
        if not valid_columns:
            logger.error(f"No valid columns for ORC conversion in {filepath.name}")
            return False
        
        # Create filtered DataFrame with only valid columns
        filtered_df = df[valid_columns].copy()
        
        # Convert all array/list columns to JSON strings (like we do for Avro)
        # This prevents Arrow type conversion errors
        for col_name in filtered_df.columns:
            original_field = schema.fields.get(col_name)
            if original_field:
                original_type = original_field.field_type
                # Check if this is an array type
                if isinstance(original_type, str):
                    if original_type == "array" or original_type.startswith("array<"):
                        # Convert all list values to JSON strings
                        filtered_df[col_name] = filtered_df[col_name].apply(
                            lambda x: json.dumps(x) if isinstance(x, list) else (str(x) if x is not None else "")
                        )
        
        # Replace None values with empty strings for columns that are all None
        # This prevents PyArrow from inferring null type
        for col in filtered_df.columns:
            if filtered_df[col].isna().all():
                logger.warning(f"Column '{col}' is all None/NaN, filling with empty strings for ORC compatibility")
                filtered_df[col] = ""
        
        # Build explicit Arrow schema to avoid null type inference
        arrow_fields = []
        for col in filtered_df.columns:
            dtype = filtered_df[col].dtype
            if pd.api.types.is_integer_dtype(dtype):
                arrow_type = pa.int64()
            elif pd.api.types.is_float_dtype(dtype):
                arrow_type = pa.float64()
            elif pd.api.types.is_bool_dtype(dtype):
                arrow_type = pa.bool_()
            else:
                # Use string for everything else to avoid null types
                arrow_type = pa.string()
            arrow_fields.append(pa.field(col, arrow_type))
        
        arrow_schema = pa.schema(arrow_fields)
        
        # Convert to PyArrow table with explicit schema
        table = pa.Table.from_pandas(filtered_df, schema=arrow_schema)
        orc.write_table(table, output_path)
        
        logger.info(f"Successfully converted {filepath.name} to {output_path}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to convert {filepath.name} to ORC: {e}")
        return False
