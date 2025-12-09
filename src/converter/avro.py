import logging
import json
import fastavro
import pandas as pd
from pathlib import Path
from typing import Optional
from src.schema_reader import SchemaReader, FileSchema
from src.json_loader import load_json_file
from src.converter.utils import prepare_dataframe

logger = logging.getLogger(__name__)

def convert_to_avro(filepath: Path, output_dir: Path, schema_reader: SchemaReader, schema: Optional[FileSchema] = None) -> bool:
    """Convert a JSON file to Avro format."""
    logger.info(f"Converting {filepath.name} to Avro...")
    
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
        
        # Convert all array/list columns to JSON strings before building Avro schema
        # This prevents unhashable type errors and ensures proper serialization
        for col_name in df.columns:
            original_field = schema.fields.get(col_name)
            if original_field:
                original_type = original_field.field_type
                # Check if this is an array type
                if isinstance(original_type, str):
                    if original_type == "array" or original_type.startswith("array<"):
                        # Convert all list values to JSON strings
                        df[col_name] = df[col_name].apply(
                            lambda x: json.dumps(x) if isinstance(x, list) else (str(x) if x is not None else None)
                        )
        
        # Generate output filename
        output_filename = filepath.stem + ".avro"
        output_path = output_dir / output_filename
        
        # Generate Avro schema from DataFrame (arrays already converted to strings)
        avro_schema = {
            "doc": f"Schema for {filepath.name}",
            "name": "Record",
            "namespace": "schemaforge",
            "type": "record",
            "fields": []
        }
        
        for col_name, dtype in df.dtypes.items():
            # All array types have been converted to strings, so no special handling needed
            if pd.api.types.is_integer_dtype(dtype):
                field_type = ["null", "long"]
            elif pd.api.types.is_float_dtype(dtype):
                field_type = ["null", "double"]
            elif pd.api.types.is_bool_dtype(dtype):
                field_type = ["null", "boolean"]
            else:
                field_type = ["null", "string"]
            
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
