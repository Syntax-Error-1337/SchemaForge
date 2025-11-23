import logging
import pyarrow as pa
import pyarrow.feather as feather
from pathlib import Path
from typing import Optional
from src.schema_reader import SchemaReader, FileSchema
from src.json_loader import load_json_file
from src.converter.utils import prepare_dataframe

logger = logging.getLogger(__name__)

def convert_to_feather(filepath: Path, output_dir: Path, schema_reader: SchemaReader, schema: Optional[FileSchema] = None) -> bool:
    """Convert a JSON file to Feather format."""
    logger.info(f"Converting {filepath.name} to Feather...")
    
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
        output_filename = filepath.stem + ".feather"
        output_path = output_dir / output_filename
        
        # Convert to PyArrow table and write
        table = pa.Table.from_pandas(df)
        feather.write_feather(table, output_path)
        
        logger.info(f"Successfully converted {filepath.name} to {output_path}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to convert {filepath.name} to Feather: {e}")
        return False
