import logging
import concurrent.futures
from pathlib import Path
from typing import Dict, Optional, Any, List

from src.schema_reader import SchemaReader, FileSchema
from src.converter.utils import (
    flatten_dict, 
    coerce_type, 
    extract_columns_from_metadata, 
    convert_array_row_to_object, 
    prepare_dataframe
)
from src.converter.parquet import convert_to_parquet
from src.converter.csv import convert_to_csv
from src.converter.avro import convert_to_avro
from src.converter.orc import convert_to_orc
from src.converter.feather import convert_to_feather

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
    
    # Expose helper methods for backward compatibility or internal use if needed,
    # though they are now static/standalone.
    def _flatten_dict(self, d: Dict, parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
        return flatten_dict(d, parent_key, sep)
    
    def _coerce_type(self, value: Any, field_type: Any) -> Any:
        return coerce_type(value, field_type)
        
    def _extract_columns_from_metadata(self, metadata: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        return extract_columns_from_metadata(metadata)
        
    def _convert_array_row_to_object(self, row: List[Any], columns: List[Dict[str, Any]]) -> Dict[str, Any]:
        return convert_array_row_to_object(row, columns)
        
    def _prepare_dataframe(self, records: List[Dict[str, Any]], schema: FileSchema):
        return prepare_dataframe(records, schema)

    def convert_to_parquet(self, filepath: Path, schema: Optional[FileSchema] = None) -> bool:
        """Convert a JSON file to Parquet format."""
        return convert_to_parquet(filepath, self.output_dir, self.schema_reader, schema)
    
    def convert_to_csv(self, filepath: Path, schema: Optional[FileSchema] = None) -> bool:
        """Convert a JSON file to CSV format."""
        return convert_to_csv(filepath, self.output_dir, self.schema_reader, schema)
    
    def convert_to_avro(self, filepath: Path, schema: Optional[FileSchema] = None) -> bool:
        """Convert a JSON file to Avro format."""
        return convert_to_avro(filepath, self.output_dir, self.schema_reader, schema)
    
    def convert_to_orc(self, filepath: Path, schema: Optional[FileSchema] = None) -> bool:
        """Convert a JSON file to ORC format."""
        return convert_to_orc(filepath, self.output_dir, self.schema_reader, schema)
    
    def convert_to_feather(self, filepath: Path, schema: Optional[FileSchema] = None) -> bool:
        """Convert a JSON file to Feather format."""
        return convert_to_feather(filepath, self.output_dir, self.schema_reader, schema)
    
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
                    
                    # Pass the standalone functions and necessary arguments
                    # We pass self.output_dir and self.schema_reader explicitly
                    if format_type.lower() == "parquet":
                        future = executor.submit(convert_to_parquet, json_file, self.output_dir, self.schema_reader, schema)
                    elif format_type.lower() == "csv":
                        future = executor.submit(convert_to_csv, json_file, self.output_dir, self.schema_reader, schema)
                    elif format_type.lower() == "avro":
                        future = executor.submit(convert_to_avro, json_file, self.output_dir, self.schema_reader, schema)
                    elif format_type.lower() == "orc":
                        future = executor.submit(convert_to_orc, json_file, self.output_dir, self.schema_reader, schema)
                    elif format_type.lower() == "feather":
                        future = executor.submit(convert_to_feather, json_file, self.output_dir, self.schema_reader, schema)
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
                    elif format_type.lower() == "feather":
                        success = self.convert_to_feather(json_file, schema)
                    else:
                        logger.error(f"Unsupported format: {format_type}")
                        success = False
                    
                    results[json_file.name] = success
                except Exception as e:
                    logger.error(f"File {json_file.name} generated an exception: {e}")
                    results[json_file.name] = False
        
        return results
