"""
Validator Module

This module provides functionality to validate JSON files against inferred schemas.
"""

import logging
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from src.schema_reader import SchemaReader, FileSchema
from src.json_loader import load_json_file

logger = logging.getLogger(__name__)

class SchemaValidator:
    """Class for validating data against schemas."""
    
    def __init__(self, schema_report_path: str):
        """
        Initialize the Validator.
        
        Args:
            schema_report_path: Path to the schema report JSON file.
        """
        self.schema_report_path = Path(schema_report_path)
        if not self.schema_report_path.exists():
            raise FileNotFoundError(f"Schema report not found: {schema_report_path}")
            
        self.schemas = self._load_schemas()

    def _load_schemas(self) -> Dict[str, Dict[str, Any]]:
        """Load schemas from the report file."""
        with open(self.schema_report_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def validate_file(self, filepath: Path) -> Dict[str, Any]:
        """
        Validate a single file against its schema.
        
        Returns:
            Dict containing validation results (valid, error_count, errors).
        """
        filename = filepath.name
        schema_data = self.schemas.get(filename)
        
        if not schema_data:
            return {"valid": False, "error": "No schema found in report"}
            
        fields_def = schema_data.get('fields', {})
        records = load_json_file(filepath)
        
        errors = []
        error_count = 0
        max_errors = 100
        
        for i, record in enumerate(records):
            if not isinstance(record, dict):
                # Normalized records should be dicts, but just in case
                continue
                
            for field, value in record.items():
                if field not in fields_def:
                    # Extra field (optional: strict mode could flag this)
                    continue
                
                expected_type = fields_def[field]['type']
                if not self._validate_type(value, expected_type):
                    error_count += 1
                    if len(errors) < max_errors:
                        errors.append(f"Row {i}, Field '{field}': Expected {expected_type}, got {type(value).__name__} ({value})")
        
        return {
            "valid": error_count == 0,
            "error_count": error_count,
            "errors": errors
        }

    def _validate_type(self, value: Any, expected_type: str) -> bool:
        """Check if value matches expected type."""
        if value is None:
            return True # Assume nullable for now
            
        if expected_type == "integer":
            return isinstance(value, int)
        elif expected_type == "float":
            return isinstance(value, (int, float))
        elif expected_type == "string":
            return isinstance(value, str)
        elif expected_type == "boolean":
            return isinstance(value, bool)
        elif expected_type == "array":
            return isinstance(value, list)
        elif expected_type == "object":
            return isinstance(value, dict)
        # For specialized types like timestamp, url, etc., we treat as string for basic validation
        # or implement stricter checks if needed.
        return True

    def validate_all(self, data_dir: str) -> Dict[str, Any]:
        """Validate all files in the directory."""
        data_path = Path(data_dir)
        results = {}
        
        for filepath in data_path.glob("*.json"):
            logger.info(f"Validating {filepath.name}...")
            results[filepath.name] = self.validate_file(filepath)
            
        return results
