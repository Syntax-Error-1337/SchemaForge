# Schema Reader and Converter

A robust Python tool for inferring schemas from JSON files and converting them to Parquet and CSV formats. This project is designed for data engineering workflows where you need to understand and transform JSON data efficiently with a **schema-first approach**.

## Overview

This tool follows a **two-phase workflow** that ensures consistency and reliability:

1. **Schema Discovery Phase**: Analyze JSON files and generate comprehensive schema reports
2. **Conversion Phase**: Use the generated schema reports to convert JSON files to Parquet or CSV

This approach ensures that:
- Schemas are analyzed once and reused consistently
- Conversions use the same schema definitions
- You can review and validate schemas before conversion
- Large datasets can be processed efficiently with schema caching

## Features

### Core Capabilities
- **Smart Schema Inference**: Automatically detects field names, data types, nested structures, and nullable fields
- **Schema-First Workflow**: Generate schema reports before conversion for consistency
- **Multiple JSON Format Support**: Handles various JSON structures and formats
- **Format Conversion**: Converts JSON to Parquet and CSV with proper type handling
- **Nested Structure Handling**: Flattens nested objects with dot notation (e.g., `user.address.city`)
- **Configurable Sampling**: Supports sampling strategies for large files
- **Dual Report Format**: Generates both human-readable Markdown and machine-readable JSON schema reports
- **Robust Error Handling**: Gracefully handles malformed files and continues processing

### Advanced JSON Format Support
- **Standard JSON Arrays**: `[{...}, {...}]`
- **NDJSON (Newline-Delimited JSON)**: One JSON object per line
- **Array-Based Tabular Data**: Arrays of arrays with column metadata (Socrata/OpenData format)
- **Wrapper Objects**: Objects containing data arrays (`data`, `results`, `items`, `records`, `rows`, `entries`)
- **GeoJSON**: FeatureCollection and Feature formats
- **Single JSON Objects**: Treated as single-record datasets

## Architecture

### System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      CLI Interface                          │
│                   (src/cli.py)                             │
└──────────────┬──────────────────────────┬──────────────────┘
               │                          │
               ▼                          ▼
    ┌──────────────────┐      ┌──────────────────┐
    │  Schema Reader    │      │    Converter     │
    │ (schema_reader.py)│      │  (converter.py)  │
    └────────┬─────────┘      └────────┬─────────┘
             │                         │
             │                         │
             ▼                         ▼
    ┌──────────────────┐      ┌──────────────────┐
    │  JSON Loader     │      │  JSON Loader     │
    │  (Multi-format)  │      │  (Multi-format)  │
    └────────┬─────────┘      └────────┬─────────┘
             │                         │
             └──────────┬──────────────┘
                        ▼
              ┌──────────────────┐
              │   JSON Files     │
              │   (data/*.json)  │
              └──────────────────┘
```

### Workflow Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Phase 1: Schema Discovery                │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  1. Scan JSON Files                                          │
│     └─> Detect format (array, NDJSON, tabular, etc.)        │
│                                                               │
│  2. Load & Parse Data                                       │
│     └─> Handle multiple JSON formats                         │
│     └─> Extract column metadata (if available)               │
│     └─> Convert array-based data to objects                  │
│                                                               │
│  3. Infer Schemas                                            │
│     └─> Analyze field types                                  │
│     └─> Detect nested structures                             │
│     └─> Identify nullable fields                             │
│     └─> Sample data (if configured)                          │
│                                                               │
│  4. Generate Reports                                         │
│     └─> Markdown report (human-readable)                     │
│     └─> JSON report (machine-readable)                      │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Phase 2: Conversion                      │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  1. Load Schema Report                                       │
│     └─> Read JSON schema report                              │
│     └─> Validate schema exists                               │
│                                                               │
│  2. Load JSON Data                                           │
│     └─> Use same format detection as schema phase            │
│     └─> Apply column mappings (for tabular data)            │
│                                                               │
│  3. Apply Schema                                             │
│     └─> Flatten nested structures                            │
│     └─> Coerce types according to schema                     │
│     └─> Handle missing fields                                │
│                                                               │
│  4. Convert to Target Format                                 │
│     └─> Parquet: Use PyArrow for type-safe conversion        │
│     └─> CSV: Use Pandas with proper encoding                 │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### Component Details

#### Schema Reader (`src/schema_reader.py`)
- **Purpose**: Analyze JSON files and infer their structure
- **Key Classes**:
  - `SchemaReader`: Main class for schema inference
  - `FileSchema`: Represents schema of a single file
  - `SchemaField`: Represents a single field with its properties
- **Key Methods**:
  - `scan_directory()`: Scan all JSON files in a directory
  - `infer_schema()`: Infer schema for a single file
  - `generate_report()`: Generate Markdown and JSON reports
  - `_load_json_file()`: Smart JSON loader supporting multiple formats
  - `_extract_columns_from_metadata()`: Extract column definitions from metadata

#### Converter (`src/converter.py`)
- **Purpose**: Convert JSON files to Parquet/CSV using schema reports
- **Key Classes**:
  - `Converter`: Main conversion class
- **Key Methods**:
  - `convert_all()`: Convert all files using schema report
  - `convert_to_parquet()`: Convert single file to Parquet
  - `convert_to_csv()`: Convert single file to CSV
  - `_load_json_file()`: Same smart JSON loader as schema reader
  - `_prepare_dataframe()`: Prepare DataFrame with schema-based type coercion

#### CLI (`src/cli.py`)
- **Purpose**: Command-line interface for the tool
- **Commands**:
  - `scan-schemas`: Generate schema reports
  - `convert`: Convert JSON files to Parquet/CSV

## Project Structure

```
project_root/
  data/                    # Input JSON files (place your JSON files here)
    ├── *.json            # Your JSON data files
  output/                  # Converted outputs (Parquet, CSV)
    ├── *.parquet         # Parquet output files
    └── *.csv             # CSV output files
  reports/                 # Schema reports
    ├── schema_report.md  # Human-readable Markdown report
    └── schema_report.json # Machine-readable JSON report
  src/
    ├── __init__.py
    ├── schema_reader.py  # Schema inference module
    ├── converter.py      # Format conversion module
    └── cli.py            # Command line interface
  tests/
    ├── __init__.py
    ├── test_schema_reader.py
    └── test_converter.py
  README.md
  requirements.txt
  pytest.ini
```

## Installation

1. **Clone or download this project**

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

   Required packages:
   - `pandas` (>=2.0.0) - Data manipulation and CSV export
   - `pyarrow` (>=12.0.0) - Parquet file support
   - `pytest` (>=7.0.0) - For running tests

## Usage

### Complete Workflow

The tool follows a **two-phase workflow** that must be executed in order:

#### Phase 1: Schema Discovery

Scan all JSON files and generate schema reports:

```bash
python -m src.cli scan-schemas
```

This command:
- Scans all `.json` files in the `data/` directory
- Detects the JSON format automatically
- Infers schemas for each file
- Generates two reports:
  - `reports/schema_report.md` - Human-readable Markdown report
  - `reports/schema_report.json` - Machine-readable JSON report (used by converter)

**Command Options:**
- `--data-dir`: Specify custom data directory (default: `data`)
- `--output-report`: Specify custom report path (default: `reports/schema_report.md`)
- `--max-sample-size`: Limit number of records to analyze per file (default: all records)
- `--sampling-strategy`: Choose `first` or `random` sampling (default: `first`)

**Examples:**
```bash
# Basic usage
python -m src.cli scan-schemas

# Custom data directory
python -m src.cli scan-schemas --data-dir my_data

# Sample only first 1000 records per file
python -m src.cli scan-schemas --max-sample-size 1000

# Use random sampling
python -m src.cli scan-schemas --sampling-strategy random --max-sample-size 500
```

#### Phase 2: Conversion

Convert JSON files to Parquet or CSV using the generated schema report:

```bash
# Convert to Parquet
python -m src.cli convert --format parquet

# Convert to CSV
python -m src.cli convert --format csv
```

**Important**: The converter **requires** a schema report to be generated first. If no schema report is found, the conversion will fail with a clear error message.

**Command Options:**
- `--format`: Output format (`parquet` or `csv`) - **Required**
- `--data-dir`: Input directory (default: `data`)
- `--output-dir`: Output directory (default: `output`)
- `--schema-report`: Path to schema report JSON file (default: `reports/schema_report.json`)
- `--schema-report-md`: Path to schema report Markdown file (alternative to `--schema-report`)

**Examples:**
```bash
# Convert to Parquet (uses default schema report)
python -m src.cli convert --format parquet

# Convert to CSV with custom directories
python -m src.cli convert --format csv --data-dir my_data --output-dir csv_output

# Use custom schema report
python -m src.cli convert --format parquet --schema-report custom_report.json
```

### Quick Start Example

```bash
# 1. Place your JSON files in the data directory
cp my_data/*.json data/

# 2. Generate schema reports
python -m src.cli scan-schemas

# 3. Review the schema report
cat reports/schema_report.md

# 4. Convert to Parquet
python -m src.cli convert --format parquet

# 5. Convert to CSV
python -m src.cli convert --format csv
```

## Supported JSON Formats

The tool intelligently detects and handles multiple JSON formats:

### 1. Standard JSON Array
```json
[
  {"id": 1, "name": "Alice", "age": 30},
  {"id": 2, "name": "Bob", "age": 25}
]
```

### 2. Newline-Delimited JSON (NDJSON)
```
{"id": 1, "name": "Alice", "age": 30}
{"id": 2, "name": "Bob", "age": 25}
```

### 3. Wrapper Objects with Data Arrays
```json
{
  "data": [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"}
  ]
}
```

The tool automatically detects data in fields named: `data`, `results`, `items`, `records`, `rows`, `entries`

### 4. Array-Based Tabular Data (Socrata/OpenData Format)
```json
{
  "meta": {
    "view": {
      "columns": [
        {"name": "id", "fieldName": "id", "position": 0, "dataTypeName": "number"},
        {"name": "name", "fieldName": "name", "position": 1, "dataTypeName": "text"}
      ]
    }
  },
  "data": [
    [1, "Alice"],
    [2, "Bob"]
  ]
}
```

**Features:**
- Automatically extracts column definitions from metadata
- Converts array rows to objects using column names
- Skips hidden/meta columns automatically
- Supports multiple metadata paths: `meta.view.columns`, `view.columns`, `columns`, `schema.fields`

### 5. GeoJSON Format
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {"name": "Location 1", "value": 100},
      "geometry": {...}
    }
  ]
}
```

The tool extracts the `properties` field from GeoJSON features.

### 6. Single JSON Object
```json
{
  "id": 1,
  "name": "Alice",
  "address": {
    "city": "New York"
  }
}
```

Treated as a single-record dataset.

## Schema Inference

The schema reader automatically detects:

### Basic Types
- **string**: Text data
- **integer**: Whole numbers
- **float**: Decimal numbers
- **boolean**: True/false values
- **null**: Null values

### Complex Types
- **array**: Lists of values
  - `array<string>`: Array of strings
  - `array<integer>`: Array of integers
  - `array<mixed>`: Array with mixed types
- **object**: Nested structures (flattened with dot notation)

### Special Types
- **timestamp**: Detected from common date/time patterns:
  - ISO dates: `2023-01-01`
  - ISO datetime: `2023-01-01T10:00:00Z`
  - Unix timestamps (seconds or milliseconds)

### Field Properties
- **Nullable Fields**: Fields that contain `null` values are marked as nullable
- **Mixed Types**: Fields with inconsistent types across records are detected
- **Nested Structures**: Objects within objects are flattened with dot notation

### Example Schema Output

```json
{
  "filename": "data.json",
  "record_count": 100,
  "fields": {
    "id": {
      "name": "id",
      "field_type": "integer",
      "nullable": false,
      "example_value": 1
    },
    "user.name": {
      "name": "user.name",
      "field_type": "string",
      "nullable": false,
      "example_value": "Alice"
    },
    "user.address.city": {
      "name": "user.address.city",
      "field_type": "string",
      "nullable": true,
      "example_value": "New York"
    }
  }
}
```

## Nested Structure Handling

Nested objects are automatically flattened using dot notation:

**Input JSON:**
```json
{
  "id": 1,
  "user": {
    "name": "Alice",
    "address": {
      "city": "New York",
      "zip": "10001"
    }
  },
  "tags": ["python", "data"]
}
```

**Output Columns:**
- `id` (integer)
- `user.name` (string)
- `user.address.city` (string)
- `user.address.zip` (string)
- `tags` (array<string> or JSON string)

**Note**: Arrays of objects are converted to JSON strings in CSV/Parquet output for compatibility with flat file formats.

## Schema Report Formats

### Markdown Report (`schema_report.md`)

Human-readable report with:
- File information (record count, field count)
- Field details table:
  - Field name
  - Data type
  - Nullable status
  - Example values
  - Notes (nested, mixed types, etc.)

### JSON Report (`schema_report.json`)

Machine-readable report used by the converter:
- Complete schema definitions
- Field types and properties
- Example values
- Record counts

The converter automatically loads this JSON report to ensure consistent schema usage.

## Type Coercion

During conversion, the tool applies type coercion based on the inferred schema:

- **Integer**: Attempts to convert strings/floats to integers
- **Float**: Converts strings/integers to floats
- **Boolean**: Converts strings like "true", "1", "yes" to boolean
- **String**: Converts all values to strings
- **Timestamp**: Preserved as string (can be parsed later)

If type coercion fails, the original value is preserved with a warning logged.

## Error Handling

The tool is designed to be robust and handle errors gracefully:

### Schema Discovery Phase
- **Empty Directory**: Warns if no JSON files are found
- **Malformed JSON**: Logs errors and continues processing other files
- **Unsupported Format**: Attempts multiple format detection strategies
- **Large Files**: Supports sampling to reduce memory usage

### Conversion Phase
- **Missing Schema Report**: Clear error message directing user to run `scan-schemas` first
- **Schema Mismatch**: Warns if file structure doesn't match schema
- **Type Coercion Failures**: Logs warnings but continues processing
- **Missing Fields**: Fills with `None`/`null` according to schema
- **Per-File Processing**: One bad file doesn't stop the entire batch

## Performance Considerations

### Large Files

For very large JSON files:

1. **Use Sampling**: Limit records analyzed during schema discovery
   ```bash
   python -m src.cli scan-schemas --max-sample-size 10000
   ```

2. **Memory Usage**: 
   - Schema discovery loads entire file into memory
   - Conversion processes file in chunks where possible
   - Consider splitting very large files

3. **Sampling Strategies**:
   - `first`: Analyze first N records (faster, may miss edge cases)
   - `random`: Random sample of N records (more representative, slower)

### Best Practices

- **Schema Discovery**: Run once per dataset, reuse schema reports
- **Incremental Updates**: Regenerate schema reports when data structure changes
- **Validation**: Review Markdown reports before conversion
- **Testing**: Use small samples first, then process full datasets

## Testing

Run the test suite:

```bash
pytest tests/
```

Or run specific test files:

```bash
pytest tests/test_schema_reader.py
pytest tests/test_converter.py
```

Run with verbose output:

```bash
pytest tests/ -v
```

## Known Limitations

1. **Memory**: Large files are loaded entirely into memory. For very large files, use the `--max-sample-size` option for schema inference.

2. **Array Handling**: Arrays of objects are stored as JSON strings in CSV/Parquet output. This is a design choice to maintain compatibility with flat file formats.

3. **Type Coercion**: Type coercion is best-effort. Values that cannot be coerced are preserved as-is with a warning.

4. **Timestamp Detection**: Timestamp detection uses pattern matching. Complex or non-standard date formats may not be detected.

5. **Encoding**: Files are assumed to be UTF-8 encoded.

6. **File Format Detection**: The tool attempts to auto-detect JSON format, but may fail on edge cases. In such cases, try explicitly structuring your data in a supported format.

## Extending the Project

The codebase is modular and designed for extension:

### Adding New Output Formats
Add conversion methods to `converter.py`:
```python
def convert_to_avro(self, filepath: Path, schema: Optional[FileSchema] = None) -> bool:
    # Implementation here
```

### Custom Type Inference
Extend `_infer_type()` in `schema_reader.py`:
```python
def _infer_type(self, value: Any) -> str:
    # Add custom type detection logic
```

### Different Sampling Strategies
Implement new strategies in `SchemaReader._sample_records()`:
```python
def _sample_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if self.sampling_strategy == "stratified":
        # Custom sampling logic
```

### Schema Validation
Add validation logic using the inferred schemas:
```python
def validate_data(self, data: Dict, schema: FileSchema) -> List[str]:
    # Return list of validation errors
```

## Troubleshooting

### Common Issues

**Issue**: "Schema report not found" error during conversion
- **Solution**: Run `python -m src.cli scan-schemas` first to generate the schema report

**Issue**: "No schemas found in the schema report"
- **Solution**: Regenerate the schema report. The data structure may have changed.

**Issue**: Conversion fails with type errors
- **Solution**: Review the schema report to understand data types. Consider adjusting type coercion logic.

**Issue**: Memory errors with large files
- **Solution**: Use `--max-sample-size` to limit records analyzed during schema discovery.

**Issue**: Array-based data not converting correctly
- **Solution**: Ensure column metadata is present in the JSON file. The tool looks for columns in `meta.view.columns` or similar paths.

## License

This project is provided as-is for educational and data engineering purposes.

## Contributing

Feel free to extend this project with additional features such as:
- Support for more output formats (Avro, ORC, etc.)
- Schema validation against inferred schemas
- Incremental schema updates
- Database export capabilities
- More sophisticated nested structure handling
- Streaming processing for very large files
- Schema versioning and migration tools

## Acknowledgments

This tool is designed to handle various JSON formats commonly found in:
- API responses
- Data exports from platforms like Socrata, CKAN, and other open data portals
- Log files in JSON format
- Database exports
- ETL pipeline outputs
