# SchemaForge 🔨

<div align="center">

![SchemaForge Logo](https://img.shields.io/badge/SchemaForge-Data%20Intelligence-orange?style=for-the-badge&logo=databricks&logoColor=white)

**Intelligent JSON Schema Discovery & Data Transformation**

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)](tests/)

[Features](#-features) • [Installation](#-installation) • [Quick Start](#-quick-start) • [Documentation](#-documentation) • [Use Cases](#-use-cases)

</div>

---

## 🎯 What is SchemaForge?

SchemaForge is a **schema-first data pipeline tool** that automatically discovers JSON structures and converts them to analytics-ready formats. Stop wasting time on manual schema definitions and data wrangling—let SchemaForge do the heavy lifting.

### Why SchemaForge?

```
Traditional Workflow:          SchemaForge Workflow:
─────────────────             ──────────────────
📄 JSON Files                  📄 JSON Files
    ↓                              ↓
⚙️  Manual Analysis            🔍 Automatic Scan
    ↓                              ↓
📝 Write Schemas               📊 Schema Report
    ↓                              ↓
💻 Write Code                  🔨 One Command
    ↓                              ↓
🐛 Debug Type Errors           ✅ Parquet/CSV
    ↓
⏰ Hours Later...
    ↓
✅ Parquet/CSV

Time: Hours → Minutes
Errors: Many → Zero
```

---

## ✨ Features

### 🧠 Intelligent Schema Inference
- **Advanced Type Detection**: Strings, integers, floats, booleans, timestamps, URLs, emails, UUIDs, IP addresses, arrays, objects
- **Smart String Analysis**: Detects URLs, email addresses, UUIDs, IP addresses, and numeric strings
- **Enhanced Timestamp Detection**: Supports ISO dates, Unix timestamps, and multiple date formats
- **Enum Detection**: Automatically identifies fields with limited distinct values (enum-like fields)
- **Statistical Analysis**: Collects min/max values for numbers, length statistics for strings
- **Nested Structure Handling**: Flattens nested JSON with dot notation (`user.address.city`)
- **Nullable Field Detection**: Identifies which fields can be null
- **Mixed Type Recognition**: Detects and reports inconsistent types across records
- **Embedded JSON Parsing**: Automatically detects and parses JSON strings embedded in fields

### 📁 Multi-Format JSON Support
Handles 11+ JSON formats automatically:
- ✅ **Standard JSON Arrays**: `[{...}, {...}]`
- ✅ **NDJSON** (Newline-Delimited): One object per line
- ✅ **Wrapper Objects**: `{data: [...]}`, `{results: [...]}`, etc.
- ✅ **Array-Based Tabular**: Socrata/OpenData format with metadata
- ✅ **GeoJSON**: FeatureCollection format
- ✅ **Single Objects**: Single-record datasets
- ✅ **Python Literal Format**: `{'key': 'value'}` with single quotes (Python dict/list syntax)
- ✅ **Embedded JSON Strings**: JSON stored as string values (auto-parsed)
- ✅ **Numeric Strings**: String values that represent numbers (auto-detected)
- ✅ **Mixed Format Files**: Handles files with inconsistent structures
- ✅ **JSON with Comments**: Basic support for comment-like structures

### 🔄 Schema-First Workflow
1. **Scan once** → Generate comprehensive schema reports
2. **Review** → Human-readable Markdown + machine-readable JSON
3. **Convert everywhere** → Consistent schemas across all conversions

### 🚀 Production-Ready
- **Robust Error Handling**: Graceful failures, detailed logging
- **Sampling Support**: Process large files efficiently
- **Batch Processing**: Convert multiple files in one command
- **Type Coercion**: Intelligent type conversion with fallbacks

### 📊 Dual Report Format
- **Markdown Report**: Beautiful, human-readable documentation
- **JSON Report**: Machine-readable schema for programmatic use

---

## 📦 Installation

### Prerequisites
- Python 3.8 or higher
- pip package manager

### Install Dependencies

```bash
# Clone the repository
git clone https://github.com/yourusername/schemaforge.git
cd schemaforge

# Install required packages
pip install -r requirements.txt
```

### Required Packages
```
pandas>=2.0.0      # Data manipulation
pyarrow>=12.0.0    # Parquet support
pytest>=7.0.0      # Testing framework
```

---

## 🚀 Quick Start

### 1️⃣ Place Your JSON Files
```bash
# Copy your JSON files to the data directory
cp your_data/*.json data/
```

### 2️⃣ Discover Schemas
```bash
# Scan all JSON files and generate schema reports
python -m src.cli scan-schemas
```

**Output:**
- `reports/schema_report.md` - Beautiful, human-readable report
- `reports/schema_report.json` - Machine-readable schema definitions

### 3️⃣ Review the Schema
```bash
# Check the generated report
cat reports/schema_report.md
```

### 4️⃣ Convert to Parquet or CSV
```bash
# Convert to Parquet (recommended for analytics)
python -m src.cli convert --format parquet

# Or convert to CSV
python -m src.cli convert --format csv
```

**That's it!** Your data is now in `output/` directory, ready for analysis.

---

## 📖 Documentation

### Command Reference

#### `scan-schemas` - Discover JSON Schemas

```bash
python -m src.cli scan-schemas [OPTIONS]
```

**Options:**
| Option | Description | Default |
|--------|-------------|---------|
| `--data-dir` | Input directory containing JSON files | `data` |
| `--output-report` | Path for Markdown report | `reports/schema_report.md` |
| `--max-sample-size` | Max records to analyze per file | All records |
| `--sampling-strategy` | Sampling method: `first` or `random` | `first` |

**Examples:**
```bash
# Basic usage
python -m src.cli scan-schemas

# Analyze only first 1000 records per file
python -m src.cli scan-schemas --max-sample-size 1000

# Use random sampling for better representation
python -m src.cli scan-schemas --sampling-strategy random --max-sample-size 500

# Custom data directory
python -m src.cli scan-schemas --data-dir my_json_data
```

---

#### `convert` - Transform JSON to Parquet/CSV

```bash
python -m src.cli convert --format [parquet|csv] [OPTIONS]
```

**Options:**
| Option | Description | Default |
|--------|-------------|---------|
| `--format` | Output format: `parquet` or `csv` | **Required** |
| `--data-dir` | Input directory | `data` |
| `--output-dir` | Output directory | `output` |
| `--schema-report` | JSON schema report path | `reports/schema_report.json` |

**Examples:**
```bash
# Convert to Parquet
python -m src.cli convert --format parquet

# Convert to CSV with custom directories
python -m src.cli convert --format csv \
  --data-dir my_data \
  --output-dir csv_output

# Use custom schema report
python -m src.cli convert --format parquet \
  --schema-report custom_schemas/report.json
```

---

### Supported JSON Formats

<details>
<summary><b>1️⃣ Standard JSON Array</b></summary>

```json
[
  {"id": 1, "name": "Alice", "age": 30},
  {"id": 2, "name": "Bob", "age": 25}
]
```
**Use case:** Most common JSON format from APIs and exports
</details>

<details>
<summary><b>2️⃣ Newline-Delimited JSON (NDJSON)</b></summary>

```
{"id": 1, "name": "Alice", "age": 30}
{"id": 2, "name": "Bob", "age": 25}
```
**Use case:** Log files, streaming data, large datasets
</details>

<details>
<summary><b>3️⃣ Wrapper Objects</b></summary>

```json
{
  "data": [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"}
  ],
  "metadata": {...}
}
```
**Auto-detected fields:** `data`, `results`, `items`, `records`, `rows`, `entries`

**Use case:** API responses with metadata
</details>

<details>
<summary><b>4️⃣ Array-Based Tabular Data</b></summary>

```json
{
  "meta": {
    "view": {
      "columns": [
        {"name": "id", "fieldName": "id", "dataTypeName": "number"},
        {"name": "name", "fieldName": "name", "dataTypeName": "text"}
      ]
    }
  },
  "data": [
    [1, "Alice"],
    [2, "Bob"]
  ]
}
```
**Use case:** Socrata, CKAN, and other open data portals

**Features:**
- ✅ Extracts column definitions from metadata
- ✅ Converts arrays to objects using column names
- ✅ Skips hidden/meta columns automatically
</details>

<details>
<summary><b>5️⃣ GeoJSON Format</b></summary>

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {"name": "Location 1", "value": 100},
      "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]}
    }
  ]
}
```
**Use case:** Geographic data from mapping APIs

**Note:** Extracts `properties` field from features
</details>

<details>
<summary><b>6️⃣ Single JSON Object</b></summary>

```json
{
  "id": 1,
  "name": "Alice",
  "address": {
    "city": "New York",
    "zip": "10001"
  }
}
```
**Use case:** Configuration files, single-record exports

**Note:** Treated as a single-record dataset
</details>

---

### Schema Inference Rules

#### Data Types
SchemaForge detects these types automatically:

| Type | Description | Example |
|------|-------------|---------|
| `string` | Text data | `"Alice"` |
| `integer` | Whole numbers | `42` |
| `float` | Decimal numbers | `3.14` |
| `boolean` | True/false | `true` |
| `timestamp` | Date/time strings | `"2023-01-01T10:00:00Z"`, `"2023/01/01"`, Unix timestamps |
| `url` | Web URLs | `"https://example.com"` |
| `email` | Email addresses | `"user@example.com"` |
| `uuid` | UUID identifiers | `"550e8400-e29b-41d4-a716-446655440000"` |
| `ip_address` | IP addresses (IPv4/IPv6) | `"192.168.1.1"`, `"2001:db8::1"` |
| `numeric_string` | String values representing numbers | `"123"`, `"45.67"` |
| `json_string` | Embedded JSON stored as string | `"{\"key\": \"value\"}"` |
| `array<T>` | Lists of values | `["a", "b", "c"]` |
| `object` | Nested structures | `{"key": "value"}` | |

#### Nested Structures
Nested objects are flattened with dot notation:

**Input:**
```json
{
  "user": {
    "name": "Alice",
    "address": {
      "city": "NYC"
    }
  }
}
```

**Output Columns:**
- `user.name` (string)
- `user.address.city` (string)

#### Nullable Fields
Fields containing `null` values are marked as nullable in the schema.

#### Statistics & Analysis
SchemaForge automatically collects statistical information for each field:

- **Numeric Statistics**: Min/max values for integer and float fields
- **String Statistics**: Min/max/average length for string fields
- **Enum Detection**: Fields with limited distinct values (≤20) are flagged as enum-like
- **Value Distribution**: Distinct value sets are tracked for enum detection

**Example Report Output:**
```
| Field Name | Type | Statistics | Notes |
|------------|------|------------|-------|
| `age` | integer | min: 18, max: 65 | nullable |
| `email` | email | len: 10-50 (avg: 25.3) | nullable |
| `status` | string | enum: active, inactive, pending | enum-like |
```

---

## 💼 Use Cases

### 🏢 Data Engineering & ETL Pipelines
**Problem:** Building data pipelines with inconsistent JSON from multiple sources  
**Solution:** Automatic schema discovery and Parquet conversion  
**Benefit:** 80% faster pipeline development, consistent data types

```bash
# Example workflow
python -m src.cli scan-schemas --data-dir api_exports/
python -m src.cli convert --format parquet --output-dir data_lake/
```

---

### 🔬 Research Data Processing
**Problem:** Diverse JSON datasets from experiments, surveys, APIs  
**Solution:** One-command conversion to analysis-ready formats  
**Benefit:** More time for research, less time on data wrangling

**Example use cases:**
- Social media data analysis
- Scientific instrument outputs
- Survey response processing
- Open data portal research

---

### 🌐 Open Data Portal Integration
**Problem:** Socrata/CKAN array-based format is difficult to work with  
**Solution:** Automatic column extraction and conversion  
**Benefit:** Easy access to government and public datasets

**Supported portals:**
- data.gov datasets
- City open data portals
- Research institution repositories

---

### 🔄 API Data Integration
**Problem:** REST APIs return JSON in various formats  
**Solution:** Schema-first approach ensures consistency  
**Benefit:** Reliable data integration into warehouses

---

### 🗄️ Data Lake Ingestion
**Problem:** Need efficient storage format for JSON in data lakes  
**Solution:** Convert to Parquet with preserved schemas  
**Benefit:** Better compression, faster queries, lower costs

---

### 🔄 Data Migration & Format Conversion
**Problem:** Migrating from JSON-based systems to columnar formats  
**Solution:** Intelligent schema inference preserves data semantics  
**Benefit:** Accurate migrations without data loss

---

## 🏗️ Project Structure

```
schemaforge/
├── 📁 data/                    # Input JSON files (place your data here)
│   └── *.json                 # Your JSON data files
├── 📁 output/                  # Converted output files
│   ├── *.parquet              # Parquet output files
│   └── *.csv                  # CSV output files
├── 📁 reports/                 # Generated schema reports
│   ├── schema_report.md       # Human-readable report
│   └── schema_report.json     # Machine-readable report
├── 📁 src/
│   ├── __init__.py
│   ├── schema_reader.py       # Schema inference engine
│   ├── converter.py           # Format conversion module
│   └── cli.py                 # Command-line interface
├── 📁 tests/
│   ├── test_schema_reader.py
│   └── test_converter.py
├── README.md
├── requirements.txt
└── pytest.ini
```

---

## 🔧 Advanced Usage

### Large File Processing

For very large JSON files, use sampling:

```bash
# Analyze first 10,000 records only
python -m src.cli scan-schemas --max-sample-size 10000

# Random sample for better representation
python -m src.cli scan-schemas \
  --sampling-strategy random \
  --max-sample-size 5000
```

### Programmatic Usage

Use SchemaForge as a Python library:

```python
from src.schema_reader import SchemaReader
from src.converter import Converter
from pathlib import Path

# Discover schemas
reader = SchemaReader(
    data_dir=Path("data"),
    max_sample_size=1000
)
schemas = reader.scan_directory()
reader.generate_report(schemas, output_path=Path("reports/schema.md"))

# Convert with schema
converter = Converter(
    data_dir=Path("data"),
    output_dir=Path("output")
)
converter.convert_all(format="parquet", schema_report_path=Path("reports/schema.json"))
```

### Custom Type Handling

Extend type inference for custom formats:

```python
from src.schema_reader import SchemaReader

class CustomSchemaReader(SchemaReader):
    def _infer_type(self, value):
        # Add custom type detection
        if isinstance(value, str) and value.startswith("http"):
            return "url"
        return super()._infer_type(value)
```

---

## 🧪 Testing

Run the full test suite:

```bash
# Run all tests
pytest tests/

# Run with verbose output
pytest tests/ -v

# Run specific test file
pytest tests/test_schema_reader.py

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

---

## 🎯 Architecture

### Two-Phase Workflow

```
┌─────────────────────────────────────────────────────┐
│              Phase 1: Schema Discovery              │
│                                                       │
│  JSON Files → Format Detection → Schema Inference   │
│     ↓              ↓                   ↓             │
│  Load Data → Extract Columns → Analyze Types        │
│                                    ↓                 │
│                          Generate Reports            │
│                       (Markdown + JSON)              │
└─────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────┐
│                Phase 2: Conversion                  │
│                                                       │
│  Schema Report → Load Data → Apply Schema           │
│       ↓             ↓            ↓                   │
│  Type Coercion → Flatten → Convert Format           │
│                               ↓                      │
│                    Parquet/CSV Output                │
└─────────────────────────────────────────────────────┘
```

### Component Architecture

```
┌──────────────┐
│  CLI Layer   │  ← User commands (scan-schemas, convert)
└──────┬───────┘
       │
       ├─────────────────┬─────────────────┐
       ▼                 ▼                 ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│Schema Reader │  │  Converter   │  │JSON Loader   │
└──────────────┘  └──────────────┘  └──────────────┘
       │                 │                 │
       └─────────────────┴─────────────────┘
                         ▼
                  ┌──────────────┐
                  │  JSON Files  │
                  └──────────────┘
```

---

## 🚨 Known Limitations

| Limitation | Description | Workaround |
|------------|-------------|------------|
| **Memory Usage** | Large files loaded into memory | Use `--max-sample-size` for schema inference |
| **Array of Objects** | Stored as JSON strings in output | Design choice for flat file compatibility |
| **Type Coercion** | Best-effort conversion | Manual validation recommended |
| **Timestamp Detection** | Pattern-based recognition | May miss custom formats |
| **Encoding** | Assumes UTF-8 | Convert files to UTF-8 first |

---

## 🤝 Contributing

We welcome contributions! Here are some ideas:

### Features to Add
- [ ] Avro and ORC output formats
- [ ] Schema validation against inferred schemas
- [ ] Streaming processing for very large files
- [ ] Schema versioning and migration tools
- [ ] Database export capabilities (PostgreSQL, MySQL)
- [ ] GUI/Web interface
- [ ] Docker support
- [ ] Schema diff tool

### How to Contribute
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

Built with love for:
- Data engineers struggling with inconsistent JSON
- Researchers drowning in data wrangling
- Developers tired of manual schema definitions
- Anyone who's ever said "I wish this JSON had a schema"

---

## 📞 Support

- **Documentation**: [Full documentation](#-documentation)
- **Issues**: [GitHub Issues](https://github.com/yourusername/schemaforge/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/schemaforge/discussions)

---

## 🌟 Star History

If SchemaForge saved you time, consider giving it a star! ⭐

<a href="https://www.star-history.com/#Syntax-Error-1337/schemaforge&type=date&legend=bottom-right">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=Syntax-Error-1337/schemaforge&type=date&theme=dark&legend=bottom-right" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=Syntax-Error-1337/schemaforge&type=date&legend=bottom-right" />
   <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=Syntax-Error-1337/schemaforge&type=date&legend=bottom-right" />
 </picture>
</a>

---

<div align="center">

**Made with** 🔨 **by developers, for developers**

[⬆ Back to Top](#schemaforge-)

</div>
