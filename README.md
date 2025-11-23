# SchemaForge 🔨

<div align="center">

![SchemaForge Logo](https://img.shields.io/badge/SchemaForge-Data%20Intelligence-orange?style=for-the-badge&logo=databricks&logoColor=white)

**Intelligent JSON Schema Discovery & Data Transformation**

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)](tests/)

[Features](#-features) • [Installation](#-installation) • [Quick Start](#-quick-start) • [CLI Reference](#-cli-reference) • [Documentation](#-documentation) • [Use Cases](#-use-cases)

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
🐛 Debug Type Errors           ✅ Parquet/CSV/Avro/ORC
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

### 📁 Multi-Format Support
**Input:** 11+ JSON formats automatically detected
- ✅ Standard JSON Arrays
- ✅ NDJSON (Newline-Delimited)
- ✅ Wrapper Objects
- ✅ Array-Based Tabular (Socrata/OpenData)
- ✅ GeoJSON
- ✅ Single Objects
- ✅ Python Literal Format
- ✅ Embedded JSON Strings

**Output:** 4 analytics-ready formats
- ✅ **Parquet** (recommended for big data)
- ✅ **CSV** (universal compatibility)
- ✅ **Avro** (schema evolution support)
- ✅ **ORC** (optimized for Hadoop)

### 🔄 Schema-First Workflow
1. **Scan once** → Generate comprehensive schema reports
2. **Review** → Human-readable Markdown + machine-readable JSON
3. **Convert everywhere** → Consistent schemas across all conversions
4. **Validate** → Ensure data quality and schema compliance
5. **Benchmark** → Measure performance and optimize workflows

### 🚀 Production-Ready
- **Robust Error Handling**: Graceful failures, detailed logging
- **Sampling Support**: Process large files efficiently
- **Batch Processing**: Convert multiple files in one command
- **Type Coercion**: Intelligent type conversion with fallbacks
- **Performance Benchmarking**: Built-in benchmarking suite for optimization
- **Schema Validation**: Validate JSON files against inferred schemas

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
git clone https://github.com/Syntax-Error-1337/SchemaForge.git
cd SchemaForge

# Install required packages
pip install -r requirements.txt
```

### Required Packages
```
pandas>=2.0.0      # Data manipulation and analysis
pyarrow>=12.0.0    # Parquet and ORC support
pytest>=7.0.0      # Testing framework
ijson>=3.2.0       # Streaming JSON parser
json5>=0.9.0       # JSON5 format support
fastavro>=1.8.0    # Avro format support
psutil>=5.9.0      # System performance monitoring
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

### 4️⃣ Convert to Your Preferred Format
```bash
# Convert to Parquet (recommended for analytics)
python -m src.cli convert --format parquet

# Or convert to CSV
python -m src.cli convert --format csv

# Or convert to Avro
python -m src.cli convert --format avro

# Or convert to ORC
python -m src.cli convert --format orc
```

**That's it!** Your data is now in `output/` directory, ready for analysis.

---

## 🔧 CLI Reference

SchemaForge provides four powerful commands for different workflows:

### Command Overview

| Command | Purpose | Use Case |
|---------|---------|----------|
| `scan-schemas` | Discover JSON structures | Initial data exploration |
| `convert` | Transform to analytics formats | Data pipeline integration |
| `validate` | Verify schema compliance | Data quality assurance |
| `benchmark` | Measure performance | Optimization and testing |

---

### 📋 `scan-schemas` - Discover JSON Schemas

Analyze JSON files and generate comprehensive schema reports with type inference and statistics.

```bash
python -m src.cli scan-schemas [OPTIONS]
```

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--data-dir` | string | `data` | Input directory containing JSON files |
| `--output-report` | string | `reports/schema_report.md` | Path for Markdown report |
| `--max-sample-size` | integer | None | Max records to analyze per file |
| `--sampling-strategy` | choice | `first` | Sampling method: `first` or `random` |

#### Examples

**Basic usage - Scan all files:**
```bash
python -m src.cli scan-schemas
```

**Analyze only first 1000 records per file:**
```bash
python -m src.cli scan-schemas --max-sample-size 1000
```

**Use random sampling for better representation:**
```bash
python -m src.cli scan-schemas --sampling-strategy random --max-sample-size 500
```

**Custom data directory:**
```bash
python -m src.cli scan-schemas --data-dir my_json_data
```

**Custom output location:**
```bash
python -m src.cli scan-schemas --output-report custom_reports/my_schema.md
```

**Large file processing with random sampling:**
```bash
python -m src.cli scan-schemas \
  --data-dir large_datasets \
  --max-sample-size 10000 \
  --sampling-strategy random \
  --output-report reports/large_schema.md
```

#### Output Files
- **Markdown Report** (`*.md`): Human-readable schema documentation with statistics
- **JSON Report** (`*.json`): Machine-readable schema for programmatic use (auto-generated alongside Markdown)

---

### 🔄 `convert` - Transform JSON to Analytics Formats

Convert JSON files to Parquet, CSV, Avro, or ORC using inferred schemas.

```bash
python -m src.cli convert --format [parquet|csv|avro|orc] [OPTIONS]
```

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--format` | choice | **Required** | Output format: `parquet`, `csv`, `avro`, or `orc` |
| `--data-dir` | string | `data` | Input directory containing JSON files |
| `--output-dir` | string | `output` | Output directory for converted files |
| `--schema-report` | string | `reports/schema_report.json` | JSON schema report path |
| `--schema-report-md` | string | None | Alternative: Markdown report path (auto-finds JSON) |

> **⚠️ Important:** You must run `scan-schemas` first to generate a schema report before using `convert`.

#### Examples

**Convert to Parquet (recommended for big data):**
```bash
python -m src.cli convert --format parquet
```

**Convert to CSV:**
```bash
python -m src.cli convert --format csv
```

**Convert to Avro (schema evolution support):**
```bash
python -m src.cli convert --format avro
```

**Convert to ORC (Hadoop optimized):**
```bash
python -m src.cli convert --format orc
```

**Custom directories:**
```bash
python -m src.cli convert --format parquet \
  --data-dir my_data \
  --output-dir parquet_output
```

**Use custom schema report:**
```bash
python -m src.cli convert --format parquet \
  --schema-report custom_schemas/report.json
```

**Complete ETL pipeline example:**
```bash
# Step 1: Scan schemas
python -m src.cli scan-schemas --data-dir api_exports/

# Step 2: Convert to Parquet for data lake
python -m src.cli convert --format parquet \
  --data-dir api_exports/ \
  --output-dir data_lake/raw/
```

---

### ✅ `validate` - Verify Schema Compliance

Validate JSON files against inferred schemas to ensure data quality and consistency.

```bash
python -m src.cli validate [OPTIONS]
```

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--data-dir` | string | `data` | Directory containing JSON files to validate |
| `--schema-report` | string | `reports/schema_report.json` | JSON schema report for validation |

#### Examples

**Basic validation:**
```bash
python -m src.cli validate
```

**Validate custom directory:**
```bash
python -m src.cli validate --data-dir new_data/
```

**Validate against custom schema:**
```bash
python -m src.cli validate \
  --data-dir production_data/ \
  --schema-report schemas/production_schema.json
```

#### Output Example
```
2025-11-23 10:00:00 - INFO - Starting validation...
2025-11-23 10:00:05 - INFO - Validation complete: 5/6 files valid
2025-11-23 10:00:05 - WARNING - Invalid files:
  - corrupted_data.json: 3 errors
    Type mismatch for field 'age': expected integer, got string
    Missing required field 'email'
    Invalid value for field 'status': not in enum
```

---

### 🏁 `benchmark` - Performance Benchmarking

Run performance benchmarks to measure schema inference and conversion speeds.

```bash
python -m src.cli benchmark [OPTIONS]
```

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--type` | choice | `all` | Benchmark type: `schema`, `conversion`, or `all` |
| `--data-dir` | string | `data` | Directory containing JSON files |
| `--result-dir` | string | `result` | Directory for benchmark results |
| `--max-sample-size` | integer | None | Max sample size for schema benchmark |
| `--formats` | string | `parquet,csv,avro,orc` | Comma-separated list of formats |

#### Examples

**Run all benchmarks:**
```bash
python -m src.cli benchmark
```

**Benchmark only schema inference:**
```bash
python -m src.cli benchmark --type schema
```

**Benchmark only conversion (all formats):**
```bash
python -m src.cli benchmark --type conversion
```

**Benchmark specific formats:**
```bash
python -m src.cli benchmark \
  --type conversion \
  --formats parquet,avro
```

**Custom benchmark configuration:**
```bash
python -m src.cli benchmark \
  --type all \
  --data-dir large_datasets \
  --result-dir benchmarks/2025-11-23 \
  --max-sample-size 5000
```

**Test performance with sampling:**
```bash
python -m src.cli benchmark \
  --type schema \
  --max-sample-size 1000 \
  --result-dir benchmarks/sampled
```

#### Benchmark Output

Results are saved to the `result-dir` with detailed performance metrics:
- **Schema Benchmark**: Time taken for schema inference, memory usage
- **Conversion Benchmark**: Conversion time per format, file sizes, compression ratios
- **CSV Reports**: Machine-readable performance data for analysis

See [`BENCHMARK_OPTIMIZATION.md`](BENCHMARK_OPTIMIZATION.md) for optimization details.

---

## 📖 Documentation

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

```json
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
| `object` | Nested structures | `{"key": "value"}` |

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
```markdown
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
**Solution:** Automatic schema discovery and multi-format conversion  
**Benefit:** 80% faster pipeline development, consistent data types

```bash
# Example workflow
python -m src.cli scan-schemas --data-dir api_exports/
python -m src.cli convert --format parquet --output-dir data_lake/
python -m src.cli validate --data-dir api_exports/
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
**Solution:** Convert to Parquet/Avro/ORC with preserved schemas  
**Benefit:** Better compression, faster queries, lower costs

---

### 🔄 Data Migration & Format Conversion

**Problem:** Migrating from JSON-based systems to columnar formats  
**Solution:** Intelligent schema inference preserves data semantics  
**Benefit:** Accurate migrations without data loss

---

## 🏗️ Project Structure

```
SchemaForge/
├── 📁 data/                    # Input JSON files (place your data here)
│   ├── .gitkeep
│   └── *.json                 # Your JSON data files
├── 📁 output/                  # Converted output files
│   ├── *.parquet              # Parquet output files
│   ├── *.csv                  # CSV output files
│   ├── *.avro                 # Avro output files
│   └── *.orc                  # ORC output files
├── 📁 reports/                 # Generated schema reports
│   ├── schema_report.md       # Human-readable report
│   └── schema_report.json     # Machine-readable report
├── 📁 result/                  # Benchmark results
│   └── *.csv                  # Performance metrics
├── 📁 src/                     # Source code
│   ├── __init__.py
│   ├── schema_reader.py       # Schema inference engine
│   ├── converter.py           # Format conversion module
│   ├── json_loader.py         # JSON format detection & loading
│   ├── validator.py           # Schema validation module
│   ├── benchmark.py           # Performance benchmarking suite
│   └── cli.py                 # Command-line interface
├── 📁 tests/                   # Test suite
│   ├── test_schema_reader.py
│   ├── test_converter.py
│   ├── test_json_loader.py
│   └── test_universal_json.py
├── 📄 README.md               # This file
├── 📄 BENCHMARK_OPTIMIZATION.md  # Benchmark optimization guide
├── 📄 LICENSE                 # MIT License
├── 📄 requirements.txt        # Python dependencies
└── 📄 pytest.ini              # pytest configuration
```

---

## 🔧 Advanced Usage

### Large File Processing

For very large JSON files, use sampling to speed up schema inference:

```bash
# Analyze first 10,000 records only
python -m src.cli scan-schemas --max-sample-size 10000

# Random sample for better representation
python -m src.cli scan-schemas \
  --sampling-strategy random \
  --max-sample-size 5000
```

### Complete Data Pipeline Workflow

```bash
# 1. Scan and analyze schemas
python -m src.cli scan-schemas \
  --data-dir raw_data \
  --output-report reports/production_schema.md

# 2. Validate data quality
python -m src.cli validate \
  --data-dir raw_data \
  --schema-report reports/production_schema.json

# 3. Convert to Parquet for analytics
python -m src.cli convert --format parquet \
  --data-dir raw_data \
  --output-dir processed/parquet

# 4. Also convert to CSV for compatibility
python -m src.cli convert --format csv \
  --data-dir raw_data \
  --output-dir processed/csv

# 5. Benchmark performance
python -m src.cli benchmark \
  --type all \
  --data-dir raw_data \
  --result-dir benchmarks/$(date +%Y%m%d)
```

### Programmatic Usage

Use SchemaForge as a Python library:

```python
from src.schema_reader import SchemaReader
from src.converter import Converter
from src.validator import SchemaValidator
from pathlib import Path

# 1. Discover schemas
reader = SchemaReader(
    data_dir=Path("data"),
    max_sample_size=1000,
    sampling_strategy="random"
)
schemas = reader.scan_directory()
reader.generate_report(Path("reports/schema.md"))

# 2. Validate data
validator = SchemaValidator(schema_report_path="reports/schema.json")
validation_results = validator.validate_all("data")

# 3. Convert with schema
converter = Converter(
    data_dir=Path("data"),
    output_dir=Path("output"),
    schema_report_path=Path("reports/schema.json")
)
results = converter.convert_all(format="parquet")

# 4. Handle results
for filename, success in results.items():
    if success:
        print(f"✅ {filename} converted successfully")
    else:
        print(f"❌ {filename} conversion failed")
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
        # Add more custom logic here
        return super()._infer_type(value)

# Use custom reader
reader = CustomSchemaReader(data_dir="data")
schemas = reader.scan_directory()
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

# Run specific test function
pytest tests/test_converter.py::test_parquet_conversion

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run only conversion tests
pytest tests/test_converter.py -v
```

### Test Coverage

The test suite includes:
- ✅ Schema inference for all data types
- ✅ JSON format detection (11+ formats)
- ✅ Parquet/CSV/Avro/ORC conversion
- ✅ Nested structure flattening
- ✅ Type coercion and edge cases
- ✅ Error handling and validation

---

## 🎯 Performance & Optimization

### Benchmark Optimization

SchemaForge includes a built-in benchmarking suite with significant optimizations:

**Before Optimization:**
- Schemas inferred **5 times** (1 inference + 4 file loads)
- Redundant file I/O operations
- Slower benchmarking

**After Optimization:**
- Schemas inferred **only ONCE**
- Reused across all format conversions
- ~4x faster benchmarking

See [`BENCHMARK_OPTIMIZATION.md`](BENCHMARK_OPTIMIZATION.md) for detailed optimization guide.

### Performance Tips

1. **Use Sampling for Large Files:**
   ```bash
   python -m src.cli scan-schemas --max-sample-size 10000 --sampling-strategy random
   ```

2. **Choose the Right Format:**
   - **Parquet**: Best for big data analytics (smallest file size, fastest reads)
   - **Avro**: Best for schema evolution and streaming
   - **ORC**: Best for Hadoop/Hive ecosystems
   - **CSV**: Best for universal compatibility

3. **Benchmark Your Workflow:**
   ```bash
   python -m src.cli benchmark --type all --result-dir benchmarks/
   ```

4. **Monitor Memory Usage:**
   - The benchmark suite includes memory profiling via `psutil`
   - Check `result/` directory for performance metrics

---

## 🚨 Known Limitations

| Limitation | Description | Workaround |
|------------|-------------|------------|
| **Memory Usage** | Large files loaded into memory | Use `--max-sample-size` for schema inference |
| **Array of Objects** | Stored as JSON strings in output | Design choice for flat file compatibility |
| **Type Coercion** | Best-effort conversion | Manual validation recommended with `validate` command |
| **Timestamp Detection** | Pattern-based recognition | May miss custom formats |
| **Encoding** | Assumes UTF-8 | Convert files to UTF-8 first |
| **Schema Versioning** | No built-in versioning | Use git or file naming conventions |

---

## 🤝 Contributing

We welcome contributions! Here are some ideas:

### Features to Add
- [ ] Schema versioning and migration tools
- [ ] Streaming processing for very large files
- [ ] Database export capabilities (PostgreSQL, MySQL)
- [ ] GUI/Web interface
- [ ] Docker support
- [ ] Schema diff tool
- [ ] Support for more output formats (JSON Lines, Protocol Buffers)
- [ ] Data quality metrics and profiling
- [ ] Schema registry integration (Confluent, AWS Glue)

### How to Contribute
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/SchemaForge.git
cd SchemaForge

# Install dependencies
pip install -r requirements.txt

# Run tests to ensure everything works
pytest tests/ -v

# Make your changes and test
pytest tests/ --cov=src

# Submit your PR!
```

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

### Built With
- **pandas** - Data manipulation and analysis
- **pyarrow** - Parquet and ORC support
- **fastavro** - High-performance Avro I/O
- **ijson** - Streaming JSON parser
- **pytest** - Testing framework

---

## 📞 Support

- **Documentation**: You're reading it! 📖
- **Issues**: [GitHub Issues](https://github.com/Syntax-Error-1337/SchemaForge/issues)
- **Discussions**: [GitHub Discussions](https://github.com/Syntax-Error-1337/SchemaForge/discussions)
- **Email**: Open an issue for support questions

### Getting Help

**Before opening an issue:**
1. Check existing issues and discussions
2. Review the documentation thoroughly
3. Try running with `--help` flag: `python -m src.cli [command] --help`
4. Run tests to verify installation: `pytest tests/ -v`

---

## 🌟 Star History

If SchemaForge saved you time, consider giving it a star! ⭐

<a href="https://www.star-history.com/#Syntax-Error-1337/SchemaForge&type=date&legend=bottom-right">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=Syntax-Error-1337/SchemaForge&type=date&theme=dark&legend=bottom-right" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=Syntax-Error-1337/SchemaForge&type=date&legend=bottom-right" />
   <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=Syntax-Error-1337/SchemaForge&type=date&legend=bottom-right" />
 </picture>
</a>

---

## 🎓 Example Workflows

### Workflow 1: Data Analysis Pipeline
```bash
# Start with raw JSON from API
python -m src.cli scan-schemas --data-dir api_data/

# Validate data quality
python -m src.cli validate --data-dir api_data/

# Convert to Parquet for fast analytics
python -m src.cli convert --format parquet --output-dir analytics/

# Your data is ready for pandas, Spark, or any analytics tool!
```

### Workflow 2: Data Lake Ingestion
```bash
# Scan large datasets with sampling
python -m src.cli scan-schemas \
  --data-dir raw_lake/ \
  --max-sample-size 10000 \
  --sampling-strategy random

# Convert to multiple formats for different use cases
python -m src.cli convert --format parquet --output-dir lake/parquet/
python -m src.cli convert --format avro --output-dir lake/avro/
python -m src.cli convert --format orc --output-dir lake/orc/
```

### Workflow 3: Research Data Processing
```bash
# Process survey data from multiple sources
python -m src.cli scan-schemas --data-dir surveys/

# Convert to CSV for Excel/R compatibility
python -m src.cli convert --format csv --output-dir results/

# Also create Parquet for Python/Jupyter notebooks
python -m src.cli convert --format parquet --output-dir results/
```

### Workflow 4: Performance Testing
```bash
# Benchmark your data processing
python -m src.cli benchmark --type all --result-dir benchmarks/

# Analyze results and optimize
cat benchmarks/schema_benchmark_*.csv
cat benchmarks/conversion_benchmark_*.csv
```

---

<div align="center">

**Made with** 🔨 **by developers, for developers**

**SchemaForge** - Transform Data Chaos into Analytics Gold

[⬆ Back to Top](#schemaforge-)

</div>
