# SchemaForge 🔨

<div align="center">

**Transform JSON Chaos into Analytics-Ready Data**

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

[Quick Start](#-quick-start) • [Features](#-features) • [Documentation](#-documentation) • [CLI Reference](#-cli-reference)

</div>

---

## 🎯 What is SchemaForge?

SchemaForge automatically discovers JSON schemas and converts them to analytics-ready formats. **Stop wasting hours on manual data wrangling**—let SchemaForge handle type detection, schema inference, and format conversion in seconds. 

### The Problem vs. The Solution

<table>
<tr>
<td width="50%">

**❌ Traditional Workflow**
```
📄 JSON Files
    ↓ (manual analysis)
📝 Write Schemas
    ↓ (write conversion code)
🐛 Debug Type Errors
    ↓ (fix, repeat)
⏰ Hours Later...
    ↓
✅ Ready for Analysis
```

</td>
<td width="50%">

**✅ SchemaForge Workflow**
```
📄 JSON Files
    ↓ (one command)
🔍 Auto Schema Discovery
    ↓ (one command)
✅ Parquet/CSV/Avro/ORC/Feather
    ↓
⚡ Minutes Later!
```

</td>
</tr>
</table>

**Time Saved:** Hours → Minutes | **Errors:** Many → Zero

---

## ✨ Features

### 🧠 Intelligent Schema Discovery
- **Advanced Type Detection** - Strings, numbers, booleans, timestamps, URLs, emails, UUIDs, IPs, and more
- **Smart Pattern Recognition** - Automatically detects enums, embedded JSON, and numeric strings
- **Statistical Analysis** - Collects min/max, length stats, and value distributions
- **Nested Structure Handling** - Flattens complex JSON with intuitive dot notation

### 📁 Universal Format Support
**Input:** 11+ JSON formats auto-detected
- Standard JSON Arrays • NDJSON • Wrapper Objects • GeoJSON • Socrata/OpenData • Single Objects • Python Literals • Embedded JSON

**Output:** 4 analytics-ready formats
- **Parquet** (recommended) • **CSV** • **Avro** • **ORC** • **Feather**

### 🚀 Production-Ready Tools
- **Schema Validation** - Verify data quality before processing
- **Performance Benchmarking** - Measure and optimize your pipelines
- **Batch Processing** - Convert multiple files in one command
- **Sampling Support** - Handle massive files efficiently

### ⚡ High-Performance Architecture
- **Memory-SAFE Processing** - Automatic 80% memory limit protection (no OOM crashes)
- **Dynamic Scaling** - Auto-scales worker processes based on available RAM
- **Smart Chunking** - Automatically chunks large files to fit in memory
- **Parallel Processing** - Multi-core conversion for maximum throughput

---

## 💾 Memory Management

SchemaForge is built for big data. It automatically manages system resources to ensure stability and speed.

### Intelligent Resource Scaling
1.  **Memory Protection**: Continuously monitors RAM usage, capping at **80%** (default) to keep your system responsive.
2.  **Adaptive Workers**: Calculates the optimal number of parallel workers (1-8) based on your specific hardware config.
3.  **Smart Chunking**: Large files (>50MB) are automatically processed in memory-safe chunks.

> **💡 Zero Configuration Needed:** These optimizations happen automatically. Just run `convert` and let SchemaForge handle the resources.

## 📦 Installation

```bash
# Clone the repository
git clone https://github.com/Syntax-Error-1337/SchemaForge.git
cd SchemaForge

# Install dependencies
pip install -r requirements.txt
```

**Requirements:** Python 3.8+, pandas, pyarrow, fastavro, ijson

---

## 🚀 Quick Start

### Three Simple Steps

```bash
# 1️⃣ Place your JSON files in the data directory
cp your_data/*.json data/

# 2️⃣ Discover schemas
python -m src.cli scan-schemas

# 3️⃣ Convert to your preferred format
python -m src.cli convert --format parquet
```

**That's it!** Your data is now in `output/`, ready for analysis. 🎉

### What Just Happened?

- ✅ All JSON structures automatically analyzed
- ✅ Types inferred with statistical confidence
- ✅ Nested objects flattened intelligently
- ✅ Schema reports generated (Markdown + JSON)
- ✅ Data converted to optimized format

---

## 🔧 CLI Reference

### Core Commands

| Command | Purpose | Example |
|---------|---------|---------|
| `scan-schemas` | Analyze JSON structure | `python -m src.cli scan-schemas` |
| `convert` | Transform to analytics formats | `python -m src.cli convert --format parquet` |
| `validate` | Verify schema compliance | `python -m src.cli validate` |
| `benchmark` | Measure performance | `python -m src.cli benchmark` |

### `scan-schemas` - Discover JSON Schemas

```bash
python -m src.cli scan-schemas [OPTIONS]
```

**Options:**
- `--data-dir` - Input directory (default: `data`)
- `--output-report` - Report path (default: `reports/schema_report.md`)
- `--max-sample-size` - Sample size for large files
- `--sampling-strategy` - `first` or `random` sampling

**Examples:**
```bash
# Basic usage
python -m src.cli scan-schemas

# Large files with random sampling
python -m src.cli scan-schemas --max-sample-size 10000 --sampling-strategy random

# Custom directory
python -m src.cli scan-schemas --data-dir my_json_data --output-report custom/schema.md
```

**Output:**
- `schema_report.md` - Human-readable documentation
- `schema_report.json` - Machine-readable schema

---

### `convert` - Transform to Analytics Formats

```bash
python -m src.cli convert --format [parquet|csv|avro|orc|feather] [OPTIONS]
```

**Options:**
- `--format` - **Required:** Output format
- `--data-dir` - Input directory (default: `data`)
- `--output-dir` - Output directory (default: `output`)
- `--schema-report` - Schema JSON path (default: `reports/schema_report.json`)

**Examples:**
```bash
# Convert to Parquet (recommended for big data)
python -m src.cli convert --format parquet

# Convert to CSV (universal compatibility)
python -m src.cli convert --format csv

# Convert to Avro (schema evolution)
python -m src.cli convert --format avro

# Convert to Feather (fast I/O for Pandas/Arrow)
python -m src.cli convert --format feather

# Custom directories
python -m src.cli convert --format parquet --data-dir raw_data --output-dir lake/
```

> **⚠️ Note:** Run `scan-schemas` first to generate the schema report.

---

### `validate` - Verify Data Quality

```bash
python -m src.cli validate [OPTIONS]
```

**Options:**
- `--data-dir` - Directory to validate (default: `data`)
- `--schema-report` - Schema for validation (default: `reports/schema_report.json`)

**Example:**
```bash
python -m src.cli validate --data-dir production_data
```

---

### `benchmark` - Performance Testing

```bash
python -m src.cli benchmark [OPTIONS]
```

**Options:**
- `--type` - Benchmark type: `schema`, `conversion`, or `all` (default: `all`)
- `--formats` - Formats to test (default: `parquet,csv,avro,orc,feather`)
- `--result-dir` - Results directory (default: `result`)

**Example:**
```bash
python -m src.cli benchmark --type all --result-dir benchmarks/
```

---

## 💼 Use Cases

### 🏢 Data Engineering
**Challenge:** Inconsistent JSON from multiple APIs  
**Solution:** Unified schema discovery and conversion  
**Result:** 80% faster pipeline development

### 🔬 Research Data
**Challenge:** Diverse datasets from experiments and surveys  
**Solution:** One-command conversion to analysis-ready formats  
**Result:** More time analyzing, less time wrangling

### 🌐 Open Data
**Challenge:** Complex formats from Socrata/CKAN portals  
**Solution:** Automatic column extraction and transformation  
**Result:** Easy access to government datasets

### 🗄️ Data Lakes
**Challenge:** Efficient storage for massive JSON collections  
**Solution:** Convert to optimized columnar formats  
**Result:** Better compression, faster queries, lower costs

---

## 📖 Documentation

### Supported JSON Formats

SchemaForge automatically detects and handles:

1. **Standard JSON Array** - `[{...}, {...}]`
2. **NDJSON** - Newline-delimited records
3. **Wrapper Objects** - `{"data": [...], "meta": {...}}`
4. **Socrata/OpenData** - Array-based tabular format
5. **GeoJSON** - Geographic feature collections
6. **Single Objects** - Individual JSON records
7. **Embedded JSON** - JSON strings within fields

### Schema Inference

**Detected Types:**
- Basic: `string`, `integer`, `float`, `boolean`
- Advanced: `timestamp`, `url`, `email`, `uuid`, `ip_address`
- Structured: `array<T>`, `object`, `json_string`
- Special: `numeric_string`, `enum`

**Features:**
- ✅ Nested structure flattening (`user.address.city`)
- ✅ Nullable field detection
- ✅ Mixed type recognition
- ✅ Statistical profiling (min/max, length, distributions)
- ✅ Enum detection for categorical fields

### Complete Workflow Example

```bash
# 1. Scan and analyze
python -m src.cli scan-schemas --data-dir raw_data

# 2. Validate quality
python -m src.cli validate --data-dir raw_data

# 3. Convert for analytics
python -m src.cli convert --format parquet --output-dir processed/

# 4. Benchmark performance
python -m src.cli benchmark --type all
```

---

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run specific test
pytest tests/test_schema_reader.py
```

**Test Coverage:**
- ✅ All 11+ JSON formats
- ✅ Type inference for all data types
- ✅ Format conversion (Parquet/CSV/Avro/ORC/Feather)
- ✅ Error handling and edge cases

---

## 🎯 Performance Tips

1. **Use Sampling for Large Files**
   ```bash
   python -m src.cli scan-schemas --max-sample-size 10000 --sampling-strategy random
   ```

2. **Choose the Right Format**
   - **Parquet** → Big data analytics (best compression)
   - **Avro** → Schema evolution & streaming
   - **ORC** → Hadoop/Hive ecosystems
   - **Feather** → Fast I/O for Pandas/Arrow workflows
   - **CSV** → Universal compatibility

3. **Monitor Performance**
   ```bash
   python -m src.cli benchmark --type all
   ```

See [BENCHMARK_OPTIMIZATION.md](BENCHMARK_OPTIMIZATION.md) for detailed optimization guide.

---

## 🏗️ Project Structure

```
SchemaForge/
├── data/              # Input JSON files
├── output/            # Converted files
├── reports/           # Schema reports (.md + .json)
├── result/            # Benchmark results
├── src/
│   ├── schema_reader/      # Schema inference engine
│   │   ├── core.py         # Main SchemaReader logic
│   │   ├── inference.py    # Type detection & analysis
│   │   ├── reporting.py    # Report generation
│   │   └── types.py        # Data models
│   ├── converter/          # Format conversion
│   │   ├── core.py         # Main Converter logic
│   │   ├── parquet.py      # Parquet support
│   │   ├── feather.py      # Feather support
│   │   ├── avro.py         # Avro support
│   │   ├── orc.py          # ORC support
│   │   └── csv.py          # CSV support
│   ├── benchmark/          # Performance testing
│   │   ├── core.py         # Benchmark suite
│   │   ├── schema.py       # Schema benchmarks
│   │   └── conversion.py   # Conversion benchmarks
│   ├── json_loader.py      # JSON format detection
│   ├── validator.py        # Schema validation
│   └── cli.py              # Command-line interface
└── tests/             # Test suite
└── tests/             # Test suite
```

---

## 🤝 Contributing

We welcome contributions! Here's how:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing`)
3. Make your changes with tests
4. Run tests (`pytest tests/ -v`)
5. Submit a Pull Request

**Ideas for Contributions:**
- Schema versioning tools
- Streaming processing for huge files
- GUI/Web interface
- Database export support
- Additional output formats

---

## 📝 License

MIT License - see [LICENSE](LICENSE) for details.

---

## 🙏 Acknowledgments

Built for data engineers, researchers, and developers who are tired of manual schema definitions.

**Powered by:** pandas • pyarrow • fastavro • ijson • pytest

---

## 📞 Support

- **Issues:** [GitHub Issues](https://github.com/Syntax-Error-1337/SchemaForge/issues)
- **Discussions:** [GitHub Discussions](https://github.com/Syntax-Error-1337/SchemaForge/discussions)
- **Documentation:** This README

**Before opening an issue:**
1. Check existing issues
2. Try `python -m src.cli [command] --help`
3. Run `pytest tests/ -v` to verify installation

---

<div align="center">

**SchemaForge** - Transform Data Chaos into Analytics Gold 🔨

⭐ **Star us on GitHub if SchemaForge saved you time!** ⭐

[⬆ Back to Top](#schemaforge-)

</div>
