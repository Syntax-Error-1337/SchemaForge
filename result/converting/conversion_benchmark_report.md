# Conversion Benchmark Report

**Total Files**: 5

**Formats Tested**: parquet, csv, avro, orc

## 📊 Format Comparison Summary

| Format | Time (s) | Memory (MB) | CPU (%) | Output Size | Compression (%) |
|--------|----------|-------------|---------|-------------|------------------|
| **PARQUET** | 14.169 | 99.95 | 99.4 | 15.20 MB | 85.86% |
| **CSV** | 15.177 | 103.04 | 99.1 | 85.74 MB | 20.24% |
| **AVRO** | 24.802 | 106.21 | 99.5 | 88.92 MB | 17.28% |
| **ORC** | 14.402 | 109.57 | 99.8 | 75.45 MB | 29.82% |

### 🏆 Winner Analysis

- **⚡ Fastest Conversion**: PARQUET (14.169s)
- **🗜️ Best Compression**: PARQUET (85.86%)
- **📦 Smallest Output**: PARQUET (15.20 MB)

---

## PARQUET Format

### Overall Performance

- **Execution Time**: 14.169s
- **Peak Memory**: 99.95 MB
- **CPU Usage**: 99.4%
- **Total Input Size**: 107.50 MB
- **Total Output Size**: 15.20 MB
- **Compression Ratio**: 85.86%

### Per-File Results

| File | Input Size | Output Size | Compression | Size Reduction |
|------|------------|-------------|-------------|----------------|
| global_gross_production_of_crops_livestock.json | 14.80 KB | 7.90 KB | 46.63% | 6.90 KB |
| iris.json | 15.43 KB | 4.76 KB | 69.14% | 10.67 KB |
| rows.json | 107.46 MB | 15.18 MB | 85.87% | 92.28 MB |
| stackoverflow_combined_info.json | 8.03 KB | 3.76 KB | 53.24% | 4.28 KB |
| top_coffee_producing_countries.json | 3.41 KB | 4.00 KB | -17.33% | -605.00 B |

## CSV Format

### Overall Performance

- **Execution Time**: 15.177s
- **Peak Memory**: 103.04 MB
- **CPU Usage**: 99.1%
- **Total Input Size**: 107.50 MB
- **Total Output Size**: 85.74 MB
- **Compression Ratio**: 20.24%

### Per-File Results

| File | Input Size | Output Size | Compression | Size Reduction |
|------|------------|-------------|-------------|----------------|
| global_gross_production_of_crops_livestock.json | 14.80 KB | 3.27 KB | 77.92% | 11.54 KB |
| iris.json | 15.43 KB | 3.91 KB | 74.66% | 11.52 KB |
| rows.json | 107.46 MB | 85.73 MB | 20.22% | 21.73 MB |
| stackoverflow_combined_info.json | 8.03 KB | 108.00 B | 98.69% | 7.93 KB |
| top_coffee_producing_countries.json | 3.41 KB | 1.26 KB | 62.9% | 2.14 KB |

## AVRO Format

### Overall Performance

- **Execution Time**: 24.802s
- **Peak Memory**: 106.21 MB
- **CPU Usage**: 99.5%
- **Total Input Size**: 107.50 MB
- **Total Output Size**: 88.92 MB
- **Compression Ratio**: 17.28%

### Per-File Results

| File | Input Size | Output Size | Compression | Size Reduction |
|------|------------|-------------|-------------|----------------|
| global_gross_production_of_crops_livestock.json | 14.80 KB | 3.79 KB | 74.39% | 11.01 KB |
| iris.json | 15.43 KB | 7.21 KB | 53.25% | 8.22 KB |
| rows.json | 107.46 MB | 88.91 MB | 17.27% | 18.55 MB |
| stackoverflow_combined_info.json | 8.03 KB | 499.00 B | 93.93% | 7.54 KB |
| top_coffee_producing_countries.json | 3.41 KB | 1.58 KB | 53.57% | 1.83 KB |

## ORC Format

### Overall Performance

- **Execution Time**: 14.402s
- **Peak Memory**: 109.57 MB
- **CPU Usage**: 99.8%
- **Total Input Size**: 107.50 MB
- **Total Output Size**: 75.45 MB
- **Compression Ratio**: 29.82%

### Per-File Results

| File | Input Size | Output Size | Compression | Size Reduction |
|------|------------|-------------|-------------|----------------|
| global_gross_production_of_crops_livestock.json | 14.80 KB | 3.80 KB | 74.33% | 11.00 KB |
| iris.json | 15.43 KB | 6.81 KB | 55.87% | 8.62 KB |
| rows.json | 107.46 MB | 75.43 MB | 29.8% | 32.03 MB |
| stackoverflow_combined_info.json | 8.03 KB | 958.00 B | 88.35% | 7.10 KB |
| top_coffee_producing_countries.json | 3.41 KB | 1.86 KB | 45.37% | 1.55 KB |

