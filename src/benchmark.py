"""
Benchmark Module

This module provides comprehensive benchmarking for schema inference and conversion operations.
"""

import time
import logging
import json
import psutil
import os
from pathlib import Path
from typing import Dict, Any, List
from src.schema_reader import SchemaReader
from src.converter import Converter

logger = logging.getLogger(__name__)

class BenchmarkSuite:
    """Class for running performance benchmarks."""
    
    def __init__(self, data_dir: str = "data", result_dir: str = "result"):
        """
        Initialize the Benchmark Suite.
        
        Args:
            data_dir: Directory containing JSON files to benchmark.
            result_dir: Directory to store benchmark results.
        """
        self.data_dir = Path(data_dir)
        self.result_dir = Path(result_dir)
        self.result_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories for different benchmark types
        self.schema_result_dir = self.result_dir / "schema"
        self.converting_result_dir = self.result_dir / "converting"
        self.schema_result_dir.mkdir(parents=True, exist_ok=True)
        self.converting_result_dir.mkdir(parents=True, exist_ok=True)
        
        # Get process for CPU/memory monitoring
        self.process = psutil.Process(os.getpid())

    def _get_file_size(self, filepath: Path) -> int:
        """Get file size in bytes."""
        return filepath.stat().st_size if filepath.exists() else 0

    def _format_size(self, size_bytes: int) -> str:
        """Format size in human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} TB"

    def _measure_resources(self, func, *args, **kwargs) -> Dict[str, Any]:
        """
        Measure execution time, CPU usage, and memory for a function.
        
        Returns:
            Dict with metrics: execution_time, peak_memory_mb, avg_cpu_percent
        """
        # Reset CPU percent measurement
        self.process.cpu_percent(interval=None)
        
        # Record initial memory
        mem_before = self.process.memory_info().rss / (1024 * 1024)  # MB
        
        # Start timing
        start_time = time.time()
        
        # Execute function
        result = func(*args, **kwargs)
        
        # End timing
        execution_time = time.time() - start_time
        
        # Record peak memory
        mem_after = self.process.memory_info().rss / (1024 * 1024)  # MB
        peak_memory = mem_after
        
        # Get CPU usage (since start of measurement)
        cpu_percent = self.process.cpu_percent(interval=None)
        
        return {
            "result": result,
            "execution_time": execution_time,
            "peak_memory_mb": peak_memory,
            "memory_increase_mb": mem_after - mem_before,
            "cpu_percent": cpu_percent
        }

    def run_schema_benchmark(self, max_sample_size: int = None) -> Dict[str, Any]:
        """
        Run comprehensive schema inference benchmarks.
        
        Returns:
            Dict containing benchmark results.
        """
        logger.info("Starting schema inference benchmark...")
        
        reader = SchemaReader(data_dir=str(self.data_dir), max_sample_size=max_sample_size)
        
        # Get list of JSON files
        json_files = list(self.data_dir.glob("*.json"))
        
        results = {
            "summary": {
                "total_files": len(json_files),
                "max_sample_size": max_sample_size
            },
            "per_file": {},
            "total": {}
        }
        
        # Benchmark each file individually
        for json_file in json_files:
            logger.info(f"Benchmarking schema inference for {json_file.name}...")
            
            file_size = self._get_file_size(json_file)
            
            metrics = self._measure_resources(reader.infer_schema, json_file)
            
            schema = metrics["result"]
            
            results["per_file"][json_file.name] = {
                "input_file_size": file_size,
                "input_file_size_formatted": self._format_size(file_size),
                "record_count": schema.record_count if schema else 0,
                "field_count": len(schema.fields) if schema else 0,
                "execution_time_seconds": round(metrics["execution_time"], 3),
                "peak_memory_mb": round(metrics["peak_memory_mb"], 2),
                "memory_increase_mb": round(metrics["memory_increase_mb"], 2),
                "cpu_percent": round(metrics["cpu_percent"], 2),
                "throughput_records_per_second": round(schema.record_count / metrics["execution_time"], 2) if schema and metrics["execution_time"] > 0 else 0
            }
        
        # Overall benchmark (scan all)
        logger.info("Running full directory scan benchmark...")
        overall_metrics = self._measure_resources(reader.scan_directory)
        
        results["total"] = {
            "execution_time_seconds": round(overall_metrics["execution_time"], 3),
            "peak_memory_mb": round(overall_metrics["peak_memory_mb"], 2),
            "memory_increase_mb": round(overall_metrics["memory_increase_mb"], 2),
            "cpu_percent": round(overall_metrics["cpu_percent"], 2)
        }
        
        # Save results
        output_file = self.schema_result_dir / "schema_benchmark.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Schema benchmark results saved to {output_file}")
        
        # Generate markdown report
        self._generate_schema_markdown_report(results)
        
        return results

    def run_conversion_benchmark(self, formats: List[str] = None) -> Dict[str, Any]:
        """
        Run comprehensive conversion benchmarks for all formats.
        
        Args:
            formats: List of formats to benchmark (default: ['parquet', 'csv', 'avro', 'orc'])
        
        Returns:
            Dict containing benchmark results.
        """
        if formats is None:
            formats = ['parquet', 'csv', 'avro', 'orc']
        
        logger.info(f"Starting conversion benchmark for formats: {formats}...")
        logger.info("Inferring schemas once for all conversions...")
        
        # Infer schemas ONCE and reuse them for all formats
        reader = SchemaReader(data_dir=str(self.data_dir))
        reader.scan_directory()
        schemas = reader.schemas  # Store the schemas dict
        
        # Save report for reference (but we'll pass schemas directly to converters)
        report_path = self.result_dir / "temp_schema_report.md"
        reader.generate_report(str(report_path))
        
        json_files = list(self.data_dir.glob("*.json"))
        
        results = {
            "summary": {
                "total_files": len(json_files),
                "formats_tested": formats
            },
            "per_format": {}
        }
        
        # Create a single converter with the schemas pre-loaded
        # We'll use it for all formats to avoid re-loading schemas
        for format_type in formats:
            logger.info(f"Benchmarking {format_type} conversion...")
            
            # Use output/{format} directory structure (same as regular convert command)
            output_dir = Path("output") / format_type
            
            # Create converter with direct schema access (no schema_report_path)
            # Pass the reader which already has schemas loaded
            converter = Converter(
                data_dir=str(self.data_dir),
                output_dir=str(output_dir),
                schema_reader=reader,  # Reuse the same reader with pre-loaded schemas
                schema_report_path=None  # Don't load from file
            )
            
            format_results = {
                "per_file": {},
                "total": {}
            }
            
            # Benchmark overall conversion
            overall_metrics = self._measure_resources(converter.convert_all, format_type)
            
            format_results["total"] = {
                "execution_time_seconds": round(overall_metrics["execution_time"], 3),
                "peak_memory_mb": round(overall_metrics["peak_memory_mb"], 2),
                "memory_increase_mb": round(overall_metrics["memory_increase_mb"], 2),
                "cpu_percent": round(overall_metrics["cpu_percent"], 2)
            }
            
            # Analyze output files
            total_input_size = 0
            total_output_size = 0
            
            for json_file in json_files:
                input_size = self._get_file_size(json_file)
                total_input_size += input_size
                
                # Find corresponding output file
                output_ext = f".{format_type}" if format_type != "csv" else ".csv"
                output_file = output_dir / f"{json_file.stem}{output_ext}"
                output_size = self._get_file_size(output_file)
                total_output_size += output_size
                
                compression_ratio = (1 - (output_size / input_size)) * 100 if input_size > 0 else 0
                
                format_results["per_file"][json_file.name] = {
                    "input_size": input_size,
                    "input_size_formatted": self._format_size(input_size),
                    "output_size": output_size,
                    "output_size_formatted": self._format_size(output_size),
                    "compression_ratio_percent": round(compression_ratio, 2),
                    "size_reduction": self._format_size(input_size - output_size)
                }
            
            format_results["total"]["total_input_size"] = total_input_size
            format_results["total"]["total_input_size_formatted"] = self._format_size(total_input_size)
            format_results["total"]["total_output_size"] = total_output_size
            format_results["total"]["total_output_size_formatted"] = self._format_size(total_output_size)
            format_results["total"]["overall_compression_ratio_percent"] = round((1 - (total_output_size / total_input_size)) * 100, 2) if total_input_size > 0 else 0
            
            results["per_format"][format_type] = format_results
        
        # Save results
        output_file = self.converting_result_dir / "conversion_benchmark.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Conversion benchmark results saved to {output_file}")
        
        # Generate markdown report
        self._generate_conversion_markdown_report(results)
        
        return results

    def _generate_schema_markdown_report(self, results: Dict[str, Any]):
        """Generate markdown report for schema benchmark."""
        output_file = self.schema_result_dir / "schema_benchmark_report.md"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("# Schema Inference Benchmark Report\n\n")
            f.write(f"**Total Files**: {results['summary']['total_files']}\n\n")
            f.write(f"**Max Sample Size**: {results['summary']['max_sample_size'] or 'All records'}\n\n")
            
            f.write("## Overall Performance\n\n")
            f.write(f"- **Execution Time**: {results['total']['execution_time_seconds']}s\n")
            f.write(f"- **Peak Memory**: {results['total']['peak_memory_mb']} MB\n")
            f.write(f"- **CPU Usage**: {results['total']['cpu_percent']}%\n\n")
            
            f.write("## Per-File Results\n\n")
            f.write("| File | Size | Records | Fields | Time (s) | Memory (MB) | Throughput (rec/s) |\n")
            f.write("|------|------|---------|--------|----------|-------------|--------------------|\n")
            
            for filename, data in results["per_file"].items():
                f.write(f"| {filename} | {data['input_file_size_formatted']} | {data['record_count']} | {data['field_count']} | {data['execution_time_seconds']} | {data['peak_memory_mb']} | {data['throughput_records_per_second']} |\n")
        
        logger.info(f"Schema benchmark markdown report saved to {output_file}")

    def _generate_conversion_markdown_report(self, results: Dict[str, Any]):
        """Generate markdown report for conversion benchmark."""
        output_file = self.converting_result_dir / "conversion_benchmark_report.md"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("# Conversion Benchmark Report\n\n")
            f.write(f"**Total Files**: {results['summary']['total_files']}\n\n")
            f.write(f"**Formats Tested**: {', '.join(results['summary']['formats_tested'])}\n\n")
            
            # Format Comparison Summary
            f.write("## 📊 Format Comparison Summary\n\n")
            f.write("| Format | Time (s) | Memory (MB) | CPU (%) | Output Size | Compression (%) |\n")
            f.write("|--------|----------|-------------|---------|-------------|------------------|\n")
            
            for format_type, format_data in results["per_format"].items():
                time_val = format_data['total']['execution_time_seconds']
                mem_val = format_data['total']['peak_memory_mb']
                cpu_val = format_data['total']['cpu_percent']
                size_val = format_data['total']['total_output_size_formatted']
                compress_val = format_data['total']['overall_compression_ratio_percent']
                f.write(f"| **{format_type.upper()}** | {time_val} | {mem_val} | {cpu_val} | {size_val} | {compress_val}% |\n")
            
            f.write("\n### 🏆 Winner Analysis\n\n")
            
            # Find winners
            best_time_format = min(results["per_format"].items(), key=lambda x: x[1]['total']['execution_time_seconds'])
            best_compression_format = max(results["per_format"].items(), key=lambda x: x[1]['total']['overall_compression_ratio_percent'])
            best_size_format = min(results["per_format"].items(), key=lambda x: x[1]['total']['total_output_size'])
            
            f.write(f"- **⚡ Fastest Conversion**: {best_time_format[0].upper()} ({best_time_format[1]['total']['execution_time_seconds']}s)\n")
            f.write(f"- **🗜️ Best Compression**: {best_compression_format[0].upper()} ({best_compression_format[1]['total']['overall_compression_ratio_percent']}%)\n")
            f.write(f"- **📦 Smallest Output**: {best_size_format[0].upper()} ({best_size_format[1]['total']['total_output_size_formatted']})\n\n")
            
            f.write("---\n\n")
            
            # Detailed per-format results
            for format_type, format_data in results["per_format"].items():
                f.write(f"## {format_type.upper()} Format\n\n")
                f.write("### Overall Performance\n\n")
                f.write(f"- **Execution Time**: {format_data['total']['execution_time_seconds']}s\n")
                f.write(f"- **Peak Memory**: {format_data['total']['peak_memory_mb']} MB\n")
                f.write(f"- **CPU Usage**: {format_data['total']['cpu_percent']}%\n")
                f.write(f"- **Total Input Size**: {format_data['total']['total_input_size_formatted']}\n")
                f.write(f"- **Total Output Size**: {format_data['total']['total_output_size_formatted']}\n")
                f.write(f"- **Compression Ratio**: {format_data['total']['overall_compression_ratio_percent']}%\n\n")
                
                f.write("### Per-File Results\n\n")
                f.write("| File | Input Size | Output Size | Compression | Size Reduction |\n")
                f.write("|------|------------|-------------|-------------|----------------|\n")
                
                for filename, file_data in format_data["per_file"].items():
                    f.write(f"| {filename} | {file_data['input_size_formatted']} | {file_data['output_size_formatted']} | {file_data['compression_ratio_percent']}% | {file_data['size_reduction']} |\n")
                
                f.write("\n")
        
        logger.info(f"Conversion benchmark markdown report saved to {output_file}")
