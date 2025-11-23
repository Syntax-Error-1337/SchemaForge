import logging
import json
from pathlib import Path
from typing import Dict, Any, List, Optional

from src.schema_reader import SchemaReader
from src.converter import Converter
from src.benchmark.utils import get_file_size, format_size, measure_resources
from src.benchmark.reporting import generate_conversion_markdown_report

logger = logging.getLogger(__name__)

def run_conversion_benchmark(data_dir: Path, result_dir: Path, formats: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Run comprehensive conversion benchmarks for all formats.
    
    Args:
        data_dir: Directory containing JSON files.
        result_dir: Directory to store results.
        formats: List of formats to benchmark (default: ['parquet', 'csv', 'avro', 'orc'])
    
    Returns:
        Dict containing benchmark results.
    """
    if formats is None:
        formats = ['parquet', 'csv', 'avro', 'orc', 'feather']
    
    logger.info(f"Starting conversion benchmark for formats: {formats}...")
    logger.info("Inferring schemas once for all conversions...")
    
    converting_result_dir = result_dir / "converting"
    converting_result_dir.mkdir(parents=True, exist_ok=True)
    
    # Infer schemas ONCE and reuse them for all formats
    reader = SchemaReader(data_dir=str(data_dir))
    reader.scan_directory()
    schemas = reader.schemas  # Store the schemas dict
    
    # Save report for reference (but we'll pass schemas directly to converters)
    report_path = result_dir / "temp_schema_report.md"
    reader.generate_report(str(report_path))
    
    json_files = list(data_dir.glob("*.json"))
    
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
            data_dir=str(data_dir),
            output_dir=str(output_dir),
            schema_reader=reader,  # Reuse the same reader with pre-loaded schemas
            schema_report_path=None  # Don't load from file
        )
        
        format_results = {
            "per_file": {},
            "total": {}
        }
        
        # Benchmark overall conversion
        overall_metrics = measure_resources(converter.convert_all, format_type)
        
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
            input_size = get_file_size(json_file)
            total_input_size += input_size
            
            # Find corresponding output file
            output_ext = f".{format_type}" if format_type != "csv" else ".csv"
            output_file = output_dir / f"{json_file.stem}{output_ext}"
            output_size = get_file_size(output_file)
            total_output_size += output_size
            
            compression_ratio = (1 - (output_size / input_size)) * 100 if input_size > 0 else 0
            
            format_results["per_file"][json_file.name] = {
                "input_size": input_size,
                "input_size_formatted": format_size(input_size),
                "output_size": output_size,
                "output_size_formatted": format_size(output_size),
                "compression_ratio_percent": round(compression_ratio, 2),
                "size_reduction": format_size(input_size - output_size)
            }
        
        format_results["total"]["total_input_size"] = total_input_size
        format_results["total"]["total_input_size_formatted"] = format_size(total_input_size)
        format_results["total"]["total_output_size"] = total_output_size
        format_results["total"]["total_output_size_formatted"] = format_size(total_output_size)
        format_results["total"]["overall_compression_ratio_percent"] = round((1 - (total_output_size / total_input_size)) * 100, 2) if total_input_size > 0 else 0
        
        results["per_format"][format_type] = format_results
    
    # Save results
    output_file = converting_result_dir / "conversion_benchmark.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
    
    logger.info(f"Conversion benchmark results saved to {output_file}")
    
    # Generate markdown report
    generate_conversion_markdown_report(results, converting_result_dir)
    
    return results
