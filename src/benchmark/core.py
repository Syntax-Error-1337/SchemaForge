import logging
import psutil
import os
from pathlib import Path
from typing import Dict, Any, List, Optional

from src.benchmark.utils import get_file_size, format_size, measure_resources
from src.benchmark.schema import run_schema_benchmark
from src.benchmark.conversion import run_conversion_benchmark

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

    # Expose helper methods for backward compatibility
    def _get_file_size(self, filepath: Path) -> int:
        return get_file_size(filepath)

    def _format_size(self, size_bytes: int) -> str:
        return format_size(size_bytes)

    def _measure_resources(self, func, *args, **kwargs) -> Dict[str, Any]:
        return measure_resources(func, *args, **kwargs)

    def run_schema_benchmark(self, max_sample_size: int = None) -> Dict[str, Any]:
        """
        Run comprehensive schema inference benchmarks.
        
        Returns:
            Dict containing benchmark results.
        """
        return run_schema_benchmark(self.data_dir, self.result_dir, max_sample_size)

    def run_conversion_benchmark(self, formats: List[str] = None) -> Dict[str, Any]:
        """
        Run comprehensive conversion benchmarks for all formats.
        
        Args:
            formats: List of formats to benchmark (default: ['parquet', 'csv', 'avro', 'orc', 'feather'])
        
        Returns:
            Dict containing benchmark results.
        """
        return run_conversion_benchmark(self.data_dir, self.result_dir, formats)
    
    # Report generation methods are now handled inside the run_* functions, 
    # but we can keep them here if needed for direct access, or just rely on the run functions.
    # Since they were internal methods (_generate...), we don't strictly need to expose them,
    # but the run methods already call them.
