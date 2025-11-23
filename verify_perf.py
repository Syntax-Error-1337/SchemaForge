import time
import json
import os
from pathlib import Path
from src.schema_reader import SchemaReader
from src.converter import Converter

def create_dummy_data(data_dir, num_files=5, num_records=10000):
    data_dir = Path(data_dir)
    data_dir.mkdir(exist_ok=True)
    
    print(f"Creating {num_files} files with {num_records} records each...")
    for i in range(num_files):
        data = [{"id": k, "value": f"value_{k}", "nested": {"a": k}} for k in range(num_records)]
        with open(data_dir / f"file_{i}.json", "w") as f:
            json.dump(data, f)

def main():
    data_dir = "data_perf_test"
    output_dir = "output_perf_test"
    
    create_dummy_data(data_dir)
    
    print("\n--- Testing Schema Inference ---")
    start_time = time.time()
    reader = SchemaReader(data_dir=data_dir)
    schemas = reader.scan_directory()
    duration = time.time() - start_time
    print(f"Inferred schemas for {len(schemas)} files in {duration:.2f} seconds")
    
    report_path = Path("reports/perf_report.md")
    reader.generate_report(str(report_path))
    
    print("\n--- Testing Conversion ---")
    converter = Converter(data_dir=data_dir, output_dir=output_dir, schema_report_path=str(report_path.with_suffix('.json')))
    start_time = time.time()
    results = converter.convert_all("parquet")
    duration = time.time() - start_time
    print(f"Converted {len(results)} files in {duration:.2f} seconds")
    
    # Cleanup
    import shutil
    shutil.rmtree(data_dir)
    shutil.rmtree(output_dir)

if __name__ == "__main__":
    main()
