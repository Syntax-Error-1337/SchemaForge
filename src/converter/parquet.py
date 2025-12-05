import logging
import itertools
import gc
import multiprocessing as mp
from multiprocessing import Queue, Process, Manager
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Optional, Iterator, List, Dict, Any, Tuple
from src.schema_reader import SchemaReader, FileSchema
from src.json_loader import load_json_file
from src.converter.utils import prepare_dataframe, flatten_dict, coerce_type

logger = logging.getLogger(__name__)

# Default chunk size for processing large files - reduced for memory efficiency
# Smaller chunks = less memory per batch, but more I/O operations
DEFAULT_CHUNK_SIZE = 1000  # Reduced significantly to minimize memory usage

# Number of worker processes for parallel processing
# Use CPU count - 1 to leave one core for I/O and coordination
DEFAULT_NUM_WORKERS = max(1, mp.cpu_count() - 1)

def _prepare_record_batch(records: List[Dict[str, Any]], schema: FileSchema) -> Optional[pa.Table]:
    """Prepare a PyArrow table from a batch of records with memory-efficient processing."""
    if not records:
        return None
    
    try:
        # Flatten records (do this in one pass to minimize memory)
        flattened_records = []
        for record in records:
            flattened = flatten_dict(record)
            flattened_records.append(flattened)
        
        # Use pandas but with minimal memory footprint
        import pandas as pd
        df = pd.DataFrame(flattened_records)
        
        # Explicitly delete flattened_records to free memory
        del flattened_records
        
        # Ensure all schema fields are present (fill missing with None)
        schema_fields = sorted(schema.fields.keys())
        for field_name in schema_fields:
            if field_name not in df.columns:
                df[field_name] = None
        
        # Reorder columns to match schema order
        existing_fields = [f for f in schema_fields if f in df.columns]
        df = df[existing_fields]
        
        # Apply type coercion based on schema (in-place where possible)
        for field_name, field in schema.fields.items():
            if field_name in df.columns:
                df[field_name] = df[field_name].apply(
                    lambda x: coerce_type(x, field.field_type)
                )
        
        # Convert to PyArrow table
        table = pa.Table.from_pandas(df, preserve_index=False)
        
        # Explicitly delete DataFrame to free memory immediately
        del df
        
        return table
    except Exception as e:
        logger.warning(f"Failed to create PyArrow table from batch: {e}")
        return None

def _chunk_iterator(iterator: Iterator[Dict[str, Any]], chunk_size: int) -> Iterator[List[Dict[str, Any]]]:
    """Split an iterator into chunks of specified size."""
    while True:
        chunk = list(itertools.islice(iterator, chunk_size))
        if not chunk:
            break
        yield chunk

def _worker_process_chunk(args: Tuple[Tuple[int, List[Dict[str, Any]]], Dict]) -> Optional[Tuple[int, bytes]]:
    """
    Worker function to process a chunk of records into a PyArrow table.
    Returns (chunk_id, serialized_table_bytes) or None if failed.
    This function must be at module level for multiprocessing.
    """
    chunk_data, schema_dict = args
    chunk_id, records = chunk_data
    
    try:
        # Reconstruct FileSchema from dict (simplified for multiprocessing)
        from src.schema_reader.types import SchemaField
        
        # Prepare flattened records
        flattened_records = []
        for record in records:
            flattened = flatten_dict(record)
            flattened_records.append(flattened)
        
        # Use pandas to create DataFrame
        import pandas as pd
        df = pd.DataFrame(flattened_records)
        del flattened_records
        
        # Ensure all schema fields are present
        schema_fields = sorted(schema_dict.keys())
        for field_name in schema_fields:
            if field_name not in df.columns:
                df[field_name] = None
        
        # Reorder columns
        existing_fields = [f for f in schema_fields if f in df.columns]
        df = df[existing_fields]
        
        # Apply type coercion
        for field_name, field_info in schema_dict.items():
            if field_name in df.columns:
                field_type = field_info.get('type', 'string')
                df[field_name] = df[field_name].apply(
                    lambda x: coerce_type(x, field_type)
                )
        
        # Convert to PyArrow table
        table = pa.Table.from_pandas(df, preserve_index=False)
        del df
        
        # Serialize table to bytes for inter-process communication
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        buffer = sink.getvalue()
        serialized = buffer.to_pybytes()
        
        return (chunk_id, serialized)
    except Exception as e:
        logger.error(f"Worker failed to process chunk {chunk_id}: {e}")
        return None

def convert_to_parquet(filepath: Path, output_dir: Path, schema_reader: SchemaReader, schema: Optional[FileSchema] = None) -> bool:
    """Convert a JSON file to Parquet format using chunked processing for large files."""
    logger.info(f"Converting {filepath.name} to Parquet...")
    
    try:
        # Load schema if not provided
        if schema is None:
            schema = schema_reader.infer_schema(filepath)
            if schema is None:
                logger.error(f"Failed to infer schema for {filepath.name}")
                return False
        
        # Check file size to determine processing strategy
        file_size_mb = filepath.stat().st_size / (1024 * 1024)
        use_streaming = file_size_mb > 50  # Use chunked processing for files > 50MB
        
        # Generate output filename
        output_filename = filepath.stem + ".parquet"
        output_path = output_dir / output_filename
        
        if use_streaming:
            # Use chunked processing for large files
            logger.info(f"Using chunked processing for large file ({file_size_mb:.1f}MB)")
            return _convert_large_file_to_parquet(filepath, output_path, schema)
        else:
            # Use standard processing for smaller files
            records = load_json_file(filepath, stream=False)
            
            if not records:
                logger.warning(f"No records to convert in {filepath.name}")
                return False
            
            # Prepare DataFrame
            df = prepare_dataframe(records, schema)
            
            if df.empty:
                logger.warning(f"Empty DataFrame created for {filepath.name}")
                return False
            
            # Convert to PyArrow table and write
            table = pa.Table.from_pandas(df)
            pq.write_table(table, output_path)
            
            logger.info(f"Successfully converted {filepath.name} to {output_path}")
            return True
    
    except Exception as e:
        logger.error(f"Failed to convert {filepath.name} to Parquet: {e}")
        import traceback
        logger.debug(f"Traceback: {traceback.format_exc()}")
        return False

def _normalize_table_schema(table: pa.Table, target_schema: pa.Schema) -> pa.Table:
    """Normalize a table's schema to match the target schema, handling type mismatches."""
    if table.schema.equals(target_schema):
        return table
    
    # Build a mapping of field names to target types
    target_fields = {field.name: field for field in target_schema}
    current_fields = {field.name: field for field in table.schema}
    
    # Create a list of fields for the new schema, matching target schema order
    new_arrays = []
    
    for target_field in target_schema:
        field_name = target_field.name
        
        if field_name in current_fields:
            current_field = current_fields[field_name]
            current_array = table[field_name]
            
            # If types match, use as-is
            if current_field.type.equals(target_field.type):
                new_arrays.append(current_array)
            else:
                # Types don't match - need to cast or handle
                try:
                    # For list types with different item types, handle specially
                    if isinstance(current_field.type, pa.ListType) and isinstance(target_field.type, pa.ListType):
                        current_item_type = current_field.type.value_type
                        target_item_type = target_field.type.value_type
                        
                        # If current has null items and target has non-null items, convert null lists to empty lists
                        if pa.types.is_null(current_item_type) and not pa.types.is_null(target_item_type):
                            # Convert: create empty lists of the target type
                            # Create empty list array using ListArray.from_arrays
                            num_rows = len(current_array)
                            offsets = pa.array(list(range(num_rows + 1)), type=pa.int32())
                            values = pa.array([], type=target_item_type)
                            new_array = pa.ListArray.from_arrays(offsets, values)
                            new_arrays.append(new_array)
                        else:
                            # Try to cast directly
                            try:
                                new_arrays.append(current_array.cast(target_field.type, safe=True))
                            except Exception:
                                # If cast fails, use empty lists as fallback
                                empty_lists_data = [[] for _ in range(len(current_array))]
                                new_array = pa.array(empty_lists_data, type=target_field.type)
                                new_arrays.append(new_array)
                    else:
                        # For non-list types, try to cast
                        new_arrays.append(current_array.cast(target_field.type, safe=True))
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError, Exception) as e:
                    # If casting fails, create null array of target type
                    logger.debug(f"Could not cast field {field_name} from {current_field.type} to {target_field.type}: {e}. Using nulls.")
                    new_arrays.append(pa.nulls(len(current_array), target_field.type))
        else:
            # Field missing in current table, add as nulls
            new_arrays.append(pa.nulls(len(table), target_field.type))
    
    # Create new table with normalized schema
    try:
        normalized_table = pa.Table.from_arrays(new_arrays, schema=target_schema)
        return normalized_table
    except Exception as e:
        logger.error(f"Failed to normalize table schema: {e}")
        raise

def _convert_large_file_to_parquet(filepath: Path, output_path: Path, schema: FileSchema, num_workers: int = None) -> bool:
    """
    Convert a large JSON file to Parquet using multiprocessing for parallel chunk processing.
    
    Uses a producer-consumer pattern:
    - Producer: Reads JSON and sends chunks to workers
    - Workers: Process chunks in parallel, prepare PyArrow tables
    - Consumer: Writes tables sequentially to maintain schema consistency
    """
    if num_workers is None:
        num_workers = DEFAULT_NUM_WORKERS
    
    # Check file size - use multiprocessing for very large files
    file_size_mb = filepath.stat().st_size / (1024 * 1024)
    use_multiprocessing = file_size_mb > 500  # Use multiprocessing for files > 500MB
    
    if use_multiprocessing:
        logger.info(f"Using multiprocessing with {num_workers} workers for large file ({file_size_mb:.1f}MB)")
        return _convert_large_file_to_parquet_parallel(filepath, output_path, schema, num_workers)
    else:
        logger.info(f"Using single-threaded processing for file ({file_size_mb:.1f}MB)")
        return _convert_large_file_to_parquet_sequential(filepath, output_path, schema)

def _convert_large_file_to_parquet_sequential(filepath: Path, output_path: Path, schema: FileSchema) -> bool:
    """Convert a large JSON file to Parquet using single-threaded processing."""
    parquet_writer = None
    total_records = 0
    chunk_size = DEFAULT_CHUNK_SIZE
    unified_schema = None
    
    try:
        # Stream JSON records
        records_iter = load_json_file(filepath, stream=True)
        
        # Process in chunks with explicit memory cleanup
        for chunk_num, chunk in enumerate(_chunk_iterator(records_iter, chunk_size), 1):
            if not chunk:
                break
            
            try:
                # Prepare PyArrow table from chunk
                table = _prepare_record_batch(chunk, schema)
                
                # Explicitly delete chunk to free memory immediately
                del chunk
                
                if table is None or len(table) == 0:
                    continue
                
                # Initialize writer with schema from first chunk
                if parquet_writer is None:
                    unified_schema = table.schema
                    parquet_writer = pq.ParquetWriter(output_path, unified_schema)
                    logger.info(f"Initialized Parquet writer with schema: {len(unified_schema)} columns")
                else:
                    # Normalize subsequent chunks to match the unified schema
                    try:
                        normalized_table = _normalize_table_schema(table, unified_schema)
                        # Delete original table before using normalized one
                        del table
                        table = normalized_table
                    except Exception as e:
                        # Clean up on error
                        del table
                        logger.error(f"Failed to normalize schema for chunk {chunk_num}: {e}")
                        raise
                
                # Write chunk immediately
                chunk_records = len(table)
                parquet_writer.write_table(table)
                total_records += chunk_records
                
                # Explicitly delete table to free memory immediately
                del table
                
                # Force garbage collection every 50 chunks to free memory
                if chunk_num % 50 == 0:
                    gc.collect()
                
                # Log progress every 20 chunks or every 50k records
                if chunk_num % 20 == 0 or total_records % 50000 == 0:
                    logger.info(f"Processed {chunk_num} chunks ({total_records:,} records so far)...")
                    
            except Exception as e:
                # Clean up on error
                gc.collect()
                raise
        
        # Close writer
        if parquet_writer is not None:
            parquet_writer.close()
            logger.info(f"Successfully converted {filepath.name} to {output_path} ({total_records:,} total records)")
            return True
        else:
            logger.warning(f"No records to convert in {filepath.name}")
            return False
    
    except Exception as e:
        # Ensure writer is closed on error
        if parquet_writer is not None:
            try:
                parquet_writer.close()
            except:
                pass
        # Force cleanup
        gc.collect()
        raise

def _convert_large_file_to_parquet_parallel(filepath: Path, output_path: Path, schema: FileSchema, num_workers: int) -> bool:
    """Convert a large JSON file to Parquet using multiprocessing for parallel processing."""
    chunk_size = DEFAULT_CHUNK_SIZE
    
    # Prepare schema dict for serialization (FileSchema is not easily picklable)
    schema_dict = {}
    for field_name, field in schema.fields.items():
        field_type = field.field_type if isinstance(field.field_type, str) else str(field.field_type)
        schema_dict[field_name] = {'type': field_type}
    
    # Stream chunks and process in parallel
    records_iter = load_json_file(filepath, stream=True)
    
    parquet_writer = None
    unified_schema = None
    total_records = 0
    chunk_id = 0
    
    try:
        with mp.Pool(processes=num_workers) as pool:
            # Use imap for streaming processing (processes chunks as they come)
            chunk_iterator = _chunk_iterator(records_iter, chunk_size)
            
            # Process chunks in batches to balance parallelism and memory
            batch = []
            batch_size = num_workers * 2  # Keep 2x workers worth of chunks in flight
            
            while True:
                # Fill batch
                try:
                    while len(batch) < batch_size:
                        chunk = next(chunk_iterator)
                        chunk_id += 1
                        batch.append((chunk_id, chunk))
                except StopIteration:
                    # No more chunks
                    if not batch:
                        break
                
                # Process batch in parallel
                worker_args = [((cid, ch), schema_dict) for cid, ch in batch]
                results = pool.map(_worker_process_chunk, worker_args)
                
                # Write results in order
                for i, result in enumerate(results):
                    if result is None:
                        continue
                    
                    result_chunk_id, serialized = result
                    
                    # Deserialize table
                    buffer = pa.BufferReader(serialized)
                    reader = pa.ipc.open_stream(buffer)
                    table = reader.read_all()
                    
                    # Initialize writer with schema from first chunk
                    if parquet_writer is None:
                        unified_schema = table.schema
                        parquet_writer = pq.ParquetWriter(output_path, unified_schema)
                        logger.info(f"Initialized Parquet writer with schema: {len(unified_schema)} columns")
                    else:
                        # Normalize schema if needed
                        if not table.schema.equals(unified_schema):
                            table = _normalize_table_schema(table, unified_schema)
                    
                    # Write table
                    chunk_records = len(table)
                    parquet_writer.write_table(table)
                    total_records += chunk_records
                    
                    # Clean up
                    del table
                    del serialized
                    
                    # Log progress
                    if result_chunk_id % 20 == 0 or total_records % 50000 == 0:
                        logger.info(f"Processed {result_chunk_id} chunks ({total_records:,} records so far)...")
                
                # Clean up batch
                del batch
                del worker_args
                del results
                batch = []
                
                # Periodic garbage collection
                if chunk_id % 50 == 0:
                    gc.collect()
        
        # Close writer
        if parquet_writer is not None:
            parquet_writer.close()
            logger.info(f"Successfully converted {filepath.name} to {output_path} ({total_records:,} total records)")
            return True
        else:
            logger.warning(f"No records to convert in {filepath.name}")
            return False
            
    except Exception as e:
        # Ensure writer is closed on error
        if parquet_writer is not None:
            try:
                parquet_writer.close()
            except:
                pass
        logger.error(f"Parallel processing error: {e}")
        import traceback
        logger.debug(f"Traceback: {traceback.format_exc()}")
        raise
    finally:
        # Clean up
        gc.collect()
