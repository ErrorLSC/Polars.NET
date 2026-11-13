"""Dumps the footer of every .parquet file under this directory, as pyarrow reads it, into footers.json.

Run with pyarrow installed (`python -I parquet_footer_check.py`); the C# footer tests compare against the output.
A file pyarrow cannot open is recorded with its error message instead. Each file is read in a child process, as
pyarrow aborts the process on some damaged footers.
"""
import json
import os
import subprocess
import sys

import pyarrow
import pyarrow.parquet as pq

root = os.path.dirname(os.path.abspath(__file__))


def column(c):
    s = c.statistics if c.is_stats_set else None
    return {
        "path": c.path_in_schema,
        "physical": c.physical_type,
        "compression": c.compression,
        "encodings": list(c.encodings),
        "num_values": c.num_values,
        "total_compressed": c.total_compressed_size,
        "total_uncompressed": c.total_uncompressed_size,
        "file_offset": c.file_offset,
        "data_page_offset": c.data_page_offset,
        "dictionary_page_offset": c.dictionary_page_offset if c.has_dictionary_page else None,
        "has_offset_index": c.has_offset_index,
        "has_column_index": c.has_column_index,
        "bloom_filter_offset": c.bloom_filter_offset,
        "bloom_filter_length": c.bloom_filter_length,
        "null_count": s.null_count if s is not None and s.has_null_count else None,
        "distinct_count": s.distinct_count if s is not None and s.has_distinct_count else None,
    }


def leaf(c):
    return {
        "path": c.path,
        "physical": c.physical_type,
        "converted": c.converted_type,
        "length": c.length,
        "precision": c.precision,
        "scale": c.scale,
        "max_def": c.max_definition_level,
        "max_rep": c.max_repetition_level,
    }


def dump(path):
    md = pq.ParquetFile(path).metadata
    kv = md.metadata or {}
    return {
        "created_by": md.created_by,
        "num_rows": md.num_rows,
        "serialized_size": md.serialized_size,
        "key_value": {k.decode("utf-8", "replace"): v.decode("utf-8", "replace") for k, v in kv.items()},
        "leaves": [leaf(md.schema.column(i)) for i in range(md.num_columns)],
        "row_groups": [
            {
                "num_rows": g.num_rows,
                "total_byte_size": g.total_byte_size,
                "columns": [column(g.column(i)) for i in range(g.num_columns)],
            }
            for g in (md.row_group(i) for i in range(md.num_row_groups))
        ],
    }


if len(sys.argv) == 3 and sys.argv[1] == "--one":
    try:
        print(json.dumps(dump(sys.argv[2])))
    except Exception as e:  # recorded: the C# side must fail too, or be listed as a known difference
        print(json.dumps({"error": f"{type(e).__name__}: {e}"}))
    sys.exit(0)

result = {"pyarrow": pyarrow.__version__, "files": {}}
for directory, _, names in sorted(os.walk(root)):
    for name in sorted(names):
        if not (name.endswith(".parquet") or name.endswith(".parquet.encrypted")):
            continue
        full = os.path.join(directory, name)
        key = os.path.relpath(full, root).replace(os.sep, "/")
        child = subprocess.run([sys.executable, "-I", os.path.abspath(__file__), "--one", full], capture_output=True, text=True)
        if child.returncode == 0:
            result["files"][key] = json.loads(child.stdout)
        else:
            result["files"][key] = {"error": "pyarrow crashed: " + child.stderr.strip().splitlines()[-1] if child.stderr.strip() else "pyarrow crashed"}

with open(os.path.join(root, "footers.json"), "w", encoding="utf-8") as f:
    json.dump(result, f, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    f.write("\n")
print(f"{len(result['files'])} files", file=sys.stderr)
