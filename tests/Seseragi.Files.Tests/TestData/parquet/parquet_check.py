"""Reads every .parquet file under this directory with pyarrow and with DuckDB and records, per top-level column,
the Arrow type, the row and null counts, and a hash of the values in a canonical text form, into values.json.

Run with pyarrow and duckdb installed (`python -I parquet_check.py`); the C# reader tests render their own batches
the same way and compare. Each file is read in a child process (pyarrow aborts on some damaged files).

Canonical form (one string per value, joined by newlines, then SHA-256; the first 16 hex digits are kept):
  null                      a null at any level
  true / false              booleans
  123                       integers (any width, signed or not)
  f:<hex bits>              floats: the IEEE bits of the value at its own width (f16, f32, f64)
  d:<unscaled>e<-scale>     decimals: the unscaled integer and the scale
  D:<days>                  dates (days since 1970-01-01)
  t:<ns>                    times of day, in nanoseconds
  T:<ns>                    timestamps (instants or local), nanoseconds since the epoch (any range)
  P:<ns>                    durations, in nanoseconds
  I:<months>,<days>,<ns>    month-day-nano intervals
  u:<hex>                   strings: their UTF-8 bytes
  b:<hex>                   binary and fixed-size binary
  [a,b]                     lists (and fixed-size lists)
  {name:v,...}              structs
  M{k=>v,...}               maps
Dictionary-encoded and extension columns render as their values.
"""
import hashlib
import json
import os
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq

root = os.path.dirname(os.path.abspath(__file__))

# Files neither oracle should read here: they need far more memory than a test run may use.
SKIP = {"apache/data/large_string_map.brotli.parquet": "a 2 GiB string chunk"}

NS = {"s": 10**9, "ms": 10**6, "us": 10**3, "ns": 1}


def render(arr):
    """The canonical strings of a (non-chunked) array."""
    t = arr.type
    if isinstance(t, pa.BaseExtensionType):  # canonical extensions (uuid, json) too
        return render(arr.storage)
    if pa.types.is_dictionary(t):
        return render(arr.dictionary_decode())
    valid = arr.is_valid().to_pylist()
    n = len(arr)
    if pa.types.is_null(t):
        out = ["null"] * n
    elif pa.types.is_boolean(t):
        out = ["true" if v else "false" for v in arr.to_pylist()]
    elif pa.types.is_integer(t):
        out = [str(v) for v in arr.to_pylist()]
    elif pa.types.is_floating(t):
        width = t.bit_width
        bits = arr.view({16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}[width]).to_pylist()
        out = ["f:%0*x" % (width // 4, b) if b is not None else "" for b in bits]
    elif pa.types.is_decimal(t):
        out = []
        for d in arr.to_pylist():
            if d is None:
                out.append("")
            else:
                sign, digits, exponent = d.as_tuple()
                unscaled = int("".join(map(str, digits)) or "0") * 10 ** (exponent + t.scale)
                out.append(f"d:{-unscaled if sign else unscaled}e-{t.scale}")
    elif pa.types.is_date32(t):
        out = [f"D:{v}" for v in arr.view(pa.int32()).to_pylist()]
    elif pa.types.is_date64(t):
        out = [f"D:{v // 86_400_000 if v is not None else 0}" for v in arr.view(pa.int64()).to_pylist()]
    elif pa.types.is_time(t):
        f = NS[t.unit]
        out = [f"t:{v * f if v is not None else 0}" for v in arr.view(pa.int32() if t.bit_width == 32 else pa.int64()).to_pylist()]
    elif pa.types.is_timestamp(t):
        f = NS[t.unit]
        out = [f"T:{v * f if v is not None else 0}" for v in arr.view(pa.int64()).to_pylist()]
    elif pa.types.is_duration(t):
        f = NS[t.unit]
        out = [f"P:{v * f if v is not None else 0}" for v in arr.view(pa.int64()).to_pylist()]
    elif t == pa.month_day_nano_interval():
        out = [f"I:{v.months},{v.days},{v.nanoseconds}" if v is not None else "" for v in arr.to_pylist()]
    elif pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_string_view(t):
        out = ["u:" + v.encode("utf-8").hex() if v is not None else "" for v in arr.to_pylist()]
    elif pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t) or pa.types.is_binary_view(t):
        out = ["b:" + v.hex() if v is not None else "" for v in arr.to_pylist()]
    elif pa.types.is_map(t):
        keys, items, offsets = render(arr.keys), render(arr.items), arr.offsets.to_pylist()
        out = ["M{" + ",".join(f"{keys[k]}=>{items[k]}" for k in range(offsets[i], offsets[i + 1])) + "}" for i in range(n)]
    elif pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_list_view(t):
        values, offsets = render(arr.values), arr.offsets.to_pylist()
        out = ["[" + ",".join(values[offsets[i]:offsets[i + 1]]) + "]" for i in range(n)]
    elif pa.types.is_fixed_size_list(t):
        values, size = render(arr.flatten()), t.list_size
        out, at = [], 0
        for i in range(n):
            if valid[i]:
                out.append("[" + ",".join(values[at:at + size]) + "]")
                at += size
            else:
                out.append("")
    elif pa.types.is_struct(t):
        fields = [(t.field(j).name, render(arr.field(j))) for j in range(t.num_fields)]
        out = ["{" + ",".join(f"{name}:{vals[i]}" for name, vals in fields) + "}" for i in range(n)]
    else:
        out = [repr(v) for v in arr.to_pylist()]
    return [o if valid[i] else "null" for i, o in enumerate(out)]


def summary(table):
    columns = []
    for name, column in zip(table.column_names, table.columns):
        values = render(column.combine_chunks()) if column.num_chunks != 1 else render(column.chunk(0))
        digest = hashlib.sha256("\n".join(values).encode("utf-8")).hexdigest()[:16]
        columns.append({
            "name": name,
            "type": str(column.type),
            "rows": len(values),
            "nulls": column.null_count,
            "hash": digest,
            "sample": [v[:80] for v in values[:3]],
        })
    return {"rows": table.num_rows, "columns": columns}


def read_pyarrow(path):
    return pq.read_table(path, coerce_int96_timestamp_unit="us")


def read_duckdb(path):
    import duckdb
    con = duckdb.connect()
    return con.execute("SELECT * FROM read_parquet(?)", [path]).to_arrow_table()


if len(sys.argv) == 4 and sys.argv[1] == "--one":
    try:
        print(json.dumps(summary({"pyarrow": read_pyarrow, "duckdb": read_duckdb}[sys.argv[2]](sys.argv[3]))))
    except Exception as e:
        print(json.dumps({"error": f"{type(e).__name__}: {e}"[:400]}))
    sys.exit(0)

result = {"pyarrow": pa.__version__, "files": {}}
for directory, _, names in sorted(os.walk(root)):
    for name in sorted(names):
        if not name.endswith(".parquet"):
            continue
        full = os.path.join(directory, name)
        key = os.path.relpath(full, root).replace(os.sep, "/")
        if key in SKIP:
            result["files"][key] = {"skipped": SKIP[key]}
            continue
        entry = {}
        for oracle in ("pyarrow", "duckdb"):
            child = subprocess.run([sys.executable, "-I", os.path.abspath(__file__), "--one", oracle, full], capture_output=True, text=True)
            if child.returncode == 0 and child.stdout.strip():
                entry[oracle] = json.loads(child.stdout)
            else:
                last = child.stderr.strip().splitlines()[-1] if child.stderr.strip() else "no output"
                entry[oracle] = {"error": f"{oracle} crashed: {last}"[:400]}
        result["files"][key] = entry

with open(os.path.join(root, "values.json"), "w", encoding="utf-8") as f:
    json.dump(result, f, sort_keys=True, ensure_ascii=False, indent=0)
    f.write("\n")
print(f"{len(result['files'])} files", file=sys.stderr)
