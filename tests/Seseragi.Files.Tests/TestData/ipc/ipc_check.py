"""The IPC oracles. Writes two files that the C# reader tests compare against:

- values.json: every IPC file here as pyarrow and Python Polars read it: per column the Arrow type, row and null
  counts, a hash of the values in the canonical form of arrow_canonical.py and a sample; per record batch its row
  count and a hash of its column hashes. pyarrow also validates every batch fully; failures are listed under
  "invalid" with the values still given (its full validation includes value checks the format does not require:
  date64 values that are not whole days, decimals beyond their precision). pyarrow cannot wrap year-month and day-time interval arrays; for those columns of the
  apache golden files the values come from the golden JSON next to them (oracle "json").
- messages.json: every file's framing and metadata as ipc_meta.py reads it (versions, dictionary ids and deltas,
  compression and buffer prefixes, variadic counts, nodes, buffers, custom metadata, footer blocks).

Each file is read in a child process; the fuzz files under apache/fuzz in a scope capped at 2 GiB with a timeout,
since they are built to make readers over-allocate. Run with pyarrow 26 and Polars 2.1: `python -I ipc_check.py`.
"""
import gzip
import json
import os
import subprocess
import sys

root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, root)
import arrow_canonical as ac  # noqa: E402
import ipc_meta  # noqa: E402

SUFFIXES = (".arrow", ".arrows", ".feather", ".arrow_file", ".stream")
INTERVALS = {"month_interval": "YEAR_MONTH", "day_time_interval": "DAY_TIME"}


def is_file_format(path):
    with open(path, "rb") as f:
        head = f.read(6)
        f.seek(-6, os.SEEK_END)
        return head == b"ARROW1" and f.read(6) == b"ARROW1"


def json_columns(path):
    """Canonical values of the interval columns from the golden JSON next to an apache file, per batch."""
    golden = os.path.splitext(path)[0] + ".json.gz"
    if not os.path.exists(golden):
        return None
    with gzip.open(golden, "rt", encoding="utf-8") as f:
        doc = json.load(f)
    units = {fld["name"]: fld["type"].get("unit") for fld in doc["schema"]["fields"] if fld["type"]["name"] == "interval"}
    out = {}
    for batch in doc["batches"]:
        for col in batch["columns"]:
            unit = units.get(col["name"])
            if unit == "YEAR_MONTH":
                vals = [f"Y:{v}" for v in col["DATA"]]
            elif unit == "DAY_TIME":
                vals = [f"J:{v['days']},{v['milliseconds']}" for v in col["DATA"]]
            else:
                continue
            out.setdefault(col["name"], []).append([v if ok else "null" for v, ok in zip(vals, col["VALIDITY"])])
    return out


def summarize(schema, batches, from_json=None):
    names = schema.names
    per_batch = []  # per batch, per column: canonical values
    for i, b in enumerate(batches):
        cols = []
        for j, name in enumerate(names):
            if str(schema.field(j).type) in INTERVALS:
                if not from_json or name not in from_json:
                    raise TypeError(f"no oracle for {schema.field(j).type}")
                cols.append(from_json[name][i])
            else:
                cols.append(ac.render(b.column(j)))
        per_batch.append(cols)
    columns = []
    for j, name in enumerate(names):
        values = [v for cols in per_batch for v in cols[j]]
        columns.append({
            "name": name,
            "type": str(schema.field(j).type),
            "rows": len(values),
            "nulls": sum(1 for v in values if v == "null"),
            "hash": ac.digest(values),
            "sample": [v[:80] for v in values[:3]],
        })
    return {
        "rows": sum(b.num_rows for b in batches),
        "batches": [[b.num_rows, ac.digest([ac.digest(c) for c in cols])] for b, cols in zip(batches, per_batch)],
        "columns": columns,
    }


def summarize_columns(values_by_name, types):
    columns = [{"name": n, "type": types[n], "rows": len(v), "nulls": sum(1 for x in v if x == "null"),
                "hash": ac.digest(v), "sample": [x[:80] for x in v[:3]]} for n, v in values_by_name.items()]
    return {"rows": columns[0]["rows"], "columns": columns}


def read_pyarrow(path):
    import pyarrow.ipc as ipc
    reader = ipc.open_file(path) if is_file_format(path) else ipc.open_stream(path)
    if is_file_format(path):
        batches = [reader.get_batch(i) for i in range(reader.num_record_batches)]
    else:
        batches = list(reader)
    invalid = []
    for i, b in enumerate(batches):
        try:
            b.validate(full=True)
        except Exception as e:
            invalid.append(f"batch {i}: {e}"[:300])
    out = summarize(reader.schema, batches, json_columns(path) if "/apache/" in os.path.abspath(path) else None)
    if invalid:  # pyarrow's full validation includes value checks the format does not require (date64 whole days,
        out["invalid"] = invalid  # decimal precision); read the message before treating an entry as structural
    return out


def read_polars(path):
    import polars as pl
    frame = pl.read_ipc(path) if is_file_format(path) else pl.read_ipc_stream(path)
    wide = [c for c, t in frame.schema.items() if t in (pl.Int128, pl.UInt128)]
    if wide:  # no Arrow type: render through Polars (integers are their decimal text)
        out = summarize_columns({c: [v if v is not None else "null" for v in frame[c].cast(pl.String).to_list()] for c in wide},
                                {c: str(frame.schema[c]) for c in wide})
        frame = frame.drop(wide)
        if frame.width == 0:
            return out
        raise TypeError("mixed 128-bit and Arrow columns")
    table = frame.to_arrow()
    return summarize(table.schema, table.to_batches() or [])


def child(oracle, path, capped):
    cmd = [sys.executable, "-I", os.path.abspath(__file__), "--one", oracle, path]
    if capped:
        cmd = ["systemd-run", "--user", "--scope", "-q", "-p", "MemoryMax=2G", "-p", "MemorySwapMax=0", "timeout", "60"] + cmd
    run = subprocess.run(cmd, capture_output=True, text=True)
    if run.returncode == 0 and run.stdout.strip():
        return json.loads(run.stdout)
    last = run.stderr.strip().splitlines()[-1] if run.stderr.strip() else "no output"
    return {"error": f"{oracle} crashed (exit {run.returncode}): {last}"[:400]}


if len(sys.argv) == 4 and sys.argv[1] == "--one":
    try:
        result = {"pyarrow": read_pyarrow, "polars": read_polars}[sys.argv[2]](sys.argv[3])
    except Exception as e:
        result = {"error": f"{type(e).__name__}: {e}"[:400]}
    print(json.dumps(result))
    sys.exit(0)

import polars  # noqa: E402
import pyarrow  # noqa: E402

values = {"pyarrow": pyarrow.__version__, "polars": polars.__version__, "files": {}}
messages = {"files": {}}
for directory, dirs, names in os.walk(root):
    dirs.sort()
    for name in sorted(names):
        full = os.path.join(directory, name)
        key = os.path.relpath(full, root).replace(os.sep, "/")
        fuzz = key.startswith("apache/fuzz/")
        if not (fuzz or name.endswith(SUFFIXES)):
            continue
        with open(full, "rb") as f:
            data = f.read()
        try:
            messages["files"][key] = ipc_meta.describe(data)
        except Exception as e:  # damaged files: whatever the walker could not read
            messages["files"][key] = {"error": f"{type(e).__name__}: {e}"[:400]}
        if data[:4] == b"FEA1":
            values["files"][key] = {"feather_v1": True}
            continue
        values["files"][key] = {oracle: child(oracle, full, fuzz) for oracle in ("pyarrow", "polars")}
        print(key, file=sys.stderr)

for name, doc in (("values.json", values), ("messages.json", messages)):
    with open(os.path.join(root, name), "w", encoding="utf-8") as f:
        json.dump(doc, f, sort_keys=True, ensure_ascii=False, indent=0)
        f.write("\n")
print(f"{len(values['files'])} files", file=sys.stderr)
