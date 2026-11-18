"""Reads IPC files our writer wrote with pyarrow (fully validated) and Python Polars, and compares what they read with
what they read from the corpus file each was written from (values.json): column hashes, row and null counts, and
for pyarrow the batches. Our writer writes the same batches back, so the oracles must read the same values.

Usage: python -I writer_interop.py <manifest.json>
The manifest maps each written file's path to the corpus file it was written from (a key of values.json). Prints a
JSON list of differences (empty when every file reads alike). Files an oracle could not read in the corpus are not
compared for that oracle.
"""
import json
import os
import sys

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)
import ipc_check  # noqa: E402

with open(sys.argv[1], encoding="utf-8") as f:
    manifest = json.load(f)
with open(os.path.join(here, "values.json"), encoding="utf-8") as f:
    corpus = json.load(f)["files"]

diffs = []
for path, key in sorted(manifest.items()):
    for oracle in ("pyarrow", "polars"):
        expected = corpus[key][oracle]
        if "error" in expected:
            continue
        actual = ipc_check.child(oracle, path, capped=False)
        if "error" in actual:
            diffs.append({"file": key, "written": os.path.basename(path), "oracle": oracle, "error": actual["error"]})
            continue
        if oracle == "pyarrow" and actual.get("invalid") != expected.get("invalid"):
            diffs.append({"file": key, "written": os.path.basename(path), "oracle": oracle, "invalid": actual.get("invalid")})
        for want, got in zip(expected["columns"], actual["columns"]):
            if (want["hash"], want["rows"], want["nulls"]) != (got["hash"], got["rows"], got["nulls"]):
                diffs.append({"file": key, "written": os.path.basename(path), "oracle": oracle, "column": want["name"],
                              "want": want["sample"], "got": got["sample"], "types": [want["type"], got["type"]]})
        if len(expected["columns"]) != len(actual["columns"]):
            diffs.append({"file": key, "written": os.path.basename(path), "oracle": oracle, "columns": len(actual["columns"])})
        if oracle == "pyarrow" and expected.get("batches") != actual.get("batches"):
            diffs.append({"file": key, "written": os.path.basename(path), "oracle": oracle, "batches": "differ"})
print(json.dumps(diffs, indent=1))
