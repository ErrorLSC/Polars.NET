# make_fixtures.py: mixed.ndjson (records with every kind of JSON value, missing fields, varying field order, escapes,
# blank and CRLF lines) and mixed.expected.json: the column types Python Polars infers for it (whole input) and per
# column aggregates of what it reads, for JsonPolarsOracleTests. The same records as JSON documents (arrays of
# objects; read_json cannot read objects or arrays as strings, so the mixed column's are their JSON text there):
# mixed.pretty.json (indented, CRLF) and mixed.compact.json, which read_json reads to the same expectations.
# Run: ~/.cache/seseragi/excel-venv/bin/python -I make_fixtures.py  (Polars 2.1)
import json, random, os
import polars as pl
here = os.path.dirname(os.path.abspath(__file__))
rnd = random.Random(2026)
lines, records = [], []
for i in range(1500):
    r = {"id": i}
    if rnd.random() < 0.9: r["score"] = rnd.choice([None, rnd.randint(-10**6, 10**6), round(rnd.uniform(-1e3, 1e3), 3)])
    if rnd.random() < 0.9: r["name"] = rnd.choice([None, f"n{i}", "tab\there", "quote\"q", "é😀", "back\\slash", "", "line\nbreak"])
    if rnd.random() < 0.8: r["flag"] = rnd.choice([None, True, False])
    if rnd.random() < 0.7: r["tags"] = rnd.choice([None, [], [f"t{k}" for k in range(rnd.randint(1, 4))]])
    if rnd.random() < 0.7: r["nums"] = [rnd.choice([None, rnd.randint(0, 99)]) for _ in range(rnd.randint(0, 5))]
    if rnd.random() < 0.8:
        o = {"a": rnd.choice([None, rnd.randint(0, 9)]), "b": rnd.choice([None, f"b{i % 7}"])}
        if rnd.random() < 0.3: o["c"] = {"d": rnd.choice([1.5, 2, None])}
        r["obj"] = rnd.choice([None, o])
    if rnd.random() < 0.3: r["mixed"] = rnd.choice([1, 2.5, "s", True, None, {"k": 1}, [1, 2]])
    if rnd.random() < 0.05: r["big"] = rnd.choice([2**64 - 1, -(2**70), 5])
    if i == 0: r["mixed"] = "first"
    if i == 1: r["big"] = 2**64 - 1
    keys = list(r)
    if i % 3 == 0: rnd.shuffle(keys)
    records.append({k: r[k] for k in keys})
    text = json.dumps(records[-1], ensure_ascii=rnd.random() < 0.5)
    lines.append(text + rnd.choice(["\n", "\n", "\r\n"]))
    if rnd.random() < 0.05: lines.append(rnd.choice(["\n", "   \n", "\r\n"]))
open(os.path.join(here, "mixed.ndjson"), "w", encoding="utf-8", newline="").write("".join(lines))

def aggs(name, dtype, expr):
    out = {"nulls": df.select(expr.null_count()).item()}
    if dtype.is_integer(): out["sum"] = str(df.select(expr.cast(pl.Int128).sum()).item())
    elif dtype.is_float(): out["sum"] = round(df.select(expr.sum()).item(), 6)
    elif dtype == pl.String: out["bytes"] = df.select(expr.str.len_bytes().sum()).item()
    elif dtype == pl.Boolean: out["true"] = df.select(expr.sum()).item()
    elif isinstance(dtype, pl.List): out["items"] = df.select(expr.list.len().sum()).item()
    elif isinstance(dtype, pl.Struct):
        out["fields"] = {f.name: aggs(f"{name}.{f.name}", f.dtype, expr.struct.field(f.name)) for f in dtype.fields}
    return out
def expect(frame, name=None):
    global df
    df = frame
    expected = {"rows": df.height, "schema": {k: str(v) for k, v in df.schema.items()},
                "columns": {k: aggs(k, v, pl.col(k)) for k, v in df.schema.items()}}
    if name: json.dump(expected, open(os.path.join(here, name), "w"), indent=1)
    print(name, df.schema)
    return expected

lines_expected = expect(pl.read_ndjson(os.path.join(here, "mixed.ndjson"), infer_schema_length=None), "mixed.expected.json")
records = [{k: (json.dumps(v) if k == "mixed" and isinstance(v, (dict, list)) else v) for k, v in r.items()} for r in records]
open(os.path.join(here, "mixed.pretty.json"), "w", encoding="utf-8", newline="\r\n").write(json.dumps(records, indent=2, ensure_ascii=False) + "\n")
open(os.path.join(here, "mixed.compact.json"), "w", encoding="utf-8", newline="").write(json.dumps(records, separators=(",", ":")))
for doc in ["mixed.pretty.json", "mixed.compact.json"]:
    assert expect(pl.read_json(os.path.join(here, doc), infer_schema_length=None)) == lines_expected, doc + " reads differently"
