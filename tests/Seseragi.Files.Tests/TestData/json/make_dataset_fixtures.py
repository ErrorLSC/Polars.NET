# make_dataset_fixtures.py: JSON datasets as Python Polars 2.1 reads and writes them, for JsonDatasetPolarsOracleTests.
# - datasets/list/: NDJSON files (the last gzip-compressed) whose later files miss fields, add fields (top-level and in
#   structs) and order fields differently; list.expected.ndjson = pl.scan_ndjson(files, include_file_paths="file")
#   (types inferred from the first file; the file column relative to datasets/), written by write_ndjson.
# - datasets/bad.jsonl: a record whose id does not parse as the first file's Int64 (Polars: "cannot parse").
# - datasets/part/: written by Polars, sink_ndjson(pl.PartitionBy(..., key=["k", "year"])): partition columns in the
#   files AND the paths, nulls as __HIVE_DEFAULT_PARTITION__; part.expected.ndjson = the frame written.
# Run: ~/.cache/seseragi/excel-venv/bin/python -I make_dataset_fixtures.py  (Polars 2.1)
import gzip, json, os, random, shutil
import polars as pl
here = os.path.dirname(os.path.abspath(__file__))
root = os.path.join(here, "datasets")
shutil.rmtree(root, ignore_errors=True)
os.makedirs(os.path.join(root, "list"))
rnd = random.Random(20261123)

def record(i, file):
    r = {"id": i, "name": rnd.choice([None, f"n{i}", "é😀", "q\"uote", ""]), "x": rnd.choice([None, i * 0.5, i]),
         "flag": rnd.choice([None, True, False]), "tags": rnd.choice([None, [], [f"t{k}" for k in range(rnd.randint(1, 3))]]),
         "obj": rnd.choice([None, {"a": rnd.choice([None, i % 9]), "b": rnd.choice([None, f"b{i % 5}"])}])}
    if file == 0:
        if i == 0: r.update(x=0.25, name="first", tags=["t0"], flag=True, obj={"a": 1, "b": "b"})
        return r
    if rnd.random() < 0.3: del r[rnd.choice(list(r)[1:])]            # a missing field
    if rnd.random() < 0.2: r["extra"] = rnd.choice([1, "e", [1]])      # a field the first file does not have
    if rnd.random() < 0.2 and isinstance(r.get("obj"), dict): r["obj"]["z"] = 1  # a struct field it does not have
    keys = list(r)
    if rnd.random() < 0.5: rnd.shuffle(keys)
    return {k: r[k] for k in keys}

i = 0
names = ["a.jsonl", "b.jsonl", "c.jsonl", "d.jsonl.gz"]
for f, name in enumerate(names):
    lines = []
    for _ in range(rnd.randint(150, 400)):
        lines.append(json.dumps(record(i, f), ensure_ascii=rnd.random() < 0.5))
        i += 1
    text = ("\n".join(lines) + "\n").encode()
    path = os.path.join(root, "list", name)
    if name.endswith(".gz"):
        with gzip.open(path, "wb") as g: g.write(text)
    else:
        open(path, "wb").write(text)

files = [os.path.join(root, "list", n) for n in names]
read = pl.scan_ndjson(files, include_file_paths="file").collect()
read = read.with_columns(pl.col("file").str.replace(root + os.sep, "", literal=True))
read.write_ndjson(os.path.join(root, "list.expected.ndjson"))

open(os.path.join(root, "bad.jsonl"), "w").write('{"id":"abc","name":"z","x":1.5}\n')
try:
    pl.scan_ndjson([files[0], os.path.join(root, "bad.jsonl")]).collect()
    raise SystemExit("bad.jsonl was read")
except pl.exceptions.ComputeError as e:
    assert "cannot parse" in str(e), e

frame = pl.DataFrame({
    "id": list(range(60)),
    "k": [None if n % 7 == 0 else f"k{n % 3}" for n in range(60)],
    "year": [2020 + n % 4 for n in range(60)],
    "v": [None if n % 5 == 0 else n * 1.25 for n in range(60)],
    "s": [f"s{n}" for n in range(60)],
})
frame.lazy().sink_ndjson(pl.PartitionBy(os.path.join(root, "part"), key=["k", "year"]))
frame.write_ndjson(os.path.join(root, "part.expected.ndjson"))
print("ok", read.shape, read.schema)
