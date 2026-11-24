# make_dataset_fixtures.py: CSV datasets as Python Polars 2.1 reads and writes them, for CsvDatasetPolarsOracleTests.
# - datasets/list/: CSV files written by Polars (the last gzip-compressed), the same columns in the same order (Polars'
#   default), whose kinds differ across files: x integers in a, floats in c (Float64); t dates in a, datetimes in b
#   and c (Datetime); strings with separators, quotes, line ends and non-ASCII text, nulls. list.expected.ndjson =
#   pl.scan_csv(files, try_parse_dates=True, include_file_paths="file") (the file column relative to datasets/),
#   written by write_ndjson.
# - datasets/bad.csv: an x that does not parse as the first file's Int64 (read with infer_schema_files=1: an error).
# - datasets/part/: written by Polars, sink_csv(pl.PartitionBy(..., key=["k", "year"])): partition columns in the files
#   AND the paths, nulls as __HIVE_DEFAULT_PARTITION__; part.expected.ndjson = the frame written.
# Run: ~/.cache/seseragi/avro-venv/bin/python -I make_dataset_fixtures.py  (Polars 2.1)
import datetime as dt, gzip, os, random, shutil
import polars as pl
here = os.path.dirname(os.path.abspath(__file__))
root = os.path.join(here, "datasets")
shutil.rmtree(root, ignore_errors=True)
os.makedirs(os.path.join(root, "list"))
rnd = random.Random(20261124)

def frame(first, count, file):
    ids = list(range(first, first + count))
    names = [rnd.choice([None, f"n{i}", "é😀", 'q"uote', "a,b", "two\nlines", "  padded "]) for i in ids]
    x = [None if rnd.random() < 0.1 else (i * 0.5 if file == 2 else i) for i in ids]
    day = lambda i: dt.date(2024, 1, 1) + dt.timedelta(days=i % 400)
    if file == 0:
        t = pl.Series("t", [None if rnd.random() < 0.1 else day(i) for i in ids], dtype=pl.Date)
    else:
        t = pl.Series("t", [None if rnd.random() < 0.1 else dt.datetime.combine(day(i), dt.time(i % 24, i % 60, i % 60, (i % 7) * 1000)) for i in ids],
                      dtype=pl.Datetime("us"))
    flag = [rnd.choice([None, True, False]) for _ in ids]
    return pl.DataFrame({"id": ids, "name": names, "x": x, "t": t, "flag": flag},
                        schema_overrides={"x": pl.Float64 if file == 2 else pl.Int64})

first = 0
names = ["a.csv", "b.csv", "c.csv", "d.csv.gz"]
for f, name in enumerate(names):
    count = rnd.randint(150, 400)
    text = frame(first, count, f).write_csv(datetime_format="%Y-%m-%dT%H:%M:%S%.6f" if f == 2 else None).encode()
    first += count
    path = os.path.join(root, "list", name)
    if name.endswith(".gz"):
        with gzip.open(path, "wb") as g: g.write(text)
    else:
        open(path, "wb").write(text)

files = [os.path.join(root, "list", n) for n in names]
read = pl.scan_csv(files, try_parse_dates=True, include_file_paths="file").collect()
assert read.schema["x"] == pl.Float64 and read.schema["t"] == pl.Datetime("us"), read.schema
read = read.with_columns(pl.col("file").str.replace(root + os.sep, "", literal=True))
read.write_ndjson(os.path.join(root, "list.expected.ndjson"))

open(os.path.join(root, "bad.csv"), "w").write("id,name,x,t,flag\n1,z,abc,2024-01-02,true\n")
try:
    pl.scan_csv([files[0], os.path.join(root, "bad.csv")], try_parse_dates=True, infer_schema_files=1).collect()
    raise SystemExit("bad.csv was read")
except pl.exceptions.ComputeError as e:
    assert "could not parse" in str(e), e

part = pl.DataFrame({
    "id": list(range(60)),
    "k": [None if n % 7 == 0 else f"k{n % 3}" for n in range(60)],
    "year": [2020 + n % 4 for n in range(60)],
    "v": [None if n % 5 == 0 else n * 1.25 for n in range(60)],
    "s": [f"s{n}" for n in range(60)],
})
part.lazy().sink_csv(pl.PartitionBy(os.path.join(root, "part"), key=["k", "year"]), mkdir=True)
part.write_ndjson(os.path.join(root, "part.expected.ndjson"))
print("ok", read.shape, read.schema)
