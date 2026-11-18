"""Writes the Python Polars IPC corpus: one frame with every Polars type, in both formats, at the newest and the oldest
compatibility level, uncompressed, LZ4 and ZSTD (files in three record batches, streams in one). Each file is checked with ipc_meta.py.

Usage: python -I write_polars.py <output directory>    (Polars 2.1; deterministic: fixed seeds)
"""
import datetime
import decimal
import os
import random
import sys

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(here))
import ipc_meta  # noqa: E402

import polars as pl  # noqa: E402

out = sys.argv[1]
os.makedirs(out, exist_ok=True)
r = random.Random(20261118)
N = 120


def nulls(values, every=5):
    return [None if i % every == every - 1 else v for i, v in enumerate(values)]


words = ["", "a", "exactly12byt", "thirteen byte", "ü日本語", "a long string that is stored outside the view"]
frame = pl.DataFrame([
    pl.Series("i8", nulls([r.randint(-128, 127) for _ in range(N)]), pl.Int8),
    pl.Series("i16", nulls([r.randint(-2**15, 2**15 - 1) for _ in range(N)]), pl.Int16),
    pl.Series("i32", nulls([r.randint(-2**31, 2**31 - 1) for _ in range(N)]), pl.Int32),
    pl.Series("i64", nulls([r.randint(-2**63, 2**63 - 1) for _ in range(N)]), pl.Int64),
    pl.Series("u8", nulls([r.randint(0, 255) for _ in range(N)]), pl.UInt8),
    pl.Series("u16", nulls([r.randint(0, 2**16 - 1) for _ in range(N)]), pl.UInt16),
    pl.Series("u32", nulls([r.randint(0, 2**32 - 1) for _ in range(N)]), pl.UInt32),
    pl.Series("u64", nulls([r.randint(0, 2**64 - 1) for _ in range(N)]), pl.UInt64),
    pl.Series("f16", nulls([r.uniform(-100, 100) for _ in range(N)]), pl.Float16),
    pl.Series("f32", nulls([r.choice([0.0, -0.0, float("nan"), float("inf"), r.uniform(-1e6, 1e6)]) for _ in range(N)]), pl.Float32),
    pl.Series("f64", nulls([r.uniform(-1e300, 1e300) for _ in range(N)]), pl.Float64),
    pl.Series("bool", nulls([r.random() < 0.5 for _ in range(N)], 3), pl.Boolean),
    pl.Series("str", nulls([r.choice(words) + str(r.randint(0, 999)) for _ in range(N)]), pl.String),
    pl.Series("bin", nulls([r.randbytes(r.choice([0, 5, 13, 30])) for _ in range(N)]), pl.Binary),
    pl.Series("date", nulls([datetime.date(1970, 1, 1) + datetime.timedelta(days=r.randint(-50_000, 50_000)) for _ in range(N)]), pl.Date),
    pl.Series("dt_ms", nulls([datetime.datetime(2000, 1, 1) + datetime.timedelta(seconds=r.randint(-10**9, 10**9)) for _ in range(N)]), pl.Datetime("ms")),
    pl.Series("dt_us_utc", nulls([datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(microseconds=r.randint(-10**15, 10**15)) for _ in range(N)]), pl.Datetime("us", "UTC")),
    pl.Series("dt_ns_tokyo", nulls([datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(microseconds=r.randint(-10**15, 10**15)) for _ in range(N)]), pl.Datetime("ns", "UTC")).dt.convert_time_zone("Asia/Tokyo"),
    pl.Series("dur_ms", nulls([datetime.timedelta(milliseconds=r.randint(-10**12, 10**12)) for _ in range(N)]), pl.Duration("ms")),
    pl.Series("dur_ns", nulls([datetime.timedelta(microseconds=r.randint(-10**12, 10**12)) for _ in range(N)]), pl.Duration("ns")),
    pl.Series("time", nulls([datetime.time(r.randint(0, 23), r.randint(0, 59), r.randint(0, 59), r.randint(0, 999_999)) for _ in range(N)]), pl.Time),
    pl.Series("dec", nulls([decimal.Decimal(r.randint(-10**20, 10**20)).scaleb(-6) for _ in range(N)]), pl.Decimal(38, 6)),
    pl.Series("cat", nulls([r.choice(["red", "green", "blue"]) for _ in range(N)]), pl.Categorical),
    pl.Series("enum", nulls([r.choice(["lo", "mid", "hi"]) for _ in range(N)]), pl.Enum(["lo", "mid", "hi"])),
    pl.Series("null", [None] * N, pl.Null),
    pl.Series("list_i32", nulls([[r.randint(0, 9) for _ in range(r.randint(0, 4))] for _ in range(N)]), pl.List(pl.Int32)),
    pl.Series("list_str", nulls([[r.choice(words) for _ in range(r.randint(0, 3))] for _ in range(N)]), pl.List(pl.String)),
    pl.Series("list_cat", nulls([[r.choice(["x", "y"]) for _ in range(r.randint(0, 3))] for _ in range(N)]), pl.List(pl.Categorical)),
    pl.Series("array_f64x2", nulls([[r.random(), r.random()] for _ in range(N)]), pl.Array(pl.Float64, 2)),
    pl.Series("struct", nulls([{"a": r.randint(0, 9), "b": r.choice(words), "c": [r.random() < 0.5]} for _ in range(N)], 6),
              pl.Struct({"a": pl.Int16, "b": pl.String, "c": pl.List(pl.Boolean)})),
    pl.Series("list_struct", nulls([[{"k": r.choice(words), "v": r.randint(0, 9)}] * r.randint(0, 2) for _ in range(N)]),
              pl.List(pl.Struct({"k": pl.String, "v": pl.Int64}))),
])


def check(name, **expect):
    with open(os.path.join(out, name), "rb") as f:
        summary = ipc_meta.describe(f.read())
    errors = [m["error"] for m in summary["messages"] if "error" in m] + ([summary["error"]] if "error" in summary else [])
    if errors:
        raise SystemExit(f"{name}: {errors}")
    got = ipc_meta.features(summary)
    for key, want in expect.items():
        ok = want(got[key]) if callable(want) else got[key] == want
        if not ok:
            raise SystemExit(f"{name}: expected {key} {want}, got {got[key]}")
    print(f"{name}: {got}", file=sys.stderr)


several = lambda n: n >= 3  # noqa: E731
for compat, tag in ((None, ""), (pl.CompatLevel.oldest(), "_oldest")):
    kw = {} if compat is None else {"compat_level": compat}
    for codec, label in (("uncompressed", []), ("lz4", ["LZ4_FRAME"]), ("zstd", ["ZSTD"])):
        name = f"frame{tag}_{codec}.arrow"
        frame.write_ipc(os.path.join(out, name), compression=codec, record_batch_size=50, **kw)
        check(name, format="file", codecs=label, record_batches=several)
    name = f"frame{tag}.arrows"
    # The stream writer has no batch size and rechunks: three chunks still come out as one batch (asserted, so a
    # change in Polars shows up here).
    chunked = pl.concat([frame.slice(0, 50), frame.slice(50, 50), frame.slice(100)], rechunk=False)
    chunked.write_ipc_stream(os.path.join(out, name), **kw)
    check(name, format="stream", record_batches=1, eos=True)
chunked.write_ipc_stream(os.path.join(out, "frame_zstd.arrows"), compression="zstd")
check("frame_zstd.arrows", format="stream", codecs=["ZSTD"])
# Polars writes Int128 and UInt128 as Arrow Int with bitWidth 128, which no other Arrow reader accepts: own file.
wide = pl.DataFrame([
    pl.Series("i128", nulls([r.randint(-2**127, 2**127 - 1) for _ in range(N)]), pl.Int128),
    pl.Series("u128", nulls([r.randint(0, 2**128 - 1) for _ in range(N)]), pl.UInt128),
])
wide.write_ipc(os.path.join(out, "int128.arrow"))
check("int128.arrow", format="file")
# Buffers over 64 KiB: LZ4 frames of several linked blocks (Polars links them and checksums blocks and content).
big = pl.DataFrame({"i64": pl.Series([i % 1000 for i in range(100_000)], dtype=pl.Int64),
                    "str": pl.Series([f"value {i % 777}" for i in range(100_000)], dtype=pl.String)})
big.write_ipc(os.path.join(out, "big_lz4.arrow"), compression="lz4")
check("big_lz4.arrow", format="file", codecs=["LZ4_FRAME"])
pl.DataFrame(schema=frame.schema).write_ipc(os.path.join(out, "empty.arrow"))
check("empty.arrow", format="file")
print("ok", file=sys.stderr)
