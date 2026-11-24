# make_write_fixtures.py: what Python Polars 2.1 writes to CSV, for the CsvWriter oracle tests.
#   <frame>.arrow                 the frame as Polars hands it to Arrow (string views, dictionaries)
#   <frame>.<option set>.csv      Polars' write_csv of it with that option set (OPTION_SETS below, also in options.json)
#   formats.json                  {frame: {format: csv text or "ERROR: …"}} for chrono format strings, one token at a time
#   refusals.json                 {column type: Polars' error} for the types write_csv refuses, and the option values it
#                                 refuses ({option set: error})
# Frames: "written" (every type CSV takes, edge values, nulls, names that need quotes), "floats64" / "floats32" (a
# sweep of magnitudes and random values), "zoned" (zoned datetimes only: %z needs every datetime column zoned), "empty"
# (no rows), "nulls" (one column of nulls: blank lines). Int128 / UInt128 are not in the Arrow files (Polars writes a
# 128-bit integer type other Arrow readers refuse): "int128.csv" holds their text, the tests build the column.
# Run: ~/.cache/seseragi/excel-venv/bin/python -I make_write_fixtures.py  (Polars 2.1)
import datetime as dt, decimal, io, json, math, os, random, struct
import polars as pl

here = os.path.dirname(os.path.abspath(__file__))
rows = 14

def col(values, dtype=None):
    values = (values * rows)[:rows] if len(values) < rows else values[:rows]
    return pl.Series(values, dtype=dtype, strict=False)

strings = ["", "plain", " lead", "trail ", "a,b", "a;b", "a\tb", 'q"q', "q'q", "x\ny", "x\ry", "x\r\ny", None, "é😀中",
           "#c", "1", "-1.5", "NaN", "true", "NA"]
ms = [0, 1, -1, 1_700_000_000_123, -86_400_000, 1_000, 253_402_300_799_999, -62_135_596_800_000, None, 5, 120, 1_500, 999, -1_001]
ns = [0, 1, -1, 1_700_000_000_123_456_789, 1_000, 1_000_000, 999_999_999, -999_999_999, None, 123_000_000, 123_456_000,
      86_400_000_000_000, -1_000_000_000, 7]
zoned = [dt.datetime(2024, 1, 2, 3, 4, 5), dt.datetime(2024, 7, 2, 3, 4, 5), dt.datetime(2024, 3, 10, 6, 59, 59),
         dt.datetime(2024, 3, 10, 7, 0, 0), dt.datetime(2024, 11, 3, 5, 30), dt.datetime(2024, 11, 3, 6, 30),
         dt.datetime(1969, 12, 31, 23, 59, 59), dt.datetime(1, 1, 1), dt.datetime(9999, 12, 31, 23, 59, 59), None,
         dt.datetime(1900, 1, 1), dt.datetime(2038, 1, 19, 3, 14, 8), dt.datetime(2024, 10, 6, 15, 45), dt.datetime(2024, 4, 6, 15, 15)]

def zone(unit, tz):
    # Nanoseconds reach 1677 to 2262 only.
    values = [v.replace(year=1700) if v and unit == "ns" and v.year < 1700 else v.replace(year=2200) if v and unit == "ns" and v.year > 2200 else v for v in zoned]
    return pl.Series(values, dtype=pl.Datetime(unit, "UTC")).dt.convert_time_zone(tz)

frames = {}
frames["written"] = pl.DataFrame({
    "i8": col([-128, 127, 0, None], pl.Int8), "i16": col([-32768, 32767, None], pl.Int16),
    "i32": col([-2**31, 2**31 - 1, None, 1234567], pl.Int32), "i64": col([-2**63, 2**63 - 1, 0, None, -1234567], pl.Int64),
    "u8": col([0, 255, None], pl.UInt8), "u16": col([65535, None], pl.UInt16), "u32": col([2**32 - 1, None], pl.UInt32),
    "u64": col([0, 2**64 - 1, None, 1000], pl.UInt64),
    "f64": col([0.0, -0.0, 0.1 + 0.2, 1e-7, 1234.5, 1e15, 1e16, None, float("nan"), float("inf"), float("-inf"), 5e-324, 1.7976931348623157e308, -1234567.25], pl.Float64),
    "f32": col([1.1, 3.4e38, 1e-7, None, 16777216.0, -0.0, 0.1, 1e20, float("nan"), 2.5, 1234.5, 1.4e-45, -7.0, 0.3], pl.Float32),
    "f16": col([1.5, 0.1, None, float("nan"), 65504.0, 6e-8, -2.0, 1000.0], pl.Float16),
    "bool": col([True, False, None], pl.Boolean),
    "s": col(strings, pl.String), "cat": col(["a", "b,c", None, "a", 'q"'], pl.Categorical), "enum": col(["x", "y", None], pl.Enum(["x", "y"])),
    "date": col([dt.date(1, 1, 1), dt.date(9999, 12, 31), dt.date(1969, 12, 31), dt.date(1970, 1, 1), None, dt.date(2024, 2, 29), dt.date(2024, 2, 9)], pl.Date),
    "dt_ms": col(ms, pl.Int64).cast(pl.Datetime("ms")), "dt_us": col([v * 1000 if v is not None else None for v in ms], pl.Int64).cast(pl.Datetime("us")),
    "dt_ns": col(ns, pl.Int64).cast(pl.Datetime("ns")),
    "utc_us": zone("us", "UTC"), "tokyo_ms": zone("ms", "Asia/Tokyo"), "ny_us": zone("us", "America/New_York"),
    "kolkata_ns": zone("ns", "Asia/Kolkata"), "lord_howe_us": zone("us", "Australia/Lord_Howe"),
    "time": col([0, 1, 1_000, 1_000_000, 86_399_999_999_999, 43_200_000_000_000, None, 123_456_789, 120_000_000], pl.Int64).cast(pl.Time),
    "dec": col([decimal.Decimal("-1234.50"), decimal.Decimal("0.00"), decimal.Decimal("123456789.12"), None, decimal.Decimal("0.01")], pl.Decimal(12, 2)),
    "dec0": col([decimal.Decimal("-5"), decimal.Decimal("0"), None, decimal.Decimal("99999")], pl.Decimal(5, 0)),
    "dec38": col([decimal.Decimal("1.0000000000000000000000000000000000001"), decimal.Decimal("-0.0000000000000000000000000000000000001"), None], pl.Decimal(38, 37)),
    "null": pl.Series([None] * rows, dtype=pl.Null),
    'name "q", ;\t\né': col([1, None], pl.Int64),
    " lead": col([None, 2], pl.Int64),
})

def float_sweep(rng, f32):
    values = [0.0, -0.0, 0.1 + 0.2, 1 / 3, 2 / 3, 123456789012345680.0, 9007199254740993.0, float("nan"), float("inf"), float("-inf")]
    top = 38 if f32 else 308
    for e in range(-45 if f32 else -324, top + 1):
        for k, m in enumerate((1.0, 9.999999, 2.2250738585072014)):
            v = m * 10.0 ** e if e > -300 else float(f"{m}e{e}")
            if math.isfinite(v) and v != 0: values.append(-v if (e + k) % 5 == 0 else v)
    for _ in range(300):
        while True:
            v = struct.unpack("<d", rng.getrandbits(64).to_bytes(8, "little"))[0]
            if math.isfinite(v): break
        values.append(v)
        values.append(rng.uniform(-1e6, 1e6))
        values.append(round(rng.uniform(-1000, 1000), rng.randrange(0, 7)))
    values += [None] * 20
    s = pl.Series(values, dtype=pl.Float64)
    if f32:
        s = s.filter(s.is_null() | s.is_nan() | s.is_infinite() | (s.abs() <= 3.4028234663852886e38)).cast(pl.Float32)
    return s

rng = random.Random(20261124)
frames["floats64"] = pl.DataFrame({"f64": float_sweep(rng, False)})
frames["floats32"] = pl.DataFrame({"f32": float_sweep(rng, True)})
frames["zoned"] = pl.DataFrame({"utc_us": zone("us", "UTC"), "tokyo_ms": zone("ms", "Asia/Tokyo"), "ny_us": zone("us", "America/New_York"),
                                "kolkata_ns": zone("ns", "Asia/Kolkata"), "lord_howe_us": zone("us", "Australia/Lord_Howe")})
frames["empty"] = frames["written"].clear()
frames["nulls"] = pl.DataFrame({"x": pl.Series([1, None, 3, None], dtype=pl.Int64)})

OPTION_SETS = {
    "default": {}, "noheader": {"include_header": False}, "bom": {"include_bom": True},
    "semicolon": {"separator": ";"}, "tab": {"separator": "\t"}, "pipe": {"separator": "|"},
    "quote_single": {"quote_char": "'"},
    "always": {"quote_style": "always"}, "non_numeric": {"quote_style": "non_numeric"}, "never": {"quote_style": "never"},
    "null_NA": {"null_value": "NA"}, "null_quoted": {"null_value": "a,b"}, "crlf": {"line_terminator": "\r\n"},
    "term_multi": {"line_terminator": "|\n"}, "term_empty": {"line_terminator": ""},
    "sci_true": {"float_scientific": True}, "sci_false": {"float_scientific": False},
    "prec0": {"float_precision": 0}, "prec3": {"float_precision": 3}, "prec17": {"float_precision": 17},
    "sci_true_prec2": {"float_scientific": True, "float_precision": 2}, "sci_false_prec2": {"float_scientific": False, "float_precision": 2},
    "dc": {"decimal_comma": True}, "dc_semicolon": {"decimal_comma": True, "separator": ";"},
    "dc_semicolon_prec2": {"decimal_comma": True, "separator": ";", "float_precision": 2},
    "dc_always": {"decimal_comma": True, "quote_style": "always"}, "dc_never": {"decimal_comma": True, "quote_style": "never"},
    "fmt_dmy": {"date_format": "%d/%m/%Y", "time_format": "%H:%M", "datetime_format": "%d.%m.%Y %H:%M:%S%.3f"},
    "fmt_words": {"date_format": "%A %e %B %Y (%j) %a %b", "datetime_format": "%c", "time_format": "%I:%M:%S %p"},
}
FLOAT_SETS = ["default", "sci_true", "sci_false", "prec0", "prec3", "prec17", "sci_true_prec2", "sci_false_prec2",
              "dc_semicolon", "dc_semicolon_prec2", "non_numeric", "always"]
# Not here: decimal_comma with float_scientific=True. Polars 2.1 keeps every value written before in the field's buffer
# (each field repeats all the column's earlier values: quadratic output), a bug the writer does not copy.
PER_FRAME = {"written": list(OPTION_SETS), "floats64": FLOAT_SETS, "floats32": FLOAT_SETS,
             "zoned": ["default", "fmt_dmy", "fmt_words", "non_numeric"], "empty": ["default", "noheader", "bom", "always"],
             "nulls": ["default", "null_NA", "always", "non_numeric", "noheader"]}

def write(df, opts):
    out = io.BytesIO()
    df.write_csv(out, **opts)
    return out.getvalue()

for name, df in frames.items():
    df.write_ipc(os.path.join(here, f"{name}.arrow"), compression="uncompressed")
    for set_name in PER_FRAME[name]:
        with open(os.path.join(here, f"{name}.{set_name}.csv"), "wb") as f:
            f.write(write(df, OPTION_SETS[set_name]))
with open(os.path.join(here, "options.json"), "w") as f:
    json.dump(OPTION_SETS, f, indent=1, ensure_ascii=False)

# 128-bit integers: their text (the tests build the column from these values).
i128 = pl.DataFrame({"i128": pl.Series([-(2**127), 2**127 - 1, 0, None, -1], dtype=pl.Int128),
                     "u128": pl.Series([0, 2**128 - 1, None, 1, 10**30], dtype=pl.UInt128)})
with open(os.path.join(here, "int128.csv"), "wb") as f:
    f.write(write(i128, {}))

# chrono formats, one token (or a few) at a time: on dates, times, naive datetimes, zoned datetimes.
TOKENS = ["%Y", "%C", "%y", "%m", "%b", "%B", "%h", "%d", "%e", "%a", "%A", "%w", "%u", "%U", "%W", "%G", "%g", "%V", "%j",
          "%D", "%x", "%F", "%v", "%H", "%k", "%I", "%l", "%P", "%p", "%M", "%S", "%f", "%.f", "%.3f", "%.6f", "%.9f",
          "%3f", "%6f", "%9f", "%R", "%T", "%X", "%r", "%Z", "%z", "%:z", "%::z", "%#z", "%c", "%+", "%s", "%t", "%n", "%%",
          "%-d", "%_d", "%0e", "%-m", "%-H", "%_H", "%-I", "%-j", "%-y", "%^", "%Q", "plain", "%Y-%m-%dT%H:%M:%S%.f%:z",
          "%d/%m/%Y %H:%M", "%Y%m%d", "[%H]", "%"]
dates = pl.DataFrame({"date": frames["written"]["date"]})
times = pl.DataFrame({"time": frames["written"]["time"]})
naive = pl.DataFrame({"dt_ms": frames["written"]["dt_ms"], "dt_us": frames["written"]["dt_us"], "dt_ns": frames["written"]["dt_ns"]})
formats = {}
for frame_name, df, option in (("date", dates, "date_format"), ("time", times, "time_format"),
                               ("naive", naive, "datetime_format"), ("zoned", frames["zoned"], "datetime_format")):
    results = {}
    for token in TOKENS:
        try:
            results[token] = write(df, {option: token}).decode("utf-8")
        except Exception as e:
            results[token] = "ERROR: " + str(e).splitlines()[0]
    formats[frame_name] = results
with open(os.path.join(here, "formats.json"), "w") as f:
    json.dump(formats, f, indent=1, ensure_ascii=False)

# The types write_csv refuses, and Polars' words.
refused = {
    "binary": pl.Series([b"ab", None], dtype=pl.Binary), "duration": pl.Series([1, None]).cast(pl.Duration("ms")),
    "list": pl.Series([[1, 2], None]), "array": pl.Series([[1, 2], None], dtype=pl.Array(pl.Int8, 2)),
    "struct": pl.Series([{"a": 1}, None]),
}
refusals = {}
for name, opts in {"quote_empty": {"quote_char": ""}, "separator_2_bytes": {"separator": "é"}, "separator_empty": {"separator": ""},
                   "quote_2_bytes": {"quote_char": "é"}, "z_on_naive": {"datetime_format": "%z"}}.items():
    try:
        write(frames["written"], opts)
        refusals[name] = "WRITTEN"
    except Exception as e:
        refusals[name] = str(e).splitlines()[0]
for name, s in refused.items():
    try:
        write(pl.DataFrame({name: s}), {})
        refusals[name] = "WRITTEN"
    except Exception as e:
        refusals[name] = str(e).splitlines()[0]
with open(os.path.join(here, "refusals.json"), "w") as f:
    json.dump(refusals, f, indent=1)
print({k: v.shape for k, v in frames.items()})
