"""Writes the pyarrow IPC corpus: every type pyarrow can write, both formats, compression, dictionaries (deltas and
stream replacements), metadata at every level, empty and zero batches, metadata V4, the pre-0.15 framing and
Feather. Each file is checked with ipc_meta.py for the feature it is named for; the script fails otherwise.

Usage: python -I write_pyarrow.py <output directory>    (pyarrow 26; deterministic: fixed seeds)
"""
import datetime
import decimal
import os
import random
import sys
import uuid

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(here))
import ipc_meta  # noqa: E402

import pyarrow as pa  # noqa: E402
import pyarrow.feather as feather  # noqa: E402
import pyarrow.ipc as ipc  # noqa: E402

out = sys.argv[1]
os.makedirs(out, exist_ok=True)
rng = random.Random(20261118)


def check(name, **expect):
    with open(os.path.join(out, name), "rb") as f:
        summary = ipc_meta.describe(f.read())
    errors = [m["error"] for m in summary["messages"] if "error" in m] + ([summary["error"]] if "error" in summary else [])
    if errors:
        raise SystemExit(f"{name}: {errors}")
    got = ipc_meta.features(summary)
    for key, want in expect.items():
        value = got[key]
        ok = want(value) if callable(want) else value == want
        if not ok:
            raise SystemExit(f"{name}: expected {key} {want}, got {value}")
    print(f"{name}: {got['record_batches']} batches, {os.path.getsize(os.path.join(out, name))} bytes", file=sys.stderr)


def write(name, batches, schema=None, *, stream=False, options=None, batch_metadata=None, **expect):
    schema = schema or batches[0].schema
    path = os.path.join(out, name)
    opener = ipc.new_stream if stream else ipc.new_file
    with opener(path, schema, options=options or ipc.IpcWriteOptions()) as w:
        for i, b in enumerate(batches):
            if batch_metadata and i in batch_metadata:
                w.write_batch(b, custom_metadata=batch_metadata[i])
            else:
                w.write_batch(b)
    check(name, format="stream" if stream else "file", **expect)


def nulls(values, every=4):
    return [None if i % every == every - 1 else v for i, v in enumerate(values)]


def at_least(n):
    return lambda v: v >= n


# ---------------------------------------------------------------------------------------------------------- primitives
N = 40


def primitives(n, seed):
    r = random.Random(seed)
    ints = lambda lo, hi: nulls([r.randint(lo, hi) for _ in range(n)])  # noqa: E731
    floats = nulls([r.choice([0.0, -0.0, 1.5, float("inf"), float("-inf"), float("nan"), r.uniform(-1e9, 1e9)]) for _ in range(n)])
    epoch_ns = [r.randint(-2**62, 2**62) for _ in range(n)]
    cols = {
        "i8": pa.array(ints(-128, 127), pa.int8()),
        "i16": pa.array(ints(-2**15, 2**15 - 1), pa.int16()),
        "i32": pa.array(ints(-2**31, 2**31 - 1), pa.int32()),
        "i64": pa.array(ints(-2**63, 2**63 - 1), pa.int64()),
        "u8": pa.array(ints(0, 255), pa.uint8()),
        "u16": pa.array(ints(0, 2**16 - 1), pa.uint16()),
        "u32": pa.array(ints(0, 2**32 - 1), pa.uint32()),
        "u64": pa.array(ints(0, 2**64 - 1), pa.uint64()),
        "f16": pa.array(floats, pa.float64()).cast(pa.float16(), safe=False),
        "f32": pa.array(floats, pa.float32()),
        "f64": pa.array(floats, pa.float64()),
        "bool": pa.array(nulls([r.random() < 0.5 for _ in range(n)], 3), pa.bool_()),
        "null": pa.nulls(n),
        "dec32": pa.array(nulls([decimal.Decimal(r.randint(-10**8, 10**8)).scaleb(-3) for _ in range(n)]), pa.decimal32(9, 3)),
        "dec64": pa.array(nulls([decimal.Decimal(r.randint(-10**17, 10**17)).scaleb(-4) for _ in range(n)]), pa.decimal64(18, 4)),
        "dec128": pa.array(nulls([decimal.Decimal(r.randint(-10**37, 10**37)).scaleb(-10) for _ in range(n)]), pa.decimal128(38, 10)),
        "dec256": pa.array(nulls([decimal.Decimal(r.randint(-10**75, 10**75)).scaleb(-20) for _ in range(n)]), pa.decimal256(76, 20)),
        "date32": pa.array(ints(-100_000, 100_000), pa.int32()).cast(pa.date32()),
        "date64": pa.array(nulls([r.randint(-10**6, 10**6) * 86_400_000 for _ in range(n)]), pa.int64()).cast(pa.date64()),
        "time32s": pa.array(ints(0, 86_399), pa.int32()).cast(pa.time32("s")),
        "time32ms": pa.array(ints(0, 86_399_999), pa.int32()).cast(pa.time32("ms")),
        "time64us": pa.array(ints(0, 86_399_999_999), pa.int64()).cast(pa.time64("us")),
        "time64ns": pa.array(ints(0, 86_399_999_999_999), pa.int64()).cast(pa.time64("ns")),
        "ts_s": pa.array(nulls([v // 10**9 for v in epoch_ns]), pa.int64()).cast(pa.timestamp("s")),
        "ts_ms_utc": pa.array(nulls([v // 10**6 for v in epoch_ns]), pa.int64()).cast(pa.timestamp("ms", "UTC")),
        "ts_us_tokyo": pa.array(nulls([v // 10**3 for v in epoch_ns]), pa.int64()).cast(pa.timestamp("us", "Asia/Tokyo")),
        "ts_ns_offset": pa.array(nulls(epoch_ns), pa.int64()).cast(pa.timestamp("ns", "+05:30")),
        "dur_s": pa.array(ints(-2**40, 2**40), pa.int64()).cast(pa.duration("s")),
        "dur_ms": pa.array(ints(-2**50, 2**50), pa.int64()).cast(pa.duration("ms")),
        "dur_us": pa.array(ints(-2**60, 2**60), pa.int64()).cast(pa.duration("us")),
        "dur_ns": pa.array(ints(-2**62, 2**62), pa.int64()).cast(pa.duration("ns")),
        "interval_mdn": pa.array(nulls([pa.scalar((r.randint(-1000, 1000), r.randint(-1000, 1000), r.randint(-2**62, 2**62)), pa.month_day_nano_interval()).as_py() for _ in range(n)]), pa.month_day_nano_interval()),
        "fsb5": pa.array(nulls([r.randbytes(5) for _ in range(n)]), pa.binary(5)),
    }
    return pa.record_batch(cols)


prim = [primitives(N, 1), primitives(0, 2), primitives(N + 3, 3)]
write("primitives.arrow", prim, record_batches=3)
write("primitives.arrows", prim, stream=True, record_batches=3, eos=True)

# ------------------------------------------------------------------------------------------------------------- strings
def strings(seed, n=60):
    r = random.Random(seed)
    words = ["", "a", "short", "exactly12byt", "thirteen byte", "ü日本語テキスト", "a much longer string that cannot be inlined"]
    text = nulls([r.choice(words) + (str(r.randint(0, 10**6)) if r.random() < 0.5 else "") for _ in range(n)], 5)
    blobs = nulls([r.randbytes(r.choice([0, 3, 12, 13, 40])) for _ in range(n)], 6)
    return {"utf8": pa.array(text, pa.string()), "large_utf8": pa.array(text, pa.large_string()),
            "utf8_view": pa.array(text, pa.string_view()), "binary": pa.array(blobs, pa.binary()),
            "large_binary": pa.array(blobs, pa.large_binary()), "binary_view": pa.array(blobs, pa.binary_view())}


a, b, c = strings(10), strings(11), strings(12)
# Concatenated view arrays keep each part's data buffer: three variadic buffers per view column in that batch.
multi = {k: pa.concat_arrays([a[k], b[k], c[k]]) for k in a}
write("strings.arrow", [pa.record_batch(a), pa.record_batch(multi)], max_variadic=at_least(2))
write("strings.arrows", [pa.record_batch(a), pa.record_batch(multi)], stream=True, max_variadic=at_least(2))

# -------------------------------------------------------------------------------------------------------------- nested
def nested(seed, n=30):
    r = random.Random(seed)
    lists = nulls([[r.randint(-50, 50) if r.random() < 0.9 else None for _ in range(r.randint(0, 4))] for _ in range(n)], 7)
    strs = nulls([[r.choice(["x", "yy", "", None, "long string value here"]) for _ in range(r.randint(0, 3))] for _ in range(n)], 5)
    # A list view whose items overlap and come out of order: offsets and sizes chosen by hand over a shared child.
    child = pa.array([r.randint(0, 99) for _ in range(20)], pa.int32())
    offsets = [r.randint(0, 15) for _ in range(n)]
    sizes = [r.randint(0, 20 - o) for o in offsets]
    mask = pa.array([i % 6 == 5 for i in range(n)], pa.bool_())
    lv = pa.ListViewArray.from_arrays(pa.array(offsets, pa.int32()), pa.array(sizes, pa.int32()), child, mask=mask)
    llv = pa.LargeListViewArray.from_arrays(pa.array(offsets, pa.int64()), pa.array(sizes, pa.int64()), child, mask=mask)
    fsl = nulls([[r.uniform(-1, 1) for _ in range(3)] for _ in range(n)], 4)
    structs = nulls([{"a": r.randint(0, 9) if r.random() < 0.8 else None, "b": {"c": r.choice(["p", "q", None]), "d": [1, 2][:r.randint(0, 2)]}} for _ in range(n)], 6)
    maps = nulls([[(f"k{j}", r.randint(0, 9) if r.random() < 0.8 else None) for j in range(r.randint(0, 3))] for _ in range(n)], 5)
    deep = nulls([[[{"x": r.randint(0, 9), "y": [r.random() < 0.5]}] * r.randint(0, 2)] for _ in range(n)], 9)
    return pa.record_batch({
        "list_i64": pa.array(lists, pa.list_(pa.int64())),
        "large_list_str": pa.array(strs, pa.large_list(pa.string())),
        "list_view_i32": lv,
        "large_list_view_i32": llv,
        "fsl_f32x3": pa.array(fsl, pa.list_(pa.float32(), 3)),
        "struct": pa.array(structs, pa.struct([("a", pa.int16()), ("b", pa.struct([("c", pa.string()), ("d", pa.list_(pa.int8()))]))])),
        "map": pa.array(maps, pa.map_(pa.string(), pa.int32())),
        "deep": pa.array(deep, pa.list_(pa.list_(pa.struct([("x", pa.int32()), ("y", pa.list_(pa.bool_()))])))),
    })


big = nested(21, 50)
# Sliced input: the writer rebases offsets, the file must still read as the slice.
write("nested.arrow", [nested(20), big.slice(7, 30), nested(22, 0)], record_batches=3)
write("nested.arrows", [nested(20), big.slice(7, 30)], stream=True)

# --------------------------------------------------------------------------------------------------------------- unions
def unions(n=24):
    r = random.Random(30)
    codes = pa.array([r.choice([5, 7, 9]) for _ in range(n)], pa.int8())
    ints = pa.array(nulls([r.randint(0, 99) for _ in range(n)], 5), pa.int32())
    strs = pa.array(nulls([f"s{i}" for i in range(n)], 4), pa.string())
    lists = pa.array([[i] * (i % 3) for i in range(n)], pa.list_(pa.int16()))
    sparse = pa.UnionArray.from_sparse(codes, [ints, strs, lists], ["i", "s", "l"], [5, 7, 9])
    # Dense: offsets into short children, each child used in its own order.
    seen = {5: 0, 7: 0, 9: 0}
    dense_offsets = []
    for code in codes.to_pylist():
        dense_offsets.append(seen[code])
        seen[code] += 1
    dense = pa.UnionArray.from_dense(codes, pa.array(dense_offsets, pa.int32()),
                                     [ints.slice(0, seen[5]), strs.slice(0, seen[7]), lists.slice(0, seen[9])], ["i", "s", "l"], [5, 7, 9])
    return pa.record_batch({"sparse": sparse, "dense": dense})


write("unions.arrow", [unions()])
write("unions_v4.arrow", [unions()], options=ipc.IpcWriteOptions(metadata_version=ipc.MetadataVersion.V4), versions=["V4"])

# ------------------------------------------------------------------------------------------------------ run-end encoded
def ree(seed, n=50):
    r = random.Random(seed)
    values = []
    while len(values) < n:
        values += [r.choice([None, 1, 2, 3, 1000])] * r.randint(1, 6)
    values = values[:n]
    strs = [None if v is None else f"v{v}" for v in values]
    enc = lambda vals, t, run_type: pa.compute.run_end_encode(pa.array(vals, t), run_end_type=run_type)  # noqa: E731
    return pa.record_batch({
        "ree16_i64": enc(values, pa.int64(), pa.int16()),
        "ree32_str": enc(strs, pa.string(), pa.int32()),
        "ree64_f64": enc([None if v is None else v / 3 for v in values], pa.float64(), pa.int64()),
    })


import pyarrow.compute  # noqa: E402,F401

write("ree.arrow", [ree(40), ree(41, 7)])
write("ree.arrows", [ree(40), ree(41, 7)], stream=True)

# --------------------------------------------------------------------------------------------------------- dictionaries
def dict_column(values, index_type, value_type=pa.string(), ordered=False):
    dictionary = sorted({v for v in values if v is not None}, key=str)
    indices = [None if v is None else dictionary.index(v) for v in values]
    return pa.DictionaryArray.from_arrays(pa.array(indices, index_type), pa.array(dictionary, value_type), ordered=ordered)


def dictionaries(seed, n=30):
    r = random.Random(seed)
    words = nulls([r.choice(["red", "green", "blue", "a rather long dictionary value"]) for _ in range(n)], 6)
    nums = nulls([r.choice([10, 20, 30]) for _ in range(n)], 5)
    cols = {}
    for t in [pa.int8(), pa.int16(), pa.int32(), pa.int64(), pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64()]:
        cols[f"dict_{t}"] = dict_column(words, t)
    cols["dict_ordered"] = dict_column(words, pa.int32(), ordered=True)
    cols["dict_large_utf8"] = dict_column(words, pa.int32(), pa.large_string())
    cols["dict_utf8_view"] = dict_column(words, pa.int32(), pa.string_view())
    cols["dict_i64_values"] = dict_column(nums, pa.int16(), pa.int64())
    cols["dict_all_null"] = pa.DictionaryArray.from_arrays(pa.nulls(n, pa.int32()), pa.array([], pa.string()))
    # A dictionary whose values hold a null, referenced by an index.
    cols["dict_null_value"] = pa.DictionaryArray.from_arrays(pa.array([i % 3 for i in range(n)], pa.int8()), pa.array(["x", None, "z"]))
    inner = dict_column([r.choice(["p", "q"]) for _ in range(n * 2)], pa.int8())
    cols["list_of_dict"] = pa.ListArray.from_arrays(pa.array(list(range(0, 2 * n + 1, 2)), pa.int32()), inner)
    cols["struct_of_dict"] = pa.StructArray.from_arrays([dict_column(words, pa.int16()), pa.array(range(n), pa.int32())], ["d", "n"])
    return pa.record_batch(cols)


d1 = dictionaries(50)
write("dictionary.arrow", [d1, d1.slice(3, 20)], dictionary_batches=at_least(10))
write("dictionary.arrows", [d1, d1.slice(3, 20)], stream=True)


def delta_batches():
    # The second batch's dictionary extends the first one's: a writer with deltas emits only the new values.
    first = pa.DictionaryArray.from_arrays(pa.array([0, 1, None, 1], pa.int32()), pa.array(["A", "B"]))
    second = pa.DictionaryArray.from_arrays(pa.array([2, 0, 3, None], pa.int32()), pa.array(["A", "B", "C", "D"]))
    third = pa.DictionaryArray.from_arrays(pa.array([4, 4, 1], pa.int32()), pa.array(["A", "B", "C", "D", "E"]))
    return [pa.record_batch({"d": x, "n": pa.array(range(len(x)), pa.int16())}) for x in (first, second, third)]


deltas = ipc.IpcWriteOptions(emit_dictionary_deltas=True)
write("dictionary_delta.arrow", delta_batches(), options=deltas, deltas=2, replacements=0)
write("dictionary_delta.arrows", delta_batches(), stream=True, options=deltas, deltas=2, replacements=0)


def replacement_batches():
    first = pa.DictionaryArray.from_arrays(pa.array([0, 1, 1], pa.int8()), pa.array(["A", "B"]))
    second = pa.DictionaryArray.from_arrays(pa.array([1, 0, None], pa.int8()), pa.array(["C", "A"]))  # not an extension
    third = pa.DictionaryArray.from_arrays(pa.array([0, 0], pa.int8()), pa.array(["Z"]))
    return [pa.record_batch({"d": x}) for x in (first, second, third)]


write("dictionary_replacement.arrows", replacement_batches(), stream=True, replacements=2, deltas=0)
# The file format has no replacements; pyarrow's unify_dictionaries writes one dictionary for all batches instead.
# It unifies only within one write_table call.
with ipc.new_file(os.path.join(out, "dictionary_unified.arrow"), replacement_batches()[0].schema,
                  options=ipc.IpcWriteOptions(unify_dictionaries=True)) as w:
    w.write_table(pa.Table.from_batches(replacement_batches()))
check("dictionary_unified.arrow", format="file", dictionary_batches=1, replacements=0, record_batches=3)

# ------------------------------------------------------------------------------------------------------ extension types
class Point(pa.ExtensionType):
    def __init__(self):
        super().__init__(pa.struct([("x", pa.float64()), ("y", pa.float64())]), "seseragi.point")

    def __arrow_ext_serialize__(self):
        return b'{"crs":"none"}'

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()


def extensions(n=12):
    r = random.Random(60)
    points = pa.array([{"x": r.random(), "y": r.random()} for _ in range(n)], Point().storage_type)
    tensor = pa.fixed_shape_tensor(pa.int32(), [2, 2])
    return pa.record_batch({
        "uuid": pa.ExtensionArray.from_storage(pa.uuid(), pa.array(nulls([uuid.UUID(int=r.getrandbits(128)).bytes for _ in range(n)]), pa.binary(16))),
        "json": pa.ExtensionArray.from_storage(pa.json_(), pa.array(nulls(['{"a": %d}' % i for i in range(n)]))),
        "bool8": pa.ExtensionArray.from_storage(pa.bool8(), pa.array(nulls([r.choice([0, 1, 7]) for _ in range(n)]), pa.int8())),
        "tensor": pa.ExtensionArray.from_storage(tensor, pa.array([[r.randint(0, 9) for _ in range(4)] for _ in range(n)], pa.list_(pa.int32(), 4))),
        "opaque": pa.ExtensionArray.from_storage(pa.opaque(pa.binary(), "geometry", "postgis"), pa.array([r.randbytes(4) for _ in range(n)])),
        "point": pa.ExtensionArray.from_storage(Point(), points),
    })


write("extension.arrow", [extensions()])

# -------------------------------------------------------------------------------------------------------- custom metadata
meta_schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False, metadata={"field:k": "field v", "ünï": "çödé"}),
    pa.field("s", pa.struct([pa.field("inner", pa.string(), metadata={"inner:k": "inner v"})]), metadata={"outer": "1"}),
], metadata={"schema:k": "schema v", "empty": ""})
meta_batch = pa.record_batch([pa.array([1, 2, 3], pa.int64()), pa.array([{"inner": "a"}, None, {"inner": "c"}], meta_schema.field("s").type)], schema=meta_schema)
batch_meta = {0: {"batch:k": "first"}, 2: {"batch:k": "third", "n": "2"}}
write("metadata.arrow", [meta_batch] * 3, meta_schema, batch_metadata=batch_meta, message_metadata=True)
write("metadata.arrows", [meta_batch] * 3, meta_schema, stream=True, batch_metadata=batch_meta, message_metadata=True)

# ------------------------------------------------------------------------------------------- empty, zero batches, framing
empty_schema = prim[0].schema
write("no_batches.arrow", [], empty_schema, record_batches=0)
write("no_batches.arrows", [], empty_schema, stream=True, record_batches=0)
write("empty_batches.arrow", [prim[1], prim[0], prim[1], prim[1]], record_batches=4)
write("legacy.arrows", prim, stream=True, options=ipc.IpcWriteOptions(use_legacy_format=True), legacy=True)
write("v4.arrow", prim, options=ipc.IpcWriteOptions(metadata_version=ipc.MetadataVersion.V4), versions=["V4"])

# A stream that ends without the end-of-stream marker (the writer closed the connection): drop the last 8 bytes.
with open(os.path.join(out, "primitives.arrows"), "rb") as f:
    data = f.read()
assert data[-8:] == b"\xff\xff\xff\xff\x00\x00\x00\x00"
with open(os.path.join(out, "no_eos.arrows"), "wb") as f:
    f.write(data[:-8])
check("no_eos.arrows", eos=False, record_batches=3)

# ----------------------------------------------------------------------------------------------------------- compression
def mixed(seed, n=200):
    r = random.Random(seed)
    s = strings(seed, n)
    return pa.record_batch({
        "i64": pa.array(nulls([r.randint(0, 100) for _ in range(n)]), pa.int64()),
        "f64": pa.array([r.random() for _ in range(n)], pa.float64()),
        "utf8": s["utf8"], "utf8_view": s["utf8_view"],
        "list": pa.array([[r.randint(0, 5)] * r.randint(0, 3) for _ in range(n)], pa.list_(pa.int32())),
        "dict": pa.DictionaryArray.from_arrays(pa.array([r.randint(0, 2) for _ in range(n)], pa.int8()), pa.array(["a", "b", "c"])),
        "noise": pa.array([r.randbytes(32) for _ in range(n)], pa.binary(32)),  # incompressible (pyarrow still compresses it)
        "empty_validity": pa.array([1] * n, pa.int8()),  # no nulls: an empty validity buffer
    })


comp = [mixed(70), mixed(71, 0), mixed(72)]
for codec, label in (("lz4", "LZ4_FRAME"), ("zstd", "ZSTD")):
    opts = ipc.IpcWriteOptions(compression=codec)
    # pyarrow never stores a buffer raw (-1 prefix; min_space_savings is not exposed): the apache 2.0.0-compression
    # files cover that.
    write(f"{codec}.arrow", comp, options=opts, codecs=[label], raw_buffers=0)
    write(f"{codec}.arrows", comp, stream=True, options=opts, codecs=[label], raw_buffers=0)
write("zstd_level19.arrow", comp, options=ipc.IpcWriteOptions(compression=pa.Codec("zstd", 19)), codecs=["ZSTD"])

# ------------------------------------------------------------------------------------------------------- many batches
many = [mixed(100 + i, 100) for i in range(16)]
write("many_batches.arrow", many, record_batches=16)
write("many_batches_lz4.arrow", many, options=ipc.IpcWriteOptions(compression="lz4"), record_batches=16, codecs=["LZ4_FRAME"])

# Buffers over 64 KiB: LZ4 frames of several (independent) blocks.
big = pa.record_batch({"i64": pa.array([i % 1000 for i in range(100_000)], pa.int64()),
                       "str": pa.array([f"value {i % 777}" for i in range(100_000)])})
write("big_lz4.arrow", [big], options=ipc.IpcWriteOptions(compression="lz4"), codecs=["LZ4_FRAME"])

# ---------------------------------------------------------------------------------------------------------------- feather
table = pa.Table.from_batches([mixed(80), mixed(81)])
feather.write_feather(table, os.path.join(out, "feather_default.feather"))  # Feather v2: LZ4 by default
check("feather_default.feather", format="file", codecs=["LZ4_FRAME"])
feather.write_feather(table, os.path.join(out, "feather_uncompressed.feather"), compression="uncompressed", chunksize=64)
check("feather_uncompressed.feather", format="file", codecs=[], record_batches=at_least(6))
# Feather v1 is a different format (no ARROW1 magic): a reader should refuse it with a clear message.
feather.write_feather(pa.table({"a": pa.array([1, 2, 3], pa.int64())}), os.path.join(out, "feather_v1.feather"), version=1)
with open(os.path.join(out, "feather_v1.feather"), "rb") as f:
    assert f.read(4) == b"FEA1"
print("ok", file=sys.stderr)
