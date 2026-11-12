"""Generates the Avro read fixtures with fastavro (the reference writer).

Run from this directory with a venv that has fastavro, cramjam and zstandard installed:
    python -I generate_fixtures.py .
The C# tests hard-code the values written here, so keep the two in sync.
"""
import sys
import fastavro

OUT = sys.argv[1]


def write(name, schema, records, codec="null", sync_interval=16000):
    with open(f"{OUT}/{name}.avro", "wb") as f:
        fastavro.writer(f, fastavro.parse_schema(schema), records, codec=codec, sync_interval=sync_interval)


def record(fields, name="r"):
    return {"type": "record", "name": name, "fields": [{"name": n, "type": t} for n, t in fields]}


PRIMITIVES = record([
    ("b", "boolean"), ("i", "int"), ("l", "long"), ("f", "float"), ("d", "double"),
    ("s", "string"), ("by", "bytes"), ("n", ["null", "long"]),
])
write("primitives", PRIMITIVES, [
    {"b": True, "i": 1, "l": 1, "f": 1.5, "d": 2.25, "s": "a", "by": b"\x00\x01", "n": 7},
    {"b": False, "i": -2147483648, "l": -9223372036854775808, "f": -0.0, "d": 1e300, "s": "", "by": b"", "n": None},
    {"b": True, "i": 2147483647, "l": 9223372036854775807, "f": 3.25, "d": -0.5, "s": "雷 ⚡", "by": b"\xff", "n": -1},
])

# Many rows in many small blocks, once per codec: row k has i = k, s = "s{k}", n = k or null when k % 3 == 0.
MANY = [{"b": k % 2 == 0, "i": k, "l": k * 1_000_000_007, "f": k / 4, "d": k / 8, "s": f"s{k}",
         "by": bytes([k % 256]), "n": None if k % 3 == 0 else k} for k in range(5000)]
for codec in ["null", "deflate", "snappy", "zstandard"]:
    write(f"many_{codec}", PRIMITIVES, MANY, codec=codec, sync_interval=2000)

LOGICAL = record([
    ("date", {"type": "int", "logicalType": "date"}),
    ("tms", {"type": "int", "logicalType": "time-millis"}),
    ("tus", {"type": "long", "logicalType": "time-micros"}),
    ("tsms", {"type": "long", "logicalType": "timestamp-millis"}),
    ("tsus", {"type": "long", "logicalType": "timestamp-micros"}),
    ("tsns", {"type": "long", "logicalType": "timestamp-nanos"}),
    ("ltsms", {"type": "long", "logicalType": "local-timestamp-millis"}),
    ("ltsus", {"type": "long", "logicalType": "local-timestamp-micros"}),
    ("ltsns", {"type": "long", "logicalType": "local-timestamp-nanos"}),
    ("decb", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}),
    ("decf", {"type": "fixed", "name": "D8", "size": 8, "logicalType": "decimal", "precision": 18, "scale": 3}),
    ("dec50", {"type": "bytes", "logicalType": "decimal", "precision": 50, "scale": 0}),
    ("uuid", {"type": "string", "logicalType": "uuid"}),
    ("dur", {"type": "fixed", "name": "Dur", "size": 12, "logicalType": "duration"}),
    ("badlt", {"type": "long", "logicalType": "no-such-type"}),
])
import decimal
write("logical", LOGICAL, [
    {"date": 18262, "tms": 3723004, "tus": 3723000004, "tsms": 1577836800123, "tsus": 1577836800123456,
     "tsns": 1577836800123456789, "ltsms": 1, "ltsus": 2, "ltsns": 3,
     "decb": decimal.Decimal("-12345678.91"), "decf": decimal.Decimal("123456789012345.678"),
     "dec50": decimal.Decimal("-" + "9" * 50), "uuid": "5c3d8448-a0b1-4265-83c4-0d24427e8fc2",
     "dur": (1).to_bytes(4, "little") + (2).to_bytes(4, "little") + (3).to_bytes(4, "little"), "badlt": 42},
])

COMPLEX = record([
    ("arr", {"type": "array", "items": "int"}),
    ("arrn", {"type": "array", "items": ["null", "string"]}),
    ("m", {"type": "map", "values": "long"}),
    ("rec", {"type": "record", "name": "Inner", "fields": [
        {"name": "x", "type": "int"}, {"name": "y", "type": ["null", "string"]}]}),
    ("recn", ["null", "Inner"]),
    ("e", {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN", "BLUE"]}),
    ("fx", {"type": "fixed", "name": "F3", "size": 3}),
    ("il", ["int", "long"]),
    ("fd", ["null", "float", "double"]),
    ("u", ["string", "null", "int"]),
    ("nonly", ["null"]),
    ("nul", "null"),
])
write("complex", COMPLEX, [
    {"arr": [1, 2, 3], "arrn": ["a", None], "m": {"k": 1, "j": 2}, "rec": {"x": 1, "y": "p"}, "recn": None,
     "e": "BLUE", "fx": b"abc", "il": 5, "fd": 1.5, "u": "s", "nonly": None, "nul": None},
    {"arr": [], "arrn": [], "m": {}, "rec": {"x": 2, "y": None}, "recn": {"x": 3, "y": "q"},
     "e": "RED", "fx": b"xyz", "il": 2**40, "fd": None, "u": None, "nonly": None, "nul": None},
    {"arr": [-1], "arrn": [None], "m": {"z": -5}, "rec": {"x": 4, "y": "r"}, "recn": None,
     "e": "GREEN", "fx": b"\x00\x00\x00", "il": -7, "fd": 2.5e100, "u": 9, "nonly": None, "nul": None},
], codec="snappy")

RECURSIVE = {"type": "record", "name": "Top", "fields": [
    {"name": "id", "type": "int"},
    {"name": "node", "type": {"type": "record", "name": "Node", "fields": [
        {"name": "v", "type": "int"}, {"name": "next", "type": ["null", "Node"]}]}},
    {"name": "tail", "type": "string"},
]}
write("recursive", RECURSIVE, [
    {"id": 1, "node": {"v": 1, "next": {"v": 2, "next": None}}, "tail": "a"},
    {"id": 2, "node": {"v": 3, "next": None}, "tail": "b"},
])

write("toplevel_long", "long", [1, -2, 3])

write("empty", PRIMITIVES, [])

# The Apache Avro interop schema (recursive Node), with the datum the Python interop generator uses.
import json
with open(f"{OUT}/apache/interop.avsc") as f:
    INTEROP = json.load(f)
INTEROP_DATUM = {
    "intField": 12, "longField": 15234324, "stringField": "hey", "boolField": True, "floatField": 1234.0,
    "doubleField": -1234.0, "bytesField": b"12312adf", "nullField": None,
    "arrayField": [5.0, 0.0, 12.0], "mapField": {"a": {"label": "a"}, "bee": {"label": "cee"}},
    "unionField": 12.0, "enumField": "C", "fixedField": b"1019181716151413",
    "recordField": {"label": "blah", "children": [{"label": "inner", "children": []}]},
}
with open(f"{OUT}/apache/interop.avro", "wb") as f:
    fastavro.writer(f, fastavro.parse_schema(INTEROP), [INTEROP_DATUM, dict(INTEROP_DATUM, unionField=[b"x", b""], intField=13)],
                    codec="deflate")
