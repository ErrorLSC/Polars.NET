"""A small, independent reader of Arrow IPC framing and FlatBuffer metadata (format/Message.fbs, Schema.fbs, File.fbs).

It records what pyarrow does not expose: message framing (continuation marker or the pre-0.15 form), metadata
versions, dictionary ids and the isDelta flag, body compression and per-buffer compressed-length prefixes (-1 = stored
raw, empty buffers without a prefix), variadic buffer counts, nodes and buffers, custom metadata at schema, field,
message and footer level, and the footer's blocks. ipc_check.py writes its output into messages.json; the generators
use it to check that each file has the feature it is named for.

Usage: python -I ipc_meta.py <file>   (prints the JSON summary)
"""
import json
import struct
import sys

TYPES = ["NONE", "Null", "Int", "FloatingPoint", "Binary", "Utf8", "Bool", "Decimal", "Date", "Time", "Timestamp",
         "Interval", "List", "Struct_", "Union", "FixedSizeBinary", "FixedSizeList", "Map", "Duration", "LargeBinary",
         "LargeUtf8", "LargeList", "RunEndEncoded", "BinaryView", "Utf8View", "ListView", "LargeListView"]
HEADERS = ["NONE", "Schema", "DictionaryBatch", "RecordBatch", "Tensor", "SparseTensor"]
VERSIONS = ["V1", "V2", "V3", "V4", "V5"]
TIME_UNITS = ["SECOND", "MILLISECOND", "MICROSECOND", "NANOSECOND"]
CODECS = ["LZ4_FRAME", "ZSTD"]
MAGIC = b"ARROW1"


class FlatBufferError(Exception):
    pass


class Table:
    """A FlatBuffer table at `pos` in `buf`, with bounds checks on every read."""

    def __init__(self, buf, pos):
        self.buf, self.pos = buf, pos
        self._need(pos, 4)
        vt = pos - struct.unpack_from("<i", buf, pos)[0]
        self._need(vt, 4)
        self.vt_size, _ = struct.unpack_from("<HH", buf, vt)
        self._need(vt, self.vt_size)
        self.vt = vt

    def _need(self, at, n):
        if at < 0 or at + n > len(self.buf):
            raise FlatBufferError(f"read of {n} bytes at {at} outside a {len(self.buf)}-byte buffer")

    def offset(self, field):
        slot = 4 + 2 * field
        if slot + 2 > self.vt_size:
            return 0
        return struct.unpack_from("<H", self.buf, self.vt + slot)[0]

    def scalar(self, field, fmt, default=0):
        o = self.offset(field)
        if not o:
            return default
        self._need(self.pos + o, struct.calcsize(fmt))
        return struct.unpack_from("<" + fmt, self.buf, self.pos + o)[0]

    def _indirect(self, field):
        o = self.offset(field)
        if not o:
            return None
        at = self.pos + o
        self._need(at, 4)
        return at + struct.unpack_from("<I", self.buf, at)[0]

    def table(self, field):
        at = self._indirect(field)
        return Table(self.buf, at) if at is not None else None

    def string(self, field):
        at = self._indirect(field)
        if at is None:
            return None
        self._need(at, 4)
        n = struct.unpack_from("<I", self.buf, at)[0]
        self._need(at + 4, n)
        return self.buf[at + 4:at + 4 + n].decode("utf-8", "replace")

    def vector(self, field):
        """(start, count) of a vector's elements, or None."""
        at = self._indirect(field)
        if at is None:
            return None
        self._need(at, 4)
        return at + 4, struct.unpack_from("<I", self.buf, at)[0]

    def tables(self, field):
        v = self.vector(field)
        if v is None:
            return []
        start, n = v
        self._need(start, 4 * n)
        return [Table(self.buf, start + 4 * i + struct.unpack_from("<I", self.buf, start + 4 * i)[0]) for i in range(n)]

    def structs(self, field, fmt):
        v = self.vector(field)
        if v is None:
            return []
        start, n = v
        size = struct.calcsize("<" + fmt)
        self._need(start, size * n)
        return [struct.unpack_from("<" + fmt, self.buf, start + size * i) for i in range(n)]


def key_values(t, field):
    return [[kv.string(0), kv.string(1)] for kv in t.tables(field)] or None


def type_info(field):
    tid = field.scalar(2, "B")
    name = TYPES[tid] if tid < len(TYPES) else f"unknown({tid})"
    t = field.table(3)
    info = {"type": name}
    if t is None:
        return info
    if name == "Int":
        info |= {"bitWidth": t.scalar(0, "i"), "signed": bool(t.scalar(1, "B"))}
    elif name == "FloatingPoint":
        info["precision"] = ["HALF", "SINGLE", "DOUBLE"][t.scalar(0, "h")]
    elif name == "Decimal":
        info |= {"precision": t.scalar(0, "i"), "scale": t.scalar(1, "i"), "bitWidth": t.scalar(2, "i", 128)}
    elif name == "Date":
        info["unit"] = ["DAY", "MILLISECOND"][t.scalar(0, "h", 1)]
    elif name == "Time":
        info |= {"unit": TIME_UNITS[t.scalar(0, "h", 1)], "bitWidth": t.scalar(1, "i", 32)}
    elif name == "Timestamp":
        info |= {"unit": TIME_UNITS[t.scalar(0, "h")], "timezone": t.string(1)}
    elif name == "Duration":
        info["unit"] = TIME_UNITS[t.scalar(0, "h", 1)]
    elif name == "Interval":
        info["unit"] = ["YEAR_MONTH", "DAY_TIME", "MONTH_DAY_NANO"][t.scalar(0, "h")]
    elif name == "FixedSizeBinary":
        info["byteWidth"] = t.scalar(0, "i")
    elif name == "FixedSizeList":
        info["listSize"] = t.scalar(0, "i")
    elif name == "Map":
        info["keysSorted"] = bool(t.scalar(0, "B"))
    elif name == "Union":
        info["mode"] = ["Sparse", "Dense"][t.scalar(0, "h")]
        ids = t.structs(1, "i")
        info["typeIds"] = [i[0] for i in ids] if ids else None
    return info


def field_info(f):
    out = {"name": f.string(0), "nullable": bool(f.scalar(1, "B"))} | type_info(f)
    d = f.table(4)
    if d is not None:
        index = d.table(1)
        out["dictionary"] = {
            "id": d.scalar(0, "q"),
            "indexBitWidth": index.scalar(0, "i") if index else None,
            "indexSigned": bool(index.scalar(1, "B")) if index else None,
            "ordered": bool(d.scalar(2, "B")),
        }
    children = [field_info(c) for c in f.tables(5)]
    if children:
        out["children"] = children
    md = key_values(f, 6)
    if md:
        out["metadata"] = md
    return out


def schema_info(s):
    return {
        "endianness": ["Little", "Big"][s.scalar(0, "h")],
        "fields": [field_info(f) for f in s.tables(1)],
        "metadata": key_values(s, 2),
        "features": [x[0] for x in s.structs(3, "q")] or None,
    }


def record_batch_info(rb, body, body_start):
    nodes = [list(n) for n in rb.structs(1, "qq")]
    buffers = [list(b) for b in rb.structs(2, "qq")]
    out = {"length": rb.scalar(0, "q"), "nodes": nodes, "buffers": buffers}
    comp = rb.table(3)
    if comp is not None:
        codec = comp.scalar(0, "b")
        out["compression"] = CODECS[codec] if 0 <= codec < len(CODECS) else f"unknown({codec})"
        # Per-buffer prefixes: the uncompressed length, -1 = stored raw; an empty buffer may have no prefix at all.
        prefixes = []
        for offset, length in buffers:
            if length == 0:
                prefixes.append(None)
            elif length < 8 or body is None or body_start + offset + 8 > len(body):
                prefixes.append("bad")
            else:
                prefixes.append(struct.unpack_from("<q", body, body_start + offset)[0])
        out["prefixes"] = prefixes
    counts = rb.structs(4, "q")
    if counts:
        out["variadicBufferCounts"] = [c[0] for c in counts]
    return out


def message_info(meta, data, body_start):
    m = Table(meta, struct.unpack_from("<I", meta, 0)[0])
    version = m.scalar(0, "h")
    kind = m.scalar(1, "B")
    out = {
        "version": VERSIONS[version] if 0 <= version < len(VERSIONS) else f"unknown({version})",
        "header": HEADERS[kind] if kind < len(HEADERS) else f"unknown({kind})",
        "bodyLength": m.scalar(3, "q"),
    }
    md = key_values(m, 4)
    if md:
        out["metadata"] = md
    h = m.table(2)
    if h is None:
        return out
    if out["header"] == "Schema":
        out["schema"] = schema_info(h)
    elif out["header"] == "RecordBatch":
        out |= record_batch_info(h, data, body_start)
    elif out["header"] == "DictionaryBatch":
        out["id"] = h.scalar(0, "q")
        out["isDelta"] = bool(h.scalar(2, "B"))
        rb = h.table(1)
        if rb is not None:
            out |= record_batch_info(rb, data, body_start)
    return out


def read_messages(data, at, end):
    """Messages from `at` up to `end` or an end-of-stream marker; each records its framing."""
    messages = []
    while at + 4 <= end:
        start = at
        first = struct.unpack_from("<I", data, at)[0]
        legacy = first != 0xFFFFFFFF
        if legacy:
            length = struct.unpack_from("<i", data, at)[0]
            at += 4
        else:
            if at + 8 > end:
                messages.append({"offset": start, "error": "truncated length after continuation"})
                break
            length = struct.unpack_from("<i", data, at + 4)[0]
            at += 8
        if length == 0:
            messages.append({"offset": start, "eos": True, "legacy": legacy})
            break
        if length < 0 or at + length > end:
            messages.append({"offset": start, "error": f"metadata length {length} out of range"})
            break
        meta = data[at:at + length]
        at += length
        try:
            info = message_info(meta, data, at)
        except (FlatBufferError, struct.error, IndexError) as e:
            messages.append({"offset": start, "error": f"metadata: {e}"})
            break
        info = {"offset": start, "legacy": legacy, "metadataLength": length} | info
        body = info.get("bodyLength", 0)
        if body < 0 or at + body > end:
            info["error"] = f"body length {body} out of range"
            messages.append(info)
            break
        at += body
        messages.append(info)
    return messages


def read_block(data, offset, meta_len, body_len):
    """One message located by a footer block: the metadata (with its prefix) is meta_len bytes, the body follows."""
    if offset < 8 or meta_len < 8 or body_len < 0 or offset + meta_len + body_len > len(data):
        return {"offset": offset, "error": f"block ({offset}, {meta_len}, {body_len}) out of range"}
    legacy = struct.unpack_from("<I", data, offset)[0] != 0xFFFFFFFF
    start = offset + (4 if legacy else 8)
    length = struct.unpack_from("<i", data, start - 4)[0]
    if length <= 0 or start + length > offset + meta_len:
        return {"offset": offset, "error": f"metadata length {length} outside its block"}
    try:
        info = message_info(data[start:start + length], data, offset + meta_len)
    except (FlatBufferError, struct.error, IndexError) as e:
        return {"offset": offset, "error": f"metadata: {e}"}
    info = {"offset": offset, "legacy": legacy, "metadataLength": length} | info
    if info.get("bodyLength") != body_len:
        info["blockMismatch"] = f"block body {body_len}, message body {info.get('bodyLength')}"
    return info


def leading_framing(data):
    """How the file's embedded stream starts at offset 8: 'continuation', 'legacy' (a bare int32 length) or 'bare'
    (a Message FlatBuffer with no prefix at all, as Polars 2.1 writes)."""
    first = struct.unpack_from("<I", data, 8)[0]
    if first == 0xFFFFFFFF:
        return "continuation"
    try:  # a bare FlatBuffer: the first word is the root table offset
        m = Table(data[8:], first)
        if m.scalar(1, "B") == 1 and 0 <= m.scalar(0, "h") < len(VERSIONS):
            return "bare"
    except (FlatBufferError, struct.error):
        pass
    return "legacy"


def describe(data):
    if len(data) >= 6 + 2 + 4 + 6 and data[:6] == MAGIC and data[-6:] == MAGIC:
        out = {"format": "file"}
        footer_len = struct.unpack_from("<i", data, len(data) - 10)[0]
        footer_at = len(data) - 10 - footer_len
        if footer_len <= 0 or footer_at < 8:
            out["error"] = f"footer length {footer_len} out of range"
            out["messages"] = []
            return out
        try:
            footer = data[footer_at:footer_at + footer_len]
            f = Table(footer, struct.unpack_from("<I", footer, 0)[0])
            version = f.scalar(0, "h")
            schema = f.table(1)
            out["footer"] = {
                "version": VERSIONS[version] if 0 <= version < len(VERSIONS) else f"unknown({version})",
                "schema": schema_info(schema) if schema else None,
                "dictionaries": [[b[0], b[1], b[3]] for b in f.structs(2, "qiiq")],
                "recordBatches": [[b[0], b[1], b[3]] for b in f.structs(3, "qiiq")],
                "metadata": key_values(f, 4),
            }
        except (FlatBufferError, struct.error, IndexError) as e:
            out["error"] = f"footer: {e}"
            out["messages"] = []
            return out
        out["leading"] = leading_framing(data)
        # Readers locate messages through the footer, in footer order: dictionaries first, then record batches.
        blocks = out["footer"]["dictionaries"] + out["footer"]["recordBatches"]
        out["messages"] = [read_block(data, *b) for b in blocks]
        return out
    return {"format": "stream", "messages": read_messages(data, 0, len(data))}


def features(summary):
    """The facts the generators assert on, flattened."""
    msgs = [m for m in summary["messages"] if "header" in m]
    prefixes = [p for m in msgs for p in m.get("prefixes", [])]
    dict_ids = {}
    replacements = deltas = 0
    for m in msgs:
        if m["header"] == "DictionaryBatch":
            if m["isDelta"]:
                deltas += 1
            elif m["id"] in dict_ids:
                replacements += 1
            dict_ids[m["id"]] = True
    return {
        "format": summary["format"],
        "versions": sorted({m["version"] for m in msgs}),
        "legacy": any(m.get("legacy") for m in summary["messages"]),
        "codecs": sorted({m["compression"] for m in msgs if "compression" in m}),
        "raw_buffers": sum(1 for p in prefixes if p == -1),
        "unprefixed_empty": sum(1 for p in prefixes if p is None),
        "record_batches": sum(1 for m in msgs if m["header"] == "RecordBatch"),
        "dictionary_batches": sum(1 for m in msgs if m["header"] == "DictionaryBatch"),
        "deltas": deltas,
        "replacements": replacements,
        "max_variadic": max((c for m in msgs for c in m.get("variadicBufferCounts", [])), default=0),
        "message_metadata": any("metadata" in m for m in msgs),
        "eos": any(m.get("eos") for m in summary["messages"]),
        "leading": summary.get("leading"),
    }


if __name__ == "__main__":
    with open(sys.argv[1], "rb") as fh:
        print(json.dumps(describe(fh.read()), indent=1))
