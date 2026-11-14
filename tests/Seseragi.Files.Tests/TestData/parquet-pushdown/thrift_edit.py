"""Generic Thrift compact protocol decode / encode, for editing Parquet footers and page indexes in probes.

A struct is a list of [field_id, type, value]; a list is (element_type, [values]); a map is (key_type, value_type,
[(k, v)]); binary is bytes; integers are int; bool is bool; double is float. Field types are the compact ids
(1/2 bool, 3 byte, 4 i16, 5 i32, 6 i64, 7 double, 8 binary, 9 list, 10 set, 11 map, 12 struct).
"""
import struct as _struct

BOOL, BYTE, I16, I32, I64, DOUBLE, BINARY, LIST, SET, MAP, STRUCT = 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12


class Reader:
    def __init__(self, data, pos=0):
        self.d, self.p = data, pos

    def byte(self):
        b = self.d[self.p]
        self.p += 1
        return b

    def varint(self):
        shift = result = 0
        while True:
            b = self.byte()
            result |= (b & 0x7F) << shift
            if not b & 0x80:
                return result
            shift += 7

    def zigzag(self):
        n = self.varint()
        return (n >> 1) ^ -(n & 1)

    def value(self, t):
        if t in (1, 2):
            return self.byte() == 1  # list elements; fields carry the value in the type nibble
        if t == BYTE:
            b = self.byte()
            return b - 256 if b > 127 else b
        if t in (I16, I32, I64):
            return self.zigzag()
        if t == DOUBLE:
            v = _struct.unpack_from("<d", self.d, self.p)[0]
            self.p += 8
            return v
        if t == BINARY:
            n = self.varint()
            v = bytes(self.d[self.p:self.p + n])
            self.p += n
            return v
        if t in (LIST, SET):
            h = self.byte()
            n, et = h >> 4, h & 15
            if n == 15:
                n = self.varint()
            return (et, [self.value(et) for _ in range(n)])
        if t == MAP:
            n = self.varint()
            if n == 0:
                return (0, 0, [])
            kv = self.byte()
            kt, vt = kv >> 4, kv & 15
            return (kt, vt, [(self.value(kt), self.value(vt)) for _ in range(n)])
        if t == STRUCT:
            return self.struct()
        raise ValueError(f"type {t}")

    def struct(self):
        fields, last = [], 0
        while True:
            h = self.byte()
            if h == 0:
                return fields
            delta, t = h >> 4, h & 15
            fid = last + delta if delta else self.zigzag()
            last = fid
            if t in (1, 2):
                fields.append([fid, BOOL, t == 1])
            else:
                fields.append([fid, t, self.value(t)])


class Writer:
    def __init__(self):
        self.out = bytearray()

    def varint(self, n):
        while True:
            b = n & 0x7F
            n >>= 7
            if n:
                self.out.append(b | 0x80)
            else:
                self.out.append(b)
                return

    def zigzag(self, n):
        self.varint((n << 1) ^ (n >> 63))

    def value(self, t, v):
        if t in (1, 2):
            self.out.append(1 if v else 2)
        elif t == BYTE:
            self.out.append(v & 0xFF)
        elif t in (I16, I32, I64):
            self.zigzag(v)
        elif t == DOUBLE:
            self.out += _struct.pack("<d", v)
        elif t == BINARY:
            self.varint(len(v))
            self.out += v
        elif t in (LIST, SET):
            et, items = v
            if len(items) < 15:
                self.out.append(len(items) << 4 | et)
            else:
                self.out.append(0xF0 | et)
                self.varint(len(items))
            for item in items:
                self.value(et, item)
        elif t == MAP:
            kt, vt, items = v
            self.varint(len(items))
            if items:
                self.out.append(kt << 4 | vt)
                for k, val in items:
                    self.value(kt, k)
                    self.value(vt, val)
        elif t == STRUCT:
            self.struct(v)
        else:
            raise ValueError(f"type {t}")

    def struct(self, fields):
        last = 0
        for fid, t, v in sorted(fields, key=lambda f: f[0]):
            wire = (1 if v else 2) if t in (1, 2) else t
            if 0 < fid - last <= 15:
                self.out.append((fid - last) << 4 | wire)
            else:
                self.out.append(wire)
                self.zigzag(fid)
            last = fid
            if t not in (1, 2):
                self.value(t, v)
        self.out.append(0)


def encode(fields):
    w = Writer()
    w.struct(fields)
    return bytes(w.out)


def get(fields, fid, default=None):
    for f in fields:
        if f[0] == fid:
            return f[2]
    return default


def put(fields, fid, t, v):
    for f in fields:
        if f[0] == fid:
            f[1], f[2] = t, v
            return
    fields.append([fid, t, v])


def drop(fields, fid):
    fields[:] = [f for f in fields if f[0] != fid]


class ParquetFile:
    """A file split into data and footer; edit `meta` (FileMetaData fields), add bytes with `append`, then `save`."""

    def __init__(self, path):
        data = open(path, "rb").read()
        n = int.from_bytes(data[-8:-4], "little")
        self.body = bytearray(data[:-8 - n])
        self.meta = Reader(data, len(data) - 8 - n).struct()

    def append(self, blob):
        offset = len(self.body)
        self.body += blob
        return offset

    def struct_at(self, offset):
        return Reader(self.body, offset).struct()

    def save(self, path):
        footer = encode(self.meta)
        with open(path, "wb") as f:
            f.write(self.body + footer + len(footer).to_bytes(4, "little") + b"PAR1")

    # FileMetaData 4 row_groups → RowGroup 1 columns → ColumnChunk 3 meta_data
    def chunk(self, g, c):
        return get(get(self.meta, 4)[1][g], 1)[1][c]

    def column_meta(self, g, c):
        return get(self.chunk(g, c), 3)
