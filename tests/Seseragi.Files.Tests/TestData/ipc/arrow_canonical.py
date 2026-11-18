"""The canonical text form of Arrow values, shared by the IPC oracle (ipc_check.py).

It extends the form of TestData/parquet/parquet_check.py (whose tokens are kept byte-identical, so the C#
CanonicalRender can serve both) with the types IPC carries and Parquet does not:

  null                      a null at any level, and every value of the null type
  true / false              booleans
  123                       integers (any width, signed or not)
  f:<hex bits>              floats: the IEEE bits at their own width (f16, f32, f64)
  d:<unscaled>e<-scale>     decimals (32, 64, 128 and 256 bits)
  D:<days>                  dates (days since 1970-01-01; date64 floors to days)
  t:<ns>                    times of day, in nanoseconds
  T:<ns>                    timestamps, nanoseconds since the epoch (any range)
  P:<ns>                    durations, in nanoseconds
  I:<months>,<days>,<ns>    month-day-nano intervals
  Y:<months>                year-month intervals                                 (new)
  J:<days>,<ms>             day-time intervals                                   (new)
  u:<hex>                   strings (utf8, large, view): their UTF-8 bytes
  b:<hex>                   binary, large binary, binary view, fixed-size binary
  [a,b]                     lists of any layout: list, large list, list view, large list view, fixed-size list
  {name:v,...}              structs
  M{k=>v,...}               maps
  U<type id>:<v>            union values: the type id and the child's value     (new)
Dictionary-encoded, run-end-encoded and extension columns render as their logical values.
Every array is read through its own offset, so sliced arrays render like their unsliced copies.
"""
import hashlib
import struct

import pyarrow as pa
import pyarrow.compute as pc

NS = {"s": 10**9, "ms": 10**6, "us": 10**3, "ns": 1}


def _raw(arr, fmt):
    """The fixed-width values of buffer 1 for this array's slice, unpacked with struct format `fmt`."""
    size = struct.calcsize("<" + fmt)
    buf = arr.buffers()[1]
    data = buf.to_pybytes() if buf is not None else b""
    return [struct.unpack_from("<" + fmt, data, (arr.offset + i) * size) for i in range(len(arr))]


def _valid(arr):
    t = arr.type
    if pa.types.is_union(t) or pa.types.is_run_end_encoded(t) or pa.types.is_null(t):
        return [True] * len(arr)  # no validity of their own: nulls come from the children
    return arr.is_valid().to_pylist()


def render(arr):
    """The canonical strings of a (non-chunked) array."""
    t = arr.type
    n = len(arr)
    if n == 0:  # also spares pyarrow, which crashes on the absent buffers of an empty union
        return []
    if isinstance(t, pa.BaseExtensionType):
        return render(arr.storage)
    if pa.types.is_dictionary(t):  # by hand: pyarrow has no take kernel for some value types (string_view)
        values = render(arr.dictionary)
        return [values[i] if i is not None else "null" for i in arr.indices.to_pylist()]
    if pa.types.is_run_end_encoded(t):
        return render(pc.run_end_decode(arr))
    valid = _valid(arr)
    if pa.types.is_null(t):
        return ["null"] * n
    if pa.types.is_boolean(t):
        out = ["true" if v else "false" for v in arr.to_pylist()]
    elif pa.types.is_integer(t):
        out = [str(v) for v in arr.to_pylist()]
    elif pa.types.is_floating(t):
        width = t.bit_width
        out = ["f:%0*x" % (width // 4, b[0]) for b in _raw(arr, {16: "H", 32: "I", 64: "Q"}[width])]
    elif pa.types.is_decimal(t):
        width = t.bit_width // 8
        buf = arr.buffers()[1].to_pybytes()
        out = [f"d:{int.from_bytes(buf[(arr.offset + i) * width:(arr.offset + i + 1) * width], 'little', signed=True)}e-{t.scale}"
               for i in range(n)]
    elif pa.types.is_date32(t):
        out = [f"D:{v[0]}" for v in _raw(arr, "i")]
    elif pa.types.is_date64(t):
        out = [f"D:{v[0] // 86_400_000}" for v in _raw(arr, "q")]
    elif pa.types.is_time(t):
        f = NS[t.unit]
        out = [f"t:{v[0] * f}" for v in _raw(arr, "i" if t.bit_width == 32 else "q")]
    elif pa.types.is_timestamp(t):
        f = NS[t.unit]
        out = [f"T:{v[0] * f}" for v in _raw(arr, "q")]
    elif pa.types.is_duration(t):
        f = NS[t.unit]
        out = [f"P:{v[0] * f}" for v in _raw(arr, "q")]
    elif t == pa.month_day_nano_interval():
        out = [f"I:{m},{d},{ns}" for m, d, ns in _raw(arr, "iiq")]
    elif t.id == pa.lib.Type_INTERVAL_MONTHS:
        out = [f"Y:{v[0]}" for v in _raw(arr, "i")]
    elif t.id == pa.lib.Type_INTERVAL_DAY_TIME:
        out = [f"J:{d},{ms}" for d, ms in _raw(arr, "ii")]
    elif pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_string_view(t):
        out = ["u:" + v.encode("utf-8").hex() if v is not None else "" for v in arr.to_pylist()]
    elif (pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t)
          or pa.types.is_binary_view(t)):
        out = ["b:" + v.hex() if v is not None else "" for v in arr.to_pylist()]
    elif pa.types.is_map(t):
        entries = arr.values  # the unsliced entries; offsets index into it
        keys, items = render(entries.field(0)), render(entries.field(1))
        offsets = arr.offsets.to_pylist()
        out = ["M{" + ",".join(f"{keys[k]}=>{items[k]}" for k in range(offsets[i], offsets[i + 1])) + "}" for i in range(n)]
    elif pa.types.is_list(t) or pa.types.is_large_list(t):
        values, offsets = render(arr.values), arr.offsets.to_pylist()
        out = ["[" + ",".join(values[offsets[i]:offsets[i + 1]]) + "]" for i in range(n)]
    elif pa.types.is_list_view(t) or pa.types.is_large_list_view(t):
        values, offsets, sizes = render(arr.values), arr.offsets.to_pylist(), arr.sizes.to_pylist()
        out = ["[" + ",".join(values[offsets[i]:offsets[i] + sizes[i]]) + "]" if valid[i] else "" for i in range(n)]
    elif pa.types.is_fixed_size_list(t):
        values, size = render(arr.values), t.list_size
        out = ["[" + ",".join(values[(arr.offset + i) * size:(arr.offset + i + 1) * size]) + "]" for i in range(n)]
    elif pa.types.is_struct(t):
        fields = [(t.field(j).name, render(arr.field(j))) for j in range(t.num_fields)]
        out = ["{" + ",".join(f"{name}:{vals[i]}" for name, vals in fields) + "}" for i in range(n)]
    elif pa.types.is_union(t):
        codes = arr.type_codes.to_pylist()
        child_of = {code: k for k, code in enumerate(t.type_codes)}
        children = [render(arr.field(k)) for k in range(t.num_fields)]
        if t.mode == "dense":
            offsets = arr.offsets.to_pylist()
            out = [f"U{c}:{children[child_of[c]][offsets[i]]}" for i, c in enumerate(codes)]
        else:
            out = [f"U{c}:{children[child_of[c]][i]}" for i, c in enumerate(codes)]
    else:
        raise TypeError(f"no canonical form for {t}")
    return [o if valid[i] else "null" for i, o in enumerate(out)]


def digest(values):
    return hashlib.sha256("\n".join(values).encode("utf-8")).hexdigest()[:16]
