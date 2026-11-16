"""Datasets whose files differ in their column types, for ParquetDatasetInteropTests.

`make <root>` writes one directory per case (pyarrow: a.parquet then b.parquet, a column `x` unless said otherwise).
`read <root> <out.json>` reads every directory with Polars (lossless upcasts; lossy casts too), DuckDB (first file's
types; united by name) and pyarrow (schemas unified permissively), and writes per case and engine
{"rows": [row, ...]} or {"error": message}; a row is its values rendered canonically and joined by '|'.
"""
import datetime as dt
import decimal
import json
import os
import shutil
import sys

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

D = decimal.Decimal
TS = dt.datetime(2024, 1, 2, 3, 4, 5, 123456)


def cases():
    struct = pa.struct
    return {
        'i32_then_i64': (pa.array([1], pa.int32()), pa.array([2], pa.int64())),
        'i64_then_i32': (pa.array([1], pa.int64()), pa.array([-2], pa.int32())),
        'i16_then_i8': (pa.array([1], pa.int16()), pa.array([-2], pa.int8())),
        'i64_then_u32': (pa.array([1], pa.int64()), pa.array([4294967295], pa.uint32())),
        'u64_then_u8': (pa.array([1], pa.uint64()), pa.array([255], pa.uint8())),
        'i32_then_u32': (pa.array([1], pa.int32()), pa.array([4294967295], pa.uint32())),
        'f64_then_i32': (pa.array([1.5], pa.float64()), pa.array([-2147483648], pa.int32())),
        'f64_then_i64': (pa.array([1.5], pa.float64()), pa.array([9007199254740993], pa.int64())),
        'f32_then_i16': (pa.array([1.5], pa.float32()), pa.array([2], pa.int16())),
        'f64_then_f32': (pa.array([1.5], pa.float64()), pa.array([0.1], pa.float32())),
        'f32_then_f64': (pa.array([1.5], pa.float32()), pa.array([0.1], pa.float64())),
        'f32_then_f16': (pa.array([1.5], pa.float32()), pa.array([2.5], pa.float16())),
        'd10_2_then_d5_2': (pa.array([D('1.25')], pa.decimal128(10, 2)), pa.array([D('-2.50')], pa.decimal128(5, 2))),
        'd10_3_then_d9_2': (pa.array([D('1.250')], pa.decimal128(10, 3)), pa.array([D('2.50')], pa.decimal128(9, 2))),
        'd10_3_then_d10_2': (pa.array([D('1.250')], pa.decimal128(10, 3)), pa.array([D('2.50')], pa.decimal128(10, 2))),
        'd12_2_then_i32': (pa.array([D('1.25')], pa.decimal128(12, 2)), pa.array([-7], pa.int32())),
        'd38_0_then_i64': (pa.array([D('1')], pa.decimal128(38, 0)), pa.array([-9223372036854775808], pa.int64())),
        'tsus_then_date': (pa.array([TS], pa.timestamp('us')), pa.array([dt.date(2024, 5, 6)], pa.date32())),
        'tsus_then_tsms': (pa.array([TS], pa.timestamp('us')), pa.array([TS], pa.timestamp('ms'))),
        'tsms_then_tsus': (pa.array([TS.replace(microsecond=123000)], pa.timestamp('ms')), pa.array([TS], pa.timestamp('us'))),
        'tsus_then_tsns': (pa.array([TS], pa.timestamp('us')), pa.array([TS], pa.timestamp('ns'))),
        'tsusutc_then_tsus': (pa.array([TS], pa.timestamp('us', 'UTC')), pa.array([TS], pa.timestamp('us'))),
        'tsus_then_tsusutc': (pa.array([TS], pa.timestamp('us')), pa.array([TS], pa.timestamp('us', 'UTC'))),
        'date_then_tsus': (pa.array([dt.date(2024, 5, 6)], pa.date32()), pa.array([TS], pa.timestamp('us'))),
        'timeus_then_timems': (pa.array([dt.time(1, 2, 3, 4000)], pa.time64('us')), pa.array([dt.time(1, 2, 3, 5000)], pa.time32('ms'))),
        'str_then_i64': (pa.array(['a'], pa.string()), pa.array([2], pa.int64())),
        'i64_then_str': (pa.array([1], pa.int64()), pa.array(['b'], pa.string())),
        'str_then_bin': (pa.array(['a'], pa.string()), pa.array([b'b'], pa.binary())),
        'bin_then_str': (pa.array([b'a'], pa.binary()), pa.array(['b'], pa.string())),
        'struct_missing_field': (pa.array([{'a': 1, 'b': 'x'}], struct([('a', pa.int64()), ('b', pa.string())])),
                                 pa.array([{'a': 2}], struct([('a', pa.int64())]))),
        'struct_extra_field': (pa.array([{'a': 1}], struct([('a', pa.int64())])),
                               pa.array([{'a': 2, 'c': 2.5}], struct([('a', pa.int64()), ('c', pa.float64())]))),
        'struct_field_i64_then_i32': (pa.array([{'a': 1}], struct([('a', pa.int64())])), pa.array([{'a': 2}], struct([('a', pa.int32())]))),
        'struct_order_differs': (pa.array([{'a': 1, 'b': 'x'}], struct([('a', pa.int64()), ('b', pa.string())])),
                                 pa.array([{'b': 'y', 'a': 2}], struct([('b', pa.string()), ('a', pa.int64())]))),
        'list_i64_then_i32': (pa.array([[1, None]], pa.list_(pa.int64())), pa.array([[2], None], pa.list_(pa.int32()))),
        'req_then_opt': (pa.array([1], pa.int64()), pa.array([None, 2], pa.int64())),
    }


def make(root):
    shutil.rmtree(root, ignore_errors=True)
    for name, (a, b) in cases().items():
        os.makedirs(f'{root}/{name}')
        nullable = name != 'req_then_opt'
        pq.write_table(pa.table({'x': a}, schema=pa.schema([pa.field('x', a.type, nullable=nullable)])), f'{root}/{name}/a.parquet')
        pq.write_table(pa.table({'x': b}), f'{root}/{name}/b.parquet')
    os.makedirs(f'{root}/union3')
    pq.write_table(pa.table({'x': pa.array([1], pa.int32())}), f'{root}/union3/a.parquet')
    pq.write_table(pa.table({'y': ['b'], 'x': pa.array([2], pa.int64())}), f'{root}/union3/b.parquet')
    pq.write_table(pa.table({'z': [3.5], 'y': ['c']}), f'{root}/union3/c.parquet')
    os.makedirs(f'{root}/union_conflict')
    pq.write_table(pa.table({'x': pa.array([1], pa.int64())}), f'{root}/union_conflict/a.parquet')
    pq.write_table(pa.table({'x': ['b']}), f'{root}/union_conflict/b.parquet')
    os.makedirs(f'{root}/union_struct')
    pq.write_table(pa.table({'s': pa.array([{'a': 1}], pa.struct([('a', pa.int32())]))}), f'{root}/union_struct/a.parquet')
    pq.write_table(pa.table({'s': pa.array([{'b': 'q', 'a': 2}], pa.struct([('b', pa.string()), ('a', pa.int64())]))}), f'{root}/union_struct/b.parquet')
    os.makedirs(f'{root}/extra_missing')
    pq.write_table(pa.table({'x': pa.array([1], pa.int64()), 'y': ['a']}), f'{root}/extra_missing/a.parquet')
    pq.write_table(pa.table({'z': [9.5], 'x': pa.array([2], pa.int32())}), f'{root}/extra_missing/b.parquet')


def canon(v):
    if v is None:
        return 'null'
    if isinstance(v, bool):
        return 'true' if v else 'false'
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, decimal.Decimal):
        return str(v)
    if isinstance(v, dt.datetime):
        return v.replace(tzinfo=None).isoformat(timespec='microseconds') + ('Z' if v.tzinfo is not None else '')
    if isinstance(v, (dt.date, dt.time)):
        return v.isoformat()
    if isinstance(v, bytes):
        return 'b:' + v.hex()
    if isinstance(v, dict):
        return '{' + ','.join(f'{k}:{canon(x)}' for k, x in v.items()) + '}'
    if isinstance(v, (list, tuple)):
        return '[' + ','.join(canon(x) for x in v) + ']'
    return str(v)


def run(f):
    try:
        return {'rows': ['|'.join(canon(v) for v in row) for row in f()]}
    except Exception as e:  # noqa: BLE001 - the error is reported
        return {'error': f'{type(e).__name__}: {str(e).splitlines()[0][:200]}'}


def read(root, out):
    import duckdb
    import polars as pl
    upcast = pl.ScanCastOptions(integer_cast='upcast', float_cast='upcast', datetime_cast='upcast',
                                missing_struct_fields='insert', extra_struct_fields='ignore')
    lossy = pl.ScanCastOptions(integer_cast=['upcast', 'allow-float'], float_cast=['upcast', 'downcast'], datetime_cast=['upcast', 'downcast'],
                               missing_struct_fields='insert', extra_struct_fields='ignore')

    def polars(path, options):
        df = pl.scan_parquet(path, cast_options=options, missing_columns='insert', extra_columns='ignore').collect()
        return df.iter_rows()

    def duck(path, union):
        rel = duckdb.sql(f"select * from read_parquet('{path}', union_by_name={'true' if union else 'false'})")
        return rel.fetchall()

    def unified(path):
        files = sorted(os.listdir(path))
        schema = pa.unify_schemas([pq.read_schema(f'{path}/{f}') for f in files], promote_options='permissive')
        table = ds.dataset(path, schema=schema).to_table()
        return [tuple(r.values()) for r in table.to_pylist()]

    result = {}
    for name in sorted(os.listdir(root)):
        glob = f'{root}/{name}/*.parquet'
        result[name] = {
            'polars_upcast': run(lambda: polars(glob, upcast)),
            'polars_lossy': run(lambda: polars(glob, lossy)),
            'duckdb_first': run(lambda: duck(glob, False)),
            'duckdb_union': run(lambda: duck(glob, True)),
            'pyarrow_unify': run(lambda: unified(f'{root}/{name}')),
        }
    json.dump(result, open(out, 'w'), indent=1)


if __name__ == '__main__':
    if sys.argv[1] == 'make':
        make(sys.argv[2])
    else:
        read(sys.argv[2], sys.argv[3])
