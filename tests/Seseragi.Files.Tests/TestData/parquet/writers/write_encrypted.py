"""Writes Parquet files encrypted with Parquet modular encryption, each next to the same table unencrypted
(plain.parquet), so a reader can be checked against its own plain read.

- pyarrow 26 (explicit keys: uniform encryption only): AES_GCM_V1 and AES_GCM_CTR_V1, 16/24/32-byte keys, data pages v1 and v2, an AAD
  prefix stored or to be supplied, plaintext footers; two row groups, pages of at most 100 rows, and a page index.
- Polars 2.2 (AES_GCM_V1 only, 16/32-byte keys): columns with keys of their own, a nested one among them, with an
  encrypted and a plaintext footer.

Keys (ASCII): footer 16 B "0123456789012345", 24 B "012345678901234567890123", 32 B "01234567890123456789012345678901";
column "i64" "1234567890123450", column "s.a" "1234567890123451". AAD prefix "table-7".

Usage: python -I write_encrypted.py <output directory>
"""
import os
import sys
from datetime import date, datetime, timedelta

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe

out = sys.argv[1]
os.makedirs(out, exist_ok=True)
N = 1200

K16 = b"0123456789012345"
K24 = b"012345678901234567890123"
K32 = b"01234567890123456789012345678901"
COL_I64 = b"1234567890123450"
COL_SA = b"1234567890123451"
AAD = b"table-7"

table = pa.table({
    "i32": pa.array([None if i % 17 == 3 else i - 1500 for i in range(N)], pa.int32()),
    "i64": pa.array([None if i % 13 == 5 else i * 1_000_003 for i in range(N)], pa.int64()),
    "f64": pa.array([None if i % 11 == 7 else i / 7 for i in range(N)], pa.float64()),
    "b": pa.array([None if i % 19 == 2 else i % 3 == 0 for i in range(N)], pa.bool_()),
    "str": pa.array([None if i % 23 == 4 else f"value-{i % 97}" for i in range(N)], pa.string()),
    "d": pa.array([None if i % 29 == 1 else date(2020, 1, 1) + timedelta(days=i) for i in range(N)], pa.date32()),
    "ts": pa.array([None if i % 31 == 6 else datetime(2021, 5, 1) + timedelta(seconds=i * 61) for i in range(N)], pa.timestamp("us")),
    "lst": pa.array([None if i % 37 == 8 else list(range(i % 5)) for i in range(N)], pa.list_(pa.int32())),
    "s": pa.array([None if i % 41 == 9 else {"a": i, "b": None if i % 3 == 0 else f"b{i % 7}"} for i in range(N)],
                  pa.struct([("a", pa.int64()), ("b", pa.string())])),
})

# pyarrow writes no Bloom filters with encryption ("not currently supported"); the Apache fixture
# encrypt_columns_and_footer_bloom_filter has encrypted ones.
common = dict(row_group_size=N // 2, data_page_size=4096, max_rows_per_page=100, write_page_index=True)

pq.write_table(table, os.path.join(out, "plain.parquet"), **common)


def pyarrow_file(name, key, data_page_version="1.0", **props):
    pq.write_table(table, os.path.join(out, name), encryption_properties=pe.create_encryption_properties(key, **props),
                   data_page_version=data_page_version, **common)


pyarrow_file("pa_gcm128.parquet", K16)
pyarrow_file("pa_gcm128_v2.parquet", K16, data_page_version="2.0")
pyarrow_file("pa_ctr128.parquet", K16, encryption_algorithm="AES_GCM_CTR_V1")
pyarrow_file("pa_gcm192.parquet", K24)
pyarrow_file("pa_ctr256.parquet", K32, encryption_algorithm="AES_GCM_CTR_V1")
pyarrow_file("pa_gcm256_aad_stored.parquet", K32, aad_prefix=AAD, store_aad_prefix=True)
pyarrow_file("pa_gcm128_aad_supplied.parquet", K16, aad_prefix=AAD, store_aad_prefix=False)
pyarrow_file("pa_gcm128_plaintext_footer.parquet", K16, plaintext_footer=True)
pyarrow_file("pa_ctr128_plaintext_footer.parquet", K16, encryption_algorithm="AES_GCM_CTR_V1", plaintext_footer=True)

frame = pl.from_arrow(table)
column_keys = {"i64": COL_I64, "s.a": COL_SA}
frame.write_parquet(os.path.join(out, "pl_columns.parquet"), row_group_size=N // 2, data_page_size=4096,
                    encryption=pl.ParquetEncryptionProperties(footer_key=K16, column_keys=column_keys))
frame.write_parquet(os.path.join(out, "pl_columns_plaintext_footer.parquet"), row_group_size=N // 2, data_page_size=4096,
                    encryption=pl.ParquetEncryptionProperties(footer_key=K16, column_keys=column_keys, plaintext_footer=True))

# Every file reads back as the plain table with the keys it was written with.
for name in sorted(os.listdir(out)):
    if not name.endswith(".parquet") or name == "plain.parquet":
        continue
    path = os.path.join(out, name)
    if name.startswith("pa_"):
        key = K24 if "192" in name else K32 if "256" in name else K16
        aad = AAD if "aad_supplied" in name else None
        back = pq.read_table(path, decryption_properties=pe.create_decryption_properties(key, aad_prefix=aad))
    else:
        back = pl.read_parquet(path, decryption=pl.ParquetDecryptionProperties(footer_key=K16, column_keys=column_keys)).to_arrow()
    assert back.to_pylist() == table.to_pylist(), name
    print("ok", name)
