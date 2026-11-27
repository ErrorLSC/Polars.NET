"""Reads the encrypted files ParquetEncryptionWriterTests wrote (listed in manifest.json, next to plain.parquet) with
pyarrow 26 (explicit keys: uniform encryption only) and Polars 2.2 (AES_GCM_V1 with 16- or 32-byte keys), and checks
each against plain.parquet. Prints "ok <reader> <file>", "skip <reader> <file>: <why>" or "FAIL <reader> <file>: <error>".

Usage: python -I encrypted_interop.py <directory>
"""
import json
import os
import sys

import polars as pl
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe

directory = sys.argv[1]
plain = pq.read_table(os.path.join(directory, "plain.parquet"))
expected = plain.to_pylist()
manifest = json.load(open(os.path.join(directory, "manifest.json")))


def key(hex_text):
    return bytes.fromhex(hex_text) if hex_text else None


for entry in manifest:
    name = entry["file"]
    path = os.path.join(directory, name)
    footer, aad, columns = key(entry["footer_key"]), key(entry["aad_prefix"]), entry["column_keys"]

    if columns:
        print(f"skip pyarrow {name}: explicit keys are uniform only")
    else:
        try:
            got = pq.read_table(path, decryption_properties=pe.create_decryption_properties(footer, aad_prefix=aad)).to_pylist()
            print(f"ok pyarrow {name}" if got == expected else f"FAIL pyarrow {name}: values differ")
        except Exception as e:  # noqa: BLE001 - every error is reported
            print(f"FAIL pyarrow {name}: {e}")

    if entry["ctr"] or entry["key_bytes"] == 24:
        print(f"skip polars {name}: Polars 2.2 reads AES_GCM_V1 with 16- or 32-byte keys only")
        continue
    try:
        decryption = pl.ParquetDecryptionProperties(
            footer_key=footer,
            column_keys={k: bytes.fromhex(v) for k, v in (columns or {}).items()} or None,
            aad_prefix=aad,
        )
        got = pl.read_parquet(path, decryption=decryption).to_arrow().to_pylist()
        print(f"ok polars {name}" if got == expected else f"FAIL polars {name}: values differ")
    except Exception as e:  # noqa: BLE001
        print(f"FAIL polars {name}: {e}")
