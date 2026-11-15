"""Reads the Parquet files ParquetWriterInteropTests wrote for Spark, with the vectorized reader and without.

Usage: JAVA_HOME=<jdk 21> python writer_spark.py <dir>   (pyspark 4.2)

- Every spark_*.parquet is read and written back by Spark (one file, microsecond timestamps) as
  <name>.spark-<vectorized|rows>/ for the C# test to read with our reader and compare with the values written;
  spark_rows_*.parquet (BYTE_STREAM_SPLIT, which the vectorized reader cannot read) with the row reader only.
- sorted*.parquet carry page indexes (column indexes Spark skips pages by), spark_bloom.parquet Bloom filters (Spark
  skips row groups by them): equalities at values present and absent must keep the same rows with pushdown and without.
- sorted.parquet is filtered at values from its columns with Parquet filter pushdown and without: the row counts must
  agree, or the bounds skipped rows. Mismatches are printed and fail the script.
"""
import decimal
import glob
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

directory = sys.argv[1]
spark = (SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
         .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED").config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
         .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED").config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

for path in sorted(glob.glob(os.path.join(directory, "spark_*.parquet"))):
    name = os.path.basename(path)[: -len(".parquet")]
    modes = [("rows", "false")] if name.startswith("spark_rows_") else [("vectorized", "true"), ("rows", "false")]
    for mode, vectorized in modes:
        spark.conf.set("spark.sql.parquet.enableVectorizedReader", vectorized)
        spark.read.parquet(path).coalesce(1).write.mode("overwrite").parquet(os.path.join(directory, f"{name}.spark-{mode}"))

mismatches = []
for sorted_path in [os.path.join(directory, n) for n in ("sorted.parquet", "sorted_nonnull.parquet")]:
    if not os.path.exists(sorted_path):
        continue
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
    frame = spark.read.parquet(sorted_path)
    for column in ["i32", "u32", "u64", "f64", "dec30", "dec5", "str", "day"]:
        values = sorted({r[0] for r in frame.select(column).where(F.col(column).isNotNull() & ~F.isnan(F.col(column)) if column == "f64" else F.col(column).isNotNull()).collect()})
        picks = {values[i] for i in [0, 1, len(values) // 8, len(values) // 4, len(values) // 2, -2, -1]}
        for value in sorted(picks):
            for op in ["=", "<", "<=", ">", ">="]:
                condition = {"=": F.col(column) == value, "<": F.col(column) < value, "<=": F.col(column) <= value,
                             ">": F.col(column) > value, ">=": F.col(column) >= value}[op]
                counts = []
                for pushdown in ["true", "false"]:
                    spark.conf.set("spark.sql.parquet.filterPushdown", pushdown)
                    counts.append(spark.read.parquet(sorted_path).where(condition).count())
                if counts[0] != counts[1]:
                    mismatches.append(f"spark: {column} {op} {value!r}: {counts[0]} rows with pushdown, {counts[1]} without")
    spark.conf.set("spark.sql.parquet.filterPushdown", "true")

# Bloom filters: equalities at present values (in one row group or a few) and absent ones, pushdown against none.
bloom_path = os.path.join(directory, "spark_bloom.parquet")
if os.path.exists(bloom_path):
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
    frame = spark.read.parquet(bloom_path)
    for column in ["i32", "i64", "str", "bin", "dec15", "dict_str"]:
        present = [r[0] for r in frame.select(column).where(F.col(column).isNotNull()).distinct().limit(25).collect()]
        absent = {"i32": [1_234_567_891], "i64": [987_654_321_987], "str": ["\u2603 no such value"], "bin": [bytearray(b"\xfe\xfe\xfe no")],
                  "dec15": [decimal.Decimal("123456789.123")], "dict_str": ["purple"]}[column]
        for value in present + absent:
            counts = []
            for pushdown in ["true", "false"]:
                spark.conf.set("spark.sql.parquet.filterPushdown", pushdown)
                counts.append(spark.read.parquet(bloom_path).where(F.col(column) == F.lit(value)).count())
            if counts[0] != counts[1]:
                mismatches.append(f"spark bloom: {column} = {value!r}: {counts[0]} rows with pushdown, {counts[1]} without")
    spark.conf.set("spark.sql.parquet.filterPushdown", "true")

for m in mismatches:
    print("MISMATCH", m)
print("spark done" if not mismatches else "spark FAILED")
sys.exit(1 if mismatches else 0)
