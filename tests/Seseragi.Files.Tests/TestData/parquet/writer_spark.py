"""Reads the Parquet files ParquetWriterInteropTests wrote for Spark, with the vectorized reader and without.

Usage: JAVA_HOME=<jdk 21> python writer_spark.py <dir>   (pyspark 4.2)

- Every spark_*.parquet is read and written back by Spark (one file, microsecond timestamps) as
  <name>.spark-<vectorized|rows>/ for the C# test to read with our reader and compare with the values written.
- sorted.parquet is filtered at values from its columns with Parquet filter pushdown and without: the row counts must
  agree, or the bounds skipped rows. Mismatches are printed and fail the script.
"""
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
    for mode, vectorized in [("vectorized", "true"), ("rows", "false")]:
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

for m in mismatches:
    print("MISMATCH", m)
print("spark done" if not mismatches else "spark FAILED")
sys.exit(1 if mismatches else 0)
