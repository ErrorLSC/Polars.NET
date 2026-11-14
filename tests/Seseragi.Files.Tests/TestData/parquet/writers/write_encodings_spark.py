"""Writes Spark's (parquet-mr) v2 encodings: DELTA_BINARY_PACKED integers, decimals, dates and timestamps,
DELTA_BYTE_ARRAY strings, binaries and wide decimals.

Usage: JAVA_HOME=<jdk 21> <spark venv>/bin/python write_encodings_spark.py <output directory>
"""
import datetime as dt, decimal, glob, shutil, sys
from pyspark.sql import SparkSession, types as T

out = sys.argv[1]
schema = T.StructType([
    T.StructField("i", T.IntegerType()),
    T.StructField("l", T.LongType()),
    T.StructField("s", T.StringType()),
    T.StructField("b", T.BinaryType()),
    T.StructField("d32", T.DecimalType(9, 2)),
    T.StructField("d64", T.DecimalType(18, 3)),
    T.StructField("dbig", T.DecimalType(30, 4)),
    T.StructField("date", T.DateType()),
    T.StructField("ts", T.TimestampType()),
    T.StructField("f", T.DoubleType()),
])
base = dt.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=dt.timezone.utc)
rows = [
    None if n % 17 == 5 else (
        (n * 2654435761) % 2**31 - 2**30 if n % 9 else (2**31 - 1 if n % 2 else -2**31),
        (n * 6364136223846793005) % 2**64 - 2**63,
        f"customer_{n // 13:06d}_{n % 13}" if n % 5 else "",
        bytes([(n + k) % 256 for k in range(n % 11)]),
        decimal.Decimal(n * 12345 % 10**9 - 5 * 10**8).scaleb(-2),
        decimal.Decimal(n * 1234567891 % 10**18 - 5 * 10**17).scaleb(-3),
        decimal.Decimal(n * 98765432123456789 % 10**30 - 5 * 10**29).scaleb(-4),
        dt.date(2024, 1, 1) + dt.timedelta(days=(n - 1500) * 7),
        base + dt.timedelta(minutes=n * 97),
        n / 7.0,
    )
    for n in range(3000)
]
rows = [r if r is not None else (None,) * len(schema) for r in rows]

spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
         .getOrCreate())
df = spark.createDataFrame(rows, schema).coalesce(1)
# parquet-mr settings go per write (options reach its output format; session-wide ones stick to the first context).
path = f"{out}/tmp_enc_spark_v2"
(df.write.mode("overwrite").option("compression", "snappy").option("parquet.writer.version", "v2")
 .option("parquet.enable.dictionary", "false").parquet(path))
shutil.copy(glob.glob(f"{path}/*.parquet")[0], f"{out}/enc_spark_v2.parquet")
shutil.rmtree(path)
spark.stop()
print("written")
