"""Writes the probe table with Spark: default settings, TIMESTAMP_MICROS, and a Delta table."""
import datetime as dt, decimal, glob, shutil, sys
from pyspark.sql import SparkSession, types as T
from delta import configure_spark_with_delta_pip

out = sys.argv[1]
b = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
     .config("spark.sql.session.timeZone", "UTC"))
spark = configure_spark_with_delta_pip(b).getOrCreate()
print("outputTimestampType default:", spark.conf.get("spark.sql.parquet.outputTimestampType"))
schema = T.StructType([
    T.StructField("ts", T.TimestampType()),
    T.StructField("ts_ntz", T.TimestampNTZType()),
    T.StructField("date", T.DateType()),
    T.StructField("dec", T.DecimalType(5, 2)),
    T.StructField("dec_big", T.DecimalType(30, 4)),
    T.StructField("str", T.StringType()),
    T.StructField("bin", T.BinaryType()),
    T.StructField("arr", T.ArrayType(T.IntegerType())),
    T.StructField("map", T.MapType(T.StringType(), T.IntegerType())),
    T.StructField("st", T.StructType([T.StructField("x", T.IntegerType()), T.StructField("y", T.StringType())])),
    T.StructField("dti", T.DayTimeIntervalType()),
])
ts = dt.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=dt.timezone.utc)
rows = [
    (ts, ts.replace(tzinfo=None), dt.date(2024, 1, 2), decimal.Decimal("1.23"), decimal.Decimal("12345678901234567890.1234"), "red", b"\x01", [1, 2], {"k": 1}, (1, "a"), dt.timedelta(seconds=90)),
    (None, None, None, None, None, None, None, None, None, None, None),
    (dt.datetime(1, 1, 1, tzinfo=dt.timezone.utc), dt.datetime(9999, 12, 31, 23, 59, 59), dt.date(1, 1, 1), decimal.Decimal("-9.99"), decimal.Decimal("0"), "red", b"", [], {}, (None, "c"), dt.timedelta(days=-1)),
]
df = spark.createDataFrame(rows, schema).coalesce(1)

def single(path, name):
    f = glob.glob(f"{path}/*.parquet")[0]
    shutil.copy(f, f"{out}/{name}")

for name, conf in [("spark_default.parquet", None), ("spark_micros.parquet", "TIMESTAMP_MICROS"), ("spark_int96.parquet", "INT96")]:
    if conf: spark.conf.set("spark.sql.parquet.outputTimestampType", conf)
    df.write.mode("overwrite").parquet(f"{out}/tmp_{name}")
    single(f"{out}/tmp_{name}", name)
spark.conf.unset("spark.sql.parquet.outputTimestampType")
df.drop("dti").write.format("delta").mode("overwrite").save(f"{out}/tmp_delta")
single(f"{out}/tmp_delta", "spark_delta.parquet")
for d in glob.glob(f"{out}/tmp_*"): shutil.rmtree(d)
print("written")
