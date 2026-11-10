"""Delta interop with Spark for the Seseragi kernel tests.

  read <table>...   print, per table, one JSON line: {"path", "schema", "rows"} as Spark reads it
  write <dir>       create Spark tables that use Delta features under <dir>, then read them as above
  append <table> n  append n rows, one commit each, then read the table as above
  create_v2 <table> create an empty table with V2 checkpoints every two commits

Values are canonical so that another engine can compare: dates as days since the epoch, timestamps as
microseconds since the epoch (UTC), decimals as text, binary as base64, structs as objects, arrays as lists.
Rows are sorted by the first column.
"""
import base64
import datetime
import decimal
import json
import os
import sys
import time

# pyspark hands timestamps over as naive datetimes in the Python process's local zone (whatever the session time
# zone). Make that zone UTC so a naive value is UTC.
os.environ["TZ"] = "UTC"
time.tzset()

from delta import configure_spark_with_delta_pip
from pyspark.sql import Row, SparkSession

EPOCH_DAY = datetime.date(1970, 1, 1)
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def session():
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("seseragi-interop")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def canonical(value):
    if value is None:
        return None
    if isinstance(value, Row):
        return {k: canonical(v) for k, v in value.asDict().items()}
    if isinstance(value, dict):
        return [{"key": canonical(k), "value": canonical(v)} for k, v in value.items()]
    if isinstance(value, (list, tuple)):
        return [canonical(v) for v in value]
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:  # timestamp_ntz, or a session-zone timestamp: UTC here
            value = value.replace(tzinfo=datetime.timezone.utc)
        delta = value - EPOCH
        return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
    if isinstance(value, datetime.date):
        return (value - EPOCH_DAY).days
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode()
    return value


def read(spark, path):
    df = spark.read.format("delta").load(path)
    rows = [canonical(r) for r in df.collect()]
    first = df.columns[0]
    rows.sort(key=lambda r: (r[first] is None, r[first]))
    print(json.dumps({"path": path, "schema": df.schema.simpleString(), "rows": rows}, ensure_ascii=False), flush=True)


def write(spark, root):
    """Tables that use deletion vectors, the change data feed, checkpoints and column mapping."""
    tables = []

    # Deletion vectors and the change data feed; deletes and an update leave vectors and cdc files.
    path = os.path.join(root, "dv_cdf")
    spark.sql(
        f"CREATE TABLE delta.`{path}` (id BIGINT, region STRING, amount DECIMAL(10,2), day DATE, at TIMESTAMP) "
        "USING delta PARTITIONED BY (region) "
        "TBLPROPERTIES ('delta.enableDeletionVectors'='true', 'delta.enableChangeDataFeed'='true')"
    )
    spark.sql(
        f"INSERT INTO delta.`{path}` SELECT id, IF(id % 2 = 0, 'east', 'west'), CAST(id AS DECIMAL(10,2)) / 4, "
        "DATE_ADD(DATE'2024-01-01', CAST(id AS INT)), TIMESTAMP'2024-01-01 00:00:00' + MAKE_INTERVAL(0,0,0,0,0,0,id * 1.5) "
        "FROM RANGE(1, 41)"
    )
    spark.sql(f"DELETE FROM delta.`{path}` WHERE id % 3 = 0")
    spark.sql(f"UPDATE delta.`{path}` SET amount = amount + 100 WHERE id = 4")
    tables.append(path)

    # Checkpoints every two commits, so the log has to be read through one.
    path = os.path.join(root, "checkpointed")
    spark.sql(
        f"CREATE TABLE delta.`{path}` (id BIGINT, name STRING) USING delta "
        "TBLPROPERTIES ('delta.checkpointInterval'='2')"
    )
    for i in range(5):
        spark.sql(f"INSERT INTO delta.`{path}` VALUES ({i}, 'n{i}'), ({i + 100}, NULL)")
    tables.append(path)

    # Column mapping by name with a nested struct, a renamed column and a renamed nested field.
    path = os.path.join(root, "column_mapping")
    spark.sql(
        f"CREATE TABLE delta.`{path}` (id BIGINT, info STRUCT<city: STRING, zip: INT>, tags ARRAY<STRING>) USING delta "
        "TBLPROPERTIES ('delta.columnMapping.mode'='name', 'delta.minReaderVersion'='2', 'delta.minWriterVersion'='5')"
    )
    spark.sql(f"INSERT INTO delta.`{path}` VALUES (1, named_struct('city', 'Kyoto', 'zip', 600), array('a', 'b')), (2, NULL, NULL)")
    spark.sql(f"ALTER TABLE delta.`{path}` RENAME COLUMN info TO place")
    spark.sql(f"ALTER TABLE delta.`{path}` RENAME COLUMN place.city TO town")
    spark.sql(f"INSERT INTO delta.`{path}` VALUES (3, named_struct('town', 'Nara', 'zip', 630), array())")
    tables.append(path)

    for table in tables:
        read(spark, table)

    # The change feed of the table with deletion vectors and cdc files, without the commit time.
    path = tables[0]
    df = (spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", 0).load(path)
          .drop("_commit_timestamp"))
    rows = [canonical(r) for r in df.collect()]
    rows.sort(key=lambda r: (r["_commit_version"], r["id"], r["_change_type"]))
    print(json.dumps({"path": path, "changes": True, "rows": rows}, ensure_ascii=False), flush=True)


def main():
    command, args = sys.argv[1], sys.argv[2:]
    spark = session()
    if command == "read":
        for path in args:
            read(spark, path)
    elif command == "write":
        write(spark, args[0])
    elif command == "create_v2":
        # A table that takes V2 checkpoints every two commits; columns (id BIGINT, src STRING) as "append" writes.
        spark.sql(
            f"CREATE TABLE delta.`{args[0]}` (id BIGINT, src STRING) USING delta "
            "TBLPROPERTIES ('delta.checkpointPolicy'='v2', 'delta.checkpointInterval'='2')"
        )
        read(spark, args[0])
    elif command == "append":
        # Appends one row per commit, while another writer commits to the same table.
        path, count = args[0], int(args[1])
        for i in range(count):
            spark.sql(f"INSERT INTO delta.`{path}` VALUES ({1000 + i}, 'spark')")
        read(spark, path)
    else:
        raise SystemExit(f"unknown command {command}")


if __name__ == "__main__":
    main()
