"""Delta interop with Spark for the Seseragi kernel tests.

  read <table>...   print, per table, one JSON line: {"path", "schema", "rows"} as Spark reads it
  write <dir>       create Spark tables that use Delta features under <dir>, then read them as above
  append <table> n  append n rows, one commit each, then read the table as above
  create_v2 <table> create an empty table with V2 checkpoints every two commits
  serve             read commands as JSON arrays from stdin, one per line, with one Spark session for all

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
from pyspark.sql.types import VariantVal

EPOCH_DAY = datetime.date(1970, 1, 1)
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


# Unity Catalog's Spark connector, for catalog-managed tables (matches Spark 4.2 and the UC 0.7 server image).
UC_PACKAGE = "io.unitycatalog:unitycatalog-spark_4.2_2.13:0.7.0"


def session():
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("seseragi-interop")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        # Delta rebuilds the table state in 50 partitions by default; with two cores and test-sized logs that is
        # most of the time of every statement.
        .config("spark.databricks.delta.snapshotPartitions", "2")
        # VACUUM ... RETAIN 0 HOURS in the tests.
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder, extra_packages=[UC_PACKAGE]).getOrCreate()
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
    if isinstance(value, VariantVal):
        # A variant as Spark's to_json renders it.
        return {"variant": value.toJson()}
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
    read_changes(spark, tables[0])


def read_changes(spark, path, start=0, end=None):
    """The change feed from version start (0 by default) to end (the latest by default), without the commit time."""
    reader = spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", start)
    if end is not None:
        reader = reader.option("endingVersion", end)
    df = reader.load(path).drop("_commit_timestamp")
    rows = [canonical(r) for r in df.collect()]
    rows.sort(key=lambda r: (r["_commit_version"], r["id"], r["_change_type"]))
    print(json.dumps({"path": path, "changes": True, "rows": rows}, ensure_ascii=False), flush=True)


def row_ids(spark, path):
    """Each row's id with its row id and row commit version (row tracking), by id."""
    df = spark.read.format("delta").load(path).select("id", "_metadata.row_id", "_metadata.row_commit_version")
    rows = sorted((canonical(r) for r in df.collect()), key=lambda r: r["id"])
    print(json.dumps({"path": path, "rows": rows}), flush=True)


def run(spark, command, args):
    if command == "read":
        for path in args:
            read(spark, path)
    elif command == "write":
        write(spark, args[0])
    elif command == "read_changes":
        # Each table, then its change feed.
        for path in args:
            read(spark, path)
            read_changes(spark, path)
    elif command == "read_changes_from":
        # The table, then its change feed from the given version.
        path, start = args[0], int(args[1])
        read(spark, path)
        read_changes(spark, path, start)
    elif command == "checkpoint_adds":
        # The add actions of a classic checkpoint file: the add column's schema, and per file its typed statistics
        # and partition values as JSON (by path).
        f = args[0]
        df = spark.read.parquet(f).where("add is not null")
        add_type = [x for x in df.schema.fields if x.name == "add"][0].dataType
        names = add_type.fieldNames()
        cols = ["add.path"] + [f"to_json(add.{c}) as {c}" for c in ("stats_parsed", "partitionValues_parsed") if c in names]
        rows = {r["path"]: {k: r[k] for k in r.asDict() if k != "path"} for r in df.selectExpr(*cols).collect()}
        print(json.dumps({"schema": add_type.simpleString(), "adds": rows}, ensure_ascii=False), flush=True)
    elif command == "read_changes_range":
        # The change feed from start to end only; a refused range prints its error class instead of failing.
        path, start, end = args[0], int(args[1]), int(args[2])
        try:
            read_changes(spark, path, start, end)
        except Exception as e:
            text = str(e)
            kind = next((k for k in ("DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE", "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA") if k in text), None)
            if kind is None:
                raise
            print(json.dumps({"path": path, "refused": kind}), flush=True)
    elif command == "delete":
        # DELETE FROM the table WHERE the given SQL condition; then the table.
        path, condition = args[0], args[1]
        spark.sql(f"DELETE FROM delta.`{path}` WHERE {condition}")
        read(spark, path)
    elif command == "update":
        # UPDATE the table SET the given assignments WHERE the given SQL condition; then the table.
        path, assignments, condition = args[0], args[1], args[2]
        spark.sql(f"UPDATE delta.`{path}` SET {assignments} WHERE {condition}")
        read(spark, path)
    elif command == "query":
        # The rows of a query, {table} standing for the table.
        path, statement = args[0], args[1]
        rows = [canonical(r) for r in spark.sql(statement.replace("{table}", f"delta.`{path}`")).collect()]
        print(json.dumps({"path": path, "rows": rows}, ensure_ascii=False), flush=True)
    elif command == "sql":
        # Runs a statement, {table} standing for the table; then the table.
        path, statement = args[0], args[1]
        spark.sql(statement.replace("{table}", f"delta.`{path}`"))
        read(spark, path)
    elif command == "optimize_cubes":
        # OPTIMIZE (FULL when the last argument is "full") with the given small-cube and target cube sizes, put back
        # afterwards: the session serves every test.
        path, threshold, target = args[0], args[1], args[2]
        full = len(args) > 3 and args[3] == "full"
        prefix = "spark.databricks.delta.optimize.clustering.mergeStrategy.minCubeSize."
        spark.conf.set(prefix + "threshold", threshold)
        spark.conf.set(prefix + "targetCubeSize", target)
        try:
            spark.sql(f"OPTIMIZE delta.`{path}`" + (" FULL" if full else ""))
        finally:
            spark.conf.unset(prefix + "threshold")
            spark.conf.unset(prefix + "targetCubeSize")
    elif command == "uc_sql":
        # Statements against a Unity Catalog server: [uri, catalog, statement...]; the catalog is registered in the
        # session under its own name the first time.
        uri, catalog, *statements = args
        uc_catalog(spark, uri, catalog)
        for statement in statements:
            spark.sql(statement)
    elif command == "uc_query":
        # The rows of a query against a Unity Catalog server: [uri, catalog, statement].
        uri, catalog, statement = args
        uc_catalog(spark, uri, catalog)
        rows = [canonical(r) for r in spark.sql(statement).collect()]
        print(json.dumps({"rows": rows}, ensure_ascii=False), flush=True)
    elif command == "create_widened":
        # A table whose columns were widened after a first insert, so its first file holds the older types.
        path = args[0]
        spark.sql(
            f"CREATE TABLE delta.`{path}` (id INT, b BYTE, f FLOAT, d DATE, m DECIMAL(5,2), s STRUCT<x: SHORT>, arr ARRAY<INT>) "
            "USING delta TBLPROPERTIES ('delta.enableTypeWidening'='true', 'delta.feature.timestampNtz'='supported')"
        )
        spark.sql(
            # One file holding both rows.
            f"INSERT INTO delta.`{path}` SELECT /*+ COALESCE(1) */ * FROM VALUES "
            "(1, 1, 0.1, DATE'2024-01-02', 1.25, named_struct('x', 3), array(1, 2)), "
            "(2, -5, 0.05, DATE'2024-03-04', -99.99, NULL, NULL)"
        )
        for column, target in [("id", "BIGINT"), ("b", "INT"), ("f", "DOUBLE"), ("d", "TIMESTAMP_NTZ"),
                               ("m", "DECIMAL(10,4)"), ("s.x", "INT"), ("arr.element", "BIGINT")]:
            spark.sql(f"ALTER TABLE delta.`{path}` CHANGE COLUMN {column} TYPE {target}")
        spark.sql(
            f"INSERT INTO delta.`{path}` VALUES "
            "(3, 300000, 0.2, TIMESTAMP_NTZ'2024-05-06 07:08:09', 123456.7891, named_struct('x', 70000), array(5000000000))"
        )
        read(spark, path)
    elif command == "row_ids":
        for path in args:
            row_ids(spark, path)
    elif command == "create_identity":
        # A table (id BIGINT identity, v STRING): mode "always" or "default", start, step. Path tables take identity
        # columns only through the builder.
        from delta.tables import DeltaTable, IdentityGenerator
        path, mode, start, step = args[0], args[1], int(args[2]), int(args[3])
        generator = IdentityGenerator(start=start, step=step)
        builder = DeltaTable.create(spark).location(path)
        if mode == "always":
            builder = builder.addColumn("id", dataType="BIGINT", generatedAlwaysAs=generator)
        else:
            builder = builder.addColumn("id", dataType="BIGINT", generatedByDefaultAs=generator)
        builder.addColumn("v", "STRING").execute()
        read(spark, path)
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
        raise ValueError(f"unknown command {command}")


def uc_catalog(spark, uri, name):
    """Registers the Unity Catalog catalog `name` of the server at `uri` (no token) in the session, once."""
    key = f"spark.sql.catalog.{name}"
    if spark.conf.get(key, None) is None:
        spark.conf.set(key, "io.unitycatalog.spark.UCSingleCatalog")
        spark.conf.set(key + ".uri", uri)
        spark.conf.set(key + ".token", "")


def serve(spark):
    """One command per input line, as a JSON array [command, args...]. Its result lines follow, then a line
    {"done": true}, or {"error": "..."} when it failed. One session serves every command, so the JVM, the Spark
    context and Delta start once."""
    jvm_delta_log = spark._jvm.org.apache.spark.sql.delta.DeltaLog
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            command, *args = json.loads(line)
            # Tables change behind Spark's back between commands (other writers, deleted log files): start fresh.
            jvm_delta_log.clearCache()
            spark.catalog.clearCache()
            run(spark, command, args)
            print(json.dumps({"done": True}), flush=True)
        except Exception as e:  # report and keep serving
            import traceback
            print(json.dumps({"error": f"{type(e).__name__}: {e}", "trace": traceback.format_exc()[-4000:]}), flush=True)


def main():
    command, args = sys.argv[1], sys.argv[2:]
    spark = session()
    if command == "serve":
        serve(spark)
    else:
        run(spark, command, args)


if __name__ == "__main__":
    main()
