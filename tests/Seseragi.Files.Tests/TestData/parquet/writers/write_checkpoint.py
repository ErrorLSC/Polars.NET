"""Writes Delta checkpoints with Spark and delta-spark (classic, and V2 with a sidecar), plus a Parquet file with a
VARIANT column, for the nested reading tests.

Usage (spark venv, JAVA_HOME = a JDK 21): python -I write_checkpoint.py <output directory>
"""
import glob
import os
import shutil
import sys
import tempfile

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

out = sys.argv[1]
b = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
     .config("spark.sql.session.timeZone", "UTC")
     .config("spark.databricks.delta.snapshotPartitions", "2"))
spark = configure_spark_with_delta_pip(b).getOrCreate()
work = tempfile.mkdtemp()


def table(name, properties):
    path = os.path.join(work, name)
    spark.sql(f"""CREATE TABLE delta.`{path}` (id BIGINT, part STRING, tags ARRAY<STRING>, attrs MAP<STRING, INT>,
                  info STRUCT<a: INT, b: STRING>) USING delta PARTITIONED BY (part)
                  TBLPROPERTIES ({properties})""")
    for k in range(4):
        spark.sql(f"""INSERT INTO delta.`{path}` VALUES
                      ({k}, 'p{k % 2}', array('t{k}', NULL), map('x', {k}), named_struct('a', {k}, 'b', 's{k}')),
                      ({k + 100}, 'p{k % 2}', NULL, NULL, NULL)""")
    spark.sql(f"DELETE FROM delta.`{path}` WHERE id = 1")
    spark.sql(f"UPDATE delta.`{path}` SET tags = array('u') WHERE id = 2")
    return path


classic = table("classic", "'delta.checkpointInterval' = '3', 'delta.enableDeletionVectors' = 'true'")
v2 = table("v2", "'delta.checkpointInterval' = '3', 'delta.checkpointPolicy' = 'v2', 'delta.enableDeletionVectors' = 'true'")
spark.sql(f"ALTER TABLE delta.`{v2}` SET TBLPROPERTIES ('delta.checkpoint.writeStatsAsStruct' = 'true')")
spark.sql(f"INSERT INTO delta.`{v2}` VALUES (9, 'p1', array(), map(), named_struct('a', NULL, 'b', NULL))")
spark.sql(f"INSERT INTO delta.`{v2}` VALUES (10, 'p0', array('z'), map('k', 1, 'l', NULL), NULL)")

for f in sorted(glob.glob(f"{classic}/_delta_log/*.checkpoint.parquet"))[-1:]:
    shutil.copy(f, f"{out}/delta_checkpoint_classic.parquet")
for f in sorted(glob.glob(f"{v2}/_delta_log/*.checkpoint.*.parquet"))[-1:]:
    shutil.copy(f, f"{out}/delta_checkpoint_v2.parquet")
for f in sorted(glob.glob(f"{v2}/_delta_log/_sidecars/*.parquet"))[-1:]:
    shutil.copy(f, f"{out}/delta_checkpoint_v2_sidecar.parquet")

variant = os.path.join(work, "variant")
spark.sql("""SELECT id, parse_json(CASE WHEN id % 3 = 0 THEN NULL ELSE '{"a": ' || id || ', "b": [1, "x", null]}' END) AS v
             FROM range(20)""").coalesce(1).write.parquet(variant)
shutil.copy(glob.glob(f"{variant}/*.parquet")[0], f"{out}/spark_variant.parquet")
shutil.rmtree(work)
print("written")
