"""Writes spark-expressions.json: what Spark 4 (ANSI) gives for each expression of the Delta expression profile on
each row of a small table, the oracle of SparkExpressionConformance.fs. The table here and the rows there are the same.

Run (local Spark 4.2, JDK 21):
  JAVA_HOME=$HOME/.local/jdk-21 ~/.cache/seseragi/spark-venv/bin/python spark_expressions.py

Each case: the expression, the session time zone, Spark's type (typeof) and per row (k = 1..4) the value as Spark's
CAST(... AS STRING) writes it, null, or "ERROR:<error class>".
"""
import datetime as dt
import json
import math
import pathlib
import re
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType,
                               LongType, ShortType, StringType, StructField, StructType, TimestampNTZType,
                               TimestampType)

UTC = dt.timezone.utc

SCHEMA = StructType([
    StructField("k", LongType()), StructField("i", IntegerType()), StructField("l", LongType()),
    StructField("t", ByteType()), StructField("sm", ShortType()), StructField("d", DecimalType(4, 2)),
    StructField("f", DoubleType()), StructField("fl", FloatType()), StructField("s", StringType()),
    StructField("b", BooleanType()), StructField("dt", DateType()), StructField("ts", TimestampType()),
    StructField("tn", TimestampNTZType()), StructField("z", IntegerType()), StructField("p", StringType()),
])

ROWS = [
    (1, 1, 10, 1, 1, Decimal("12.34"), 1.5, 1.5, "Ab", True, dt.date(2021, 3, 5),
     dt.datetime(2021, 3, 5, 4, 0, 0, tzinfo=UTC), dt.datetime(2021, 3, 5, 4, 0, 0), 0, "a_c"),
    (2, 2147483647, 9223372036854775807, 127, 32767, Decimal("-99.99"), -2.5, float("nan"), "é xyz", False,
     dt.date(2020, 2, 29), dt.datetime(2021, 7, 1, 6, 30, 0, 500000, tzinfo=UTC),
     dt.datetime(1999, 12, 31, 23, 59, 59, 123456), 2, "%"),
    (3, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
    (4, -2147483648, -9223372036854775808, -128, -32768, Decimal("0.05"), float("nan"), -0.0, "", None,
     dt.date(1969, 12, 31), dt.datetime(1969, 12, 31, 23, 59, 59, tzinfo=UTC), dt.datetime(1970, 1, 1), -1,
     "abc"),
]

# (expression, session time zone); a raw string is the text exactly as Spark and the binder read it.
UTC_CASES = r"""
'it\'s'
"dq" || 'x'
'a' 'b'
r'a\nb'
'\u0041\%'
'\U0001F600' || '\101'
1Y + 0Y
1S
10L
1BD
1.5D
1.5F
2147483648
99999999999999999999
1.50
.5
0.05
1e3
DATE '2021-3-5'
TIMESTAMP '2021-03-05 04:00:00'
TIMESTAMP_NTZ '2021-03-05 04:00:00.5'
`I` + 1
NULL
i + 1
i + i
i * 2
l + 1
l * l
l - 1
t + t
sm * sm
-i
-l
-t
abs(i)
abs(l)
abs(t)
d + 1
d * d
d - d
d + i
d * l
i + f
i + fl
f * 2
d + f
i / 2
i / z
l / z
d / 3
d / z
f / z
f / 0D
i div z
7 div 2
-7 div 2
l div -1
d div 1.1
d div z
i % z
7 % -3
-7 % 3
d % 3
f % 2
pmod(-7, 3)
pmod(i, -3)
1 + 1.5
cast(i as tinyint)
cast(l as int)
cast(f as int)
cast(fl as int)
cast(d as int)
cast(d as decimal(3,1))
cast(d as decimal(4,1))
cast(i as decimal(5,2))
cast(l as decimal(20,0))
cast(d as decimal(10,4))
cast(i as string)
cast(d as string)
cast(b as string)
cast(dt as string)
cast(ts as string)
cast(tn as string)
cast(ts as date)
cast(tn as date)
cast(dt as timestamp_ntz)
cast(ts as timestamp_ntz)
cast(i as double)
cast(d as double)
cast(b as int)
cast(i as boolean)
cast(d as boolean)
cast(f as boolean)
cast('12' as int)
cast('2021-01-02' as date)
cast(i as bigint)
i = 1
i == 1
i <> 1
i != 1
i < l
d > 1
d = 12.34
f = 1.5
f = f
f > 1e300
s = 'Ab'
s < 'a'
s > 'B'
b = true
dt = '2021-03-05'
dt < ts
ts = TIMESTAMP '2021-03-05 04:00:00'
tn = ts
ts > '2021-01-01'
'1' = i
i <=> NULL
i <=> z
i IS DISTINCT FROM z
i BETWEEN 0 AND 2
i NOT BETWEEN 0 AND 2
i IN (1, 2)
i IN (1, NULL)
i NOT IN (1, NULL)
i IN (z, 1)
s IN ('Ab', 'x')
NOT b
!b
b AND i = 1
b OR i = 1
i IS NULL
s IS NOT NULL
isnull(i)
isnotnull(s)
CASE WHEN z = 0 THEN NULL ELSE i / z END
IF(z <> 0, i div z, -1)
z <> 0 AND i div z > 0
z = 0 OR i div z > 0
COALESCE(1, i div z)
nvl(i, l div z)
CASE WHEN i > 0 THEN i + 1 ELSE 0 END
CASE i WHEN 1 THEN 'one' WHEN 2147483647 THEN 'max' ELSE 'other' END
length(s)
upper(s)
lower(s)
ucase(s)
substring(s, 0, 2)
substring(s, 2)
substring(s, -2)
substring(s, -10, 3)
substring(s, -3, 2)
substr(s, 3, 100)
left(s, 2)
right(s, 2)
concat(s, 'x')
s || '!'
s || i
concat(s, NULL)
concat_ws('-', s, NULL, 'z')
concat_ws('-', NULL)
trim('  a ')
trim(s)
ltrim(' a ')
rtrim(' a ')
replace(s, 'b', '-')
replace(s, '')
lpad(s, 4, '0')
rpad(s, 1, '*')
lpad(s, 5)
startswith(s, 'A')
endswith(s, 'z')
contains(s, 'x')
s LIKE 'A%'
s LIKE '%b'
s LIKE '_b'
s LIKE '%x%'
p LIKE 'a\\_c'
p LIKE '\\%'
s ILIKE 'ab'
s NOT LIKE 'A%'
s RLIKE '^A'
s RLIKE 'b$'
s RLIKE '\\d'
s RLIKE '[a-c]+'
s RLIKE '(?i)AB'
s REGEXP 'x.z'
s RLIKE 'é'
round(d, 1)
round(d)
round(f)
round(-2.5)
bround(2.5)
bround(d, 1)
floor(f)
ceil(f)
floor(d)
ceil(d)
floor(i)
floor(f * 1e300)
ceil(f * -1e300)
greatest(i, z, 0)
least(i, z)
greatest(f, 1.0D)
least(d, 1)
greatest(s, 'B')
nullif(i, 1)
nvl2(s, 1, 2)
if(b, 'y', 'n')
year(ts)
month(dt)
day(dt)
dayofmonth(ts)
hour(ts)
minute(ts)
second(ts)
hour(tn)
quarter(dt)
dayofweek(dt)
weekday(dt)
year('2021-03-05')
date_format(ts, 'yyyy-MM')
date_format(ts, 'yyyy-MM-dd HH:mm:ss')
date_format(dt, 'yyyy/MM/dd')
date_format(tn, 'HH:mm:ss.SSS')
trunc(dt, 'MM')
trunc(dt, 'YEAR')
trunc(dt, 'QUARTER')
trunc(dt, 'WEEK')
trunc(ts, 'MM')
date_trunc('HOUR', ts)
date_trunc('DAY', ts)
date_trunc('MONTH', ts)
date_trunc('MINUTE', tn)
extract(YEAR FROM dt)
"""

LA_CASES = r"""
hour(ts)
cast(ts as date)
date_format(ts, 'yyyy-MM-dd HH')
cast(ts as string)
day(ts)
ts = TIMESTAMP '2021-03-04 20:00:00'
dt < ts
trunc(ts, 'MM')
hour(tn)
cast(ts as timestamp_ntz)
"""


def error_class(ex: Exception) -> str:
    match = re.search(r"\[([A-Z_]+)(?:\.[A-Z_]+)?\]", str(ex))
    return match.group(1) if match else type(ex).__name__


def main() -> None:
    spark = (SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("OFF")
    spark.createDataFrame(ROWS, SCHEMA).createOrReplaceTempView("t")
    cases = []
    for zone, text in (("UTC", UTC_CASES), ("America/Los_Angeles", LA_CASES)):
        spark.conf.set("spark.sql.session.timeZone", zone)
        for expr in [line for line in text.strip().split("\n") if line.strip()]:
            kind = spark.sql(f"SELECT typeof({expr}) FROM t LIMIT 1").collect()[0][0]
            rows = []
            for k in range(1, len(ROWS) + 1):
                try:
                    rows.append(spark.sql(f"SELECT CAST(({expr}) AS STRING) FROM t WHERE k = {k}").collect()[0][0])
                except Exception as ex:  # noqa: BLE001 - Spark raises Py4J and analysis exceptions alike
                    rows.append("ERROR:" + error_class(ex))
            cases.append({"expr": expr, "zone": zone, "type": kind, "rows": rows})
            print(f"{zone:20} {expr:45} {kind:15} {rows}")
    out = pathlib.Path(__file__).with_name("spark-expressions.json")
    out.write_text(json.dumps({"spark": spark.version, "cases": cases}, ensure_ascii=False, indent=1) + "\n")


if __name__ == "__main__":
    main()
