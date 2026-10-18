COPY customer FROM '/home/qinglei/Projects/Polars.NET/Polars.TPCH.Tests/Data/customer.parquet' (FORMAT 'parquet');
COPY lineitem FROM '/home/qinglei/Projects/Polars.NET/Polars.TPCH.Tests/Data/lineitem.parquet' (FORMAT 'parquet');
COPY nation FROM '/home/qinglei/Projects/Polars.NET/Polars.TPCH.Tests/Data/nation.parquet' (FORMAT 'parquet');
COPY orders FROM '/home/qinglei/Projects/Polars.NET/Polars.TPCH.Tests/Data/orders.parquet' (FORMAT 'parquet');
COPY part FROM '/home/qinglei/Projects/Polars.NET/Polars.TPCH.Tests/Data/part.parquet' (FORMAT 'parquet');
COPY partsupp FROM '/home/qinglei/Projects/Polars.NET/Polars.TPCH.Tests/Data/partsupp.parquet' (FORMAT 'parquet');
COPY region FROM '/home/qinglei/Projects/Polars.NET/Polars.TPCH.Tests/Data/region.parquet' (FORMAT 'parquet');
COPY supplier FROM '/home/qinglei/Projects/Polars.NET/Polars.TPCH.Tests/Data/supplier.parquet' (FORMAT 'parquet');
