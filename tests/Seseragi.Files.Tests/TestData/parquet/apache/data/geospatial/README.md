<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Geospatial Test Files


These test files cover the main and corner case functionality of the
[Parquet Geospatial Types](https://github.com/apache/parquet-format/blob/master/Geospatial.md)
GEOMETRY and GEOGRAPHY.

- `geospatial.parquet`: Contains row groups with specific combinations of
  geometry types to test statistics generation and geometry type coverage.
  The file contains columns `group` (string identifier of the group name),
  `wkt` (the human-readable well-known text representation of the geometry)
  and `geometry` (a Parquet GEOMETRY column). A human-readable version of
  the file is available in `geospatial.yaml`.

- `geospatial-with-nan.parquet`: Contains a single row group with a GEOMETRY
  column whose contents contains two valid geometries and one invalid LINESTRING
  whose coordinates contain a `NaN` value in all dimensions. Such a geometry is
  not valid and the behaviour of it is not defined; however, implementations should
  not generate statistics that would prevent the other (valid) geometries in the
  column chunk from appearing in the case of predicate pushdown. Notably,
  implementations should *not* generate statistics that contain `NaN` for this case.

  Note that POINT EMPTY is represented by convention in well-known binary as
  a POINT whose coordinates are all `NaN`, which should be treated as a valid
  (but empty) geometry.

- `crs-default.parquet`: Contains a GEOMETRY column with the crs
  omitted. This should be interpreted as OGC:CRS84 (i.e., longitude/latitude).

- `crs-geography.parquet`: Contains a GEOGRAPHY column with the crs
  omitted. This should be interpreted as OGC:CRS84 (i.e., longitude/latitude).

- `crs-projjson.parquet`: Contains a GEOMETRY column with the crs parameter
  set to `projjson:projjson_epsg_5070` and a metadata field with the key
  `projjson_epsg_5070` and a value consisting of the appropriate PROJJSON
  value for EPSG:5070.

- `crs-srid.parquet`: Contains a GEOMETRY column with the crs parameter set
  to `srid:5070`. The Parquet format does not mention the EPSG database in
  any way, but otherwise out-of-context SRID values are commonly interpreted
  as the corresponding EPSG:xxxx value. Producers of SRIDs may wish to
  avoid valid EPSG:xxxx values where this is not the intended usage to minimize
  the chances they will be misinterpreted by consumers who make this assumption.

- `crs-arbitrary-value.parquet`: Contains a GEOMETRY column with the crs
  parameter set to an arbitrary string value. The Parquet format does not
  restrict the value of the crs parameter and implementations may choose to
  attempt interpreting the value or error.

- `geography-points.parquet`: Contains a GEOGRAPHY column with 500 points
  roughly equally spaced on a sphere using the golden ratio method. Points
  are sorted by Hilbert curve and split into row groups of 10 rows each
  to test spatial predicate pushdown. The points in the file contain
  both the North and South Poles (`POINT (0 90)` and `POINT (0 -90)`).
  At least one row group contains wraparound statistics (i.e., `xmin > xmax`).

- `geography-lines.parquet`: Contains a GEOGRAPHY column with lines connecting
  consecutive points from a spatially sorted point set. Lines are sorted by
  Hilbert curve and split into row groups of 10 rows each to test spatial
  predicate pushdown. There are two lines that intersect the North Pole,
  two lines that intersect the South Pole, and several lines that contain
  vertices on either side of the antimeridian. At least one row group
  contains wraparound statistics (i.e., `xmin > xmax`).

- `geography-polygons.parquet`: Contains a GEOGRAPHY column with polygons
  created by buffering each of the 500 roughly equally spaced sphere points
  with a 500km radius. Polygons are sorted by Hilbert curve and split into
  row groups of 10 rows each to test spatial predicate pushdown. There is
  a polygon that contains the North Pole and a polygon that contains the
  South Pole and several polygons that contain vertices on either side of
  the antimeridian. At least one row group contains wraparound statistics
  (i.e., `xmin > xmax`).
