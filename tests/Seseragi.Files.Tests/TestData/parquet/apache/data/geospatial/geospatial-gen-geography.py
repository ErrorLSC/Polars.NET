# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import numpy as np
import pyarrow as pa
import pyarrow.parquet as parquet
import geoarrow.pyarrow as ga
import sedona.db

from pathlib import Path

HERE = Path(__file__).parent


def generate_points(n):
    # Generate roughly equally spaced points on the sphere
    golden_ratio = (1 + np.sqrt(5)) / 2
    indices = np.arange(n)

    phi = np.arcsin(2 * indices / (n - 1) - 1) if n > 1 else np.array([0.0])
    theta = 2 * np.pi * indices / golden_ratio

    lat = np.degrees(phi)
    lon = np.degrees(theta) % 360 - 180

    # Explicitly set poles at longitude 0 for numerical consistency
    lon[0] = 0.0  # South pole (lat=-90)
    lon[-1] = 0.0  # North pole (lat=90)

    array = ga.with_edge_type(
        ga.with_crs(ga.make_point(lon, lat), ga.OGC_CRS84), ga.EdgeType.SPHERICAL
    )
    return pa.table({"id": indices, "geometry": ga.as_wkb(array)})


def write_points():
    sd = sedona.db.connect()
    sd.create_data_frame(generate_points(500)).to_parquet(
        HERE / "geography-points.parquet",
        sort_by="geometry",
        max_row_group_size=10,
        geoparquet_version="none",
    )


def write_lines():
    sd = sedona.db.connect()
    sd.create_data_frame(generate_points(500)).to_view("pts")

    # Sort so that points make sense sequentially
    points_tab = sd.sql(
        "SELECT * FROM pts ORDER BY sd_order(geometry)"
    ).to_arrow_table()

    # Shift geometry column using PyArrow slicing
    geom = points_tab.column("geometry")
    ids = points_tab.column("id").slice(0, len(geom) - 1)
    next_geom = geom.slice(1)  # geometry[1:]
    geom = geom.slice(0, len(geom) - 1)  # geometry[:-1]

    paired = pa.table({"id": ids, "geometry": geom, "next_geom": next_geom})

    sd.create_data_frame(paired).to_view("geoms")
    sd.sql("""
        SELECT id, ST_MakeLine(geometry, next_geom) AS geometry
        FROM geoms
    """).to_parquet(
        HERE / "geography-lines.parquet",
        sort_by="geometry",
        max_row_group_size=10,
        geoparquet_version="none",
    )


def write_polygons():
    sd = sedona.db.connect()
    sd.create_data_frame(generate_points(500)).to_view("pts")
    sd.sql(
        "SELECT id, ST_Buffer(geometry, 500000, 'quad_segs=1') as geometry from pts"
    ).to_parquet(
        HERE / "geography-polygons.parquet",
        sort_by="geometry",
        max_row_group_size=10,
        geoparquet_version="none",
    )


def test_geography_parquet_files():
    """Test that geography parquet files have correct logical type and wraparound statistics."""
    files = [
        HERE / "geography-points.parquet",
        HERE / "geography-lines.parquet",
        HERE / "geography-polygons.parquet",
    ]

    for path in files:
        f = parquet.ParquetFile(path)

        # Check for Geography logical type
        logical_type = f.metadata.schema.column(1).logical_type.to_json()
        assert logical_type == '{"Type": "Geography"}', (
            f"{path.name}: Expected Geography logical type, got {logical_type}"
        )
        print(f"{path.name}: Has Geography logical type")

        # Check that at least one row group has wraparound statistics (xmin > xmax)
        has_wraparound = 0
        for i in range(f.metadata.num_row_groups):
            stats = f.metadata.row_group(i).column(1).geo_statistics
            if stats is not None and stats.xmin > stats.xmax:
                has_wraparound += 1

        assert has_wraparound > 0, (
            f"{path.name}: Expected at least one row group with antimeridian wraparound"
        )
        print(f"{path.name}: Has {has_wraparound} row groups with wraparound stats")


def test_poles_intersection():
    """Test that each geography parquet file has at least one geometry intersecting each pole."""
    sd = sedona.db.connect()
    files = [
        ("geography-points.parquet", HERE / "geography-points.parquet"),
        ("geography-lines.parquet", HERE / "geography-lines.parquet"),
        ("geography-polygons.parquet", HERE / "geography-polygons.parquet"),
    ]

    for name, path in files:
        sd.read_parquet(str(path)).to_view("geom_table", overwrite=True)

        # Test North Pole intersection
        north_pole_count = (
            sd.sql("""
            SELECT COUNT(*) as cnt FROM geom_table
            WHERE ST_Intersects(geometry, ST_GeogPoint(0, 90))
        """)
            .to_arrow_table()
            .column("cnt")[0]
            .as_py()
        )

        assert north_pole_count >= 1, (
            f"{name}: Expected at least 1 geometry intersecting North Pole (0, 90), got {north_pole_count}"
        )
        print(f"{name}: {north_pole_count} geometries intersect North Pole")

        # Test South Pole intersection
        south_pole_count = (
            sd.sql("""
            SELECT COUNT(*) as cnt FROM geom_table
            WHERE ST_Intersects(geometry, ST_GeogPoint(0, -90))
        """)
            .to_arrow_table()
            .column("cnt")[0]
            .as_py()
        )

        assert south_pole_count >= 1, (
            f"{name}: Expected at least 1 geometry intersecting South Pole (0, -90), got {south_pole_count}"
        )
        print(f"{name}: {south_pole_count} geometries intersect South Pole")

    print("\nPole intersection tests passed!")


if __name__ == "__main__":
    write_points()
    write_lines()
    write_polygons()
    test_geography_parquet_files()
    test_poles_intersection()
