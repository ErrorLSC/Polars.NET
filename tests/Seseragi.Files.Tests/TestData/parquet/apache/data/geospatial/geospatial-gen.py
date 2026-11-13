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

import json
from pathlib import Path
import re

import geoarrow.pyarrow as ga
import numpy as np
import pyarrow as pa
from pyarrow import parquet
import shapely
import shapely.testing
import yaml

# M value support was added in Shapely 2.1.0
if tuple(shapely.__version__.split(".")) < ("2", "1", "0"):
    raise ImportError("shapely >= 2.1.0 is required")

HERE = Path(__file__).parent

# Shapely doesn't propagate the geometry type for MULTI* Z/M/ZM, so
# we have to write our own geometry type detector to check statistics output
GEOMETRY_TYPE_CODE = {
    "POINT": 1,
    "LINESTRING": 2,
    "POLYGON": 3,
    "MULTIPOINT": 4,
    "MULTILINESTRING": 5,
    "MULTIPOLYGON": 6,
    "GEOMETRYCOLLECTION": 7,
}

DIMENSIONS_CODE = {None: 0, "Z": 1000, "M": 2000, "ZM": 3000}


def geometry_type_code(wkt):
    if wkt is None:
        return None

    geometry_type, _, dimensions = re.match(r"([A-Z]+)( ([ZM]+)?)?", wkt).groups()
    return GEOMETRY_TYPE_CODE[geometry_type] + DIMENSIONS_CODE[dimensions]


def write_geospatial():
    with open(HERE / "geospatial.yaml") as f:
        examples = yaml.safe_load(f)

    schema = pa.schema({"group": pa.utf8(), "wkt": pa.utf8(), "geometry": ga.wkb()})

    with parquet.ParquetWriter(
        HERE / "geospatial.parquet",
        schema,
        store_schema=False,
        compression="none",
    ) as writer:
        for group_name, geometries_wkt in examples.items():
            # Unfortunately we can't use Shapely to generate the test WKB
            # because of https://github.com/libgeos/geos/issues/888, so we use
            # geoarrow.pyarrow.as_wkb() instead.
            # geometries = shapely.from_wkt(geometries_wkt)
            # wkbs = shapely.to_wkb(geometries, flavor="iso")
            # wkb_array = ga.wkb().wrap_array(pa.array(wkbs, pa.binary()))
            wkt_array = pa.array(geometries_wkt, pa.utf8())

            batch = pa.record_batch(
                {
                    "group": [group_name] * len(geometries_wkt),
                    "wkt": wkt_array,
                    "geometry": ga.as_wkb(wkt_array),
                }
            )

            writer.write_batch(batch)


def write_geospatial_with_nan():
    geometries_wkt = [
        "POINT ZM (10 20 30 40)",
        "POINT ZM (50 60 70 80)",
        "LINESTRING ZM (90 100 110 120, nan nan nan nan, 130 140 150 160)",
    ]

    schema = pa.schema({"group": pa.utf8(), "wkt": pa.utf8(), "geometry": ga.wkb()})

    with parquet.ParquetWriter(
        HERE / "geospatial-with-nan.parquet",
        schema,
        store_schema=False,
        compression="none",
    ) as writer:
        geometries = shapely.from_wkt(geometries_wkt)
        wkbs = shapely.to_wkb(geometries, flavor="iso")
        wkb_array = ga.wkb().wrap_array(pa.array(wkbs, pa.binary()))

        batch = pa.record_batch(
            {
                "group": ["with-nan"] * len(geometries_wkt),
                "wkt": geometries_wkt,
                "geometry": wkb_array,
            }
        )

        writer.write_batch(batch)


def check_geospatial_schema(name):
    file = parquet.ParquetFile(HERE / name)

    col = file.schema.column(2)
    col_dict = json.loads(col.logical_type.to_json())
    col_type = col_dict["Type"]
    if col_type != "Geometry":
        raise ValueError(f"Expected 'Geometry' logical type but got '{col_type}'")


def check_geospatial_values(name):
    tab = parquet.read_table(HERE / name, arrow_extensions_enabled=False)
    geometries_from_wkt = shapely.from_wkt(tab["wkt"])
    geometries_from_wkb = shapely.from_wkb(tab["geometry"])
    shapely.testing.assert_geometries_equal(geometries_from_wkt, geometries_from_wkb)


def iso_type_code(shapely_type_id, has_z, has_m):
    # item was null
    if shapely_type_id < 0:
        return None

    # GEOS type ids are not quite WKB type ids
    iso_type_id = shapely_type_id if shapely_type_id >= 3 else shapely_type_id + 1
    iso_dimensions = (
        3000 if has_z and has_m else 2000 if has_m else 1000 if has_z else 0
    )
    return int(iso_dimensions + iso_type_id)


def calc_stats_shapely(wkts):
    geometries = shapely.from_wkt(wkts)

    # Calculate the list of iso type codes
    any_not_null = any(geom is not None for geom in geometries)
    type_codes = set(geometry_type_code(wkt) for wkt in wkts)

    # Calculate min/max ignoring nan values
    coords = shapely.get_coordinates(geometries, include_z=True, include_m=True)
    coord_mins = [
        None if np.isposinf(x) else float(x)
        for x in np.nanmin(coords, 0, initial=np.inf)
    ]
    coord_maxes = [
        None if np.isneginf(x) else float(x)
        for x in np.nanmax(coords, 0, initial=-np.inf)
    ]

    # Assemble stats in the same format as returned by geo_statistics.to_dict()
    stats = {}
    stats["geospatial_types"] = list(
        sorted(code for code in type_codes if code is not None)
    ) if any_not_null else None
    stats["xmin"], stats["ymin"], stats["zmin"], stats["mmin"] = coord_mins
    stats["xmax"], stats["ymax"], stats["zmax"], stats["mmax"] = coord_maxes

    return stats


def check_batch_statistics(stats, wkts, group):
    if stats is None:
        raise ValueError(f"geo_statistics is missing for group {group}")

    shapely_stats = calc_stats_shapely(wkts)
    file_stats = stats.to_dict()
    if file_stats != shapely_stats:
        raise ValueError(
            f"stats mismatch calculated:\n{shapely_stats}\nvs file\n{file_stats}"
        )


def check_geospatial_statistics(name):
    file = parquet.ParquetFile(HERE / name)
    for i in range(file.num_row_groups):
        batch = file.read_row_group(i)
        column_metadata = file.metadata.row_group(i).column(2)
        group = batch["group"][0].as_py()

        check_batch_statistics(
            column_metadata.geo_statistics, batch["wkt"].to_pylist(), group
        )


if __name__ == "__main__":
    write_geospatial()
    write_geospatial_with_nan()

    check_geospatial_schema("geospatial.parquet")
    check_geospatial_values("geospatial.parquet")
    check_geospatial_statistics("geospatial.parquet")

    check_geospatial_schema("geospatial-with-nan.parquet")
    check_geospatial_statistics("geospatial-with-nan.parquet")
