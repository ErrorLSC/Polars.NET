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

import pyarrow as pa
from pyarrow import parquet
import pyproj
import shapely

HERE = Path(__file__).parent


# Using Wyoming because it is the easiest state to inline into a Python file
WYOMING_LOWRES = (
    "POLYGON ((-111.0 45.0, -111.0 41.0, -104.0 41.0, -104.0 45.0, -111.0 45.0))"
)

# We densify the edges such that there is a point every 0.1 degrees to minimize
# the effect of the edge algorithm and coordinate transformation.
WYOMING_HIRES = shapely.from_wkt(WYOMING_LOWRES).segmentize(0.1).wkt


class WkbType(pa.ExtensionType):
    """Minimal geoarrow.wkb implementation"""

    def __init__(self, crs=None, edges=None, *, storage_type=pa.binary(), **kwargs):
        self.crs = crs
        self.edges = edges
        super().__init__(storage_type, "geoarrow.wkb")

    def __arrow_ext_serialize__(self):
        obj = {"crs": self.crs, "edges": self.edges}
        return json.dumps({k: v for k, v in obj.items() if v}).encode()

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        obj: dict = json.loads(serialized)
        return WkbType(**obj, storage_type=storage_type)


pa.register_extension_type(WkbType())


def write_crs(type, geometry, name, col_name="geometry", metadata=None):
    schema = pa.schema({"wkt": pa.utf8(), col_name: type})

    with parquet.ParquetWriter(
        HERE / name,
        schema,
        # Not sure if there's a way to write metadata without
        # storing the Arrow schema
        store_schema=metadata is not None,
        compression="none",
    ) as writer:
        batch = pa.record_batch(
            {
                "wkt": [geometry.wkt],
                col_name: type.wrap_array(pa.array([geometry.wkb])),
            }
        )
        writer.write_batch(batch)

        if metadata is not None:
            writer.add_key_value_metadata(metadata)


def write_crs_files():
    # Create the Shapely geometry
    geometry = shapely.from_wkt(WYOMING_HIRES)

    # A general purpose coordinate system for the United States
    crs_not_lonlat = pyproj.CRS("EPSG:5070")
    transformer = pyproj.Transformer.from_crs(
        "OGC:CRS84", crs_not_lonlat, always_xy=True
    )
    geometry_not_lonlat = shapely.transform(
        geometry, transformer.transform, interleaved=False
    )

    # Write with the default CRS (i.e., lon/lat)
    write_crs(WkbType(), geometry, "crs-default.parquet")

    # Write a Geography column with the default CRS
    write_crs(
        WkbType(edges="spherical"),
        geometry,
        "crs-geography.parquet",
        col_name="geography",
    )

    # Write a file with the projjson format in the specification
    # and the appropriate metadata key
    write_crs(
        WkbType(crs="projjson:projjson_epsg_5070"),
        geometry_not_lonlat,
        "crs-projjson.parquet",
        metadata={"projjson_epsg_5070": crs_not_lonlat.to_json()},
    )

    # Write a file with the srid format in the specification
    write_crs(WkbType(crs="srid:5070"), geometry_not_lonlat, "crs-srid.parquet")

    # Write a file with an arbitrary value (theoretically allowed by the format
    # and consumers may choose to error or attempt to interpret the value)
    write_crs(
        WkbType(crs=crs_not_lonlat.to_json_dict()),
        geometry_not_lonlat,
        "crs-arbitrary-value.parquet",
    )


def check_crs_schema(name, expected_col_type):
    file = parquet.ParquetFile(HERE / name)

    col = file.schema.column(1)
    col_dict = json.loads(col.logical_type.to_json())
    col_type = col_dict["Type"]
    if col_type != expected_col_type:
        raise ValueError(
            f"Expected '{expected_col_type}' logical type but got '{col_type}'"
        )


def check_crs_crs(name, expected_crs):
    expected_crs = pyproj.CRS(expected_crs)

    file = parquet.ParquetFile(HERE / name, arrow_extensions_enabled=True)
    ext_type = file.schema_arrow.field(1).type
    actual_crs = pyproj.CRS(ext_type.crs)
    if actual_crs != expected_crs:
        raise ValueError(f"Expected '{expected_crs}' crs but got '{actual_crs}'")


def check_crs(name, expected_col_type, expected_crs):
    check_crs_schema(name, expected_col_type)
    check_crs_crs(name, expected_crs)


if __name__ == "__main__":
    write_crs_files()

    check_crs("crs-default.parquet", "Geometry", "OGC:CRS84")
    check_crs("crs-geography.parquet", "Geography", "OGC:CRS84")
    check_crs("crs-projjson.parquet", "Geometry", "EPSG:5070")
    check_crs("crs-srid.parquet", "Geometry", "EPSG:5070")
    check_crs("crs-arbitrary-value.parquet", "Geometry", "EPSG:5070")
