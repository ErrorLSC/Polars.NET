"""Writes small IPC datasets whose files differ in layout and dictionary encoding (pyarrow 26, Polars 2.1), and
expected.json: per dataset, its rows (files in path order) rendered as ArrowRender renders them.

Run: ~/.cache/seseragi/avro-venv/bin/python write_datasets.py (from this directory).
"""
import json
import os
import shutil

import polars as pl
import pyarrow as pa
import pyarrow.feather as feather
import pyarrow.parquet as pq

HERE = os.path.dirname(os.path.abspath(__file__))
LONG = "a string longer than twelve bytes"


def render(v):
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, str):
        return '"' + v + '"'
    if isinstance(v, int):
        return str(v)
    if isinstance(v, list):
        return "[" + ", ".join(render(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{" + ", ".join(f"{k}: {render(x)}" for k, x in v.items()) + "}"
    raise TypeError(type(v))


def write(dataset, name, table):
    os.makedirs(os.path.join(HERE, dataset), exist_ok=True)
    feather.write_feather(table, os.path.join(HERE, dataset, name), compression="uncompressed")


def write_polars(dataset, name, frame):
    os.makedirs(os.path.join(HERE, dataset), exist_ok=True)
    frame.write_ipc(os.path.join(HERE, dataset, name), compression="uncompressed")


def rows(dataset):
    out = []
    for name in sorted(os.listdir(os.path.join(HERE, dataset))):
        path = os.path.join(HERE, dataset, name)
        table = pq.read_table(path) if name.endswith(".parquet") else feather.read_table(path)
        for row in table.to_pylist():
            out.append(" | ".join(render(row[c]) for c in table.column_names))
    return out


def main():
    for entry in os.listdir(HERE):
        if os.path.isdir(os.path.join(HERE, entry)) and entry != "__pycache__":
            shutil.rmtree(os.path.join(HERE, entry))
    expected = {}

    # Strings in three layouts: plain (pyarrow), large (pyarrow), views (Polars).
    values = ["x", None, "", LONG, "y"]
    write("strings", "a.arrow", pa.table({"s": pa.array(values, pa.string())}))
    write("strings", "b.arrow", pa.table({"s": pa.array(values[::-1], pa.large_string())}))
    write_polars("strings", "c.arrow", pl.DataFrame({"s": pl.Series(["z", LONG + "!", None], dtype=pl.String)}))
    expected["strings"] = rows("strings")

    # Lists in three layouts, elements Int64 first then Int32: list, large list, list view (overlapping views).
    write("lists", "a.arrow", pa.table({"l": pa.array([[1, 2], None, [], [3]], pa.list_(pa.int64()))}))
    write("lists", "b.arrow", pa.table({"l": pa.array([[4], [5, 6, 7], None], pa.large_list(pa.int32()))}))
    view = pa.ListViewArray.from_arrays(pa.array([2, 0, 1, 0], pa.int32()), pa.array([2, 3, 0, 1], pa.int32()),
                                        pa.array([10, 11, 12, 13], pa.int32()), mask=pa.array([False, False, False, True]))
    write("lists", "c.arrow", pa.table({"l": view}))
    expected["lists"] = rows("lists")

    # Dictionaries of several index types and value layouts: Int8 / plain, Int32 / large, Polars Categorical
    # (UInt32 / views).
    write("dicts", "a.arrow", pa.table({"d": pa.array(["p", "q", None, "p"]).dictionary_encode().cast(pa.dictionary(pa.int8(), pa.string()))}))
    write("dicts", "b.arrow", pa.table({"d": pa.array(["q", LONG, "r"], pa.large_string()).dictionary_encode()}))
    write_polars("dicts", "c.arrow", pl.DataFrame({"d": pl.Series(["r", "s", None, LONG], dtype=pl.Categorical)}))
    expected["dicts"] = rows("dicts")

    # A dictionary first, plain strings after (FirstFile encodes them; UnionByName reads plain strings).
    write("dictplain", "a.arrow", pa.table({"d": pa.array(["p", "q", "p"]).dictionary_encode()}))
    write("dictplain", "b.arrow", pa.table({"d": pa.array(["q", None, "z"], pa.string())}))
    expected["dictplain"] = rows("dictplain")

    # Nested: a struct of a dictionary and a list of strings, in other index types and layouts per file.
    struct_a = pa.StructArray.from_arrays(
        [pa.array(["k", "l", "k"]).dictionary_encode().cast(pa.dictionary(pa.int8(), pa.string())),
         pa.array([["x"], [], None], pa.list_(pa.string()))], names=["c", "l"])
    struct_b = pa.StructArray.from_arrays(
        [pa.array(["l", "m"]).dictionary_encode().cast(pa.dictionary(pa.int16(), pa.string())),
         pa.array([[LONG, "y"], ["z"]], pa.large_list(pa.large_string()))], names=["c", "l"])
    write("nested", "a.arrow", pa.table({"s": struct_a}))
    write("nested", "b.arrow", pa.table({"s": struct_b}))
    expected["nested"] = rows("nested")

    # Polars Enums: alike (kept), of other categories (refused), against plain strings (refused).
    enum = pl.Enum(["lo", "hi"])
    write_polars("enum_same", "a.arrow", pl.DataFrame({"e": pl.Series(["lo", "hi"], dtype=enum)}))
    write_polars("enum_same", "b.arrow", pl.DataFrame({"e": pl.Series(["hi", None], dtype=enum)}))
    expected["enum_same"] = rows("enum_same")
    write_polars("enum_diff", "a.arrow", pl.DataFrame({"e": pl.Series(["lo"], dtype=enum)}))
    write_polars("enum_diff", "b.arrow", pl.DataFrame({"e": pl.Series(["mid"], dtype=pl.Enum(["lo", "mid", "hi"]))}))
    write_polars("enum_plain", "a.arrow", pl.DataFrame({"e": pl.Series(["lo"], dtype=enum)}))
    write_polars("enum_plain", "b.arrow", pl.DataFrame({"e": pl.Series(["lo"], dtype=pl.String)}))

    # Nested Enums: list items alike in two layouts (Polars' large list; pyarrow's list, the Enum's metadata on its
    # item field) unite; struct fields of other categories are refused.
    write_polars("enum_list", "a.arrow", pl.DataFrame({"l": pl.Series([["lo", "hi"], None], dtype=pl.List(enum))}))
    polars_item = feather.read_table(os.path.join(HERE, "enum_list", "a.arrow")).schema.field("l").type.value_field
    item = pa.field("item", pa.dictionary(pa.uint8(), pa.string_view(), ordered=True), metadata=polars_item.metadata)
    items = pa.DictionaryArray.from_arrays(pa.array([1], pa.uint8()), pa.array(["lo", "hi"], pa.string_view()), ordered=True)
    write("enum_list", "b.arrow", pa.table({"l": pa.ListArray.from_arrays(pa.array([0, 1, 1], pa.int32()), items, type=pa.list_(item))}))
    expected["enum_list"] = rows("enum_list")
    write_polars("enum_struct", "a.arrow", pl.DataFrame({"s": pl.Series([{"e": "lo"}], dtype=pl.Struct({"e": enum}))}))
    write_polars("enum_struct", "b.arrow", pl.DataFrame({"s": pl.Series([{"e": "mid"}], dtype=pl.Struct({"e": pl.Enum(["lo", "mid", "hi"])}))}))

    # Polars-written Parquet Enums of other categories.
    os.makedirs(os.path.join(HERE, "parquet_enum_diff"), exist_ok=True)
    pl.DataFrame({"e": pl.Series(["lo"], dtype=enum)}).write_parquet(os.path.join(HERE, "parquet_enum_diff", "a.parquet"))
    pl.DataFrame({"e": pl.Series(["mid"], dtype=pl.Enum(["lo", "mid", "hi"]))}).write_parquet(os.path.join(HERE, "parquet_enum_diff", "b.parquet"))

    # Parquet files with dictionary columns (ARROW:schema), two row groups each: a dictionary per row group.
    os.makedirs(os.path.join(HERE, "parquet_dicts"), exist_ok=True)
    pq.write_table(pa.table({"d": pa.array(["p", "q", "p", LONG]).dictionary_encode()}),
                   os.path.join(HERE, "parquet_dicts", "a.parquet"), row_group_size=2)
    pq.write_table(pa.table({"d": pa.array([LONG, None, "z", "q"]).dictionary_encode()}),
                   os.path.join(HERE, "parquet_dicts", "b.parquet"), row_group_size=2)
    expected["parquet_dicts"] = rows("parquet_dicts")

    with open(os.path.join(HERE, "expected.json"), "w", encoding="utf-8") as f:
        json.dump(expected, f, indent=1, ensure_ascii=False)


if __name__ == "__main__":
    main()
