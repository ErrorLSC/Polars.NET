"""Checks xlsx files written by the C# writer with reference readers: openpyxl, Python Polars (calamine through
fastexcel) and, when `soffice` is on the path, LibreOffice (the file is re-saved by LibreOffice and read back).

Usage: python -I interop_check.py <wide_shared.xlsx> <wide_inline.xlsx> [fixture dir]
  Both hold ExcelWriteTests.Wide(): shared strings, and inline strings with autofit, frozen header and the sheet
  name "Wide & co". With the fixture directory, Python Polars also reads the read fixtures, as an oracle for the
  values ExcelReadTests expects (columns where both readers' rules agree). Exits non-zero with a message listing
  every mismatch. Needs openpyxl and polars.
"""
import datetime as dt
import math
import pathlib
import shutil
import subprocess
import sys
import tempfile

import openpyxl
import polars as pl

failures = []


def check(what, actual, expected):
    if isinstance(expected, float) and isinstance(actual, float):
        if math.isclose(actual, expected, rel_tol=1e-14):
            return
    elif actual == expected:
        return
    failures.append(f"{what}: expected {expected!r}, got {actual!r}")


EMOJI = "\U0001F600"

# Column values as openpyxl reports them (datetimes rounded to milliseconds; time-formatted values of a whole day
# become datetimes; error cells are their code).
OPENPYXL = {
    "b": [True, False, None],
    "i8": [-128, 127, None],
    "u32": [0, 4294967295, 1],
    "i64": [9007199254740992, -5, None],
    "big": ["9007199254740993", 1, None],
    "u64": ["18446744073709551615", 2, 3],
    "f32": [0.1, -1.5, "#NUM!"],
    "f64": [1e300, "#NUM!", 2.5],
    "dec": [0.1, 123.45, None],
    "bigdec": ["12345678901234567.89", 0.1, None],
    "dict": ["x", "y", "x"],
    "d32": [dt.datetime(1900, 1, 1), dt.datetime(2024, 2, 29), dt.datetime(9999, 12, 31)],
    "ts_ns": [dt.datetime(2024, 5, 6, 7, 8, 9, 123000), dt.datetime(1900, 3, 1), None],
    "ts_tz": [dt.datetime(2024, 1, 1, 9, 0), None, None],
    "t64": [dt.time(0, 0), dt.datetime(1900, 1, 1), dt.time(1, 2, 3)],
    "t32": [dt.time(0, 0, 1), None, dt.time(0, 0)],
    "dur": [dt.timedelta(seconds=-1.5), dt.timedelta(hours=30, minutes=15), dt.timedelta(0)],
    "n": [None, None, None],
}

# Python Polars (calamine): temporal values in milliseconds; time-formatted values as datetimes on 1899-12-31.
POLARS = {
    "b": [True, False, None],
    "i8": [-128, 127, None],
    "i64": [9007199254740992, -5, None],
    "big": ["9007199254740993", "1", None],
    "u64": ["18446744073709551615", "2", "3"],
    "f32": [0.1, -1.5, None],
    "f64": [1e300, None, 2.5],
    "dec": [0.1, 123.45, None],
    "bigdec": ["12345678901234567.89", "0.1", None],
    "s": ["  pad ", 'a&b<c>"q" _x0041_ \x01 \r\n' + EMOJI, None],
    "dict": ["x", "y", "x"],
    "d32": [dt.date(1900, 1, 1), dt.date(2024, 2, 29), dt.date(9999, 12, 31)],
    "ts_ns": [dt.datetime(2024, 5, 6, 7, 8, 9, 123000), dt.datetime(1900, 3, 1), None],
    "ts_tz": [dt.datetime(2024, 1, 1, 9, 0), None, None],
    "dur": [dt.timedelta(seconds=-1.5), dt.timedelta(hours=30, minutes=15), dt.timedelta(0)],
}


def check_openpyxl(path, label, data_only=False, skip=()):
    wb = openpyxl.load_workbook(path, data_only=data_only)
    ws = wb.active
    columns = {col[0]: list(col[1:]) for col in ws.iter_cols(values_only=True)}
    for name, expected in OPENPYXL.items():
        if name in skip:
            continue
        actual = columns.get(name)
        for i, e in enumerate(expected):
            check(f"{label} {name}[{i}]", actual[i] if actual else "<missing>", e)
    return ws


def check_polars(path, label):
    df = pl.read_excel(path)
    for name, expected in POLARS.items():
        check(f"{label} polars {name}", df[name].to_list(), expected)


def check_fixtures(directory):
    """Python Polars reading the fixtures the C# read tests use (the same values, where the type rules agree)."""
    d = pathlib.Path(directory)
    df = pl.read_excel(d / "types_openpyxl.xlsx")
    check("fixture types int", df["int"].to_list(), [1, -2, 9007199254740992])
    check("fixture types float", df["float"].to_list(), [1.5, -0.25, 1e300])
    check("fixture types mixed_num", df["mixed_num"].to_list(), [1.0, 2.5, 3.0])
    check("fixture types bool", df["bool"].to_list(), [True, False, True])
    check("fixture types text", df["text"].to_list(), ["a", "b", None])
    check("fixture types date", df["date"].to_list(), [dt.date(2024, 1, 2), dt.date(1900, 1, 1), dt.date(9999, 12, 31)])
    check("fixture types gaps", df["gaps"].to_list(), [10, None, 30])
    df = pl.read_excel(d / "headers.xlsx")
    check("fixture headers names", df.columns, ["a", "__UNNAMED__1", "a_1", "b", "a_2", "a_1_1"])
    check("fixture headers a", df["a"].to_list(), [1.0, 1.5])
    df = pl.read_excel(d / "sparse.xlsx")
    check("fixture sparse", df.rows(), [("a", 1), ("b", 2)])
    df = pl.read_excel(d / "workbook_xlsxwriter.xlsx", sheet_name="Data")
    check("fixture xlsxwriter", df.select("id", "name").rows(),
          [(1, "  padded  "), (2, "emoji " + EMOJI), (3, "_x0041_ literal"), (4, "rich bold text")])
    df = pl.read_excel(d / "date1904.xlsx")
    check("fixture 1904", df["d"].to_list(), [dt.date(2024, 1, 2), dt.date(1904, 1, 1)])


def check_xlsb(directory):
    """pyxlsb (raw cell values) and Python Polars over calamine's .xlsb fixtures: the values XlsbReadTests expects."""
    try:
        import pyxlsb
    except ImportError:
        return
    d = pathlib.Path(directory) / "calamine"

    def raw(name, sheet):
        with pyxlsb.open_workbook(str(d / f"{name}.xlsb")) as wb:
            with wb.get_sheet(sheet) as s:
                return [[c.v for c in row] for row in s.rows()]

    # Serial 44197 is 2021-01-01 in the 1900 system, 42735 in the 1904 one; 10.632060185185 days is 255:10:10.
    check("xlsb date", raw("date", 1), [[44197.0, 15.0], [44198.0, 16.0], [10.6320601851852, 17.0]])
    check("xlsb date_1904", raw("date_1904", 1), [[42735.0, 15.0], [42736.0, 16.0], [10.6320601851852, 17.0]])
    check("xlsb date_rk_int", raw("date_rk_int", 1), [[44197.0, 15.0], [44198.0, 16.0], [10.6320601851852, 17.0]])
    check("xlsb float_values", raw("float_values", 1), [[1.23], [12.34], [123.45], [1234.56], [12345.67]])
    check("xlsb formula_values", raw("formula_values", 1), [[3.0], ["Ab"], [False]])
    check("xlsb issue2", raw("issues", "issue2"), [[1.0, "a"], [2.0, "b"], [3.0, "c"]])
    check("xlsb spc_chrs", [r[0] for r in raw("issues", "spc_chrs")], ["&", "<", ">", "aaa ' aaa", '"', "☺", "֍", "àâéêèçöïî«»"])
    with pyxlsb.open_workbook(str(d / "any_sheets.xlsb")) as wb:
        check("xlsb any_sheets", wb.sheets, ["Visible", "Hidden", "VeryHidden", "Chart"])
    df = pl.read_excel(d / "issues.xlsb", sheet_name="issue2", has_header=False)
    check("xlsb polars issue2", df.rows(), [(1, "a"), (2, "b"), (3, "c")])
    df = pl.read_excel(d / "formula_values.xlsb", has_header=False)
    check("xlsb polars formula_values", df.rows(), [("3",), ("Ab",), ("false",)])


def main(shared, inline, fixtures=None):
    ws = check_openpyxl(shared, "shared")
    check("shared title", ws.title, "Sheet1")
    # openpyxl leaves _xHHHH_ escapes as they are; the shared-string reader only turns _x005F_ into '_'.
    check("shared s", [c.value for c in ws["K"][1:]], ["  pad ", 'a&b<c>"q" _x0041_ _x0001_ _x000D_\n' + EMOJI, None])

    ws = check_openpyxl(inline, "inline")
    check("inline title", ws.title, "Wide & co")
    check("inline freeze", ws.freeze_panes, "A2")
    check("inline widths set", all((ws.column_dimensions[c].width or 0) > 0 for c in "ABCK"), True)
    check("inline s", [c.value for c in ws["K"][1:]], ["  pad ", 'a&b<c>"q" _x005F_x0041_ _x0001_ _x000D_\n' + EMOJI, None])

    check_polars(shared, "shared")
    check_polars(inline, "inline")

    soffice = shutil.which("soffice")
    if soffice:
        with tempfile.TemporaryDirectory() as out:
            for path, label in ((shared, "libreoffice shared"), (inline, "libreoffice inline")):
                subprocess.run([soffice, "--headless", "--convert-to", "xlsx", "--outdir", out, path],
                               check=True, capture_output=True, timeout=180)
                resaved = pathlib.Path(out) / pathlib.Path(path).name
                # LibreOffice writes numbers with 15 significant digits and booleans/errors as formulas (cached
                # values read with data_only); control characters and CR do not survive its string model.
                check_openpyxl(resaved, label, data_only=True, skip=("i64",))
                cells = openpyxl.load_workbook(resaved, data_only=True).active
                check(f"{label} i64", [c.value for c in cells["D"][1:]], [9007199254740990, -5, None])
                check(f"{label} s[0]", cells["K2"].value, "  pad ")

    if fixtures:
        check_fixtures(fixtures)
        check_xlsb(fixtures)

    if failures:
        print("\n".join(failures))
        sys.exit(1)
    print("ok")


if __name__ == "__main__":
    main(*sys.argv[1:4])
