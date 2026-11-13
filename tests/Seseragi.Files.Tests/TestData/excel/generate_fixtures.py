"""Writes the Excel fixtures of Seseragi.Files.Tests (the C# tests hard-code the values written here).

Run with the oracle environment (openpyxl, xlsxwriter):

    ~/.cache/seseragi/excel-venv/bin/python -I tests/Seseragi.Files.Tests/TestData/excel/generate_fixtures.py
"""

import datetime as dt
import pathlib
import shutil
import subprocess
import zipfile

import openpyxl
import xlsxwriter

HERE = pathlib.Path(__file__).resolve().parent


def types_openpyxl():
    """One column per inferred type, written by openpyxl (inline cell values, shared strings, number formats)."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Types"
    ws.append(["int", "float", "mixed_num", "bool", "text", "date", "datetime", "time", "duration", "mixed", "gaps"])
    rows = [
        [1, 1.5, 1, True, "a", dt.date(2024, 1, 2), dt.datetime(2024, 5, 6, 7, 8, 9, 123456), dt.time(1, 2, 3), dt.timedelta(hours=30, minutes=15), "x", 10],
        [-2, -0.25, 2.5, False, "b", dt.date(1900, 1, 1), dt.datetime(1999, 12, 31, 23, 59, 59), dt.time(23, 59, 59, 500000), dt.timedelta(seconds=1), 5, None],
        [9007199254740993, 1e300, 3, True, "", dt.date(9999, 12, 31), dt.datetime(1900, 3, 1, 12, 0), dt.time(0, 0), dt.timedelta(0), True, 30],
    ]
    for r in rows:
        ws.append(r)
    ws["J4"].number_format = "General"
    # An error value in the int column: reads as null.
    ws.append([None, None, None, None, None, None, None, None, None, None, None])
    ws["A5"] = "#N/A"
    ws["A5"].data_type = "e"
    wb.save(HERE / "types_openpyxl.xlsx")


def workbook_xlsxwriter():
    """Several sheets (one hidden), a table with a totals row, special strings; written by xlsxwriter."""
    wb = xlsxwriter.Workbook(HERE / "workbook_xlsxwriter.xlsx")
    data = wb.add_worksheet("Data")
    data.write_row(0, 0, ["id", "name", "note"])
    data.write_row(1, 0, [1, "  padded  ", "tab\there"])
    data.write_row(2, 0, [2, "emoji \U0001F600", "ctrl\x01char"])
    data.write_row(3, 0, [3, "_x0041_ literal", "line\nbreak"])
    bold = wb.add_format({"bold": True})
    data.write_rich_string(4, 1, "rich ", bold, "bold", " text")
    data.write_number(4, 0, 4)
    data.write_formula(4, 2, '=B5&"!"', None, "rich bold text!")

    hidden = wb.add_worksheet("Hidden")
    hidden.write(0, 0, "h")
    hidden.write(1, 0, 1)
    hidden.hide()

    tables = wb.add_worksheet("Tables")
    tables.write(0, 0, "title above the table")
    tables.add_table("B3:D6", {
        "name": "Sales",
        "columns": [{"header": "region"}, {"header": "amount", "total_function": "sum"}, {"header": "units"}],
        "data": [["east", 10.5, 1], ["west", 20.25, 2]],
        "total_row": True,
    })
    wb.close()


def chartsheet():
    """A chart sheet as the first tab, then the worksheet it charts."""
    wb = xlsxwriter.Workbook(HERE / "chartsheet.xlsx")
    chart_sheet = wb.add_chartsheet("Chart")
    data = wb.add_worksheet("Data")
    data.write_row(0, 0, ["x", "y"])
    data.write_row(1, 0, [1, 10])
    data.write_row(2, 0, [2, 20])
    chart = wb.add_chart({"type": "line"})
    chart.add_series({"categories": "=Data!$A$2:$A$3", "values": "=Data!$B$2:$B$3"})
    chart_sheet.set_chart(chart)
    wb.close()


def date1904():
    wb = xlsxwriter.Workbook(HERE / "date1904.xlsx", {"date_1904": True})
    ws = wb.add_worksheet()
    fmt = wb.add_format({"num_format": "yyyy-mm-dd"})
    ws.write(0, 0, "d")
    ws.write_datetime(1, 0, dt.datetime(2024, 1, 2), fmt)
    ws.write_datetime(2, 0, dt.datetime(1904, 1, 1), fmt)
    wb.close()


def headers():
    """Blank and duplicate header names, and an unnamed empty column."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["a", None, "a", "b", "a", None, "a_1"])
    ws.append([1, 2, 3, None, 5, None, 7])
    ws.append([1.5, None, "x", None, True, None, 8])
    wb.save(HERE / "headers.xlsx")


def sparse():
    """Data starting at C3, empty rows between, a styled but empty trailing row."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws["C3"] = "k"
    ws["D3"] = "v"
    ws["C4"] = "a"
    ws["D4"] = 1
    ws["C7"] = "b"
    ws["D7"] = 2
    ws["C9"].number_format = "0.00"
    ws["C9"].font = openpyxl.styles.Font(bold=True)
    wb.save(HERE / "sparse.xlsx")


EDGE_SHEET = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<x:worksheet xmlns:x="http://purl.oclc.org/ooxml/spreadsheetml/main" xmlns:r="http://purl.oclc.org/ooxml/officeDocument/relationships">
<!-- no dimension; rows and cells without r; prefixed elements (strict namespace) -->
<x:sheetData>
<x:row><x:c t="inlineStr"><x:is><x:t>kind</x:t></x:is></x:c><x:c t="inlineStr"><x:is><x:t>value</x:t></x:is></x:c></x:row>
<x:row><x:c t="inlineStr"><x:is><x:t>inline</x:t></x:is></x:c><x:c t="inlineStr"><x:is><x:r><x:t xml:space="preserve">ri</x:t></x:r><x:r><x:rPr><x:b/></x:rPr><x:t>ch</x:t></x:r><x:rPh sb="0" eb="1"><x:t>RUBY</x:t></x:rPh></x:is></x:c></x:row>
<x:row><x:c t="inlineStr"><x:is><x:t>formula string</x:t></x:is></x:c><x:c t="str"><x:f>"a"&amp;"b"</x:f><x:v>a&amp;b &lt;tag&gt;</x:v></x:c></x:row>
<x:row r="5"><x:c r="A5" t="inlineStr"><x:is><x:t>iso date</x:t></x:is></x:c><x:c r="B5" t="d"><x:v>2024-02-29T13:45:00</x:v></x:c></x:row>
<x:row r="6"><x:c r="A6" t="inlineStr"><x:is><x:t>spill</x:t></x:is></x:c><x:c r="B6" t="e"><x:f>A1:A2</x:f><x:v>#SPILL!</x:v></x:c></x:row>
<x:row r="7" spans="1:2"/>
<x:row r="8"><x:c r="A8" t="inlineStr"><x:is><x:t><![CDATA[cdata <raw> & text]]></x:t></x:is></x:c><x:c r="B8" t="inlineStr"><x:is><x:t>esc_x0009_tab</x:t></x:is></x:c></x:row>
<x:row r="9"><x:c r="A9" t="inlineStr"><x:is><x:t>empty v</x:t></x:is></x:c><x:c r="B9" s="0"/></x:row>
</x:sheetData>
</x:worksheet>
"""

EDGE_WORKBOOK = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://purl.oclc.org/ooxml/spreadsheetml/main" xmlns:r="http://purl.oclc.org/ooxml/officeDocument/relationships">
<workbookPr/>
<sheets><sheet name="Edge &amp; Co" sheetId="1" r:id="rId1"/></sheets>
</workbook>
"""

EDGE_WORKBOOK_RELS = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://purl.oclc.org/ooxml/officeDocument/relationships/worksheet" Target="/xl/worksheets/edge.xml"/>
</Relationships>
"""

EDGE_ROOT_RELS = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://purl.oclc.org/ooxml/officeDocument/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>
"""

EDGE_CONTENT_TYPES = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Default Extension="xml" ContentType="application/xml"/>
<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
<Override PartName="/xl/worksheets/edge.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>
"""


def edge():
    """Hand-written strict-namespace package: no styles, no shared strings, every cell type and odd XML."""
    with zipfile.ZipFile(HERE / "edge.xlsx", "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", EDGE_CONTENT_TYPES)
        z.writestr("_rels/.rels", EDGE_ROOT_RELS)
        z.writestr("xl/workbook.xml", EDGE_WORKBOOK)
        z.writestr("xl/_rels/workbook.xml.rels", EDGE_WORKBOOK_RELS)
        z.writestr("xl/worksheets/edge.xml", EDGE_SHEET)


if __name__ == "__main__":
    types_openpyxl()
    workbook_xlsxwriter()
    date1904()
    chartsheet()
    headers()
    sparse()
    edge()
    # LibreOffice's copies of the fixtures: .xls (Excel 97-2003, BIFF8), .ods and flat .fods.
    if shutil.which("soffice"):
        names = ["sparse", "types_openpyxl", "workbook_xlsxwriter", "date1904", "headers", "edge", "chartsheet"]
        subprocess.run(["soffice", "--headless", "--convert-to", "xls", "--outdir", str(HERE), *(str(HERE / f"{n}.xlsx") for n in names)],
                       check=True, capture_output=True, timeout=180)
        for fmt, sources in (("ods", ["sparse", "types_openpyxl", "workbook_xlsxwriter", "date1904", "headers"]),
                             ("fods", ["sparse", "types_openpyxl"])):
            subprocess.run(["soffice", "--headless", "--convert-to", fmt, "--outdir", str(HERE), *(str(HERE / f"{n}.xlsx") for n in sources)],
                           check=True, capture_output=True, timeout=180)
