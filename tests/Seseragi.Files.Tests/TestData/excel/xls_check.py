"""Compares the raw cells our .xls reader produced with xlrd's reading of the same workbooks.

Usage: python -I xls_check.py <cells.jsonl>
  Each line is a workbook ({"file", "date1904", "sheets": [[name, kind, visibility], ...]}) or a cell of a worksheet
  ({"sheet": index, "r", "c", "t": n|s|b|e, "v", "date": bool}), cells following their workbook line. xlrd reads every
  workbook too; per worksheet the non-empty cells must agree: kind, value (numbers exactly), and whether the cell's
  number format makes it a date. Empty text (formula "" results) is no cell on either side. Exits non-zero listing
  every mismatch. Needs xlrd, and olefile for version 4 compound files (4096-byte sectors), which xlrd misplaces: their
  workbook stream is taken out with olefile and handed to xlrd as a bare BIFF stream.
"""
import json
import sys

import olefile
import xlrd

failures = []
KIND = {xlrd.XL_CELL_TEXT: "s", xlrd.XL_CELL_NUMBER: "n", xlrd.XL_CELL_DATE: "n", xlrd.XL_CELL_BOOLEAN: "b", xlrd.XL_CELL_ERROR: "e"}
VISIBILITY = {0: "Visible", 1: "Hidden", 2: "VeryHidden"}


def theirs(sheet):
    cells = {}
    for r in range(sheet.nrows):
        for c in range(sheet.ncols):
            cell = sheet.cell(r, c)
            kind = KIND.get(cell.ctype)
            if kind is None or (kind == "s" and cell.value == ""):
                continue
            value = cell.value
            if kind == "b":
                value = bool(value)
            elif kind == "e":
                value = None
            cells[(r, c)] = (kind, value, cell.ctype == xlrd.XL_CELL_DATE)
    return cells


def compare(path, info, ours):
    with open(path, "rb") as f:
        major = int.from_bytes(f.read(28)[26:28], "little")
    if major == 4:
        with olefile.OleFileIO(path, raise_defects=olefile.DEFECT_INCORRECT) as ole:
            book = xlrd.open_workbook(file_contents=ole.openstream("Workbook").read())
    else:
        book = xlrd.open_workbook(path)
    if book.datemode != (1 if info["date1904"] else 0):
        failures.append(f"{path}: datemode {book.datemode}, ours 1904={info['date1904']}")
    worksheets = [s for s in info["sheets"] if s[1] == "Worksheet"]
    names = [s.name for s in book.sheets()]
    if [s[0] for s in worksheets] != names:
        failures.append(f"{path}: xlrd worksheets {names}, ours {[s[0] for s in worksheets]}")
        return
    for k, sheet in enumerate(book.sheets()):
        if VISIBILITY[sheet.visibility] != worksheets[k][2]:
            failures.append(f"{path}[{sheet.name}]: visibility {sheet.visibility}, ours {worksheets[k][2]}")
        index = info["sheets"].index(worksheets[k])
        mine = ours.get(index, {})
        expected = theirs(sheet)
        for key in sorted(set(mine) | set(expected)):
            a, b = mine.get(key), expected.get(key)
            if a != b:
                failures.append(f"{path}[{sheet.name}] {xlrd.cellname(*key)}: ours {a!r}, xlrd {b!r}")


def main():
    current, info, ours = None, None, {}
    with open(sys.argv[1], encoding="utf-8") as f:
        for line in f:
            item = json.loads(line)
            if "file" in item:
                if current:
                    compare(current, info, ours)
                current, info, ours = item["file"], item, {}
            else:
                ours.setdefault(item["sheet"], {})[(item["r"], item["c"])] = (item["t"], item["v"], item["date"])
    if current:
        compare(current, info, ours)
    if failures:
        print("\n".join(failures[:200]))
        print(f"{len(failures)} mismatches")
        sys.exit(1)


main()
