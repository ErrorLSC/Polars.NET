# calamine's test workbooks

These workbooks come from the `tests/` directory of [calamine](https://github.com/tafia/calamine)
(© 2016 Johann Tuffe, MIT License, see `LICENSE-MIT.md` here).

* The .xlsb files were written by Microsoft Excel, which no open-source library can stand in for when it comes to
  .xlsb. `XlsbReadTests` reads them; the opt-in interop test compares the reader with pyxlsb, Python Polars and
  LibreOffice's .xlsx copies of them.
* The .xls files are Excel's and other writers' workbooks, BIFF5 ones and malformed ones (truncated or hostile
  compound files) included. `XlsReadTests` reads them; the opt-in interop tests compare the BIFF8 ones cell by cell
  with xlrd and with LibreOffice's .xlsx copies. `adhocallbabynames1996to2016.xls` (18.8 MB) is left out.

* The .ods files were written by LibreOffice (and by hand, for the malformed ones). `OdsReadTests` reads them; the
  opt-in interop test compares the reader with LibreOffice's .xlsx copies of them.

To refresh: download `https://raw.githubusercontent.com/tafia/calamine/master/tests/<name>` for each file here.
