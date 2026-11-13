# calamine's .xlsb test workbooks

These binary workbooks come from the `tests/` directory of [calamine](https://github.com/tafia/calamine)
(© 2016 Johann Tuffe, MIT License, see `LICENSE-MIT.md` here). They were written by Microsoft Excel, which no
open-source library can stand in for when it comes to .xlsb. `XlsbReadTests` reads them; the opt-in interop test
compares the reader with pyxlsb, Python Polars and LibreOffice's .xlsx copies of them.

To refresh: download `https://raw.githubusercontent.com/tafia/calamine/master/tests/<name>.xlsb` for each file here.
