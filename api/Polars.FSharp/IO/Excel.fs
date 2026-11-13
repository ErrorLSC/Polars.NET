namespace Polars.FSharp

open System.IO
open System.IO.Compression
open Polars.NET.Core.Wrappers

[<AutoOpen>]
module Excel =
    let private schemaHandle (schema: PolarsSchema option) =
        match schema with
        | Some s -> s.Handle
        | None -> null

    let private toArray (columns: seq<string> option) =
        match columns with
        | Some c -> Seq.toArray c
        | None -> null

    type DataFrame with
        /// <summary>
        /// Read a sheet of an Excel workbook into a DataFrame. .xlsx, .xlsb, .xls, .ods and flat .fods workbooks are
        /// streamed by the managed reader of Kaminari.Seseragi.Files (the format is detected from the content).
        /// Column types are inferred from the first inferSchemaLen rows (whole numbers Int64, other numbers Float64,
        /// dates Date, dates with times Datetime(μs), times Time, elapsed times Duration(μs), other mixes String); a
        /// later value that does not fit raises an error naming its cell. Error values read as nulls.
        /// </summary>
        /// <param name="path">Path to the workbook.</param>
        /// <param name="sheetName">Name of the sheet to read (case-insensitive). Takes precedence over sheetIndex.</param>
        /// <param name="sheetIndex">Index of the sheet to read (0-based tab position; hidden sheets and chart sheets count, and a chart sheet cannot be read). Default is 0.</param>
        /// <param name="schema">Column types by name instead of inferred ones.</param>
        /// <param name="hasHeader">The first row with a value holds the column names. Default is true.</param>
        /// <param name="inferSchemaLen">Rows the column types are inferred from (0 reads every column as String). Default is 100.</param>
        /// <param name="dropEmptyRows">Skip rows without values. Default is true.</param>
        /// <param name="raiseIfEmpty">Raise an error if the sheet has no data rows. Default is true.</param>
        /// <param name="columns">Columns to read, by name (in sheet order).</param>
        /// <param name="dropEmptyColumns">Leave out columns without a header name and without values in the first rows. Default is true.</param>
        /// <param name="tableName">Read a table by name instead of a sheet. .xlsx and .xlsb only (.xls and .ods have none).</param>
        static member ReadExcel(path: string,
                                ?sheetName: string,
                                ?sheetIndex: uint64,
                                ?schema: PolarsSchema,
                                ?hasHeader: bool,
                                ?inferSchemaLen: uint64,
                                ?dropEmptyRows: bool,
                                ?raiseIfEmpty: bool,
                                ?columns: seq<string>,
                                ?dropEmptyColumns: bool,
                                ?tableName: string) : DataFrame =
            new DataFrame(
                PolarsWrapper.ReadExcel(
                    path,
                    Option.toObj sheetName,
                    defaultArg sheetIndex 0UL,
                    schemaHandle schema,
                    defaultArg hasHeader true,
                    defaultArg inferSchemaLen 100UL,
                    defaultArg dropEmptyRows true,
                    defaultArg raiseIfEmpty true,
                    toArray columns,
                    defaultArg dropEmptyColumns true,
                    Option.toObj tableName))

        /// <summary>
        /// Read a sheet of a workbook in a stream into a DataFrame (the stream is left open). The parameters
        /// are those of the path overload.
        /// </summary>
        static member ReadExcel(stream: Stream,
                                ?sheetName: string,
                                ?sheetIndex: uint64,
                                ?schema: PolarsSchema,
                                ?hasHeader: bool,
                                ?inferSchemaLen: uint64,
                                ?dropEmptyRows: bool,
                                ?raiseIfEmpty: bool,
                                ?columns: seq<string>,
                                ?dropEmptyColumns: bool,
                                ?tableName: string) : DataFrame =
            new DataFrame(
                PolarsWrapper.ReadExcel(
                    stream,
                    Option.toObj sheetName,
                    defaultArg sheetIndex 0UL,
                    schemaHandle schema,
                    defaultArg hasHeader true,
                    defaultArg inferSchemaLen 100UL,
                    defaultArg dropEmptyRows true,
                    defaultArg raiseIfEmpty true,
                    toArray columns,
                    defaultArg dropEmptyColumns true,
                    Option.toObj tableName))

        /// <summary>
        /// Write the DataFrame as one sheet of an .xlsx workbook (managed streaming writer). Integers and decimals are
        /// numbers when a double holds them exactly, text otherwise; NaN and infinities become #NUM! errors; temporal
        /// values are serial numbers with the given formats (zoned datetimes as their wall-clock time). Values Excel
        /// cannot hold are refused (binary and nested columns, sub-microsecond times, dates before 1900, strings over
        /// 32,767 characters, more than 1,048,576 rows).
        /// </summary>
        /// <param name="path">Destination file (a failure deletes the partial file).</param>
        /// <param name="sheetName">Sheet name (default "Sheet1").</param>
        /// <param name="dateFormat">Number format of Date columns (default "yyyy-mm-dd").</param>
        /// <param name="datetimeFormat">Number format of Datetime columns (default "yyyy-mm-dd hh:mm:ss").</param>
        /// <param name="timeFormat">Number format of Time columns (default "hh:mm:ss").</param>
        /// <param name="durationFormat">Number format of Duration columns (default "[h]:mm:ss").</param>
        /// <param name="includeHeader">Write the column names as the first row. Default is true.</param>
        /// <param name="autofit">Set column widths from the header and the first 100 rows.</param>
        /// <param name="freezeHeader">Keep the header row in view when scrolling.</param>
        /// <param name="compressionLevel">Zip compression: Optimal (default, the smallest files) or Fastest (about three
        /// times faster, about 20% larger files).</param>
        member this.WriteExcel(path: string,
                               ?sheetName: string,
                               ?dateFormat: string,
                               ?datetimeFormat: string,
                               ?timeFormat: string,
                               ?durationFormat: string,
                               ?includeHeader: bool,
                               ?autofit: bool,
                               ?freezeHeader: bool,
                               ?compressionLevel: CompressionLevel) =
            PolarsWrapper.WriteExcel(
                this.Handle, path, Option.toObj sheetName, Option.toObj dateFormat, Option.toObj datetimeFormat,
                Option.toObj timeFormat, Option.toObj durationFormat, defaultArg includeHeader true,
                defaultArg autofit false, defaultArg freezeHeader false,
                defaultArg compressionLevel CompressionLevel.Optimal)

        /// <summary>Write the DataFrame as one sheet of an .xlsx workbook into a stream (flushed, left open).</summary>
        member this.WriteExcel(stream: Stream,
                               ?sheetName: string,
                               ?dateFormat: string,
                               ?datetimeFormat: string,
                               ?timeFormat: string,
                               ?durationFormat: string,
                               ?includeHeader: bool,
                               ?autofit: bool,
                               ?freezeHeader: bool,
                               ?compressionLevel: CompressionLevel) =
            PolarsWrapper.WriteExcel(
                this.Handle, stream, Option.toObj sheetName, Option.toObj dateFormat, Option.toObj datetimeFormat,
                Option.toObj timeFormat, Option.toObj durationFormat, defaultArg includeHeader true,
                defaultArg autofit false, defaultArg freezeHeader false,
                defaultArg compressionLevel CompressionLevel.Optimal)

    type LazyFrame with
        /// <summary>
        /// Lazily scan a sheet of a workbook: the header and the first inferSchemaLen rows are read now for the
        /// schema; the sheet is streamed each time the query runs. The parameters are those of DataFrame.ReadExcel.
        /// </summary>
        static member ScanExcel(path: string,
                                ?sheetName: string,
                                ?sheetIndex: uint64,
                                ?schema: PolarsSchema,
                                ?hasHeader: bool,
                                ?inferSchemaLen: uint64,
                                ?dropEmptyRows: bool,
                                ?raiseIfEmpty: bool,
                                ?columns: seq<string>,
                                ?dropEmptyColumns: bool,
                                ?tableName: string) : LazyFrame =
            new LazyFrame(
                PolarsWrapper.ScanExcel(
                    path,
                    Option.toObj sheetName,
                    defaultArg sheetIndex 0UL,
                    schemaHandle schema,
                    defaultArg hasHeader true,
                    defaultArg inferSchemaLen 100UL,
                    defaultArg dropEmptyRows true,
                    defaultArg raiseIfEmpty true,
                    toArray columns,
                    defaultArg dropEmptyColumns true,
                    Option.toObj tableName))

        /// <summary>
        /// Run the query on the streaming engine and write the result as one sheet of an .xlsx workbook into a stream
        /// as batches arrive. The stream is flushed but not disposed.
        /// </summary>
        member this.SinkExcel(stream: Stream,
                              ?sheetName: string,
                              ?dateFormat: string,
                              ?datetimeFormat: string,
                              ?timeFormat: string,
                              ?durationFormat: string,
                              ?includeHeader: bool,
                              ?autofit: bool,
                              ?freezeHeader: bool,
                              ?compressionLevel: CompressionLevel) : unit =
            use sink =
                new PolarsWrapper.ExcelBatchSink(
                    stream, Option.toObj sheetName, Option.toObj dateFormat, Option.toObj datetimeFormat,
                    Option.toObj timeFormat, Option.toObj durationFormat, defaultArg includeHeader true,
                    defaultArg autofit false, defaultArg freezeHeader false,
                    defaultArg compressionLevel CompressionLevel.Optimal)
            let newHandle = PolarsWrapper.SinkBatches(this.CloneHandle(), System.Action<Apache.Arrow.RecordBatch>(sink.Write))
            use lfRes = new LazyFrame(newHandle)
            use _ = lfRes.Collect(Engine.Streaming)
            sink.Complete(this.Handle)

        /// <summary>
        /// Run the query on the streaming engine and write the result to an .xlsx file as batches arrive (a failure
        /// deletes the partial file).
        /// </summary>
        member this.SinkExcel(path: string,
                              ?sheetName: string,
                              ?dateFormat: string,
                              ?datetimeFormat: string,
                              ?timeFormat: string,
                              ?durationFormat: string,
                              ?includeHeader: bool,
                              ?autofit: bool,
                              ?freezeHeader: bool,
                              ?compressionLevel: CompressionLevel) : unit =
            try
                use stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None)
                this.SinkExcel(
                    stream, ?sheetName = sheetName, ?dateFormat = dateFormat, ?datetimeFormat = datetimeFormat,
                    ?timeFormat = timeFormat, ?durationFormat = durationFormat, ?includeHeader = includeHeader,
                    ?autofit = autofit, ?freezeHeader = freezeHeader, ?compressionLevel = compressionLevel)
            with _ ->
                File.Delete path
                reraise ()
