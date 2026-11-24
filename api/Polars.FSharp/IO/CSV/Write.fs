namespace Polars.FSharp

open System.IO
open System.IO.Pipelines
open System.Threading
open System.Threading.Tasks
open Polars.NET.Core.Csv
open Polars.NET.Core.IO

[<AutoOpen>]
module CsvWrite =
    /// The parameters of a CSV write or sink, as given (None: the default).
    type internal CsvWriteParams =
        { IncludeHeader: bool option
          IncludeBom: bool option
          Separator: char option
          QuoteChar: char option
          QuoteStyle: QuoteStyle option
          NullValue: string option
          LineTerminator: string option
          FloatScientific: bool option
          FloatPrecision: int option
          DecimalComma: bool option
          DateFormat: string option
          TimeFormat: string option
          DatetimeFormat: string option
          Compression: ExternalCompression option
          CompressionLevel: int option
          Encoding: CsvEncoding option
          ThousandsSeparator: string option }

        static member Empty =
            { IncludeHeader = None; IncludeBom = None; Separator = None; QuoteChar = None; QuoteStyle = None; NullValue = None
              LineTerminator = None; FloatScientific = None; FloatPrecision = None; DecimalComma = None; DateFormat = None
              TimeFormat = None; DatetimeFormat = None; Compression = None; CompressionLevel = None; Encoding = None
              ThousandsSeparator = None }

        /// The parameters of the memory and stream overloads (floats: -1 unset, 0 false, 1 true; precision -1 unset; level 0 by default).
        static member OfBytes(includeBom, includeHeader, compressionCode, compressionLevel: int option, dateFormat, timeFormat, datetimeFormat,
                              floatScientific: int option, floatPrecision: int option, decimalComma, separator: byte option, quoteChar: byte option,
                              nullValue, lineTerminator, quoteStyle, encoding, thousandsSeparator) =
            { IncludeHeader = includeHeader; IncludeBom = includeBom; Separator = separator |> Option.map char
              QuoteChar = quoteChar |> Option.map char; QuoteStyle = quoteStyle; NullValue = nullValue; LineTerminator = lineTerminator
              FloatScientific = floatScientific |> Option.bind (function 0 -> Some false | 1 -> Some true | _ -> None)
              FloatPrecision = floatPrecision |> Option.filter (fun p -> p >= 0); DecimalComma = decimalComma; DateFormat = dateFormat
              TimeFormat = timeFormat; DatetimeFormat = datetimeFormat; Compression = compressionCode
              CompressionLevel = Some(defaultArg compressionLevel 0); Encoding = encoding; ThousandsSeparator = thousandsSeparator }

        /// The managed writer's arguments.
        member p.ToArgs() =
            let encoding = defaultArg p.Encoding CsvEncoding.UTF8
            CsvWriteArgs(
                IncludeHeader = defaultArg p.IncludeHeader true,
                IncludeBom = defaultArg p.IncludeBom false,
                Separator = defaultArg p.Separator ',',
                QuoteChar = defaultArg p.QuoteChar '"',
                QuoteStyle = (defaultArg p.QuoteStyle QuoteStyle.Necessary).ToNative(),
                NullValue = Option.toObj p.NullValue,
                LineTerminator = defaultArg p.LineTerminator "\n",
                FloatScientific = Option.toNullable p.FloatScientific,
                FloatPrecision = Option.toNullable p.FloatPrecision,
                DecimalComma = defaultArg p.DecimalComma false,
                DateFormat = Option.toObj p.DateFormat,
                TimeFormat = Option.toObj p.TimeFormat,
                DatetimeFormat = Option.toObj p.DatetimeFormat,
                ThousandsSeparator = Option.toObj p.ThousandsSeparator,
                Encoding = encoding.Given,
                LossyEncoding = encoding.IsLossy,
                Compression = (defaultArg p.Compression ExternalCompression.Uncompressed).ToNative(),
                CompressionLevel = defaultArg p.CompressionLevel -1)

    type LazyFrame with
        /// <summary>
        /// Run the query and write its rows as CSV to a file, local or on a cloud store, batch by batch as the streaming
        /// engine produces them (larger than memory works): encoded on every core, published whole once written (a
        /// failed sink leaves no file).
        /// </summary>
        /// <param name="path">The output file path (<c>s3://…</c>, <c>az://…</c>, <c>gs://…</c> on a store).</param>
        /// <param name="includeHeader">Whether to write the header row. Defaults to true.</param>
        /// <param name="includeBom">Whether to start with the encoding's byte order mark. Defaults to false.</param>
        /// <param name="separator">The field separator. Defaults to ','.</param>
        /// <param name="quoteChar">The quote. Defaults to '"'.</param>
        /// <param name="quoteStyle">When fields are quoted. Defaults to Necessary.</param>
        /// <param name="nullValue">The text of nulls. Defaults to an empty field.</param>
        /// <param name="lineTerminator">The line end. Defaults to "\n".</param>
        /// <param name="floatScientific">Floats always (true) or never (false) in scientific notation; by default as is shortest.</param>
        /// <param name="floatPrecision">Floats with this many decimals; by default with the digits that read back the same value.</param>
        /// <param name="decimalComma">',' as the decimal separator. Defaults to false.</param>
        /// <param name="dateFormat">The format of Date columns (strftime). ISO 8601 by default.</param>
        /// <param name="timeFormat">The format of Time columns (strftime). ISO 8601 by default.</param>
        /// <param name="datetimeFormat">The format of Datetime columns (strftime). ISO 8601 by default.</param>
        /// <param name="checkExtension">Whether the path's extension must fit the compression (.gz, .zst). Defaults to true.</param>
        /// <param name="compression">Compression of the whole file (Gzip/Zstd). Defaults to none.</param>
        /// <param name="compressionLevel">The compression level; -1 (default) for the codec's default.</param>
        /// <param name="maintainOrder">Whether the rows keep the query's order. Defaults to true.</param>
        /// <param name="syncOnClose">Whether the file is flushed to the disk before it is published. Defaults to NoSync.</param>
        /// <param name="mkdir">Create the file's directory when it is missing. Defaults to false.</param>
        /// <param name="batchSize">Accepted for compatibility; the writer sizes its units itself.</param>
        /// <param name="cloudOptions">Options for cloud storage.</param>
        /// <param name="encoding">The output's encoding (UTF-8 by default; see <see cref="CsvEncoding"/>). Strict: a character it cannot hold is an error naming the row, unless lossy.</param>
        /// <param name="thousandsSeparator">A digit group separator written in integers and floats ("." with decimalComma, " ", "'"). Off by default.</param>
        /// <remarks>Durations are written as ISO 8601 (PT1.5S). Errors are <c>CsvWriteException</c>, naming the row and the column.</remarks>
        member this.SinkCsv
            (
                path: string,
                ?includeHeader: bool,
                ?includeBom: bool,
                ?separator: char,
                ?quoteChar: char,
                ?quoteStyle: QuoteStyle,
                ?nullValue: string,
                ?lineTerminator: string,
                ?floatScientific: bool,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?checkExtension: bool,
                ?compression: ExternalCompression,
                ?compressionLevel: int,
                ?maintainOrder: bool,
                ?syncOnClose: SyncOnClose,
                ?mkdir: bool,
                ?batchSize: int,
                ?cloudOptions: CloudOptions,
                ?encoding: CsvEncoding,
                ?thousandsSeparator: string
            ) =
            ignore batchSize
            let args =
                { CsvWriteParams.Empty with
                    IncludeHeader = includeHeader; IncludeBom = includeBom; Separator = separator; QuoteChar = quoteChar
                    QuoteStyle = quoteStyle; NullValue = nullValue; LineTerminator = lineTerminator; FloatScientific = floatScientific
                    FloatPrecision = floatPrecision; DecimalComma = decimalComma; DateFormat = dateFormat; TimeFormat = timeFormat
                    DatetimeFormat = datetimeFormat; Compression = compression; CompressionLevel = compressionLevel
                    Encoding = encoding; ThousandsSeparator = thousandsSeparator }
            ManagedCsv.SinkCsv(
                this.CloneHandle(), path, args.ToArgs(), defaultArg checkExtension true, defaultArg maintainOrder true,
                (defaultArg syncOnClose SyncOnClose.NoSync).ToNative(), defaultArg mkdir false, CloudOptions.ToCloudArgs cloudOptions)

        /// <summary>
        /// Run the query and write its rows as a dataset of CSV files under <paramref name="path"/>, in Hive partitions of
        /// <paramref name="partitionBy"/> (<c>key=value</c> directories), batch by batch as the streaming engine produces
        /// them. Files are named <c>part-{index:D5}-{write id}.csv</c> (<c>.gz</c>, <c>.zst</c> when compressed), each with
        /// its header, each published whole.
        /// </summary>
        /// <param name="path">The dataset's root directory.</param>
        /// <param name="partitionBy">The columns the rows are partitioned by.</param>
        /// <param name="includeKeys">Whether the files also hold the key columns. Defaults to true.</param>
        /// <param name="keysPreGrouped">The rows come grouped by their keys: each partition's files are finished as its keys end. Rows not grouped write more files, never wrong ones.</param>
        /// <param name="maxRowsPerFile">The most rows a file holds; 0 means no limit.</param>
        /// <param name="approxBytesPerFile">About how many bytes a file holds; 0 means no limit.</param>
        /// <param name="checkExtension">Unused: the files are named by the writer.</param>
        /// <param name="mkdir">Unused: the dataset's directories are created.</param>
        /// <param name="existingData">What happens to files under <paramref name="path"/> already (an error by default).</param>
        member this.SinkCsvPartitioned
            (
                path: string,
                partitionBy: Selector,
                ?includeKeys: bool,
                ?keysPreGrouped: bool,
                ?maxRowsPerFile: int,
                ?approxBytesPerFile: int64,
                ?includeHeader: bool,
                ?includeBom: bool,
                ?separator: char,
                ?quoteChar: char,
                ?quoteStyle: QuoteStyle,
                ?nullValue: string,
                ?lineTerminator: string,
                ?floatScientific: bool,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?checkExtension: bool,
                ?compression: ExternalCompression,
                ?compressionLevel: int,
                ?maintainOrder: bool,
                ?syncOnClose: SyncOnClose,
                ?mkdir: bool,
                ?batchSize: int,
                ?cloudOptions: CloudOptions,
                ?encoding: CsvEncoding,
                ?thousandsSeparator: string,
                ?existingData: ExistingData
            ) =
            ignore (checkExtension, mkdir, batchSize)
            let maxRows = defaultArg maxRowsPerFile 0
            let approxBytes = defaultArg approxBytesPerFile 0L
            // A copy of the selector, as the C# API takes one: the caller's stays untouched whatever the call does with it.
            let keys =
                use selector = partitionBy.Clone()
                ManagedFrames.SelectedColumns(this.Handle, selector.Handle)
            let args =
                { CsvWriteParams.Empty with
                    IncludeHeader = includeHeader; IncludeBom = includeBom; Separator = separator; QuoteChar = quoteChar
                    QuoteStyle = quoteStyle; NullValue = nullValue; LineTerminator = lineTerminator; FloatScientific = floatScientific
                    FloatPrecision = floatPrecision; DecimalComma = decimalComma; DateFormat = dateFormat; TimeFormat = timeFormat
                    DatetimeFormat = datetimeFormat; Compression = compression; CompressionLevel = compressionLevel
                    Encoding = encoding; ThousandsSeparator = thousandsSeparator }
            ManagedCsv.SinkCsvPartitioned(
                this.CloneHandle(),
                path,
                keys,
                defaultArg includeKeys true,
                defaultArg keysPreGrouped false,
                (if maxRows > 0 then uint64 maxRows else 0UL),
                (if approxBytes > 0L then uint64 approxBytes else 0UL),
                args.ToArgs(),
                defaultArg maintainOrder true,
                (defaultArg syncOnClose SyncOnClose.NoSync).ToNative(),
                (defaultArg existingData ExistingData.Error).ToNative(),
                CloudOptions.ToCloudArgs cloudOptions)

        /// <summary>
        /// Run the query and return its rows as CSV bytes.
        /// </summary>
        /// <param name="includeBom">Whether to start with the encoding's byte order mark. Defaults to false.</param>
        /// <param name="includeHeader">Whether to write the header row. Defaults to true.</param>
        /// <param name="batchSize">Accepted for compatibility; the writer sizes its units itself.</param>
        /// <param name="checkExtension">Unused: there is no path.</param>
        /// <param name="compressionCode">Compression of the whole output (Gzip/Zstd). Defaults to none.</param>
        /// <param name="compressionLevel">The compression level (0 by default); -1 for the codec's default.</param>
        /// <param name="dateFormat">The format of Date columns (strftime). ISO 8601 by default.</param>
        /// <param name="timeFormat">The format of Time columns (strftime). ISO 8601 by default.</param>
        /// <param name="datetimeFormat">The format of Datetime columns (strftime). ISO 8601 by default.</param>
        /// <param name="floatScientific">Floats always (1) or never (0) in scientific notation; -1 (default) as is shortest.</param>
        /// <param name="floatPrecision">Floats with this many decimals; -1 (default) with the digits that read back the same value.</param>
        /// <param name="decimalComma">',' as the decimal separator. Defaults to false.</param>
        /// <param name="separator">The field separator. Defaults to ','.</param>
        /// <param name="quoteChar">The quote. Defaults to '"'.</param>
        /// <param name="nullValue">The text of nulls. Defaults to an empty field.</param>
        /// <param name="lineTerminator">The line end. Defaults to "\n".</param>
        /// <param name="quoteStyle">When fields are quoted. Defaults to Necessary.</param>
        /// <param name="maintainOrder">Whether the rows keep the query's order. Defaults to true.</param>
        /// <param name="encoding">The output's encoding (UTF-8 by default; see <see cref="CsvEncoding"/>).</param>
        /// <param name="thousandsSeparator">A digit group separator written in integers and floats. Off by default.</param>
        member this.SinkCsvMemory
            (
                ?includeBom: bool,
                ?includeHeader: bool,
                ?batchSize: int,
                ?checkExtension: bool,
                ?compressionCode: ExternalCompression,
                ?compressionLevel: int,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?floatScientific: int,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?separator: byte,
                ?quoteChar: byte,
                ?nullValue: string,
                ?lineTerminator: string,
                ?quoteStyle: QuoteStyle,
                ?maintainOrder: bool,
                ?encoding: CsvEncoding,
                ?thousandsSeparator: string
            ) : byte[] =
            ignore (batchSize, checkExtension)
            let args =
                CsvWriteParams.OfBytes(includeBom, includeHeader, compressionCode, compressionLevel, dateFormat, timeFormat, datetimeFormat,
                    floatScientific, floatPrecision, decimalComma, separator, quoteChar, nullValue, lineTerminator, quoteStyle, encoding,
                    thousandsSeparator)
            ManagedCsv.SinkCsvToMemory(this.CloneHandle(), args.ToArgs(), defaultArg maintainOrder true)

        /// <summary>
        /// Run the query and return its rows as CSV in native memory, handed over without a copy.
        /// </summary>
        member this.SinkCsvNativeMemory
            (
                ?includeBom: bool,
                ?includeHeader: bool,
                ?batchSize: int,
                ?checkExtension: bool,
                ?compressionCode: ExternalCompression,
                ?compressionLevel: int,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?floatScientific: int,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?separator: byte,
                ?quoteChar: byte,
                ?nullValue: string,
                ?lineTerminator: string,
                ?quoteStyle: QuoteStyle,
                ?maintainOrder: bool,
                ?encoding: CsvEncoding,
                ?thousandsSeparator: string
            ) : Polars.NET.Core.NativeBuffer =
            ignore (batchSize, checkExtension)
            let args =
                CsvWriteParams.OfBytes(includeBom, includeHeader, compressionCode, compressionLevel, dateFormat, timeFormat, datetimeFormat,
                    floatScientific, floatPrecision, decimalComma, separator, quoteChar, nullValue, lineTerminator, quoteStyle, encoding,
                    thousandsSeparator)
            ManagedCsv.SinkCsvNativeMemory(this.CloneHandle(), args.ToArgs(), defaultArg maintainOrder true)

        /// <summary>
        /// Run the query and write its rows as CSV into a <see cref="T:System.IO.Stream"/>, batch by batch (flushed, not disposed).
        /// </summary>
        member this.SinkCsv
            (
                stream: Stream,
                ?includeBom: bool,
                ?includeHeader: bool,
                ?batchSize: int,
                ?checkExtension: bool,
                ?compressionCode: ExternalCompression,
                ?compressionLevel: int,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?floatScientific: int,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?separator: byte,
                ?quoteChar: byte,
                ?nullValue: string,
                ?lineTerminator: string,
                ?quoteStyle: QuoteStyle,
                ?maintainOrder: bool,
                ?encoding: CsvEncoding,
                ?thousandsSeparator: string
            ) : unit =
            ignore (batchSize, checkExtension)
            let args =
                CsvWriteParams.OfBytes(includeBom, includeHeader, compressionCode, compressionLevel, dateFormat, timeFormat, datetimeFormat,
                    floatScientific, floatPrecision, decimalComma, separator, quoteChar, nullValue, lineTerminator, quoteStyle, encoding,
                    thousandsSeparator)
            ManagedCsv.SinkCsv(this.CloneHandle(), stream, args.ToArgs(), defaultArg maintainOrder true)

        /// <summary>
        /// Run the query and write its rows as CSV into a <see cref="T:System.IO.Stream"/> with asynchronous writes only, the
        /// engine read off the caller's thread (flushed, not disposed).
        /// </summary>
        member this.SinkCsvAsync
            (
                stream: Stream,
                ?includeBom: bool,
                ?includeHeader: bool,
                ?batchSize: int,
                ?checkExtension: bool,
                ?compressionCode: ExternalCompression,
                ?compressionLevel: int,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?floatScientific: int,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?separator: byte,
                ?quoteChar: byte,
                ?nullValue: string,
                ?lineTerminator: string,
                ?quoteStyle: QuoteStyle,
                ?maintainOrder: bool,
                ?cancellationToken: CancellationToken,
                ?encoding: CsvEncoding,
                ?thousandsSeparator: string
            ) : Task =
            ignore (batchSize, checkExtension)
            let args =
                CsvWriteParams.OfBytes(includeBom, includeHeader, compressionCode, compressionLevel, dateFormat, timeFormat, datetimeFormat,
                    floatScientific, floatPrecision, decimalComma, separator, quoteChar, nullValue, lineTerminator, quoteStyle, encoding,
                    thousandsSeparator)
            ManagedCsv.SinkCsvAsync(this.CloneHandle(), stream, args.ToArgs(), defaultArg maintainOrder true,
                defaultArg cancellationToken CancellationToken.None)

        /// <summary>
        /// Run the query and write its rows as CSV into a <see cref="T:System.IO.Pipelines.PipeWriter"/> with asynchronous
        /// flushes, the engine read off the caller's thread (flushed, not completed).
        /// </summary>
        member this.SinkCsvAsync
            (
                writer: PipeWriter,
                ?includeBom: bool,
                ?includeHeader: bool,
                ?batchSize: int,
                ?checkExtension: bool,
                ?compressionCode: ExternalCompression,
                ?compressionLevel: int,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?floatScientific: int,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?separator: byte,
                ?quoteChar: byte,
                ?nullValue: string,
                ?lineTerminator: string,
                ?quoteStyle: QuoteStyle,
                ?maintainOrder: bool,
                ?cancellationToken: CancellationToken,
                ?encoding: CsvEncoding,
                ?thousandsSeparator: string
            ) : Task =
            ignore (batchSize, checkExtension)
            let args =
                CsvWriteParams.OfBytes(includeBom, includeHeader, compressionCode, compressionLevel, dateFormat, timeFormat, datetimeFormat,
                    floatScientific, floatPrecision, decimalComma, separator, quoteChar, nullValue, lineTerminator, quoteStyle, encoding,
                    thousandsSeparator)
            ManagedCsv.SinkCsvAsync(this.CloneHandle(), writer, args.ToArgs(), defaultArg maintainOrder true,
                defaultArg cancellationToken CancellationToken.None)

    type DataFrame with
        /// <summary>
        /// Write the DataFrame as CSV to a file, local or on a cloud store: encoded on every core, published whole once
        /// written (a failed write leaves no file).
        /// </summary>
        /// <param name="path">The output file path (<c>s3://…</c>, <c>az://…</c>, <c>gs://…</c> on a store).</param>
        /// <param name="includeHeader">Whether to write the header row. Defaults to true.</param>
        /// <param name="includeBom">Whether to start with the encoding's byte order mark. Defaults to false.</param>
        /// <param name="separator">The field separator. Defaults to ','.</param>
        /// <param name="quoteChar">The quote. Defaults to '"'.</param>
        /// <param name="quoteStyle">When fields are quoted. Defaults to Necessary.</param>
        /// <param name="nullValue">The text of nulls. Defaults to an empty field.</param>
        /// <param name="lineTerminator">The line end. Defaults to "\n".</param>
        /// <param name="floatScientific">Floats always (true) or never (false) in scientific notation; by default as is shortest.</param>
        /// <param name="floatPrecision">Floats with this many decimals; by default with the digits that read back the same value.</param>
        /// <param name="decimalComma">',' as the decimal separator. Defaults to false.</param>
        /// <param name="dateFormat">The format of Date columns (strftime). ISO 8601 by default.</param>
        /// <param name="timeFormat">The format of Time columns (strftime). ISO 8601 by default.</param>
        /// <param name="datetimeFormat">The format of Datetime columns (strftime). ISO 8601 by default.</param>
        /// <param name="checkExtension">Whether the path's extension must fit the compression (.gz, .zst). Defaults to true.</param>
        /// <param name="compression">Compression of the whole file (Gzip/Zstd). Defaults to none.</param>
        /// <param name="compressionLevel">The compression level; -1 (default) for the codec's default.</param>
        /// <param name="maintainOrder">Accepted for compatibility: rows are written in order.</param>
        /// <param name="syncOnClose">Whether the file is flushed to the disk before it is published. Defaults to NoSync.</param>
        /// <param name="mkdir">Create the file's directory when it is missing. Defaults to false.</param>
        /// <param name="batchSize">Accepted for compatibility; the writer sizes its units itself.</param>
        /// <param name="cloudOptions">Options for cloud storage.</param>
        /// <param name="encoding">The output's encoding (UTF-8 by default; see <see cref="CsvEncoding"/>). Strict: a character it cannot hold is an error naming the row, unless lossy.</param>
        /// <param name="thousandsSeparator">A digit group separator written in integers and floats. Off by default.</param>
        /// <returns>The same DataFrame, for chaining.</returns>
        member this.WriteCsv
            (
                path: string,
                ?includeHeader: bool,
                ?includeBom: bool,
                ?separator: char,
                ?quoteChar: char,
                ?quoteStyle: QuoteStyle,
                ?nullValue: string,
                ?lineTerminator: string,
                ?floatScientific: bool,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?checkExtension: bool,
                ?compression: ExternalCompression,
                ?compressionLevel: int,
                ?maintainOrder: bool,
                ?syncOnClose: SyncOnClose,
                ?mkdir: bool,
                ?batchSize: int,
                ?cloudOptions: CloudOptions,
                ?encoding: CsvEncoding,
                ?thousandsSeparator: string
            ) =
            ignore (maintainOrder, batchSize)
            let args =
                { CsvWriteParams.Empty with
                    IncludeHeader = includeHeader; IncludeBom = includeBom; Separator = separator; QuoteChar = quoteChar
                    QuoteStyle = quoteStyle; NullValue = nullValue; LineTerminator = lineTerminator; FloatScientific = floatScientific
                    FloatPrecision = floatPrecision; DecimalComma = decimalComma; DateFormat = dateFormat; TimeFormat = timeFormat
                    DatetimeFormat = datetimeFormat; Compression = compression; CompressionLevel = compressionLevel
                    Encoding = encoding; ThousandsSeparator = thousandsSeparator }
            ManagedCsv.WriteCsv(
                this.Handle, path, args.ToArgs(), defaultArg checkExtension true, (defaultArg syncOnClose SyncOnClose.NoSync).ToNative(),
                defaultArg mkdir false, CloudOptions.ToCloudArgs cloudOptions)
            this

        /// <summary>
        /// Write the DataFrame as a dataset of CSV files under <paramref name="path"/>, in Hive partitions of
        /// <paramref name="partitionBy"/> (<c>key=value</c> directories); see <c>LazyFrame.SinkCsvPartitioned</c>.
        /// </summary>
        /// <returns>The same DataFrame, for chaining.</returns>
        member this.WriteCsvPartitioned
            (
                path: string,
                partitionBy: Selector,
                ?includeKeys: bool,
                ?keysPreGrouped: bool,
                ?maxRowsPerFile: int,
                ?approxBytesPerFile: int64,
                ?includeHeader: bool,
                ?includeBom: bool,
                ?separator: char,
                ?quoteChar: char,
                ?quoteStyle: QuoteStyle,
                ?nullValue: string,
                ?lineTerminator: string,
                ?floatScientific: bool,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?checkExtension: bool,
                ?compression: ExternalCompression,
                ?compressionLevel: int,
                ?maintainOrder: bool,
                ?syncOnClose: SyncOnClose,
                ?mkdir: bool,
                ?batchSize: int,
                ?cloudOptions: CloudOptions,
                ?encoding: CsvEncoding,
                ?thousandsSeparator: string,
                ?existingData: ExistingData
            ) =
            use lf = this.Lazy()
            lf.SinkCsvPartitioned(
                path,
                partitionBy,
                ?includeKeys = includeKeys,
                ?keysPreGrouped = keysPreGrouped,
                ?maxRowsPerFile = maxRowsPerFile,
                ?approxBytesPerFile = approxBytesPerFile,
                ?includeHeader = includeHeader,
                ?includeBom = includeBom,
                ?separator = separator,
                ?quoteChar = quoteChar,
                ?quoteStyle = quoteStyle,
                ?nullValue = nullValue,
                ?lineTerminator = lineTerminator,
                ?floatScientific = floatScientific,
                ?floatPrecision = floatPrecision,
                ?decimalComma = decimalComma,
                ?dateFormat = dateFormat,
                ?timeFormat = timeFormat,
                ?datetimeFormat = datetimeFormat,
                ?checkExtension = checkExtension,
                ?compression = compression,
                ?compressionLevel = compressionLevel,
                ?maintainOrder = maintainOrder,
                ?syncOnClose = syncOnClose,
                ?mkdir = mkdir,
                ?batchSize = batchSize,
                ?cloudOptions = cloudOptions,
                ?encoding = encoding,
                ?thousandsSeparator = thousandsSeparator,
                ?existingData = existingData)
            this

        /// <summary>
        /// The DataFrame as CSV bytes; parameters as <c>LazyFrame.SinkCsvMemory</c>.
        /// </summary>
        member this.WriteCsvMemory
            (
                ?includeBom: bool,
                ?includeHeader: bool,
                ?batchSize: int,
                ?checkExtension: bool,
                ?compressionCode: ExternalCompression,
                ?compressionLevel: int,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?floatScientific: int,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?separator: byte,
                ?quoteChar: byte,
                ?nullValue: string,
                ?lineTerminator: string,
                ?quoteStyle: QuoteStyle,
                ?maintainOrder: bool,
                ?encoding: CsvEncoding,
                ?thousandsSeparator: string
            ) : byte[] =
            ignore (batchSize, checkExtension, maintainOrder)
            let args =
                CsvWriteParams.OfBytes(includeBom, includeHeader, compressionCode, compressionLevel, dateFormat, timeFormat, datetimeFormat,
                    floatScientific, floatPrecision, decimalComma, separator, quoteChar, nullValue, lineTerminator, quoteStyle, encoding,
                    thousandsSeparator)
            ManagedCsv.WriteCsvToMemory(this.Handle, args.ToArgs())

        /// <summary>
        /// The DataFrame as CSV in native memory, handed over without a copy; parameters as <c>LazyFrame.SinkCsvMemory</c>.
        /// </summary>
        member this.WriteCsvNativeMemory
            (
                ?includeBom: bool,
                ?includeHeader: bool,
                ?batchSize: int,
                ?checkExtension: bool,
                ?compressionCode: ExternalCompression,
                ?compressionLevel: int,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?floatScientific: int,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?separator: byte,
                ?quoteChar: byte,
                ?nullValue: string,
                ?lineTerminator: string,
                ?quoteStyle: QuoteStyle,
                ?maintainOrder: bool,
                ?encoding: CsvEncoding,
                ?thousandsSeparator: string
            ) : Polars.NET.Core.NativeBuffer =
            ignore (batchSize, checkExtension, maintainOrder)
            let args =
                CsvWriteParams.OfBytes(includeBom, includeHeader, compressionCode, compressionLevel, dateFormat, timeFormat, datetimeFormat,
                    floatScientific, floatPrecision, decimalComma, separator, quoteChar, nullValue, lineTerminator, quoteStyle, encoding,
                    thousandsSeparator)
            ManagedCsv.WriteCsvNativeMemory(this.Handle, args.ToArgs())

        /// <summary>
        /// Write the DataFrame as CSV into a <see cref="T:System.IO.Stream"/> (flushed, not disposed); parameters as
        /// <c>LazyFrame.SinkCsvMemory</c>.
        /// </summary>
        /// <returns>The same DataFrame, for chaining.</returns>
        member this.WriteCsv(
            stream: Stream,
            ?includeBom: bool,
            ?includeHeader: bool,
            ?batchSize: int,
            ?checkExtension: bool,
            ?compressionCode: ExternalCompression,
            ?compressionLevel: int,
            ?dateFormat: string,
            ?timeFormat: string,
            ?datetimeFormat: string,
            ?floatScientific: int,
            ?floatPrecision: int,
            ?decimalComma: bool,
            ?separator: byte,
            ?quoteChar: byte,
            ?nullValue: string,
            ?lineTerminator: string,
            ?quoteStyle: QuoteStyle,
            ?maintainOrder: bool,
            ?encoding: CsvEncoding,
            ?thousandsSeparator: string
        ) : DataFrame =
            ignore (batchSize, checkExtension, maintainOrder)
            let args =
                CsvWriteParams.OfBytes(includeBom, includeHeader, compressionCode, compressionLevel, dateFormat, timeFormat, datetimeFormat,
                    floatScientific, floatPrecision, decimalComma, separator, quoteChar, nullValue, lineTerminator, quoteStyle, encoding,
                    thousandsSeparator)
            ManagedCsv.WriteCsv(this.Handle, stream, args.ToArgs())
            this

        /// <summary>
        /// Write the DataFrame as CSV into a <see cref="T:System.IO.Stream"/> with asynchronous writes only (flushed, not
        /// disposed); parameters as <c>LazyFrame.SinkCsvMemory</c>.
        /// </summary>
        member this.WriteCsvAsync(
            stream: Stream,
            ?includeBom: bool,
            ?includeHeader: bool,
            ?batchSize: int,
            ?checkExtension: bool,
            ?compressionCode: ExternalCompression,
            ?compressionLevel: int,
            ?dateFormat: string,
            ?timeFormat: string,
            ?datetimeFormat: string,
            ?floatScientific: int,
            ?floatPrecision: int,
            ?decimalComma: bool,
            ?separator: byte,
            ?quoteChar: byte,
            ?nullValue: string,
            ?lineTerminator: string,
            ?quoteStyle: QuoteStyle,
            ?maintainOrder: bool,
            ?cancellationToken: CancellationToken,
            ?encoding: CsvEncoding,
            ?thousandsSeparator: string
        ) : Task =
            ignore (batchSize, checkExtension, maintainOrder)
            let args =
                CsvWriteParams.OfBytes(includeBom, includeHeader, compressionCode, compressionLevel, dateFormat, timeFormat, datetimeFormat,
                    floatScientific, floatPrecision, decimalComma, separator, quoteChar, nullValue, lineTerminator, quoteStyle, encoding,
                    thousandsSeparator)
            ManagedCsv.WriteCsvAsync(this.Handle, stream, args.ToArgs(), defaultArg cancellationToken CancellationToken.None)

        /// <summary>
        /// Write the DataFrame as CSV into a <see cref="T:System.IO.Pipelines.PipeWriter"/> with asynchronous flushes
        /// (flushed, not completed); parameters as <c>LazyFrame.SinkCsvMemory</c>.
        /// </summary>
        member this.WriteCsvAsync(
            writer: PipeWriter,
            ?includeBom: bool,
            ?includeHeader: bool,
            ?batchSize: int,
            ?checkExtension: bool,
            ?compressionCode: ExternalCompression,
            ?compressionLevel: int,
            ?dateFormat: string,
            ?timeFormat: string,
            ?datetimeFormat: string,
            ?floatScientific: int,
            ?floatPrecision: int,
            ?decimalComma: bool,
            ?separator: byte,
            ?quoteChar: byte,
            ?nullValue: string,
            ?lineTerminator: string,
            ?quoteStyle: QuoteStyle,
            ?maintainOrder: bool,
            ?cancellationToken: CancellationToken,
            ?encoding: CsvEncoding,
            ?thousandsSeparator: string
        ) : Task =
            ignore (batchSize, checkExtension, maintainOrder)
            let args =
                CsvWriteParams.OfBytes(includeBom, includeHeader, compressionCode, compressionLevel, dateFormat, timeFormat, datetimeFormat,
                    floatScientific, floatPrecision, decimalComma, separator, quoteChar, nullValue, lineTerminator, quoteStyle, encoding,
                    thousandsSeparator)
            ManagedCsv.WriteCsvAsync(this.Handle, writer, args.ToArgs(), defaultArg cancellationToken CancellationToken.None)
