namespace Polars.FSharp

open Polars.NET.Core.Csv

[<AutoOpen>]
module CsvRead =
    open System.IO
    open System.Threading.Tasks

    /// The parameters of a CSV read or scan, as given (None: the default).
    type internal CsvReadParams =
        { Schema: PolarsSchema option
          DtypeOverride: PolarsSchema option
          HasHeader: bool option
          Separator: char option
          QuoteChar: char option
          EolChar: char option
          IgnoreErrors: bool option
          TryParseDates: bool option
          Rechunk: bool option
          RaiseIfEmpty: bool option
          Glob: bool option
          SkipRows: uint64 option
          SkipRowsAfterHeader: uint64 option
          SkipLines: uint64 option
          NRows: uint64 option
          InferSchemaLength: uint64 option
          InferSchemaFiles: uint64 option
          NThreads: uint64 option
          RowIndexName: string option
          RowIndexOffset: uint64 option
          IncludeFilePaths: string option
          Encoding: CsvEncoding option
          NullValues: seq<string> option
          MissingIsNull: bool option
          CommentPrefix: string option
          DecimalComma: bool option
          TruncateRaggedLines: bool option
          TrueValues: seq<string> option
          FalseValues: seq<string> option
          ThousandsSeparator: string option
          AllowMissingColumns: bool option
          IgnoreExtraColumns: bool option
          HivePartitioning: bool option }

        static member Empty =
            { Schema = None; DtypeOverride = None; HasHeader = None; Separator = None; QuoteChar = None; EolChar = None
              IgnoreErrors = None; TryParseDates = None; Rechunk = None; RaiseIfEmpty = None; Glob = None; SkipRows = None
              SkipRowsAfterHeader = None; SkipLines = None; NRows = None; InferSchemaLength = None; InferSchemaFiles = None
              NThreads = None; RowIndexName = None; RowIndexOffset = None; IncludeFilePaths = None; Encoding = None
              NullValues = None; MissingIsNull = None; CommentPrefix = None; DecimalComma = None; TruncateRaggedLines = None
              TrueValues = None; FalseValues = None; ThousandsSeparator = None; AllowMissingColumns = None
              IgnoreExtraColumns = None; HivePartitioning = None }

        /// The managed reader's arguments (the schemas borrowed: they stay the caller's).
        member p.ToArgs() =
            let handle (schema: PolarsSchema option) = match schema with Some s -> s.Handle | None -> null
            let strings (values: seq<string> option) = values |> Option.map (fun v -> Seq.toArray v :> System.Collections.Generic.IReadOnlyList<string>) |> Option.toObj
            let encoding = defaultArg p.Encoding CsvEncoding.UTF8
            CsvScanArgs(
                Schema = handle p.Schema,
                DtypeOverride = handle p.DtypeOverride,
                HasHeader = defaultArg p.HasHeader true,
                Separator = defaultArg p.Separator ',',
                QuoteChar = System.Nullable(defaultArg p.QuoteChar '"'),
                EolChar = defaultArg p.EolChar '\n',
                IgnoreErrors = defaultArg p.IgnoreErrors false,
                TryParseDates = defaultArg p.TryParseDates true,
                Rechunk = defaultArg p.Rechunk false,
                RaiseIfEmpty = defaultArg p.RaiseIfEmpty true,
                Glob = defaultArg p.Glob true,
                SkipRows = defaultArg p.SkipRows 0UL,
                SkipRowsAfterHeader = defaultArg p.SkipRowsAfterHeader 0UL,
                SkipLines = defaultArg p.SkipLines 0UL,
                NRows = Option.toNullable p.NRows,
                // 100 rows by default; a value above 16,777,216 (e.g. UInt64.MaxValue) infers from the whole file.
                InferSchemaLength = System.Nullable(defaultArg p.InferSchemaLength 100UL),
                // 10 files by default (UInt64.MaxValue too).
                InferSchemaFiles = Option.toNullable p.InferSchemaFiles,
                NThreads = Option.toNullable p.NThreads,
                RowIndexName = Option.toObj p.RowIndexName,
                RowIndexOffset = defaultArg p.RowIndexOffset 0UL,
                IncludeFilePaths = Option.toObj p.IncludeFilePaths,
                Encoding = encoding.Given,
                LossyEncoding = encoding.IsLossy,
                NullValues = strings p.NullValues,
                MissingIsNull = defaultArg p.MissingIsNull true,
                CommentPrefix = Option.toObj p.CommentPrefix,
                DecimalComma = defaultArg p.DecimalComma false,
                TruncateRaggedLines = defaultArg p.TruncateRaggedLines false,
                TrueValues = strings p.TrueValues,
                FalseValues = strings p.FalseValues,
                ThousandsSeparator = Option.toObj p.ThousandsSeparator,
                AllowMissingColumns = defaultArg p.AllowMissingColumns false,
                IgnoreExtraColumns = defaultArg p.IgnoreExtraColumns false,
                HivePartitioning = defaultArg p.HivePartitioning false)

    // The columns read, in their order (all when none or empty).
    let private asked (columns: seq<string> option) =
        match columns with
        | Some c ->
            let names = Seq.toArray c
            if names.Length > 0 then names else null
        | None -> null

    type LazyFrame with
        /// <summary>
        /// Lazily read CSV: a file, a directory of files, a glob, or any of them on a cloud store (one dataset: the files'
        /// columns united by name). The files are listed and the columns inferred now (from the first files); the data is
        /// read each time the query runs: only the columns it reads and the leading rows it keeps, streamed batch by batch.
        /// </summary>
        /// <param name="path">The file, directory or glob (<c>s3://…</c>, <c>az://…</c>, <c>gs://…</c> on a store).</param>
        /// <param name="schema">Every column and its type: nothing is inferred. With a header the columns are matched by name (the frame in the schema's order); without one, by position.</param>
        /// <param name="dtypeOverride">The types of some inferred columns, by name (names the files lack are passed over).</param>
        /// <param name="hasHeader">Whether the first row names the columns. Defaults to true.</param>
        /// <param name="separator">The field separator. Defaults to ','.</param>
        /// <param name="quoteChar">The quote. Defaults to '"'; '\000' reads without quoting.</param>
        /// <param name="eolChar">The line end: '\n' (the default) ends records at LF, CRLF or a CR alone; another character ends them at it.</param>
        /// <param name="ignoreErrors">A value its column cannot hold is a null instead of an error (malformed CSV still is an error).</param>
        /// <param name="tryParseDates">Infer dates, times and datetimes from text. Defaults to true.</param>
        /// <param name="lowMemory">Accepted for compatibility; reading streams its input whatever it says.</param>
        /// <param name="cache">Accepted for compatibility; it changes nothing.</param>
        /// <param name="glob">Expand <c>*</c>, <c>?</c> and <c>[…]</c> in the path. Defaults to true.</param>
        /// <param name="rechunk">Accepted for compatibility; a scan streams batches whatever it says.</param>
        /// <param name="raiseIfEmpty">Empty input is an error (else an empty frame). Defaults to true.</param>
        /// <param name="skipRows">Records skipped before the header (quote-aware). Defaults to 0.</param>
        /// <param name="skipRowsAfterHeader">Records skipped after the header. Defaults to 0.</param>
        /// <param name="skipLines">Raw lines skipped first, quotes not considered. Defaults to 0.</param>
        /// <param name="nRows">Stop after this many rows.</param>
        /// <param name="inferSchemaLength">Rows to infer the column types from (100 by default); a value above 16,777,216 infers them from the whole file; 0 reads every column as String.</param>
        /// <param name="inferSchemaFiles">The files the types are inferred from (10 by default, as dataframe engines take; UInt64.MaxValue also means 10).</param>
        /// <param name="nThreads">The threads decoding (all cores by default).</param>
        /// <param name="chunkSize">Accepted for compatibility; the reader sizes its units itself.</param>
        /// <param name="rowIndexName">If provided, adds a first column (UInt32) numbering the rows across the files.</param>
        /// <param name="rowIndexOffset">The row index's first number. Defaults to 0.</param>
        /// <param name="includeFilePaths">If provided, adds a last column with each row's file path.</param>
        /// <param name="encoding">The files' encoding (UTF-8 by default; Shift_JIS, GBK, Windows-1252, UTF-16 … see <see cref="CsvEncoding"/>).</param>
        /// <param name="nullValues">Values read as null.</param>
        /// <param name="missingIsNull">Empty unquoted fields are null (else empty strings). Defaults to true.</param>
        /// <param name="commentPrefix">Lines starting with this text are passed over.</param>
        /// <param name="decimalComma">',' is the decimal separator. Defaults to false.</param>
        /// <param name="truncateRaggedLines">Fields past the last column are dropped (else an error). Defaults to false.</param>
        /// <param name="cloudOptions">Options for cloud storage.</param>
        /// <param name="trueValues">Values read as true (localized booleans such as "VERDADEIRO"); they also steer inference.</param>
        /// <param name="falseValues">Values read as false.</param>
        /// <param name="thousandsSeparator">The digit group separator of numbers ("." with decimalComma, " ", "'", ","; groups of three are checked). Off by default.</param>
        /// <param name="allowMissingColumns">A file lacking some of the dataset's columns reads them as nulls (else an error).</param>
        /// <param name="ignoreExtraColumns">Columns of later files the first files lack are passed over (else an error).</param>
        /// <param name="hivePartitioning">Whether <c>key=value</c> directories of the paths are columns (off by default).</param>
        /// <remarks>
        /// The query's filters run on the batches read. Errors are <c>CsvFormatException</c> (malformed CSV) and
        /// <c>CsvValueException</c> (a value its column cannot hold), naming the file, the row and the column.
        /// </remarks>
        static member ScanCsv
            (
                path: string,
                ?schema: PolarsSchema,
                ?dtypeOverride: PolarsSchema,
                ?hasHeader: bool,
                ?separator: char,
                ?quoteChar: char,
                ?eolChar: char,
                ?ignoreErrors: bool,
                ?tryParseDates: bool,
                ?lowMemory: bool,
                ?cache: bool,
                ?glob: bool,
                ?rechunk: bool,
                ?raiseIfEmpty: bool,
                ?skipRows: uint64,
                ?skipRowsAfterHeader: uint64,
                ?skipLines: uint64,
                ?nRows: uint64,
                ?inferSchemaLength: uint64,
                ?inferSchemaFiles: uint64,
                ?nThreads: uint64,
                ?chunkSize: uint64,
                ?rowIndexName: string,
                ?rowIndexOffset: uint64,
                ?includeFilePaths: string,
                ?encoding: CsvEncoding,
                ?nullValues: seq<string>,
                ?missingIsNull: bool,
                ?commentPrefix: string,
                ?decimalComma: bool,
                ?truncateRaggedLines: bool,
                ?cloudOptions: CloudOptions,
                ?trueValues: seq<string>,
                ?falseValues: seq<string>,
                ?thousandsSeparator: string,
                ?allowMissingColumns: bool,
                ?ignoreExtraColumns: bool,
                ?hivePartitioning: bool
            ) : LazyFrame =
            ignore (lowMemory, cache, chunkSize)
            let args =
                { CsvReadParams.Empty with
                    Schema = schema; DtypeOverride = dtypeOverride; HasHeader = hasHeader; Separator = separator
                    QuoteChar = quoteChar; EolChar = eolChar; IgnoreErrors = ignoreErrors; TryParseDates = tryParseDates
                    Rechunk = rechunk; RaiseIfEmpty = raiseIfEmpty; Glob = glob; SkipRows = skipRows
                    SkipRowsAfterHeader = skipRowsAfterHeader; SkipLines = skipLines; NRows = nRows
                    InferSchemaLength = inferSchemaLength; InferSchemaFiles = inferSchemaFiles; NThreads = nThreads
                    RowIndexName = rowIndexName; RowIndexOffset = rowIndexOffset; IncludeFilePaths = includeFilePaths
                    Encoding = encoding; NullValues = nullValues; MissingIsNull = missingIsNull; CommentPrefix = commentPrefix
                    DecimalComma = decimalComma; TruncateRaggedLines = truncateRaggedLines; TrueValues = trueValues
                    FalseValues = falseValues; ThousandsSeparator = thousandsSeparator; AllowMissingColumns = allowMissingColumns
                    IgnoreExtraColumns = ignoreExtraColumns; HivePartitioning = hivePartitioning }
            new LazyFrame(ManagedCsv.ScanCsv(path, args.ToArgs(), CloudOptions.ToCloudArgs cloudOptions))

        // ---------------------------------------------------------
        // Scan CSV (Memory / Bytes)
        // ---------------------------------------------------------

        /// <summary>
        /// Lazily read CSV from an in-memory byte array.
        /// </summary>
        /// <remarks>The bytes are copied (into native memory the scan keeps); the <c>ReadOnlyMemory&lt;byte&gt;</c> overload reads them in place. glob and includeFilePaths are unused for memory.</remarks>
        static member ScanCsv(
                buffer: byte[],
                ?schema: PolarsSchema,
                ?dtypeOverride: PolarsSchema,
                ?hasHeader: bool,
                ?separator: char,
                ?quoteChar: char,
                ?eolChar: char,
                ?ignoreErrors: bool,
                ?tryParseDates: bool,
                ?lowMemory: bool,
                ?cache: bool,
                ?glob: bool,
                ?rechunk: bool,
                ?raiseIfEmpty: bool,
                ?skipRows: uint64,
                ?skipRowsAfterHeader: uint64,
                ?skipLines: uint64,
                ?nRows: uint64,
                ?inferSchemaLength: uint64,
                ?inferSchemaFiles: uint64,
                ?nThreads: uint64,
                ?chunkSize: uint64,
                ?rowIndexName: string,
                ?rowIndexOffset: uint64,
                ?includeFilePaths: string,
                ?encoding: CsvEncoding,
                ?nullValues: seq<string>,
                ?missingIsNull: bool,
                ?commentPrefix: string,
                ?decimalComma: bool,
                ?truncateRaggedLines: bool,
                ?trueValues: seq<string>,
                ?falseValues: seq<string>,
                ?thousandsSeparator: string
            ) : LazyFrame =
            ignore (lowMemory, cache, chunkSize, glob, includeFilePaths)
            if isNull buffer then nullArg (nameof buffer)
            let args =
                { CsvReadParams.Empty with
                    Schema = schema; DtypeOverride = dtypeOverride; HasHeader = hasHeader; Separator = separator
                    QuoteChar = quoteChar; EolChar = eolChar; IgnoreErrors = ignoreErrors; TryParseDates = tryParseDates
                    Rechunk = rechunk; RaiseIfEmpty = raiseIfEmpty; SkipRows = skipRows; SkipRowsAfterHeader = skipRowsAfterHeader
                    SkipLines = skipLines; NRows = nRows; InferSchemaLength = inferSchemaLength; InferSchemaFiles = inferSchemaFiles
                    NThreads = nThreads; RowIndexName = rowIndexName; RowIndexOffset = rowIndexOffset; Encoding = encoding
                    NullValues = nullValues; MissingIsNull = missingIsNull; CommentPrefix = commentPrefix; DecimalComma = decimalComma
                    TruncateRaggedLines = truncateRaggedLines; TrueValues = trueValues; FalseValues = falseValues
                    ThousandsSeparator = thousandsSeparator }
            new LazyFrame(ManagedCsv.ScanCsv(System.ReadOnlySpan<byte>(buffer), args.ToArgs()))

        /// <summary>
        /// Lazily read CSV from in-memory bytes, read in place each time the query runs.
        /// </summary>
        /// <remarks>Reads the memory in place, without a copy: it stays pinned until the last frame reading it is released, and must not change meanwhile. Free unmanaged memory only once it is unpinned (<c>MemoryManager.Unpin</c>): a query can still be finishing on a background thread right after its frames are released. glob and includeFilePaths are unused for memory.</remarks>
        static member ScanCsv
            (
                buffer: System.ReadOnlyMemory<byte>,
                ?schema: PolarsSchema,
                ?dtypeOverride: PolarsSchema,
                ?hasHeader: bool,
                ?separator: char,
                ?quoteChar: char,
                ?eolChar: char,
                ?ignoreErrors: bool,
                ?tryParseDates: bool,
                ?lowMemory: bool,
                ?cache: bool,
                ?glob: bool,
                ?rechunk: bool,
                ?raiseIfEmpty: bool,
                ?skipRows: uint64,
                ?skipRowsAfterHeader: uint64,
                ?skipLines: uint64,
                ?nRows: uint64,
                ?inferSchemaLength: uint64,
                ?inferSchemaFiles: uint64,
                ?nThreads: uint64,
                ?chunkSize: uint64,
                ?rowIndexName: string,
                ?rowIndexOffset: uint64,
                ?includeFilePaths: string,
                ?encoding: CsvEncoding,
                ?nullValues: seq<string>,
                ?missingIsNull: bool,
                ?commentPrefix: string,
                ?decimalComma: bool,
                ?truncateRaggedLines: bool,
                ?trueValues: seq<string>,
                ?falseValues: seq<string>,
                ?thousandsSeparator: string
            ) : LazyFrame =
            ignore (lowMemory, cache, chunkSize, glob, includeFilePaths)
            let args =
                { CsvReadParams.Empty with
                    Schema = schema; DtypeOverride = dtypeOverride; HasHeader = hasHeader; Separator = separator
                    QuoteChar = quoteChar; EolChar = eolChar; IgnoreErrors = ignoreErrors; TryParseDates = tryParseDates
                    Rechunk = rechunk; RaiseIfEmpty = raiseIfEmpty; SkipRows = skipRows; SkipRowsAfterHeader = skipRowsAfterHeader
                    SkipLines = skipLines; NRows = nRows; InferSchemaLength = inferSchemaLength; InferSchemaFiles = inferSchemaFiles
                    NThreads = nThreads; RowIndexName = rowIndexName; RowIndexOffset = rowIndexOffset; Encoding = encoding
                    NullValues = nullValues; MissingIsNull = missingIsNull; CommentPrefix = commentPrefix; DecimalComma = decimalComma
                    TruncateRaggedLines = truncateRaggedLines; TrueValues = trueValues; FalseValues = falseValues
                    ThousandsSeparator = thousandsSeparator }
            new LazyFrame(ManagedCsv.ScanCsv(buffer, args.ToArgs()))

        // ---------------------------------------------------------
        // Scan CSV (Stream)
        // ---------------------------------------------------------

        /// <summary>
        /// Lazily read CSV from a Stream: read now (from its position; the stream is left open), once, into native memory
        /// the scan keeps.
        /// </summary>
        /// <remarks>glob and includeFilePaths are unused for a stream.</remarks>
        static member ScanCsv
            (
                stream: Stream,
                ?schema: PolarsSchema,
                ?hasHeader: bool,
                ?separator: char,
                ?quoteChar: char,
                ?eolChar: char,
                ?ignoreErrors: bool,
                ?tryParseDates: bool,
                ?lowMemory: bool,
                ?cache: bool,
                ?glob: bool,
                ?rechunk: bool,
                ?raiseIfEmpty: bool,
                ?skipRows: uint64,
                ?skipRowsAfterHeader: uint64,
                ?skipLines: uint64,
                ?nRows: uint64,
                ?inferSchemaLength: uint64,
                ?inferSchemaFiles: uint64,
                ?nThreads: uint64,
                ?chunkSize: uint64,
                ?rowIndexName: string,
                ?rowIndexOffset: uint64,
                ?includeFilePaths: string,
                ?encoding: CsvEncoding,
                ?nullValues: seq<string>,
                ?missingIsNull: bool,
                ?commentPrefix: string,
                ?decimalComma: bool,
                ?truncateRaggedLines: bool,
                ?trueValues: seq<string>,
                ?falseValues: seq<string>,
                ?thousandsSeparator: string
            ) : LazyFrame =
            ignore (lowMemory, cache, chunkSize, glob, includeFilePaths)
            let args =
                { CsvReadParams.Empty with
                    Schema = schema; HasHeader = hasHeader; Separator = separator; QuoteChar = quoteChar; EolChar = eolChar
                    IgnoreErrors = ignoreErrors; TryParseDates = tryParseDates; Rechunk = rechunk; RaiseIfEmpty = raiseIfEmpty
                    SkipRows = skipRows; SkipRowsAfterHeader = skipRowsAfterHeader; SkipLines = skipLines; NRows = nRows
                    InferSchemaLength = inferSchemaLength; InferSchemaFiles = inferSchemaFiles; NThreads = nThreads
                    RowIndexName = rowIndexName; RowIndexOffset = rowIndexOffset; Encoding = encoding; NullValues = nullValues
                    MissingIsNull = missingIsNull; CommentPrefix = commentPrefix; DecimalComma = decimalComma
                    TruncateRaggedLines = truncateRaggedLines; TrueValues = trueValues; FalseValues = falseValues
                    ThousandsSeparator = thousandsSeparator }
            new LazyFrame(ManagedCsv.ScanCsv(stream, args.ToArgs()))

        /// <summary> Helper: Scan CSV with default settings </summary>
        static member ScanCsv(path: string) =
            LazyFrame.ScanCsv(path, hasHeader = true)

    type DataFrame with
        /// <summary>
        /// Read CSV into a DataFrame: a file, a directory of files, a glob, or any of them on a cloud store (read as one
        /// dataset: the files' columns united by name, the types inferred from the first files), decoded on every core.
        /// </summary>
        /// <param name="path">The file, directory or glob (<c>s3://…</c>, <c>az://…</c>, <c>gs://…</c> on a store).</param>
        /// <param name="columns">The columns read, in this order (all when none or empty).</param>
        /// <param name="hasHeader">Whether the first row names the columns. Defaults to true.</param>
        /// <param name="separator">The field separator. Defaults to ','.</param>
        /// <param name="quoteChar">The quote. Defaults to '"'; '\000' reads without quoting.</param>
        /// <param name="eolChar">The line end: '\n' (the default) ends records at LF, CRLF or a CR alone; another character ends them at it.</param>
        /// <param name="ignoreErrors">A value its column cannot hold is a null instead of an error (malformed CSV still is an error).</param>
        /// <param name="tryParseDates">Infer dates, times and datetimes from text. Defaults to true.</param>
        /// <param name="lowMemory">Accepted for compatibility; reading streams its input whatever it says.</param>
        /// <param name="skipRows">Records skipped before the header (quote-aware). Defaults to 0.</param>
        /// <param name="nRows">Stop after this many rows.</param>
        /// <param name="inferSchemaLength">Rows to infer the column types from (100 by default); a value above 16,777,216 infers them from the whole file; 0 reads every column as String.</param>
        /// <param name="inferSchemaFiles">The files the types are inferred from (10 by default; UInt64.MaxValue also means 10).</param>
        /// <param name="schema">Every column and its type: nothing is inferred. With a header the columns are matched by name (the frame in the schema's order); without one, by position.</param>
        /// <param name="dtypeOverride">The types of some inferred columns, by name.</param>
        /// <param name="encoding">The files' encoding (UTF-8 by default; see <see cref="CsvEncoding"/>).</param>
        /// <param name="nullValues">Values read as null.</param>
        /// <param name="missingIsNull">Empty unquoted fields are null (else empty strings). Defaults to true.</param>
        /// <param name="commentPrefix">Lines starting with this text are passed over.</param>
        /// <param name="decimalComma">',' is the decimal separator. Defaults to false.</param>
        /// <param name="truncateRaggedLines">Fields past the last column are dropped (else an error). Defaults to false.</param>
        /// <param name="rowIndexName">If provided, adds a first column (UInt32) numbering the rows.</param>
        /// <param name="rowIndexOffset">The row index's first number. Defaults to 0.</param>
        /// <param name="cloudOptions">Options for cloud storage.</param>
        /// <param name="trueValues">Values read as true (localized booleans such as "VERDADEIRO"); they also steer inference.</param>
        /// <param name="falseValues">Values read as false.</param>
        /// <param name="thousandsSeparator">The digit group separator of numbers. Off by default.</param>
        /// <param name="allowMissingColumns">A file lacking some of the dataset's columns reads them as nulls (else an error).</param>
        /// <param name="ignoreExtraColumns">Columns of later files the first files lack are passed over (else an error).</param>
        /// <param name="hivePartitioning">Whether <c>key=value</c> directories of the paths are columns (off by default).</param>
        static member ReadCsv
            (
                path: string,
                ?columns: seq<string>,
                ?hasHeader: bool,
                ?separator: char,
                ?quoteChar: char,
                ?eolChar: char,
                ?ignoreErrors: bool,
                ?tryParseDates: bool,
                ?lowMemory: bool,
                ?skipRows: uint64,
                ?nRows: uint64,
                ?inferSchemaLength: uint64,
                ?inferSchemaFiles: uint64,
                ?schema: PolarsSchema,
                ?dtypeOverride: PolarsSchema,
                ?encoding: CsvEncoding,
                ?nullValues: seq<string>,
                ?missingIsNull: bool,
                ?commentPrefix: string,
                ?decimalComma: bool,
                ?truncateRaggedLines: bool,
                ?rowIndexName: string,
                ?rowIndexOffset: uint64,
                ?cloudOptions: CloudOptions,
                ?trueValues: seq<string>,
                ?falseValues: seq<string>,
                ?thousandsSeparator: string,
                ?allowMissingColumns: bool,
                ?ignoreExtraColumns: bool,
                ?hivePartitioning: bool
            ) : DataFrame =
            ignore lowMemory
            let args =
                { CsvReadParams.Empty with
                    Schema = schema; DtypeOverride = dtypeOverride; HasHeader = hasHeader; Separator = separator
                    QuoteChar = quoteChar; EolChar = eolChar; IgnoreErrors = ignoreErrors; TryParseDates = tryParseDates
                    SkipRows = skipRows; NRows = nRows; InferSchemaLength = inferSchemaLength; InferSchemaFiles = inferSchemaFiles
                    RowIndexName = rowIndexName; RowIndexOffset = rowIndexOffset; Encoding = encoding; NullValues = nullValues
                    MissingIsNull = missingIsNull; CommentPrefix = commentPrefix; DecimalComma = decimalComma
                    TruncateRaggedLines = truncateRaggedLines; TrueValues = trueValues; FalseValues = falseValues
                    ThousandsSeparator = thousandsSeparator; AllowMissingColumns = allowMissingColumns
                    IgnoreExtraColumns = ignoreExtraColumns; HivePartitioning = hivePartitioning }
            new DataFrame(ManagedCsv.ReadCsv(path, asked columns, args.ToArgs(), CloudOptions.ToCloudArgs cloudOptions))

        // ---------------------------------------------------------
        // Read CSV (Memory / Bytes)
        // ---------------------------------------------------------

        /// <summary>
        /// Read CSV from an in-memory byte array (read in place during the call).
        /// </summary>
        static member ReadCsv
            (
                buffer: byte[],
                ?columns: seq<string>,
                ?hasHeader: bool,
                ?separator: char,
                ?quoteChar: char,
                ?eolChar: char,
                ?ignoreErrors: bool,
                ?tryParseDates: bool,
                ?lowMemory: bool,
                ?skipRows: uint64,
                ?nRows: uint64,
                ?inferSchemaLength: uint64,
                ?inferSchemaFiles: uint64,
                ?schema: PolarsSchema,
                ?dtypeOverride: PolarsSchema,
                ?encoding: CsvEncoding,
                ?nullValues: seq<string>,
                ?missingIsNull: bool,
                ?commentPrefix: string,
                ?decimalComma: bool,
                ?truncateRaggedLines: bool,
                ?rowIndexName: string,
                ?rowIndexOffset: uint64,
                ?trueValues: seq<string>,
                ?falseValues: seq<string>,
                ?thousandsSeparator: string
            ) : DataFrame =
            ignore lowMemory
            if isNull buffer then nullArg (nameof buffer)
            let args =
                { CsvReadParams.Empty with
                    Schema = schema; DtypeOverride = dtypeOverride; HasHeader = hasHeader; Separator = separator
                    QuoteChar = quoteChar; EolChar = eolChar; IgnoreErrors = ignoreErrors; TryParseDates = tryParseDates
                    SkipRows = skipRows; NRows = nRows; InferSchemaLength = inferSchemaLength; InferSchemaFiles = inferSchemaFiles
                    RowIndexName = rowIndexName; RowIndexOffset = rowIndexOffset; Encoding = encoding; NullValues = nullValues
                    MissingIsNull = missingIsNull; CommentPrefix = commentPrefix; DecimalComma = decimalComma
                    TruncateRaggedLines = truncateRaggedLines; TrueValues = trueValues; FalseValues = falseValues
                    ThousandsSeparator = thousandsSeparator }
            new DataFrame(ManagedCsv.ReadCsv(System.ReadOnlyMemory<byte>(buffer), asked columns, args.ToArgs()))

        // ---------------------------------------------------------
        // Read CSV (Stream)
        // ---------------------------------------------------------

        /// <summary>
        /// Read CSV from a Stream, decoded as it arrives (from its position; the stream is left open).
        /// </summary>
        static member ReadCsv
            (
                stream: Stream,
                ?columns: seq<string>,
                ?hasHeader: bool,
                ?separator: char,
                ?quoteChar: char,
                ?eolChar: char,
                ?ignoreErrors: bool,
                ?tryParseDates: bool,
                ?lowMemory: bool,
                ?skipRows: uint64,
                ?nRows: uint64,
                ?inferSchemaLength: uint64,
                ?inferSchemaFiles: uint64,
                ?schema: PolarsSchema,
                ?encoding: CsvEncoding,
                ?nullValues: seq<string>,
                ?missingIsNull: bool,
                ?commentPrefix: string,
                ?decimalComma: bool,
                ?truncateRaggedLines: bool,
                ?rowIndexName: string,
                ?rowIndexOffset: uint64,
                ?trueValues: seq<string>,
                ?falseValues: seq<string>,
                ?thousandsSeparator: string
            ) : DataFrame =
            ignore lowMemory
            let args =
                { CsvReadParams.Empty with
                    Schema = schema; HasHeader = hasHeader; Separator = separator; QuoteChar = quoteChar; EolChar = eolChar
                    IgnoreErrors = ignoreErrors; TryParseDates = tryParseDates; SkipRows = skipRows; NRows = nRows
                    InferSchemaLength = inferSchemaLength; InferSchemaFiles = inferSchemaFiles; RowIndexName = rowIndexName
                    RowIndexOffset = rowIndexOffset; Encoding = encoding; NullValues = nullValues; MissingIsNull = missingIsNull
                    CommentPrefix = commentPrefix; DecimalComma = decimalComma; TruncateRaggedLines = truncateRaggedLines
                    TrueValues = trueValues; FalseValues = falseValues; ThousandsSeparator = thousandsSeparator }
            new DataFrame(ManagedCsv.ReadCsv(stream, asked columns, args.ToArgs()))

        /// <summary>
        /// Read CSV asynchronously into a DataFrame (read off the caller's thread).
        /// </summary>
        static member ReadCsvAsync
            (
                path: string,
                ?columns: seq<string>,
                ?hasHeader: bool,
                ?separator: char,
                ?quoteChar: char,
                ?eolChar: char,
                ?ignoreErrors: bool,
                ?tryParseDates: bool,
                ?lowMemory: bool,
                ?skipRows: uint64,
                ?nRows: uint64,
                ?inferSchemaLength: uint64,
                ?inferSchemaFiles: uint64,
                ?schema: PolarsSchema,
                ?encoding: CsvEncoding,
                ?nullValues: seq<string>,
                ?missingIsNull: bool,
                ?commentPrefix: string,
                ?decimalComma: bool,
                ?truncateRaggedLines: bool,
                ?rowIndexName: string,
                ?rowIndexOffset: uint64,
                ?cloudOptions: CloudOptions,
                ?trueValues: seq<string>,
                ?falseValues: seq<string>,
                ?thousandsSeparator: string,
                ?allowMissingColumns: bool,
                ?ignoreExtraColumns: bool,
                ?hivePartitioning: bool
            ) : Task<DataFrame> =
            Task.Run(fun () ->
                DataFrame.ReadCsv(
                    path,
                    ?columns = columns,
                    ?schema = schema,
                    ?hasHeader = hasHeader,
                    ?separator = separator,
                    ?quoteChar = quoteChar,
                    ?eolChar = eolChar,
                    ?ignoreErrors = ignoreErrors,
                    ?tryParseDates = tryParseDates,
                    ?lowMemory = lowMemory,
                    ?skipRows = skipRows,
                    ?nRows = nRows,
                    ?inferSchemaLength = inferSchemaLength,
                    ?inferSchemaFiles = inferSchemaFiles,
                    ?encoding = encoding,
                    ?nullValues = nullValues,
                    ?missingIsNull = missingIsNull,
                    ?commentPrefix = commentPrefix,
                    ?decimalComma = decimalComma,
                    ?truncateRaggedLines = truncateRaggedLines,
                    ?rowIndexName = rowIndexName,
                    ?rowIndexOffset = rowIndexOffset,
                    ?cloudOptions = cloudOptions,
                    ?trueValues = trueValues,
                    ?falseValues = falseValues,
                    ?thousandsSeparator = thousandsSeparator,
                    ?allowMissingColumns = allowMissingColumns,
                    ?ignoreExtraColumns = ignoreExtraColumns,
                    ?hivePartitioning = hivePartitioning))
