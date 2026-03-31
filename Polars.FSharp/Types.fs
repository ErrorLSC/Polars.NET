#nowarn "64"
#nowarn "44"
namespace Polars.FSharp

open System
open Polars.NET.Core
open Apache.Arrow
open System.Collections.Generic
open Polars.NET.Core.Arrow
open Polars.NET.Core.Helpers
open System.Threading.Tasks
open System.Collections
open System.Text
open System.Threading
open Apache.Arrow.Adbc
open Apache.Arrow.Ipc
open System.Linq
/// --- Series ---
/// <summary>
/// An eager Series holding a single column of data.
/// </summary>
type Series(handle: SeriesHandle) =

    member this.Dispose() = handle.Dispose()
    member _.Handle = handle
    member _.Name = PolarsWrapper.SeriesName handle
    member _.Length = PolarsWrapper.SeriesLen handle
    member _.Len = PolarsWrapper.SeriesLen handle
    member _.NullCount : int64 = PolarsWrapper.SeriesNullCount handle
    member internal this.CloneHandle() = PolarsWrapper.CloneSeries handle
    member this.Clone() = new Series(this.CloneHandle())
    /// <summary>
    /// Gets the number of underlying Arrow memory chunks.
    /// </summary>
    member this.NChunks : int64 = 
        int64 (PolarsWrapper.SeriesChunkCounts handle)

    /// <summary>
    /// Determines if the Series memory is physically contiguous (i.e., consists of a single chunk).
    /// </summary>
    member this.IsContiguous : bool = 
        this.NChunks = 1

    /// <summary>
    /// True if the Series is empty.
    /// </summary>
    member this.IsEmpty = this.Length = 0

    /// <remarks>
    /// Polars Operations like Appending or Filtering can create fragmented memory chunks. 
    /// Calling Rechunk() merges these chunks into a single contiguous Arrow array. 
    /// This is CRITICAL before zero-copy extracting native pointers for Tensors or FFI.
    /// </remarks>
    /// <returns>A new Series instance backed by contiguous memory.</returns>
    member this.Rechunk() = 
        new Series(PolarsWrapper.SeriesRechunk this.Handle)
    /// <summary> Rename the Series in-place. Returns self. </summary>    
    member this.Rename(name: string) = 
        PolarsWrapper.SeriesRename(this.Handle, name)
        this

    // ==========================================
    // Expression Composition (The "ApplyExpr" Pattern)
    // ==========================================

    /// <summary>
    /// Internal Helper: Wrap this Series in a temporary DataFrame, run an Expr, and extract the result.
    /// This allows Series to directly use the full power of the Expression engine without duplicating logic.
    /// </summary>
    member internal this.ApplyExpr(expr: Expr) : Series =
        use dfHandle = PolarsWrapper.SeriesToFrame handle
        use df = new DataFrame(dfHandle)

        use dfRes = df.Select [expr]

        dfRes.[0]
    // ==========================================
    // Binary Op Helper (The "ApplyBinaryExpr" Pattern)
    // ==========================================

    /// <summary>
    /// Internal Helper: Apply a binary expression using two Series.
    /// Handles name collision by creating a temporary renamed series if necessary.
    /// </summary>
    member internal this.ApplyBinaryExpr(other: Series, op: Expr -> Expr -> Expr) : Series =
        let leftName = this.Name
        let rightNameRaw = other.Name
        
        let rightName, rightSeries, tempToDispose =
            if leftName = rightNameRaw then
                let newName = "__other_temp__"
                let cloneHandle = PolarsWrapper.CloneSeries other.Handle 
                let clone = (new Series(cloneHandle)).Rename newName
                newName, clone, Some clone
            else
                rightNameRaw, other, None

        try
            let handles = [| this.Handle; rightSeries.Handle |]
            use dfHandle = PolarsWrapper.DataFrameNew handles
            use df = new DataFrame(dfHandle)

            let expr = op (Expr.Col leftName) (Expr.Col rightName)
            
            use resDf = df.Select [expr]

            resDf.[0]

        finally
            match tempToDispose with
            | Some s -> s.Handle.Dispose()
            | None -> ()

 

    /// <summary>
    /// Get the string representation of the Series Data Type (e.g., "Int64", "String").
    /// </summary>
    member _.DtypeStr = PolarsWrapper.GetSeriesDtypeString handle
    /// <summary> Get the DataType of the Series. </summary>
    member this.DataType : DataType =
        use typeHandle = PolarsWrapper.GetSeriesDataType handle
        
        DataType.FromHandle typeHandle

    // ==========================================
    // Static Constructors
    // ==========================================
    /// <summary>
    /// Supports:
    /// - Primitives ('T)
    /// - Option types ('T option)
    /// - ValueOption types ('T voption)
    /// Create a Series directly from a ReadOnlySpan.
    /// </summary>
    static member create(name: string, data: ReadOnlySpan<'T>) =
        let handle = SeriesFactory.CreateSpan(name, data)
        new Series(handle)

    /// <summary>
    /// Create a Series directly from a Span.
    /// </summary>
    static member create(name: string, data: Span<'T>) =
        let roSpan = Span<'T>.op_Implicit data
        
        let handle = SeriesFactory.CreateSpan(name, roSpan)
        new Series(handle)

    /// <summary>
    /// Create a Series from any sequence (List, Seq, etc.).
    /// Intelligently routes F# specific types (option/voption) to CreateSpan,
    /// while routing standard .NET types to the zero-allocation CreateGenericType.
    /// </summary>
    static member create(name: string, data: seq<'T>) =
        let t = typeof<'T>
        
        let isFSharpOption = 
            t.IsGenericType && 
            (t.GetGenericTypeDefinition() = typedefof<voption<_>> || 
            t.GetGenericTypeDefinition() = typedefof<option<_>>)

        if isFSharpOption then
            let arr = Seq.toArray data
            let handle = SeriesFactory.CreateSpan(name, ReadOnlySpan<'T> arr)
            
            if isNull (box handle) || handle.IsInvalid then
                new Series(SeriesFactory.Create(name, arr))
            else
                new Series(handle)
        else
            let handle = SeriesFactory.CreateGenericType(name, data)
            new Series(handle)

    /// <summary>
    /// Alias for create matching C# naming convention.
    /// </summary>
    static member From(name: string, data: seq<'T>) = 
        Series.create(name, data)
    
    // -------------------------------------------------------------------------
    // Fixed Size List / Array (Matrix)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Create a FixedSizeList Series from a 2D Array (Matrix).
    /// Shape: [Rows, Width] -> Array[Width]
    /// Supported Types: Primitives, Decimal, Int128
    /// </summary>
    static member ofArray2D<'T
        when 'T : struct 
        and 'T : unmanaged
        and 'T :> ValueType   
        and 'T : (new : unit -> 'T)> 
        (name: string, data: 'T[,]) =
            new Series(PolarsWrapper.SeriesNewFixedArray(name, data))
    // ========================================================================
    // Unified Entry Points (Delegating to SeriesFactory)
    // ========================================================================
    /// <summary>
    /// High-performance creation from any sequence. 
    /// Supports nested lists, structs, and F# Options.
    /// </summary>
    static member ofSeq<'T>(name: string, data: seq<'T>) : Series =
        let arrowArray = ArrowConverter.Build data
        
        let handle = ArrowFfiBridge.ImportSeries(name, arrowArray)
        
        new Series(handle)
    
    /// <summary>
    /// Convert Series to a typed sequence of Options.
    /// Uses high-performance Arrow reader (Zero-Copy).
    /// Supports: Primitives, String, DateTime, DateOnly, TimeOnly, List, Struct.
    /// </summary>
    member this.AsSeq<'T>() : seq<'T option> =
        seq {
            use cArray = PolarsWrapper.SeriesToArrow this.Handle
            
            let accessor = ArrowReader.GetSeriesAccessor<'T> cArray
            let len = cArray.Length

            for i in 0 .. len - 1 do
                let valObj = accessor.Invoke i
                if isNull valObj then 
                    None 
                else 
                    Some(unbox<'T> valObj)
        }
    /// <summary>
    /// Get values as a list (forces evaluation).
    /// </summary>
    member this.ToList<'T>() = this.AsSeq<'T>() |> Seq.toList
    /// <summary>
    /// Create a Series from a sequence of Options (F# style nullables).
    /// Automatically handles all supported types (int, float, string, datetime, etc.)
    /// </summary>
    static member ofOptionSeq<'T>(name: string, data: seq<'T option>) : Series =
        Series.create(name, data)

    /// <summary>
    /// Create a Series from a sequence of ValueOptions (Struct nullables).
    /// Automatically handles all supported types.
    /// </summary>
    static member ofVOptionSeq<'T>(name: string, data: seq<'T voption>) : Series =
        Series.create(name, data)
    // ==========================================
    // Operators (Arithmetic) 
    // ==========================================

    /// <summary> Modulo (remainder). </summary>
    member this.Mod(other: Series) = 
        this.ApplyBinaryExpr(other, fun l r -> l.Mod r)

    /// <summary> Modulo (scalar). </summary>
    member this.Mod(other: int) = 
        this.ApplyExpr(Expr.Col(this.Name).Mod(new Expr(PolarsWrapper.Lit other)))

    // Alias for Mod
    member this.Rem(other: Series) = this.Mod other
    member this.Rem(other: int) = this.Mod other

    /// <summary> Bitwise left shift. </summary>
    member this.BitLeftShift(n: int) = 
        this.ApplyExpr(Expr.Col(this.Name).BitLeftShift n)
    /// <summary> Bitwise right shift. </summary>
    member this.BitRightShift(n: int) = 
        this.ApplyExpr(Expr.Col(this.Name).BitRightShift n)
    static member (+) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesAdd(lhs.Handle, rhs.Handle))
    static member (-) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesSub(lhs.Handle, rhs.Handle))
    static member (*) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesMul(lhs.Handle, rhs.Handle))
    static member (/) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesDiv(lhs.Handle, rhs.Handle))
    static member (%) (lhs: Series, rhs: Series) = lhs.Mod rhs

    // --- Operators (Comparison) ---

    static member (.=) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesEq(lhs.Handle, rhs.Handle))
    static member (!=) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesNeq(lhs.Handle, rhs.Handle))
    static member (.>) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesGt(lhs.Handle, rhs.Handle))
    static member (.<) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesLt(lhs.Handle, rhs.Handle))

    static member (.>=) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesGtEq(lhs.Handle, rhs.Handle))
    static member (.<=) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesLtEq(lhs.Handle, rhs.Handle))

    // --- Broadcasting Helpers (Scalar Ops) ---
    static member (+) (lhs: Series, rhs: int) = lhs + Series.create("lit", [rhs])
    static member (+) (lhs: Series, rhs: double) = lhs + Series.create("lit", [rhs])
    static member (-) (lhs: Series, rhs: int) = lhs - Series.create("lit", [rhs])
    static member (-) (lhs: Series, rhs: double) = lhs - Series.create("lit", [rhs])
    
    static member (*) (lhs: Series, rhs: int) = lhs * Series.create("lit", [rhs])
    static member (*) (lhs: Series, rhs: double) = lhs * Series.create("lit", [rhs])
    
    static member (/) (lhs: Series, rhs: int) = lhs / Series.create("lit", [rhs])
    static member (/) (lhs: Series, rhs: double) = lhs / Series.create("lit", [rhs])
    static member (%) (lhs: Series, rhs: int) = lhs.Mod rhs
    static member (<<<) (lhs: Series, rhs: int) = lhs.BitLeftShift rhs
    static member (>>>) (lhs: Series, rhs: int) = lhs.BitRightShift rhs

    // Comparison with Scalar
    static member (.>) (lhs: Series, rhs: int) = lhs .> Series.create("lit", [rhs])
    static member (.>) (lhs: Series, rhs: double) = lhs .> Series.create("lit", [rhs])
    static member (.<) (lhs: Series, rhs: int) = lhs .< Series.create("lit", [rhs])
    static member (.<) (lhs: Series, rhs: double) = lhs .< Series.create("lit", [rhs])
    static member (.>=) (lhs: Series, rhs: int) = lhs .>= Series.create("lit", [rhs])
    static member (.<=) (lhs: Series, rhs: double) = lhs .<= Series.create("lit", [rhs])
    
    static member (.=) (lhs: Series, rhs: int) = lhs .= Series.create("lit", [rhs])
    static member (.=) (lhs: Series, rhs: double) = lhs .= Series.create("lit", [rhs])
    static member (.=) (lhs: Series, rhs: string) = lhs .= Series.create("lit", [rhs])
    static member (.!=) (lhs: Series, rhs: int) = lhs != Series.create("lit", [rhs])
    static member (.!=) (lhs: Series, rhs: string) = lhs != Series.create("lit", [rhs])
    // ==========================================
    // Unified Accessor (Fast Path + Universal Path)
    // ==========================================
    /// <summary>
    /// Get an item at the specified index.
    /// Supports primitives (int, float, bool, string) via fast native path,
    /// and complex types (Struct, List, DateTime) via Arrow infrastructure.
    /// </summary>
    member this.GetValue<'T>(index: int64) : 'T =
        let len = this.Length
        if index < 0L || index >= len then
            raise (IndexOutOfRangeException(sprintf "Index %d is out of bounds for Series length %d." index len))

        // Consistent Null Check
        if PolarsWrapper.SeriesIsNullAt(this.Handle, index) then
            Unchecked.defaultof<'T>
        else
            // 2. Getvalue
            let t = typeof<'T>
            
            // --- Integer Family ---
            if t = typeof<int> || t = typeof<int option> || t = typeof<Nullable<int>> then
                let v = int (PolarsWrapper.SeriesGetInt(this.Handle, index).Value)
                if t = typeof<int option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            else if t = typeof<int64> || t = typeof<int64 option> || t = typeof<Nullable<int64>> then
                let v = PolarsWrapper.SeriesGetInt(this.Handle, index).Value
                if t = typeof<int64 option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            else if t = typeof<Int128> || t = typeof<Int128 option> || t = typeof<Nullable<Int128>> then
                let v = PolarsWrapper.SeriesGetInt128(this.Handle, index).Value
                if t = typeof<Int128 option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            // --- Float Family ---
            else if t = typeof<double> || t = typeof<double option> || t = typeof<Nullable<double>> then
                let v = PolarsWrapper.SeriesGetDouble(this.Handle, index).Value
                if t = typeof<double option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            else if t = typeof<float32> || t = typeof<float32 option> || t = typeof<Nullable<float32>> then
                let v = float32 (PolarsWrapper.SeriesGetDouble(this.Handle, index).Value)
                if t = typeof<float32 option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            else if t = typeof<Half> || t = typeof<Half> || t = typeof<Nullable<Half>> then
                let v = PolarsWrapper.SeriesGetDouble(this.Handle, index).Value
                if t = typeof<Half option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            // --- Boolean ---
            else if t = typeof<bool> || t = typeof<bool option> || t = typeof<Nullable<bool>> then
                let v = PolarsWrapper.SeriesGetBool(this.Handle, index).Value
                if t = typeof<bool option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            // --- String ---
            else if t = typeof<string> || t = typeof<string option> then
                let v = PolarsWrapper.SeriesGetString(this.Handle, index)
                if t = typeof<string option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            // --- Decimal ---
            else if t = typeof<decimal> || t = typeof<decimal option> || t = typeof<Nullable<decimal>> then
                let v = PolarsWrapper.SeriesGetDecimal(this.Handle, index).Value
                if t = typeof<decimal option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            // --- Temporal ---
            else if t = typeof<DateOnly> || t = typeof<DateOnly option> || t = typeof<Nullable<DateOnly>> then
                let v = PolarsWrapper.SeriesGetDate(this.Handle, index).Value
                if t = typeof<DateOnly option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            else if t = typeof<TimeOnly> || t = typeof<TimeOnly option> || t = typeof<Nullable<TimeOnly>> then
                let v = PolarsWrapper.SeriesGetTime(this.Handle, index).Value
                if t = typeof<TimeOnly option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>
                
            else if t = typeof<TimeSpan> || t = typeof<TimeSpan option> || t = typeof<Nullable<TimeSpan>> then
                let v = PolarsWrapper.SeriesGetDuration(this.Handle, index).Value
                if t = typeof<TimeSpan option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            else if t = typeof<DateTime> || t = typeof<DateTime option> || t = typeof<Nullable<DateTime>> then
                let struct (dt, _) = PolarsWrapper.SeriesGetDatetime(this.Handle, index).Value
                
                if t = typeof<DateTime option> then box (Some dt) |> unbox<'T>
                else box dt |> unbox<'T>
                
            else if t = typeof<struct(DateTime * string)> || t = typeof<struct(DateTime * string) option> || t = typeof<Nullable<struct(DateTime * string)>> then
                let v = PolarsWrapper.SeriesGetDatetime(this.Handle, index).Value
                
                if t = typeof<struct(DateTime * string) option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            // --- Complex Types (Arrow Fallback) ---
            else
                use slicedHandle = PolarsWrapper.SeriesSlice(this.Handle, index, 1L)
                use dfHandle = PolarsWrapper.SeriesToFrame slicedHandle
                use batch = ArrowFfiBridge.ExportDataFrame dfHandle
                let column = batch.Column(0)
                ArrowReader.ReadItem<'T>(column, 0)
    /// <summary>
    /// Get a value as an F# List ('T list).
    /// Automatically handles conversion from .NET List (ResizeArray).
    /// </summary>
    member this.GetList<'Elem>(index: int64) : 'Elem list =
        let netList = this.GetValue<ResizeArray<'Elem>> index
        
        if isNull netList then 
            []
        else 
            netList |> List.ofSeq
                
    /// <summary>
    /// [Indexer] Access value at specific index as boxed object.
    /// Syntax: series.[index]
    /// </summary>
    member this.Item (index: int) : obj =
        let idx = int64 index
        
        match this.DataType with
        | DataType.Boolean -> box (this.GetValue<bool option> idx) 
        
        | DataType.Int8 -> box (this.GetValue<int8 option> idx)
        | DataType.Int16 -> box (this.GetValue<int16 option> idx)
        | DataType.Int32 -> box (this.GetValue<int32 option> idx)
        | DataType.Int64 -> box (this.GetValue<int64 option> idx)
        | DataType.Int128 -> box (this.GetValue<Int128 option> idx)
        
        | DataType.UInt8 -> box (this.GetValue<uint8 option> idx)
        | DataType.UInt16 -> box (this.GetValue<uint16 option> idx)
        | DataType.UInt32 -> box (this.GetValue<uint32 option> idx)
        | DataType.UInt64 -> box (this.GetValue<uint64 option> idx)
        | DataType.UInt128 -> box (this.GetValue<UInt128 option> idx)

        | DataType.Float16 -> box (this.GetValue<Half option> idx)
        | DataType.Float32 -> box (this.GetValue<float32 option> idx)
        | DataType.Float64 -> box (this.GetValue<double option> idx)
        
        | DataType.Decimal _ -> box (this.GetValue<decimal option> idx)
        
        | DataType.String -> box (this.GetValue<string option> idx)
        
        | DataType.Date -> box (this.GetValue<DateOnly option> idx)
        | DataType.Time -> box (this.GetValue<TimeOnly option> idx)
        | DataType.Datetime _ -> box (this.GetValue<DateTime option> idx)
        | DataType.Duration _ -> box (this.GetValue<TimeSpan option> idx)
        
        | DataType.Binary -> box (this.GetValue<byte[] option> idx)

        // Complex Type
        | DataType.List _ -> this.GetValue<obj> idx
        | DataType.Struct _ -> this.GetValue<obj> idx
        | DataType.Array _ -> this.GetValue<obj> idx
        
        | _ -> failwithf "Indexer not fully implemented for type: %A" this.DataType
    /// <summary>
    /// Get an item as an F# Option.
    /// Ideal for safe handling of nulls in Polars series.
    /// </summary>
    member this.GetValueOption<'T>(index: int64) : 'T option =
        this.GetValue<'T option> index
 
    // ==========================================
    // Interop 
    // ==========================================
    member this.ToFrame() : DataFrame =
        let h = PolarsWrapper.SeriesToFrame handle
        new DataFrame(h)

    member this.Cast(dtype: DataType) : Series =
        use typeHandle = dtype.CreateHandle()
        let newHandle = PolarsWrapper.SeriesCast(handle, typeHandle)
        new Series(newHandle)
    member this.ToArrow() : IArrowArray =
        PolarsWrapper.SeriesToArrow handle
    member this.FromArrow(name:string,arrowArray:IArrowArray) : Series = 
        new Series(ArrowFfiBridge.ImportSeries(name,arrowArray))
    member this.ToArray<'T>() =
        let col = this.ToArrow()
        ArrowReader.ReadColumn<'T> col

    /// <summary>
    /// Returns the string representation of the Series (ASCII table).
    /// </summary>
    override this.ToString() =
        if this.Handle.IsInvalid then 
            "Series (Disposed)"
        else 
            PolarsWrapper.SeriesToString this.Handle

    /// <summary>
    /// Print the Series to Console.
    /// </summary>
    member this.Show() = 
        printfn "%O" this

    interface IDisposable with 
        member this.Dispose() = this.Dispose()

    interface IPolarsSeries with
        member this.ToFrame() = 
            this.ToFrame() :> IPolarsDataFrame
            
        member this.DataType = 
            this.DataType :> IPolarsDataType
        member this.Name
            with get () = 
                this.Name 
            and set (value: string) = 
                this.Rename value |> ignore

// --- Frames ---

/// <summary>
/// An eager DataFrame holding data in memory.
/// <para>
/// DataFrames are 2D tabular data structures with named columns of potentially different types.
/// </para>
/// </summary>
and DataFrame(handle: DataFrameHandle) =
    let backingResources = ResizeArray<IDisposable>()
    member this.Clone() = new DataFrame(PolarsWrapper.CloneDataFrame handle)
    member internal this.CloneHandle() = PolarsWrapper.CloneDataFrame handle
    member _.Handle = handle
    
    member internal this.HoldResource(resource: IDisposable) =
        if not (isNull resource) then
            backingResources.Add resource

    member this.Dispose() =
        if not this.Handle.IsInvalid then 
            this.Handle.Dispose()

        for res in backingResources do
            res.Dispose()
            
        backingResources.Clear()
        GC.SuppressFinalize this

    interface IDisposable with
        member this.Dispose() = 
            this.Dispose()
 
    /// <summary>
    /// [ToRecords] Transform DataFrame to F# Records
    /// </summary>
    member this.ToRecords<'T>() : seq<'T> =
        use batch = ArrowFfiBridge.ExportDataFrame this.Handle
        
        ArrowReader.ReadRecordBatch<'T> batch |> Seq.toList |> List.toSeq

    /// <summary> Create a DataFrame directly from an Apache Arrow RecordBatch. </summary>
    static member FromArrow (batch: RecordBatch) : DataFrame =
        new DataFrame(PolarsWrapper.FromArrow batch)

    // ---- ADBC ----
    static member FromArrowStream(stream:IArrowArrayStream) =
    
        ArgumentNullException.ThrowIfNull stream

        let handle = ArrowStreamInterop.ImportForeignStream stream;
        
        let df = new DataFrame(handle)
        df.HoldResource stream
        df
    
    /// <summary>
    /// Zero-copy bulk ingest of the current DataFrame into an ADBC database (e.g., DuckDB, SQLite).
    /// </summary>
    /// <param name="statement">An AdbcStatement configured with ingest options (e.g., target table).</param>
    /// <returns>The UpdateResult containing the number of rows affected.</returns>
    member this.WriteToAdbc(statement: AdbcStatement) : UpdateResult =
        ArgumentNullException.ThrowIfNull statement

        try
            // Delegate all unsafe pointer handling, FFI bindings, and execution to the Core layer.
            // This ensures no raw pointers leak into the managed high-level API.
            AdbcInterop.ExecuteIngest(statement, this.Handle)
        finally
            // Crucial: Pin the DataFrame to prevent the Garbage Collector from 
            // reclaiming the underlying Rust memory while the ADBC C++ engine is actively pulling data.
            GC.KeepAlive this

    /// <summary>
    /// Export the DataFrame as a stream of Arrow RecordBatches (Zero-Copy).
    /// Calls 'onBatch' for each chunk in the DataFrame.
    /// Useful for custom eager sinks (e.g. WriteDatabase).
    /// </summary>
    member this.ExportBatches(onBatch: Action<RecordBatch>) : unit =
        PolarsWrapper.ExportBatches(this.Handle, onBatch)
    
    /// <summary>
    /// Get the schema
    /// </summary>
    member this.Schema =
        let h = PolarsWrapper.GetDataFrameSchema this.Handle 
        new PolarsSchema(h)
    member this.Lazy() : LazyFrame =
        let lfHandle = PolarsWrapper.DataFrameToLazy handle
        new LazyFrame(lfHandle)
    /// <summary>
    /// Print schema in a readable format.
    /// </summary>
    member this.PrintSchema() =
        printfn "--- DataFrame Schema ---"
        
        use sc = this.Schema

        sc.ToList() 
        |> List.iter (fun (name, dtype) -> 

            printfn "%-15s | %O" name dtype
        )
        
        printfn "------------------------"

    // ==========================================
    // Indexers (Syntax Sugar)
    // ==========================================
    member this.Item (columnName: string) : Series =
        this.Column columnName
    
    member this.Item (columnIndex: int) : Series =
        this.Column columnIndex
    /// <summary>
    /// [Indexer] Access cell value by Row Index and Column Name.
    /// Syntax: df.[rowIndex, "colName"]
    /// </summary>
    member this.Item (rowIndex: int, columnName: string) : obj =
        let series = this.Column columnName
        series.[rowIndex]

    /// <summary>
    /// [Indexer] Access cell value by Row Index and Column Index.
    /// Syntax: df.[rowIndex, colIndex]
    /// </summary>
    member this.Item (rowIndex: int, columnIndex: int) : obj =
        let series = this.Column columnIndex
        series.[rowIndex]
    // ==========================================
    // Eager Ops
    // ==========================================
    /// <summary>
    /// Create an empty (n=0) or n-row null-filled (n>0) copy of the DataFrame.
    /// Returns a n-row null-filled DataFrame with an identical schema.
    /// </summary>
    /// <param name="n">Number of (null-filled) rows to return in the cleared frame.</param>
    /// <returns>A new DataFrame.</returns>
    member this.Clear(?n: int64) = 
        use schema = this.Schema
        schema.ToDataFrame(?length=n)
    /// <summary>
    /// Convert a DataFrame to a Series of type Struct.
    /// </summary>
    /// <param name="name">Name for the struct Series.</param>
    member this.ToStruct(?name:string) =   
        let n = defaultArg name ""
        use df: DataFrame = this.Select(Expr.AsStruct [|Expr.All()|])
        let series = df[0]
        series.Rename n
    
    /// <summary>
    /// Return the number of unique rows, or the number of unique row-subsets.
    /// </summary>
    member this.NUnique(?subset: seq<string>) = 
        use df: DataFrame = this.Unique(?subset = subset)
        df.Height

    /// <summary>
    /// Return the number of unique rows, or the number of unique row-subsets.
    /// </summary>
    member this.NUnique([<ParamArray>]subset: Expr array) =
        use df = this.Unique subset
        df.Height

    /// <summary>
    /// Return the number of unique rows, or the number of unique row-subsets.
    /// </summary> 
    member this.Unique
        (
            ?subset: seq<string>,
            ?keep: UniqueKeepStrategy,
            ?maintainOrder: bool,
            ?offset: int64,
            ?len: int64
        ) =
        let keepArgs = defaultArg keep UniqueKeepStrategy.First
        let maintainArgs = defaultArg maintainOrder false

        let subsetArray =
            match subset with
            | Some s -> Array.ofSeq s
            | None -> null

        let slice =
            match offset, len with
            | Some o, Some l ->
                let safeLen = uint64 (Math.Max(0L, l))
                Nullable<struct (int64 * uint64)>(struct (o, safeLen)) 
            | _ ->
                Nullable<struct (int64 * uint64)>()

        let h = PolarsWrapper.DataFrameUnique(
            this.Handle,
            subsetArray,
            keepArgs.ToNative(),
            maintainArgs,
            slice
        )

        new DataFrame(h)

    member this.Unique
        (
            subset: seq<Expr>,
            ?keep: UniqueKeepStrategy,
            ?maintainOrder: bool,
            ?offset: int64,
            ?len: int64
        ) :DataFrame =
        let resolvedColumnNames = ResizeArray<string>()

        for expr in subset do
            let name = expr.Meta.OutputName()
            
            if not (String.IsNullOrEmpty name) then
                resolvedColumnNames.Add name
            else
                try
                    let expandedNames = (this.Head(0).Select(expr) : DataFrame).Columns
                    resolvedColumnNames.AddRange(expandedNames)
                with ex ->
                    let msg = sprintf "Cannot parse this expression to column names: %s" ex.Message
                    raise (ArgumentException(msg, ex))

        let finalSubset = 
            resolvedColumnNames 
            |> Seq.distinct 
            |> Seq.toArray

        if finalSubset.Length = 0 then
            raise (ArgumentException "No Columns Selected")

        this.Unique(
            subset = finalSubset,
            ?keep = keep,
            ?maintainOrder = maintainOrder,
            ?offset = offset,
            ?len = len
        )
    /// <summary> Add or replace columns using expressions. </summary>
    member this.WithColumns (exprs:seq<Expr>) : DataFrame =
        this.Lazy().WithColumns(exprs).Collect()
    /// <summary> Add or replace columns using generic column expressions (Expr or Selectors). </summary>
    member this.WithColumns (columns:seq<#IColumnExpr>) =
        let exprs = 
            columns 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.toList
        
        this.WithColumns exprs
    /// <summary> Add a single column. </summary>
    member this.WithColumn (expr: Expr) : DataFrame =
        this.WithColumns [|expr|]
    /// <summary> Select columns using expressions. </summary>
    member this.Select(exprs: seq<Expr>) : DataFrame =
        let lf = this.Lazy().Select exprs
        lf.Collect()

    /// <summary> Select columns using generic column expressions (Expr or Selectors). </summary>
    member this.Select(columns: seq<#IColumnExpr>) =
            let exprs = 
                columns 
                |> Seq.collect (fun x -> x.ToExprs()) 
                |> Seq.toList
            
            this.Select exprs
    /// <summary> 
    /// Select a single column using an expression.
    /// Usage: df.Select(pl.col("A"))
    /// </summary>
    member this.Select(expr: Expr) =
        this.Select [expr]

    /// <summary> Filter rows based on a boolean expression (predicate). </summary>
    member this.Filter (expr: Expr) : DataFrame = 
        this.Lazy().Filter(expr).Collect()
    /// <summary> Filter rows based on a boolean Series. </summary>
    member this.Filter (series: Series) : DataFrame = 
        this.Lazy().Filter(series).Collect()
       /// <summary>
    /// Return the number of non-null elements for each column.
    /// </summary>
    /// <returns></returns>
    member this.Count() = 
        this.Lazy().Count()
    /// <summary>
    /// Aggregate the columns in the Frame to their sum value.
    /// </summary>
    /// <returns></returns>
    member this.Sum() =
        this.Lazy().Sum()
    /// <summary>
    /// Aggregate the columns in the Frame to their maximum value.
    /// </summary>
    /// <returns></returns>
    member this.Max() =
        this.Lazy().Max()
    /// Aggregate the columns in the Frame to their minimum value.
    /// </summary>
    /// <returns></returns>
    member this.Min() =
        this.Lazy().Min()
    /// <summary>
    /// Aggregate the columns in the Frame to their mean value.
    /// </summary>
    /// <returns></returns>
    member this.Mean() =
        this.Lazy().Mean()
    /// <summary>
    /// Aggregate the columns in the Frame to their median value.
    /// </summary>
    /// <returns></returns>
    member this.Median() =
        this.Lazy().Median()
    /// <summary>
    /// Aggregate the columns in the Frame as the sum of their null value count.
    /// </summary>
    /// <returns></returns>
    member this.NullCount() = 
        this.Lazy().NullCount()
    /// <summary>
    /// Aggregate the columns in the Frame to their standard deviation value.
    /// </summary>
    /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1.</param>
    /// <returns></returns>
    member this.Std(?ddof:int) = 
        let d = defaultArg ddof 1
        this.Lazy().Std(ddof=d)
    /// <summary>
    /// Aggregate the columns in the Frame to their variance value.
    /// </summary>
    /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1.</param>
    /// <returns></returns>
    member this.Var(?ddof:int) =
        let d = defaultArg ddof 1
        this.Lazy().Var(ddof=d)
    /// <summary>
    /// Aggregate the columns in the Frame to their quantile value.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0.</param>
    /// <param name="method">['nearest’, ‘higher’, ‘lower’, ‘midpoint’, ‘linear’] Interpolation method.</param>
    /// <returns></returns>
    member this.Quantile(quantile:float, ?method: QuantileMethod) =
        let met = defaultArg method QuantileMethod.Linear
        this.Lazy().Quantile(quantile,met)
    /// <summary>
    /// General Concat method.
    /// checkDuplicates is only used when how = ConcatType.Horizontal.
    /// </summary>
    static member internal Concat (dfs: seq<DataFrame>, how: ConcatType, ?checkDuplicates: bool,?strict: bool, ?unitLengthAsScalar: bool) : DataFrame =
        let handles = dfs |> Seq.map (fun df -> df.CloneHandle()) |> Seq.toArray
        
        let check = defaultArg checkDuplicates true
        let st = defaultArg strict true
        let uni = defaultArg unitLengthAsScalar false
        let h = PolarsWrapper.Concat(handles, how.ToNative(), check, st, uni)
        new DataFrame(h)

    /// <summary>
    /// Horizontal concatenation (Index alignment).
    /// </summary>
    /// <param name="strict">For Horizontal: if true, error on height mismatch.</param>
    /// <param name="unitLengthAsScalar">For Horizontal: if true, broadcast length-1 DataFrames to match height.</param>
    static member ConcatHorizontal (dfs: seq<DataFrame>, ?checkDuplicates: bool,?strict: bool, ?unitLengthAsScalar: bool) : DataFrame =
        DataFrame.Concat(dfs, ConcatType.Horizontal, ?checkDuplicates = checkDuplicates, ?strict=strict,?unitLengthAsScalar=unitLengthAsScalar)

    /// <summary>
    /// Vertical concatenation (Column alignment).
    /// </summary>
    static member ConcatVertical (dfs: seq<DataFrame>) : DataFrame =
        DataFrame.Concat(dfs, ConcatType.Vertical)

    /// <summary>
    /// Diagonal concatenation.
    /// </summary>
    static member ConcatDiagonal (dfs: seq<DataFrame>) : DataFrame =
        DataFrame.Concat(dfs, ConcatType.Diagonal)
    /// <summary> Get the first n rows. </summary>
    member this.Head (?rows: int) : DataFrame  =
        let n = defaultArg rows 5
        let h = PolarsWrapper.Head(this.Handle, uint n) 
        new DataFrame(h)
    /// <summary> Get the last n rows. </summary>
    member this.Tail (?n: int) : DataFrame =
        let rows = defaultArg n 5
        let h = PolarsWrapper.Tail(this.Handle, uint rows) 
        new DataFrame(h)
    /// <summary>
    /// Hash and combine the rows in this DataFrame.
    /// </summary>
    member this.HashRows(?seed: uint64) =
        let s = defaultArg seed 42UL
        
        let nullableSeed = Nullable<uint64>(s)
        
        let h = PolarsWrapper.DataFrameHashRows(this.Handle, nullableSeed)
        new Series(h)

    /// <summary> 
    /// Explode list columns to rows using a Selector.
    /// </summary>
    member this.Explode(selector: Selector,?emptyAsNull:bool,?keepNulls:bool) : DataFrame =
        this.Lazy().Explode(selector,?emptyAsNull=emptyAsNull,?keepNulls=keepNulls).Collect()

    /// <summary> 
    /// Explode list columns to rows using column names.
    /// </summary>
    member this.Explode(columns: seq<string>,?emptyAsNull:bool,?keepNulls:bool) =
        let names = Seq.toArray columns
        let h = PolarsWrapper.SelectorCols names
        let sel = new Selector(h)
        this.Explode(sel,?emptyAsNull=emptyAsNull,?keepNulls=keepNulls) 

    /// <summary>Explode a single column by name. </summary>
    member this.Explode(column: string,?emptyAsNull:bool,?keepNulls:bool) =
        this.Explode([column],?emptyAsNull=emptyAsNull,?keepNulls=keepNulls)                          
    /// <summary> Decompose a struct column into multiple columns. </summary>
    member this.UnnestColumn(column: string, ?separator: string) : DataFrame =
        let cols = [| column |]
        let sep = defaultArg separator null
        let newHandle = PolarsWrapper.Unnest(this.Handle, cols, sep)
        new DataFrame(newHandle)
    /// <summary> Decompose multiple struct columns. </summary>
    member this.UnnestColumns(columns: seq<string>, ?separator: string) : DataFrame =
        let cArr = Seq.toArray columns
        let sep = defaultArg separator null
        let newHandle = PolarsWrapper.Unnest(this.Handle, cArr, sep)
        new DataFrame(newHandle)

    /// <summary>
    /// Slice the DataFrame along the rows.
    /// </summary>
    member this.Slice(offset: int64, length: uint64) = 
        new DataFrame(PolarsWrapper.Slice(this.Handle,offset, length))
    member this.Slice(offset: int64, length: int32) = 
        if length < 0 then raise(ArgumentOutOfRangeException(sprintf "Length must be non-negative."))
        else this.Slice(offset,length)
    /// <summary>
    /// Horizontally stack columns to the DataFrame.
    /// Returns a new DataFrame with the new columns appended.
    /// </summary>
    member this.HStack(columns: Series list) : DataFrame =
        let handles = 
            columns 
            |> List.map (fun s -> s.Handle) 
            |> List.toArray
        
        new DataFrame(PolarsWrapper.HStack(this.Handle, handles))

    /// <summary>
    /// Vertically stack another DataFrame to this one.
    /// Checks that the schema matches.
    /// </summary>
    member this.VStack(other: DataFrame) : DataFrame =
        new DataFrame(PolarsWrapper.VStack(this.Handle, other.Handle))

    // ==========================================
    // Printing / String Representation
    // ==========================================

    /// <summary>
    /// Returns the native Polars string representation of the DataFrame.
    /// Includes shape, header, and truncated data.
    /// </summary>
    override this.ToString() =
        PolarsWrapper.DataFrameToString handle

    /// <summary>
    /// Print the DataFrame to Console (Stdout).
    /// </summary>
    member this.Show() =
        printfn "%s" (this.ToString())
    /// <summary>
    /// Drop one or more columns from the DataFrame.
    /// Returns a new DataFrame.
    
    member this.Drop([<ParamArray>]columns: string array) =
        if isNull columns || columns.Length = 0 then
            new DataFrame(PolarsWrapper.CloneDataFrame this.Handle)
        else
            let newHandle = PolarsWrapper.Drop(this.Handle, columns)
            new DataFrame(newHandle)

    /// <summary>
    /// Drop columns using Polars Selectors or Expressions.
    /// </summary>
    member this.Drop(exprs: seq<Expr>) =
        if isNull exprs then nullArg (nameof exprs)

        use lf = this.Lazy()
        use droppedLf:LazyFrame = lf.Drop exprs
        
        droppedLf.Collect()
    member this.Drop([<ParamArray>]exprs: Expr array) =
        this.Drop(exprs :> seq<Expr>)
    /// <summary>
    /// Drop a column in-place and return it as a Series.
    /// Note: This mutates the original DataFrame.
    /// </summary>
    member this.DropInPlace(name: string) =
        if String.IsNullOrEmpty name then nullArg (nameof name)
        
        let seriesHandle = PolarsWrapper.DropInPlace(this.Handle, name)
        new Series(seriesHandle)
    /// <summary>
    /// Rename a column. Returns a new DataFrame.
    /// </summary>
    member this.Rename(oldName: string, newName: string) : DataFrame =
        new DataFrame(PolarsWrapper.Rename(handle, oldName, newName))
    
    /// <summary>
    /// Drop rows containing one or more Null values.
    /// </summary>
    member this.DropNulls(?subset: Selector) =
        use lf = this.Lazy()
        let droppedLf: LazyFrame = lf.DropNulls(?subset = subset)
        droppedLf.Collect()

    /// <summary>
    /// Drop rows with Nulls in specific columns.
    /// </summary>
    member this.DropNulls([<ParamArray>]subset: string array) =
        use lf = this.Lazy()
        let droppedLf: LazyFrame = lf.DropNulls(subset)
        droppedLf.Collect()

    /// <summary>
    /// Drop rows with Nulls by specific Expressions.
    /// </summary>
    member this.DropNulls([<ParamArray>]exprs: Expr array) =
        use lf = this.Lazy()
        let droppedLf: LazyFrame = lf.DropNulls(exprs)
        droppedLf.Collect()

    /// <summary>
    /// Drop rows containing one or more NaN values.
    /// </summary>
    member this.DropNan(?subset: Selector) =
        use lf = this.Lazy()
        let droppedLf: LazyFrame= lf.DropNan(?subset = subset)
        droppedLf.Collect()

    /// <summary>
    /// Drop rows with NaN in specific columns.
    /// </summary>
    member this.DropNan([<ParamArray>]subset: string array) =
        use lf = this.Lazy()
        let droppedLf: LazyFrame = lf.DropNan subset
        droppedLf.Collect()

    /// <summary>
    /// Drop rows with NaN by specific Expressions.
    /// </summary>
    member this.DropNan([<ParamArray>]exprs: Expr array) =
        use lf = this.Lazy()
        let droppedLf: LazyFrame = lf.DropNan exprs
        droppedLf.Collect()

    /// <summary>
    /// Sample n rows from the DataFrame.
    /// </summary>
    member this.Sample(n: int, ?withReplacement: bool, ?shuffle: bool, ?seed: uint64) : DataFrame =
        let replace = defaultArg withReplacement false
        let shuff = defaultArg shuffle true
        let s = Option.toNullable seed
        
        new DataFrame(PolarsWrapper.SampleN(handle, uint64 n, replace, shuff, s))

    /// <summary>
    /// Sample a fraction of rows from the DataFrame.
    /// </summary>
    member this.Sample(frac: double, ?withReplacement: bool, ?shuffle: bool, ?seed: uint64) : DataFrame =
        let replace = defaultArg withReplacement false
        let shuff = defaultArg shuffle true
        let s = Option.toNullable seed
        
        new DataFrame(PolarsWrapper.SampleFrac(handle, frac, replace, shuff, s))
    // Interop
    member this.ToArrow() = ArrowFfiBridge.ExportDataFrame handle
    // Properties
    member _.Rows = PolarsWrapper.DataFrameHeight handle
    member _.Height = PolarsWrapper.DataFrameHeight handle
    member _.Len = PolarsWrapper.DataFrameHeight handle
    member _.Width = PolarsWrapper.DataFrameWidth handle
    /// <summary>
    /// Get the number of chunks used by the first Column of this DataFrame.
    /// </summary>
    member this.NChunks() =
        if this.Width = 0 then 0L
        else this.Column(0).NChunks

    /// <summary>
    /// Rechunk the data in this DataFrame to a contiguous allocation.
    /// This will make sure all subsequent operations have optimal and predictable performance.
    /// </summary>
    member this.Rechunk() = new DataFrame(PolarsWrapper.DataFrameRechunk this.Handle)

    /// <summary>
    /// Get an array containing the number of chunks for all columns in this DataFrame.
    /// </summary>
    member this.NChunksAll() =
        this |> Seq.map (fun s -> s.NChunks) |> Seq.toArray
    /// <summary>
    /// True if the DataFrame contains no rows.
    /// </summary>
    member this.IsEmpty : bool = this.Height = 0L
    /// <summary>
    /// Return an estimation of the total (heap) allocated size of the DataFrame.
    /// Estimated size is given in the specified unit (bytes by default).
    /// </summary>
    /// <param name="unit">Scale the returned size to the given unit (uses 1024 base).</param>
    /// <returns>The estimated size as a double.</returns>
    member this.EstimatedSize(unit:SizeUnit) =

        let bytes = float (PolarsWrapper.DataFrameEstimatedSize handle)

        match unit with
        | SizeUnit.Bytes     -> bytes
        | SizeUnit.Kilobytes -> bytes / 1024.0
        | SizeUnit.Megabytes -> bytes / 1024.0 ** 2.0
        | SizeUnit.Gigabytes -> bytes / 1024.0 ** 3.0
        | SizeUnit.Terabytes -> bytes / 1024.0 ** 4.0

    /// <summary>
    /// Returns the shape of the DataFrame as (Height, Width).
    /// </summary>
    member this.Shape = this.Len,this.Width
    member _.ColumnNames = PolarsWrapper.GetColumnNames handle
    member _.Columns = PolarsWrapper.GetColumnNames handle

    /// <summary>
    /// Get a mask of all duplicated rows in this DataFrame.
    /// </summary>
    member this.IsDuplicated() = new Series(PolarsWrapper.DataFrameIsDuplicated handle)

    /// <summary>
    /// Get a mask of all unique rows in this DataFrame.
    /// </summary>
    member this.IsUnique() = new Series(PolarsWrapper.DataFrameIsUnique handle)


    member this.DataTypes = this.Schema.DataTypes


    /// <summary>
    /// Export DataFrame to Arrow C Data Interface Stream.
    /// Supports zero-copy and lazy chunked reading.
    /// </summary>
    /// <param name="seed">Optional seed for Native Global Shuffle.</param>
    /// <returns>Standard IArrowArrayStream</returns>
    member this.ToArrowStream(?seed: uint64) : IArrowArrayStream = 
        let nullableSeed = Option.toNullable seed
        ArrowStreamInterop.ExportToStream(this.Handle, ReadOnlySpan<int>.Empty, nullableSeed)

    /// <summary>
    /// Export DataFrame to Arrow C Data Interface Stream with Column Pruning.
    /// </summary>
    /// <param name="columnIndices">Column indices to prune the export (Projection Pushdown).</param>
    /// <param name="seed">Optional seed for Native Global Shuffle.</param>
    member this.ToArrowStream(columnIndices: ReadOnlySpan<int>, ?seed: uint64) : IArrowArrayStream = 
        let nullableSeed = Option.toNullable seed
        ArrowStreamInterop.ExportToStream(this.Handle, columnIndices, nullableSeed)

    /// <summary>
    /// Export DataFrame to Arrow C Data Interface Stream with Column Pruning (Array friendly).
    /// </summary>
    /// <param name="columnIndices">Column indices array to prune the export (Projection Pushdown).</param>
    /// <param name="seed">Optional seed for Native Global Shuffle.</param>
    member this.ToArrowStream(columnIndices: int array, ?seed: uint64) : IArrowArrayStream = 
        let span = if isNull columnIndices then ReadOnlySpan<int>.Empty else ReadOnlySpan<int> columnIndices
        let nullableSeed = Option.toNullable seed
        ArrowStreamInterop.ExportToStream(this.Handle, span, nullableSeed)


    member this.Column(name: string) : Series =
        let h = PolarsWrapper.DataFrameGetColumn(this.Handle, name)
        new Series(h)
    member this.Column(index: int) : Series =
        let h = PolarsWrapper.DataFrameGetColumnAt(this.Handle, index)
        new Series(h)
    /// <summary>
    /// Generate DataFrame from ADBC query results
    /// </summary>
    /// <param name="statement"></param>
    /// <exception cref="InvalidOperationException"></exception>
    static member ReadAdbc(statement: AdbcStatement) : DataFrame =
        ArgumentNullException.ThrowIfNull statement

        let result = statement.ExecuteQuery()

        if isNull result.Stream then
            raise (InvalidOperationException "ADBC query executed, but returned a null Arrow stream.")
            
        DataFrame.FromArrowStream result.Stream
    /// <summary>
    /// Executes a SQL query directly against an ADBC connection and reads the result into a zero-copy Polars DataFrame.
    /// Pure syntactic sugar: automatically manages the creation and disposal of the underlying AdbcStatement.
    /// </summary>
    /// <param name="connection">The active ADBC connection (e.g., DuckDB, SQLite).</param>
    /// <param name="sqlQuery">The SQL query string to execute.</param>
    static member ReadAdbc(connection: AdbcConnection, sqlQuery: string) : DataFrame =
        ArgumentNullException.ThrowIfNull connection
        
        if String.IsNullOrWhiteSpace sqlQuery then
            invalidArg "sqlQuery" "SQL query cannot be null or whitespace."

        use statement = connection.CreateStatement()
        
        statement.SqlQuery <- sqlQuery

        DataFrame.ReadAdbc statement
    // ==========================================
    // IEnumerable<Series> Support
    // ==========================================
    interface IEnumerable<Series> with
        member this.GetEnumerator() : IEnumerator<Series> =
            let seq = seq {
                let w = this.Columns.Length
                for i in 0 .. w - 1 do
                    yield this.Column(i)
            }
            seq.GetEnumerator()

    interface IEnumerable with
        member this.GetEnumerator() : IEnumerator =
            (this :> IEnumerable<Series>).GetEnumerator() :> IEnumerator
    interface IPolarsDataFrame with
        
        member this.Height = 
            int64 this.Height 
            
        member this.Schema = 
            this.Schema :> IPolarsSchema
        member this.Show(): unit = 
            this.Show()    
        member this.ToArrow() = 
            this.ToArrow()
        member this.WriteToAdbc(statement:AdbcStatement) = 
            this.WriteToAdbc statement
        member this.Column(index:int) = 
            this.Column index
        member this.ToArrowStream(columnIndices: ReadOnlySpan<int>, seed: Nullable<uint64>) =
            ArrowStreamInterop.ExportToStream(this.Handle, columnIndices, seed)
/// <summary>
/// A LazyFrame represents a logical plan of operations that will be optimized and executed only when collected.
/// <para>
/// Operations on LazyFrame are not executed immediately. Instead, they build a query plan.
/// Use <c>Collect()</c> to execute the plan and get a DataFrame.
/// </para>
/// </summary>
and LazyFrame(handle: LazyFrameHandle) =
    member _.Handle = handle
    abstract member Dispose : unit -> unit
    default x.Dispose() = 
        handle.Dispose()

    interface IDisposable with
        member x.Dispose() = x.Dispose()

    interface IPolarsLazyFrame with
        
        member this.Collect(useStreaming: bool) = 
            this.Collect useStreaming :> IPolarsDataFrame
            
        member this.Schema = 
            this.Schema :> IPolarsSchema
            
        member this.Explain(optimized: bool) = 
            this.Explain optimized
        member this.CollectAsync(useStreaming: bool, cancellationToken: CancellationToken) =
            task {
                let! df = this.CollectAsync(useStreaming, cancellationToken)
                
                return df :> IPolarsDataFrame
            }
    member internal this.CloneHandle() = PolarsWrapper.LazyClone handle
    member this.Clone() = new LazyFrame(this.CloneHandle())
    /// <summary> Execute the plan and return a DataFrame. </summary>
    member this.Collect(?streaming:bool) = 
        let stream = defaultArg streaming false
        let dfHandle = PolarsWrapper.LazyCollect(handle,stream)
        new DataFrame(dfHandle)
    member this.CollectAsync
        (
            ?useStreaming: bool, 
            ?cancellationToken: CancellationToken
        ) : Task<DataFrame> =
        let us = defaultArg useStreaming false
        let cct = defaultArg cancellationToken CancellationToken.None
        
        task {
            cct.ThrowIfCancellationRequested()

            let! dfHandle = PolarsWrapper.LazyCollectAsync(handle, us,cct)
            
            return new DataFrame(dfHandle)
        }
    /// <summary> Execute the plan using the streaming engine. </summary>
    member _.CollectStreaming() =
        let dfHandle = PolarsWrapper.CollectStreaming handle
        new DataFrame(dfHandle)

    /// <summary>
    /// Get the schema of the LazyFrame without executing it.
    /// Uses Zero-Copy native introspection.
    /// </summary>
    member this.Schema =
        let h = PolarsWrapper.GetLazySchema this.Handle 
        new PolarsSchema(h)
    member this.PrintSchema() =
        printfn "--- LazyFrame Schema ---"
        
        use sc = this.Schema

        sc.ToMap() 
        |> Map.iter (fun name dtype -> 
            printfn "%-15s | %O" name dtype
        )
        
        printfn "------------------------"
    member this.Columns = this.Schema.Names
    member this.ColumnNames = this.Columns
    member this.DataTypes = this.Schema.DataTypes
    /// <summary> Print the query plan. </summary>
    member this.Explain(?optimized: bool) = 
        let opt = defaultArg optimized true
        PolarsWrapper.Explain(handle, opt)

    /// <summary>
    /// Stream the query result in batches.
    /// This executes the query and calls 'onBatch' for each RecordBatch produced.
    /// </summary>
    member this.SinkBatches(onBatch: Action<RecordBatch>) : unit =
        let newHandle = PolarsWrapper.SinkBatches(this.CloneHandle(), onBatch)
        
        let lfRes = new LazyFrame(newHandle)
        use _ = lfRes.Collect()
        () 
    /// <summary>
    /// Create an empty (n=0) or n-row null-filled (n>0) copy of the LazyFrame.
    /// Returns a n-row null-filled LazyFrame with an identical schema.
    /// </summary>
    /// <param name="n">Number of (null-filled) rows to return in the cleared frame.</param>
    /// <returns>A new LazyFrame.</returns>
    member this.Clear(?n:int64) = 
        use schema = this.Schema
        schema.ToLazyFrame(?length=n)
    
    /// <summary>
    /// Keep unique rows (stable) based on a subset of columns defined by a Selector.
    /// </summary>
    member this.Unique
        (
            ?subset: Selector, 
            ?keep: UniqueKeepStrategy, 
            ?maintainOrder: bool
        ) =
        let keepArg = defaultArg keep UniqueKeepStrategy.First
        let maintainArg = defaultArg maintainOrder false

        let subsetHandle =
            match subset with
            | Some s -> s.CloneHandle()
            | None -> Unchecked.defaultof<SelectorHandle>

        let newHandle = PolarsWrapper.LazyUnique(
            this.CloneHandle(), 
            subsetHandle, 
            keepArg.ToNative(), 
            maintainArg
        )

        new LazyFrame(newHandle)
    /// <summary>
    /// Keep unique rows based on specific column names.
    /// </summary>
    member this.Unique
        (
            columns: seq<string>, 
            ?keep: UniqueKeepStrategy, 
            ?maintainOrder: bool
        ) =
        let columnsArray =
            match columns with
            | :? (string array) as arr -> arr
            | _ -> columns |> Seq.toArray

        if columnsArray.Length = 0 then
            this.Unique(?subset = None, ?keep = keep, ?maintainOrder = maintainOrder)
        else
            use selector = Selector.ByName columnsArray
            this.Unique(subset = selector, ?keep = keep, ?maintainOrder = maintainOrder)
    /// <summary>
    /// Drop selected columns by selector.
    /// </summary>
    member this.Drop(selector: Selector) =
        let h = PolarsWrapper.LazyFrameDrop(this.CloneHandle(), selector.CloneHandle())
        new LazyFrame(h)

    /// <summary>
    /// Drop selected columns by column names.
    /// </summary>
    member this.Drop([<ParamArray>]columns: string array) =
        if isNull columns then nullArg (nameof columns)
        
        this.Drop(Selector.ByName columns)

    /// <summary>
    /// Drop columns by specific Expressions.
    /// </summary>
    member this.Drop(exprs: seq<Expr>) =
        if isNull exprs then nullArg (nameof exprs)
        
        exprs |> Seq.fold (fun (lf: LazyFrame) expr -> lf.Drop(expr.ToSelector())) this
    member this.Drop([<ParamArray>]exprs: Expr array) =
        this.Drop(exprs :> seq<Expr>)
    // ==========================================
    // DropNulls
    // ==========================================

    /// <summary>
    /// Drop rows containing one or more Null values.
    /// </summary>
    member this.DropNulls(?subset: Selector) =
        let subsetHandle = subset |> Option.map (fun s -> s.CloneHandle()) |> Option.toObj
            
        let h = PolarsWrapper.LazyFrameDropNulls(this.CloneHandle(), subsetHandle)
        new LazyFrame(h)

    /// <summary>
    /// Drop rows with Nulls in specific columns.
    /// </summary>
    member this.DropNulls([<ParamArray>] subset: string array) =
        if isNull subset || subset.Length = 0 then 
            this.DropNulls()
        else 
            this.DropNulls(Expr.Col(subset))

    /// <summary>
    /// Drop rows with Nulls by specific Expressions.
    /// </summary>
    member this.DropNulls([<ParamArray>] exprs: Expr array) =
        if isNull exprs then nullArg (nameof exprs)
        exprs |> Seq.fold (fun (lf: LazyFrame) expr -> lf.DropNulls(expr.ToSelector())) this

    // ==========================================
    // DropNan
    // ==========================================

    /// <summary>
    /// Drop rows containing one or more NaN values.
    /// </summary>
    member this.DropNan(?subset: Selector) =
        let subsetHandle = subset |> Option.map (fun s -> s.CloneHandle()) |> Option.toObj
            
        let h = PolarsWrapper.LazyFrameDropNans(this.CloneHandle(), subsetHandle)
        new LazyFrame(h)

    /// <summary>
    /// Drop rows with NaN in specific columns.
    /// </summary>
    member this.DropNan([<ParamArray>] subset: string array) =
        if isNull subset || subset.Length = 0 then 
            this.DropNan()
        else 
            this.DropNan(Expr.Col subset)

    /// <summary>
    /// Drop rows with NaN by specific Expressions.
    /// </summary>
    member this.DropNan([<ParamArray>] exprs: Expr array) =
        if isNull exprs then nullArg (nameof exprs)
        exprs |> Seq.fold (fun (lf: LazyFrame) expr -> lf.DropNan(expr.ToSelector())) this
    /// <summary>
    /// Filter rows based on a boolean expression.
    /// <para>
    /// In a LazyFrame, this operation is added to the logical plan and is optimized before execution.
    /// Polars will attempt to push this filter down as close to the data source as possible (Predicate Pushdown).
    /// </para>
    /// </summary>
    member this.Filter (expr: Expr) : LazyFrame =
        let lfClone = this.CloneHandle()
        let exprClone = expr.CloneHandle()
        
        let h = PolarsWrapper.LazyFilter(lfClone, exprClone)
        new LazyFrame(h)

    member this.Filter (series: Series) : LazyFrame =
        if series.DataType <> DataType.Boolean then
            invalidArg "series" "Series DataType should be Boolean"
            
        let sh = PolarsWrapper.Lit(series.CloneHandle())
        use expr = new Expr(sh) 
        this.Filter(expr)
    
    member this.Select (expr: Expr) : LazyFrame =
        this.Select [|expr|]
    member this.Select (exprs: seq<Expr>) : LazyFrame =
        let lfClone = this.CloneHandle()
        let handles = exprs |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        
        let h = PolarsWrapper.LazySelect(lfClone, handles)
        new LazyFrame(h)
    member this.Select(columns: seq<IColumnExpr>) =
            let exprs = 
                columns 
                |> Seq.collect (fun x -> x.ToExprs()) 
                |> Seq.toList
            
            this.Select exprs

    // ==========================================
    // TopK / BottomK
    // ==========================================

    /// <summary>
    /// Get the top k rows based on the given columns.
    /// This is often faster than a full sort followed by a head.
    /// </summary>
    /// <param name="k">Number of rows to return.</param>
    /// <param name="by">Columns to sort by.</param>
    /// <param name="reverse">Sort direction per column. Default is false (no reverse).</param>
    member this.TopK(k: int, by: seq<#IColumnExpr>, ?reverse: seq<bool>) =
        let exprHandles = 
            by 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.map (fun e -> e.CloneHandle()) 
            |> Seq.toArray
        
        let descArr = 
            match reverse with
            | Some d -> d |> Seq.toArray
            | None -> [| false |] 

        let lfHandle = this.CloneHandle()

        let h = PolarsWrapper.LazyFrameTopK(lfHandle, uint k, exprHandles, descArr)
        new LazyFrame(h)

    /// <summary>
    /// Get the bottom k rows based on the given columns.
    /// </summary>
    member this.BottomK(k: int, by: seq<#IColumnExpr>, ?reverse: seq<bool>) =
        let exprHandles = 
            by 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.map (fun e -> e.CloneHandle()) 
            |> Seq.toArray
        
        let descArr = 
            match reverse with
            | Some d -> d |> Seq.toArray
            | None -> [| false |]

        let lfHandle = this.CloneHandle()
        let h = PolarsWrapper.LazyFrameBottomK(lfHandle, uint k, exprHandles, descArr)
        new LazyFrame(h)

    // [Overload] Sugar for single boolean reversing
    member this.TopK(k: int, by: seq<#IColumnExpr>, reverse: bool) =
        this.TopK(k, by, [| reverse |])
    
    member this.BottomK(k: int, by: seq<#IColumnExpr>, reverse: bool) =
        this.BottomK(k, by, [| reverse |])


    // ==========================================
    // Unnest
    // ==========================================

    /// <summary>
    /// Decompose a struct column into multiple columns.
    /// </summary>
    member this.Unnest(selector: Selector,?separator: string) =
        let lfHandle = this.CloneHandle()
        
        let selHandle = selector.CloneHandle()

        let sep = defaultArg separator null

        let h = PolarsWrapper.LazyFrameUnnest(lfHandle, selHandle, sep)
        new LazyFrame(h)

    /// <summary>
    /// Helper: Unnest columns by name.
    /// </summary>
    member this.Unnest(columns: string list,?separator: string) =
        let columnsArray = columns|> List.toArray
        let handle = PolarsWrapper.SelectorCols columnsArray
        let sel = new Selector(handle)
        this.Unnest (sel,?separator=separator)

    /// <summary>
    /// Helper: Unnest a single column by name.
    /// </summary>
    member this.Unnest(column: string, ?separator: string) =
        this.Unnest ([column], ?separator=separator)
    /// <summary>
    /// Limit the number of rows in the LazyFrame.
    /// This is an optimization hint that pushes down the limit to the scan if possible.
    /// </summary>
    /// <param name="n">Maximum number of rows to return.</param>
    member this.Limit (n: uint) : LazyFrame =
        let lfClone = this.CloneHandle()
        let h = PolarsWrapper.LazyLimit(lfClone, n)
        new LazyFrame(h)
    /// <summary>
    /// Add or replace a single column in the LazyFrame.
    /// </summary>
    /// <param name="expr">The expression defining the new column.</param>
    member this.WithColumn (expr: Expr) : LazyFrame =
        let lfClone = this.CloneHandle()
        let exprClone = expr.CloneHandle()
        let handles = [| exprClone |] 
        let h = PolarsWrapper.LazyWithColumns(lfClone, handles)
        new LazyFrame(h)
    /// <summary>
    /// Add or replace multiple columns in the LazyFrame.
    /// </summary>
    /// <param name="exprs">List of expressions defining the new columns.</param>
    member this.WithColumns (exprs: seq<Expr>) : LazyFrame =
        let lfClone = this.CloneHandle()
        let handles = exprs |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        let h = PolarsWrapper.LazyWithColumns(lfClone, handles)
        new LazyFrame(h)
    /// <summary>
    /// Add or replace columns using generic column expressions (Expr or Selectors).
    /// </summary>
    member this.WithColumns (columns:seq<#IColumnExpr>) =
        let exprs = 
            columns 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.toList
        
        this.WithColumns exprs

    /// <summary>
    /// Cast LazyFrame column(s) to the specified dtype(s) using a dictionary mapping.
    /// </summary>
    member this.Cast(dtypes: seq<string * DataType>, ?strict: bool) =
        let strictArg = defaultArg strict true

        let castExprs =
            dtypes
            |> Seq.map (fun (colName, dtype) -> 
                Expr.Col(colName).Cast(dtype, strictArg)
            )

        this.WithColumns castExprs

    /// <summary>
    /// Cast all columns in the LazyFrame to the specified dtype.
    /// </summary>
    member this.Cast(dtype: DataType, ?strict: bool) =
        let strictArg = defaultArg strict true
        
        let castAllExpr = Expr.Col("*").Cast(dtype, strictArg)
        
        this.Select castAllExpr
    /// <summary>
    /// Cast columns matching an Expr/Selector to a specific DataType.
    /// </summary>
    member this.Cast(expr: Expr, dtype: DataType, ?strict: bool) =
        let strictArg = defaultArg strict true
        this.WithColumns [|expr.Cast(dtype, strictArg)|]

    /// <summary>
    /// Cast multiple expressions/selectors to their target DataTypes.
    /// Example: lf.Cast([ (pl.cs.Numeric(), DataType.Float32); (col("Id"), DataType.Int32) ])
    /// </summary>
    member this.Cast(dtypes: seq<Expr * DataType>, ?strict: bool) =
        let strictArg = defaultArg strict true
        
        let castExprs = 
            dtypes 
            |> Seq.map (fun (expr, dt) -> expr.Cast(dt, strictArg))
            
        this.WithColumns castExprs
    /// <summary>
    /// Return the number of non-null elements for each column.
    /// </summary>
    /// <returns></returns>
    member this.Count() = 
        this.Select(Expr.All().Count())
    /// <summary>
    /// Aggregate the columns in the Frame to their sum value.
    /// </summary>
    /// <returns></returns>
    member this.Sum() =
        this.Select(Expr.All().Sum())
    /// <summary>
    /// Aggregate the columns in the Frame to their maximum value.
    /// </summary>
    /// <returns></returns>
    member this.Max() =
        this.Select(Expr.All().Max())
    /// <summary>
    /// Aggregate the columns in the Frame to their minimum value.
    /// </summary>
    /// <returns></returns>
    member this.Min() =
        this.Select(Expr.All().Min())
    /// <summary>
    /// Aggregate the columns in the Frame to their mean value.
    /// </summary>
    /// <returns></returns>
    member this.Mean() =
        this.Select(Expr.All().Mean())
    /// <summary>
    /// Aggregate the columns in the Frame to their median value.
    /// </summary>
    /// <returns></returns>
    member this.Median() =
        this.Select(Expr.All().Median())
    /// <summary>
    /// Aggregate the columns in the Frame as the sum of their null value count.
    /// </summary>
    /// <returns></returns>
    member this.NullCount() = 
        this.Select(Expr.All().NullCount())
    /// <summary>
    /// Aggregate the columns in the Frame to their standard deviation value.
    /// </summary>
    /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1.</param>
    /// <returns></returns>
    member this.Std(?ddof:int) = 
        let d = defaultArg ddof 1
        this.Select(Expr.All().Std(ddof=d))
    /// <summary>
    /// Aggregate the columns in the Frame to their variance value.
    /// </summary>
    /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1.</param>
    /// <returns></returns>
    member this.Var(?ddof:int) =
        let d = defaultArg ddof 1
        this.Select(Expr.All().Var(ddof=d))
    /// <summary>
    /// Aggregate the columns in the Frame to their quantile value.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0.</param>
    /// <param name="method">['nearest’, ‘higher’, ‘lower’, ‘midpoint’, ‘linear’] Interpolation method.</param>
    /// <returns></returns>
    member this.Quantile(quantile:float, ?method: QuantileMethod) =
        let met = defaultArg method QuantileMethod.Linear
        this.Select(Expr.All().Quantile(quantile,met))
 

    member this.Explode(selector: Selector,?emptyAsNull:bool,?keepNulls:bool) : LazyFrame =
        let lfClone = this.CloneHandle()
        let sh = selector.CloneHandle()
        let ean = defaultArg emptyAsNull true
        let kn = defaultArg keepNulls true
        new LazyFrame(PolarsWrapper.LazyExplode(lfClone, sh, ean, kn))

    member this.Explode(columns: seq<string>) =
        let names = Seq.toArray columns
        let h = PolarsWrapper.SelectorCols names
        let sel = new Selector(h)
        this.Explode sel

    member this.Explode(column: string) = 
        this.Explode [column]

 
    /// <summary>
    /// Slice the LazyFrame along the rows.
    /// </summary>
    member this.Slice(offset: int64, length: uint32) = 
        new LazyFrame(PolarsWrapper.LazySlice(this.Handle,offset, length))
    member this.Slice(offset: int64, length: int32) = 
        if length < 0 then raise(ArgumentOutOfRangeException(sprintf "Length must be non-negative."))
        else this.Slice(offset,length)
    /// <summary>
    /// Rename the lazyframe columns
    /// Example: lf.Rename ["colA", "col1"; "colB", "col2"]
    /// </summary>
    /// <param name="strict">
    /// If <c>true</c>, an error is raised if any column in <paramref name="existing"/> is not found in the schema. 
    /// If <c>false</c>, columns that are not found are silently ignored. Default is <c>true</c>.
    /// </param>
    member this.Rename(mapping: (string * string) list, ?strict: bool) =
        let oldNames, newNames = List.unzip mapping
        let isStrict = defaultArg strict true
        let oldNames, newNames = List.unzip mapping
        let pOldNames = List.toArray oldNames
        let pNewNames = List.toArray newNames
        let pStrict = defaultArg strict true

        let handle = PolarsWrapper.LazyRename(
            this.Handle, 
            pOldNames, 
            pNewNames, 
            pStrict
        )
        
        new LazyFrame(handle)
    static member Concat  (lfs: LazyFrame list) (how: ConcatType) : LazyFrame =
        let handles = lfs |> List.map (fun lf -> lf.CloneHandle()) |> List.toArray
        new LazyFrame(PolarsWrapper.LazyConcat(handles, how.ToNative(), false, true))

    /// <summary>
    /// Returns the native Polars string representation of the LazyFrame.
    /// Includes shape, header, and truncated data.
    /// </summary>
    override this.ToString() =
        this.Clone().Collect().ToString()

    /// <summary>
    /// Print the LazyFrame to Console (Stdout).
    /// </summary>
    member this.Show() =
        printfn "%s" (this.ToString())

/// <summary>
/// Polars Schema definition (Name -> DataType).
/// </summary>
and PolarsSchema (handle: SchemaHandle) =
    
    // --- Property ---
    member val Handle = handle

    // --- Constructors ---
    static member private CreateHandleFromFields(fields: seq<string * DataType>) =
            let names = fields |> Seq.map fst |> Seq.toArray
            let typeHandles = fields |> Seq.map (fun (_, t) -> t.CreateHandle()) |> Seq.toArray
            
            try
                PolarsWrapper.NewSchema(names, typeHandles)
            finally
                for th in typeHandles do th.Dispose()
    /// <summary> Create an empty schema </summary>
    new () = new PolarsSchema(PolarsWrapper.SchemaCreate())

    /// <summary> Create schema from field definitions </summary>
    new (fields: seq<string * DataType>) =
        new PolarsSchema(PolarsSchema.CreateHandleFromFields fields)
    /// <summary>
    /// Create a Schema directly from a .NET type (e.g., a record or class).
    /// </summary>
    /// <typeparam name="T">The record or class type.</typeparam>
    /// <returns>A PolarsSchema mapped from the type's properties.</returns>
    static member FromRecord<'T>() =
        let handle = PolarsWrapper.NewSchemaFromType(typeof<'T>)
        new PolarsSchema(handle)

    static member ofMap (m: Map<string, DataType>) = new PolarsSchema(m |> Map.toSeq)
    static member ofList (fields: (string * DataType) list) = new PolarsSchema(fields)

    // --- Inspection API (Alignment with C#) ---

    member this.Len() = PolarsWrapper.GetSchemaLen this.Handle

    /// <summary> Get column name and type at specific index </summary>
    member private this.GetFieldAt(index: uint64) =
        let mutable name = Unchecked.defaultof<string>
        let mutable typeHandle = Unchecked.defaultof<DataTypeHandle>
        
        PolarsWrapper.GetSchemaFieldAt(this.Handle, index, &name, &typeHandle)
        
        try
            let dt = DataType.FromHandle typeHandle
            name, dt
        finally
            if not typeHandle.IsInvalid then 
                typeHandle.Dispose()

    /// <summary> Get all column names </summary>
    member this.Names =
        let len = this.Len()
        [ for i in 0UL .. (len - 1UL) -> 
            let mutable name = Unchecked.defaultof<string>
            let mutable _th = Unchecked.defaultof<DataTypeHandle>
            PolarsWrapper.GetSchemaFieldAt(this.Handle, i, &name, &_th)
            if not _th.IsInvalid then _th.Dispose() 
            name 
        ]

    /// <summary> Get all column datatypes </summary>
    member this.DataTypes =
        let len = this.Len()
        [ for i in 0UL .. (len - 1UL) -> 
            let mutable name = Unchecked.defaultof<string>
            let mutable _th = Unchecked.defaultof<DataTypeHandle>
            PolarsWrapper.GetSchemaFieldAt(this.Handle, i, &name, &_th)
            
            DataType.FromHandle _th 
        ]

    /// <summary> Convert to F# ordered List of fields </summary>
    member this.ToList() =
        let len = this.Len()
        [ for i in 0UL .. (len - 1UL) do
            yield this.GetFieldAt i 
        ]

    /// <summary> Convert to F# Map (Warning: Does not preserve column order!) </summary>
    member this.ToMap() =
        this.ToList() |> Map.ofList

    /// <summary> Convert to Dictionary </summary>
    member this.ToDictionary() =
        let len = this.Len()
        let dict = Dictionary<string, DataType>(int len)
        for i in 0UL .. (len - 1UL) do
            let name, dtype = this.GetFieldAt i
            dict.[name] <- dtype
        dict

    /// <summary> Indexer: schema["col_name"] </summary>
    member this.Item 
        with get(name: string) =
            let len = this.Len()
            let rec find i =
                if i >= len then raise (KeyNotFoundException $"Column '{name}' not found in Schema.")
                else
                    let colName, dtype = this.GetFieldAt(i)
                    if colName = name then dtype
                    else 
                        find (i + 1UL)
            find 0UL

    /// <summary>
    /// Creates an empty DataFrame from this Schema.
    /// </summary>
    member this.ToDataFrame(?length: int64) =
        let len = defaultArg length 0L
        let dfHandle: DataFrameHandle = PolarsWrapper.DataFrameFromSchema(handle,uint len)
        new DataFrame(dfHandle)

    /// <summary>
    /// Creates an empty LazyFrame from this Schema.
    /// </summary>
    member this.ToLazyFrame(?length: int64) =
        use emptyDf = this.ToDataFrame(?length=length)
        emptyDf.Lazy()

    // --- Display ---
    
    override this.ToString() =
        if this.Handle.IsInvalid then "Schema: {}"
        else
            let sb = StringBuilder "Schema: {"
            let len = this.Len()
            for i in 0UL .. (len - 1UL) do
                let name, dtype = this.GetFieldAt(i)
                sb.Append $"{name}: {dtype}" |> ignore
                if i < len - 1UL then sb.Append(", ") |> ignore
            sb.Append "}" |> ignore
            sb.ToString()

    // ==========================================
    // Equality Members
    // ==========================================
    
    interface IEquatable<PolarsSchema> with
        member this.Equals(other: PolarsSchema) =
            if Object.ReferenceEquals(this, other) then true
            elif this.Handle.IsInvalid || other.Handle.IsInvalid then false
            else
                let myList = this.ToList()
                let otherList = other.ToList()
                
                if myList.Length <> otherList.Length then false
                else
                    Enumerable.SequenceEqual(myList, otherList)

    override this.Equals(obj: obj) =
        match obj with
        | :? PolarsSchema as other -> (this :> IEquatable<_>).Equals(other)
        | _ -> false

    override this.GetHashCode() =
        if this.Handle.IsInvalid then 0
        else
            let hash = HashCode()
            for name, dt in this.ToList() do
                hash.Add(name)
                hash.Add(dt) 
            hash.ToHashCode()

    static member op_Equality (left: PolarsSchema, right: PolarsSchema) =
        if Object.ReferenceEquals(left, null) then Object.ReferenceEquals(right, null)
        else left.Equals(right)

    static member op_Inequality (left: PolarsSchema, right: PolarsSchema) =
        not (left = right)

    // --- Interface ---
    
    interface IDisposable with
        member this.Dispose() = 
            if not (isNull (box this.Handle)) && not this.Handle.IsInvalid then
                this.Handle.Dispose()

    interface IPolarsSchema with
        member this.Length = 
            int (this.Len())
            
        member this.ColumnNames = 
            List<string> this.Names
            
        member this.Item
            with get (name: string) = 
                this.[name] :> IPolarsDataType
        member this.ToDictionary() =
            let dict = Dictionary<string, IPolarsDataType>()
            for kvp in this.ToDictionary() do
                dict.[kvp.Key] <- kvp.Value :> IPolarsDataType
            dict

/// <summary>
/// SQL Context for executing SQL queries on registered LazyFrames.
/// </summary>
type SqlContext() as this =
    let handle = PolarsWrapper.SqlContextNew()
    
    interface IDisposable with
        member _.Dispose() = handle.Dispose()
    
    interface IPolarsSqlContext with
        member _.Register(tableName: string, df: IPolarsDataFrame) =
            this.Register(tableName, df :?> DataFrame)

        member _.Register(tableName: string, lf: IPolarsLazyFrame) =
            this.Register(tableName, lf :?> LazyFrame)

        member _.Execute(sql: string) =
            this.Execute sql :> IPolarsLazyFrame

    /// <summary> Register a LazyFrame as a table for SQL querying. </summary>
    member _.Register(name: string, lf: LazyFrame) =
        PolarsWrapper.SqlRegister(handle, name, lf.CloneHandle())

    /// <summary> Register a DataFrame as a table for SQL querying. </summary>
    member _.Register(name: string, df: DataFrame) =
        let lf = df.Lazy()
        PolarsWrapper.SqlRegister(handle, name, lf.Handle)

    member _.UnRegister(name: string) = 
        PolarsWrapper.SqlUnRegister(handle,name)

    /// <summary> Get the names of all registered tables, in sorted order. </summary>
    member _.GetTables() =
        PolarsWrapper.SqlGetTables(handle)

    /// <summary> Execute a SQL query and return a LazyFrame. </summary>
    member _.Execute(query: string) =
        new LazyFrame(PolarsWrapper.SqlExecute(handle, query))

