namespace Polars.FSharp

[<AutoOpen>]
module ValueAccessorOps =
    open System
    type DataFrame with
        /// <summary>
        /// Get all columns as an array of Series.
        /// Order is guaranteed to match the physical column order.
        /// </summary>
        member this.GetColumns() : Series[] =
            let w = int this.Width
            let cols = Array.zeroCreate<Series> w
            for i = 0 to w - 1 do
                cols.[i] <- this.Column i
            cols

        member this.GetSeries() : Series list =
            [ for i in 0 .. int this.Width - 1 -> this.Column i ]
        /// <summary>
        /// Check if the value at the specified column and row is null.
        /// </summary>
        member this.IsNullAt(col: string, row: int,?uncheck:bool) : bool =
            use s = this.Column col
            s.IsNullAt(row,?uncheck=uncheck)
        /// <summary>
        /// Get the number of null values in a specific column.
        /// </summary>
        member this.NullCount(colName: string) : int64 =
            use s = this.Column colName
            s.NullCount
        member this.IsNan(col: string) =
            use s = this.Column col
            s.IsNan()
        member this.IsNotNan (col:string) =
            use s = this.Column col
            s.IsNotNan()
        member this.IsFinite (col:string) =
            use s = this.Column col
            s.IsFinite()
        member this.IsInfinite (col:string) =
            use s = this.Column col
            s.IsInfinite()
        /// <summary>
        /// Helper to get a cell value as an F# List directly.
        /// </summary>
        member this.CellList<'T>(colName: string,row:int) : 'T list =
            let s = this.Column colName
            s.GetList<'T>(int64 row)

        /// <summary>
        /// Get a value from the DataFrame using a generic type argument.
        /// Eliminates the need for unbox, but throws if type mismatches.
        /// </summary>
        member this.Cell<'T>(colName: string ,rowIndex: int) : 'T =
            let s = this.Column colName
            s.GetValue<'T>(int64 rowIndex)
        /// <summary>
        /// Get a value from the DataFrame using a generic type argument.
        /// Eliminates the need for unbox, but throws if type mismatches.
        /// </summary>
        member this.Cell<'T>(rowIndex: int,colName: string ) : 'T =
            let s = this.Column colName
            s.GetValue<'T>(int64 rowIndex)

        // ==========================================
        // Row Access
        // ==========================================

        /// <summary>
        /// Get data for a specific row as an object array.
        /// Similar to DataTable.Rows[i].ItemArray.
        /// </summary>
        member this.Row (index: int) : obj[] =
            let h = int64 this.Height
            if int64 index < 0L || int64 index >= h then
                raise (IndexOutOfRangeException(sprintf "Row index %d is out of bounds. Height: %d" index h))
            Array.init this.Columns.Length (fun i -> this.[index, i])
