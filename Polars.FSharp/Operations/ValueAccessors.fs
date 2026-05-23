namespace Polars.FSharp

[<AutoOpen>]
module ValueAccessorOps = 
    open Apache.Arrow
    open System
    type DataFrame with
        /// <summary>
        /// Gets an integer (Int64) value from the specified column and row.
        /// </summary>
        member this.Int(colName: string, rowIndex: int) : int64 option = 
            use series = this.Column colName
            series.GetValue<int64 option>(int64 rowIndex)

        /// <summary>
        /// Gets a float (Double) value from the specified column and row.
        /// </summary>
        member this.Float(colName: string, rowIndex: int) : float option = 
            use series = this.Column colName
            series.GetValue<float option>(int64 rowIndex)
        /// <summary>
        /// Gets a string value from the specified column and row.
        /// </summary>
        member this.String(colName: string, rowIndex: int) : string option = 
            use series = this.Column colName
            series.GetValue<string>(int64 rowIndex) |> Option.ofObj
        member this.StringList(colName: string, rowIndex: int) : string list option =
            use colDf = this.Select(Expr.Col colName)
            use arrowBatch = colDf.ToArrow()
            
            let col = arrowBatch.Column colName
            
            let extractStrings (valuesArr: IArrowArray) (startIdx: int) (endIdx: int) =
                match valuesArr with
                | :? StringArray as sa ->
                    [ for i in startIdx .. endIdx - 1 -> sa.GetString i ]
                | :? StringViewArray as sva ->
                    [ for i in startIdx .. endIdx - 1 -> sva.GetString i ]
                | _ -> [] 

            match col with
            // Case A: Arrow.ListArray 
            | :? ListArray as listArr ->
                if listArr.IsNull rowIndex then None
                else
                    let start = listArr.ValueOffsets.[rowIndex]
                    let end_ = listArr.ValueOffsets.[rowIndex + 1]
                    Some (extractStrings listArr.Values start end_)

            // Case B: Large List (64-bit offsets) 
            | :? LargeListArray as listArr ->
                if listArr.IsNull rowIndex then None
                else
                    let start = int listArr.ValueOffsets.[rowIndex]
                    let end_ = int listArr.ValueOffsets.[rowIndex + 1]
                    Some (extractStrings listArr.Values start end_)

            | _ -> 
                // System.Console.WriteLine($"[Debug] Mismatched Array Type: {col.GetType().Name}")
                None
        member this.Decimal(col: string, row: int) : decimal option =
            use s = this.Column col
            s.Decimal row
        // 1. Boolean
        member this.Bool(col: string, row: int) : bool option =
            use s = this.Column col
            s.Bool row
        // 2. Date (DateOnly)
        member this.Date(col: string, row: int) : DateOnly option =
            use s = this.Column col
            s.Date row

        // 3. Time (TimeOnly)
        member this.Time(col: string, row: int) : TimeOnly option =
            use s = this.Column col
            s.Time row

        // 4. DateTime (DateTime)
        member this.DateTime(col: string, row: int) : DateTime option =
            use s = this.Column col
            s.DateTime row

        // 5. Duration (TimeSpan)
        member this.Duration(col: string, row: int) : TimeSpan option =
            use s = this.Column col
            s.Duration row
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
        member this.IsNullAt(col: string, row: int) : bool =
            use s = this.Column col
            s.IsNullAt row
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

            let w = this.Columns.Length
            let rowData = Array.zeroCreate<obj> w

            for i in 0 .. w - 1 do
                rowData.[i] <- this.[index, i]

            rowData

        

 