namespace Polars.FSharp

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module LazyFrame =
    /// Collect the LazyFrame using the specified engine.
    let collectWithEngine (engine: Engine) (lazyFrame: LazyFrame) : DataFrame =
        lazyFrame.Collect(engine)
    /// Collect the LazyFrame using the default engine.
    let collect (lazyFrame: LazyFrame) : DataFrame =
        lazyFrame.Collect()
    /// Explain the execution plan of the LazyFrame.
    let explain (lazyFrame: LazyFrame) : string =
        lazyFrame.Explain true
    /// <summary> Explain the unoptimized LazyFrame execution plan. </summary>
    let explainUnoptimized (lf: LazyFrame) = lf.Explain false
    /// Get the head of the LazyFrame.
    let head (n: int) (lazyFrame: LazyFrame) : LazyFrame =
        lazyFrame.Head(uint n)
    /// Get the tail of the LazyFrame.
    let tail (n: uint) (lazyFrame: LazyFrame) : LazyFrame =
        lazyFrame.Tail(n)
    /// <summary> Slice rows starting at offset with length. </summary>
    let slice (offset: int64) (length: uint32) (lf: LazyFrame) : LazyFrame =
        lf.Slice(offset, length)
    /// <summary> Get the schema of the LazyFrame. </summary>
    let collectSchema (lf: LazyFrame) = lf.Schema
    /// <summary> Filter rows based on a boolean expression. </summary>
    let filter (expr: Expr) (lf: LazyFrame) : LazyFrame =
        lf.Filter expr
    /// <summary> Filter frame rows by combining multiple predicates with logical AND. </summary>
    let filterAll (predicates: seq<Expr>) (lf: LazyFrame) : LazyFrame =
        match Seq.toList predicates with
        | [] -> lf
        | [ p ] -> lf.Filter p
        | head :: tail ->
            let combined = tail |> List.fold (fun acc e -> acc .&& e) head
            lf.Filter combined
    /// <summary> Select columns from LazyFrame. </summary>
    let select (exprs: seq<#IColumnExpr>) (lf: LazyFrame) : LazyFrame =
        lf.Select exprs
    /// <summary> Sort (Order By) the LazyFrame. </summary>
    let sortAscending (exprs: seq<Expr>)(lf: LazyFrame) : LazyFrame =
        lf.Sort (exprs,false)
    /// <summary> Sort (Order By) the LazyFrame in descending order. </summary>
    let sortDescending (exprs: seq<Expr>)(lf: LazyFrame) : LazyFrame =
        lf.Sort(exprs,true)
    /// <summary> Alias for sortAscending. </summary>
    let orderByAscending (expr: seq<Expr>) (lf: LazyFrame) = sortAscending expr lf
    /// <summary> Alias for sortDescending. </summary>
    let orderByDescending (expr: seq<Expr>) (lf: LazyFrame) = sortDescending expr lf
    /// <summary> Add or replace columns in the LazyFrame. </summary>
    let withColumn (expr: Expr) (lf: LazyFrame) : LazyFrame =
        lf.WithColumns expr
    /// <summary> Add or replace multiple columns in the LazyFrame. </summary>
    let withColumns (exprs: seq<Expr>) (lf: LazyFrame) : LazyFrame =
        lf.WithColumns exprs
    /// <summary> Group by keys. </summary>
    let groupBy (keys: seq<Expr>)(lf: LazyFrame) :LazyGroupBy =
        lf.GroupBy(keys)
    /// <summary> Having clause for LazyGroupBy. </summary>
    let having (predicate: Expr) (builder: LazyGroupBy) = builder.Having(predicate)
    /// <summary> Aggregate LazyGroupBy. </summary>
    let agg (aggs: seq<Expr>) (builder: LazyGroupBy) = builder.Agg(aggs)
    /// <summary> Perform a join between two LazyFrames. </summary>
    let joinOn (other: LazyFrame) (on: Expr seq) (how: JoinType) (lf: LazyFrame) : LazyFrame =
        lf.Join(other,on,how)
    /// <summary> Perform a join between two LazyFrames using join keys. </summary>
    let join(other:LazyFrame)(leftOn:Expr seq)(rightOn: Expr seq)(how: JoinType) (lf: LazyFrame) : LazyFrame =
        lf.Join(other,leftOn,rightOn,how)
    /// <summary>
    /// Unpivot (Melt) the LazyFrame from wide to long format using Selectors.
    /// Default column names ("variable", "value") will be used.
    /// </summary>
    let unpivot (index: Selector) (on: Selector) (lf: LazyFrame) : LazyFrame =
        lf.Unpivot(index, on)
    /// <summary>
    /// Alias for LazyFrame.unpivot.
    /// </summary>
    let melt (index: Selector) (on: Selector) (lf: LazyFrame) : LazyFrame =
        lf.Unpivot(index, on)
    /// <summary>
    /// Unpivot (Melt) the LazyFrame using Selectors with custom variable and value column names.
    /// </summary>
    let unpivotNamed (index: Selector) (on: Selector) (variableName: string) (valueName: string) (lf: LazyFrame) : LazyFrame =
        lf.Unpivot(index, on, variableName = variableName, valueName = valueName)
    /// <summary>
    /// Alias for LazyFrame.unpivotNamed.
    /// </summary>
    let meltNamed (index: Selector) (on: Selector) (variableName: string) (valueName: string) (lf: LazyFrame) : LazyFrame =
        unpivotNamed index on variableName valueName lf
    /// <summary> Drop columns from the LazyFrame. </summary>
    let drop (columns: #IColumnExpr) (lf: LazyFrame) : LazyFrame =
        lf.Drop(columns.ToSelector())
    /// <summary> Drop null values from the LazyFrame. </summary>
    let dropNulls (lf: LazyFrame) : LazyFrame =
        lf.DropNulls()
    /// <summary> Drop null values from the LazyFrame with specific subset. </summary>
    let dropNullsOn (subset: Expr) (lf: LazyFrame) : LazyFrame =
        lf.DropNulls subset
    /// <summary> Drop NaN values from the LazyFrame. </summary>
    let dropNans (lf: LazyFrame) : LazyFrame =
        lf.DropNans()
    /// <summary> Drop NaN values from the LazyFrame with specific subset. </summary>
    let dropNansOn (subset: #IColumnExpr) (lf: LazyFrame) : LazyFrame =
        lf.DropNans subset
    /// <summary> Rename columns based on a mapping dictionary or key-value pairs. </summary>
    let rename (mapping: seq<string * string>) (lf: LazyFrame) : LazyFrame =
        lf.Rename(mapping)
    /// <summary> Reverse the order of the LazyFrame. </summary>
    let reverse (lf: LazyFrame) : LazyFrame =
        lf.Reverse()
    /// <summary> Fill null values in all columns with a specified value. </summary>
    let fillNull (fillValue: Expr)(lf: LazyFrame)  : LazyFrame =
        lf.FillNull fillValue
    /// <summary> Fill NaN values in all columns with a specified value. </summary>
    let fillNan (fillValue: Expr)(lf: LazyFrame)  : LazyFrame =
        lf.FillNan fillValue
    /// <summary> Concatenate multiple LazyFrames horizontally. </summary>
    let concatHorizontal (lfs: seq<LazyFrame> ): LazyFrame =
        LazyFrame.Concat(lfs, ConcatType.Horizontal)
    /// <summary> Concatenate multiple LazyFrames vertically. </summary>
    let concatVertical (lfs: seq<LazyFrame> ): LazyFrame =
        LazyFrame.Concat(lfs, ConcatType.Vertical)
    /// <summary> Concatenate multiple LazyFrames diagonally. </summary>
    let concatDiagonal (lfs: seq<LazyFrame> ): LazyFrame =
        LazyFrame.Concat(lfs, ConcatType.Diagonal)
    /// <summary> Decompose multiple struct columns. </summary>
    let unnest(columns: #IColumnExpr) (lf:LazyFrame) : LazyFrame =
        lf.Unnest(columns.ToSelector())
    /// <summary> Create a LazyFrame from DataFrame </summary>
    let ofEager (df: DataFrame) : LazyFrame =
        df.Lazy()
