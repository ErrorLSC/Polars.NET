namespace Polars.FSharp

type private ColumnExtractors =
    
    static member Build<'CoreType> (col: Series, isOption: bool, fallbackObj: obj, colName: string, fieldName: string) : (int -> obj) =
        fun r ->
            let optVal = col.GetValueOption<'CoreType>(int64 r)
            match optVal with
            | Some v ->
                if isOption then box optVal 
                else box v                 
            | None ->
                if isOption then null       
                elif not (isNull fallbackObj) then fallbackObj 
                else failwithf "Strict mode error: Column '%s' contains null, but Record field '%s' does not accept Option." colName fieldName

[<AutoOpen>]
module DataFrameConversions =
    open Microsoft.FSharp.Reflection
    open System.Reflection
    
    type DataFrame with
        /// <summary>
        /// Convert the DataFrame to a dictionary of column name to Series.
        /// </summary>
        member this.ToMap() : Map<string, Series> =
            this 
            |> Seq.map (fun series -> series.Name, series)
            |> Map.ofSeq
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
        /// [ToRecords] Transform DataFrame to F# Records
        /// </summary>
        member this.ToRecords<'T>() : seq<'T> =
                let recordType = typeof<'T>

                if not (FSharpType.IsRecord(recordType, true)) then
                    failwithf "Type '%s' is not an F# Record." recordType.Name

                let fields = FSharpType.GetRecordFields(recordType, true)
                let ctor = FSharpValue.PreComputeRecordConstructor(recordType, true)

                let helperMethod = 
                    typeof<ColumnExtractors>.GetMethod("Build", BindingFlags.Static ||| BindingFlags.Public ||| BindingFlags.NonPublic)

                let extractors = 
                    fields |> Array.map (fun f -> 
                        let col = this.Column(f.Name) 
                        let pType = f.PropertyType
                        
                        let isOption = pType.IsGenericType && pType.GetGenericTypeDefinition() = typedefof<option<_>>
                        
                        let coreType = if isOption then pType.GetGenericArguments().[0] else pType

                        let isList   = coreType.IsGenericType && coreType.GetGenericTypeDefinition() = typedefof<list<_>>
                        let isArray  = coreType.IsArray

                        let emptyListObj = 
                            if isList then 
                                let emptyCase = FSharpType.GetUnionCases(coreType) |> Array.find (fun c -> c.Name = "Empty")
                                FSharpValue.MakeUnion(emptyCase, [||])
                            else null

                        let emptyArrayObj = 
                            if isArray then 
                                System.Array.CreateInstance(coreType.GetElementType(), 0) :> obj
                            else null

                        let fallback = if isList then emptyListObj elif isArray then emptyArrayObj else null

                        let genericHelper = helperMethod.MakeGenericMethod([| coreType |])
                        let extractorObj = genericHelper.Invoke(null, [| col; box isOption; fallback; box col.Name; box f.Name |])
                        extractorObj :?> (int -> obj)
                    )

                let rowCount = int this.Height

                seq {
                    for r = 0 to rowCount - 1 do
                        let args = 
                            extractors |> Array.map (fun extract -> extract r)
                        
                        yield ctor args :?> 'T
                }