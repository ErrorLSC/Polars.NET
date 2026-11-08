namespace Polars.NET.Linq.Backend.Polars

open Polars.NET.Core
open Polars.NET.Core.Wrappers

[<AutoOpen>]
module internal NativeOpsApplier =

    /// Helper to extract all column names and their corresponding DataTypeHandle from a LazyFrame schema
    let getLazySchemaFields (lf: LazyFrameHandle) : (string * DataTypeHandle) array =
        let schema = PolarsWrapper.GetLazySchema lf
        let count = PolarsWrapper.GetSchemaLen schema

        Array.init (int count) (fun i ->
            let mutable name = null
            let mutable dtHandle = new DataTypeHandle()
            PolarsWrapper.GetSchemaFieldAt(schema, uint64 i, &name, &dtHandle)
            name, dtHandle)

    /// Aligns `targetLf` to the columns of `baseFields` (by name when the names match, else by position),
    /// casting where the types differ; `targetFields` is the schema of `targetLf`.
    let alignToFields
        (baseFields: (string * DataTypeHandle) array)
        (targetFields: (string * DataTypeHandle) array)
        (targetLf: LazyFrameHandle)
        : LazyFrameHandle =

        let baseNames = baseFields |> Array.map fst
        let targetNames = targetFields |> Array.map fst

        if Set.ofArray baseNames = Set.ofArray targetNames then
            let targetFieldMap = targetFields |> Map.ofArray

            let reorderedAndCastedExprs =
                baseFields
                |> Array.map (fun (colName, baseDt) ->
                    let targetDt = targetFieldMap.[colName]
                    let colExpr = PolarsWrapper.Col colName

                    if not (PolarsWrapper.DataTypeEq(baseDt, targetDt)) then
                        let dtExpr = PolarsWrapper.DataTypeExprFromDataType baseDt
                        PolarsWrapper.ExprCast(colExpr, dtExpr, strict = false, wrapNumerical = false)
                    else
                        colExpr)

            PolarsWrapper.LazySelect(targetLf, reorderedAndCastedExprs)
        elif baseFields.Length = targetFields.Length then
            let projectedExprs =
                Array.zip baseFields targetFields
                |> Array.map (fun ((baseName, baseDt), (targetName, targetDt)) ->
                    let colExpr = PolarsWrapper.Col targetName
                    let castedExpr =
                        if not (PolarsWrapper.DataTypeEq(baseDt, targetDt)) then
                            let dtExpr = PolarsWrapper.DataTypeExprFromDataType baseDt
                            PolarsWrapper.ExprCast(colExpr, dtExpr, strict = false, wrapNumerical = false)
                        else
                            colExpr
                    PolarsWrapper.Alias(castedExpr, baseName))
            PolarsWrapper.LazySelect(targetLf, projectedExprs)
        else
            targetLf

    /// Aligns `targetLf` to the schema of `baseLf` (see alignToFields), resolving both schemas.
    let alignSchema (baseLf: LazyFrameHandle) (targetLf: LazyFrameHandle) : LazyFrameHandle =
        alignToFields (getLazySchemaFields baseLf) (getLazySchemaFields targetLf) targetLf
