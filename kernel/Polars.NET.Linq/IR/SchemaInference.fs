namespace Polars.NET.Linq.IR

open System
open Polars.NET.Linq

/// In-memory schema and data type inference for LogicalPlan and ScalarExpr
module internal SchemaInference =

    // =========================================================================
    // 1. Scalar Type Inference
    // =========================================================================

    let rec inferScalarType (schema: LogicalSchema) (expr: ScalarExpr) : LogicalType =
        match expr with
        | ScalarExpr.Column name ->
            match schema.TryFindField name with
            | Some field -> field.DataType
            | None -> LogicalType.Unknown

        | ScalarExpr.Literal litVal -> litVal.DataType

        | ScalarExpr.Cast(_, targetType, _) -> targetType

        | ScalarExpr.Unary(op, inner) ->
            match op with
            | UnaryOp.Not
            | UnaryOp.IsNull
            | UnaryOp.IsNotNull
            | UnaryOp.IsNan -> LogicalTypes.Boolean
            | UnaryOp.Negate
            | UnaryOp.Abs -> inferScalarType schema inner

        | ScalarExpr.Binary(op, left, right) ->
            match op with
            | BinaryOp.Eq
            | BinaryOp.NotEq
            | BinaryOp.EqMissing
            | BinaryOp.NotEqMissing
            | BinaryOp.Lt
            | BinaryOp.LtEq
            | BinaryOp.Gt
            | BinaryOp.GtEq
            | BinaryOp.And
            | BinaryOp.Or
            | BinaryOp.Xor
            | BinaryOp.Contains
            | BinaryOp.StartsWith
            | BinaryOp.EndsWith
            | BinaryOp.Like
            | BinaryOp.RegexMatch -> LogicalTypes.Boolean

            | BinaryOp.StringConcat -> LogicalTypes.String

            | BinaryOp.Add
            | BinaryOp.Subtract
            | BinaryOp.Multiply
            | BinaryOp.Divide
            | BinaryOp.Modulo
            | BinaryOp.TruncatedDivide
            | BinaryOp.FloorDivide
            | BinaryOp.Power ->
                let leftType = inferScalarType schema left
                let rightType = inferScalarType schema right

                if leftType = LogicalTypes.Float64 || rightType = LogicalTypes.Float64 then
                    LogicalTypes.Float64
                elif leftType = LogicalTypes.Float32 || rightType = LogicalTypes.Float32 then
                    LogicalTypes.Float32
                elif leftType = LogicalTypes.Int64 || rightType = LogicalTypes.Int64 then
                    LogicalTypes.Int64
                elif leftType = LogicalTypes.Int32 || rightType = LogicalTypes.Int32 then
                    LogicalTypes.Int32
                elif leftType <> LogicalType.Unknown then
                    leftType
                else
                    rightType

        | ScalarExpr.Function(fn, args, returnType) ->
            if returnType <> LogicalType.Unknown then
                returnType
            else
                match fn with
                | ScalarFunction.BitLeftShift
                | ScalarFunction.BitRightShift ->
                    match args with
                    | left :: _ -> inferScalarType schema left
                    | [] -> LogicalType.Unknown
                | ScalarFunction.FloatOrderKey -> LogicalTypes.Int64
                | ScalarFunction.AssertCount _ ->
                    match args with
                    | [ _; value ] -> inferScalarType schema value
                    | _ -> LogicalType.Unknown
                // A struct field and a list element take their declared type (an Enum nested in a column).
                | ScalarFunction.StructFieldByName ->
                    match args with
                    | [ parent; ScalarExpr.Literal(LiteralValue.String name) ] ->
                        match inferScalarType schema parent with
                        | LogicalType.StructOf fields ->
                            fields |> List.tryFind (fst >> (=) name) |> Option.map snd |> Option.defaultValue LogicalType.Unknown
                        | _ -> LogicalType.Unknown
                    | _ -> LogicalType.Unknown
                | ScalarFunction.ListGet ->
                    match args with
                    | list :: _ ->
                        match inferScalarType schema list with
                        | LogicalType.ListOf element -> element
                        | _ -> LogicalType.Unknown
                    | [] -> LogicalType.Unknown
                | _ -> returnType

        | ScalarExpr.Aggregation(kind, innerOpt, _) ->
            match kind with
            | AggKind.Count
            | AggKind.Len -> LogicalTypes.Int64
            | AggKind.Mean -> LogicalTypes.Float64
            | AggKind.Median -> LogicalTypes.Float64
            | AggKind.CheckedSum ->
                match innerOpt |> Option.map (inferScalarType schema) with
                | Some(LogicalType.Arrow(:? Apache.Arrow.Types.Int32Type)) -> LogicalTypes.Int32
                | _ -> LogicalTypes.Int64
            | AggKind.Std _
            | AggKind.Var _ -> LogicalTypes.Float64
            | AggKind.Sum
            | AggKind.Min
            | AggKind.Max
            | AggKind.First
            | AggKind.Last
            | AggKind.Quantile _
            | AggKind.List ->
                match innerOpt with
                | Some inner -> inferScalarType schema inner
                | None -> LogicalType.Unknown

        | ScalarExpr.Window(inner, _, _)
        | ScalarExpr.SortBy(inner, _) -> inferScalarType schema inner

        | ScalarExpr.Conditional(_, ifTrue, _) -> inferScalarType schema ifTrue

        | ScalarExpr.Coalesce exprs ->
            match exprs with
            | first :: _ -> inferScalarType schema first
            | [] -> LogicalType.Unknown

        | ScalarExpr.ItemAccess(target, _) ->
            match inferScalarType schema target with
            | LogicalType.ListOf element -> element
            | _ -> LogicalType.Unknown
        | ScalarExpr.Slice _ -> LogicalType.Unknown

        | ScalarExpr.Subquery(_, kind) ->
            match kind with
            | SubqueryKind.Exists _
            | SubqueryKind.InSubquery _ -> LogicalTypes.Boolean
            | SubqueryKind.ScalarScalar -> LogicalType.Unknown

        | ScalarExpr.Alias(inner, _) -> inferScalarType schema inner
        | ScalarExpr.Udf(_, _, returnType, _) -> returnType
        | ScalarExpr.InList _ -> LogicalTypes.Boolean
        | ScalarExpr.EnumConvert(conversion, _, mapping) ->
            match conversion with
            | EnumConversion.ToValue -> mapping.Underlying
            | EnumConversion.FromValue -> LogicalType.Enum mapping.Categories
            | EnumConversion.ToName -> LogicalTypes.String

    // =========================================================================
    // 2. Relational Plan Schema Inference
    // =========================================================================

    let rec inferPlanSchema (plan: LogicalPlan) : LogicalSchema =
        match plan with
        | LogicalPlan.Scan(_, schema) -> schema

        | LogicalPlan.Empty schema -> schema

        | LogicalPlan.Filter(input, _)
        | LogicalPlan.Sort(input, _)
        | LogicalPlan.Limit(input, _, _)
        | LogicalPlan.Distinct(input, _, _, _)
        | LogicalPlan.Reverse input
        | LogicalPlan.SkipLast(input, _)
        | LogicalPlan.Explode(input, _, _, _) -> inferPlanSchema input

        | LogicalPlan.Project(input, projs) ->
            let inSchema = inferPlanSchema input

            let fields =
                projs
                |> List.map (fun (name, expr) ->
                    {
                        Name = name
                        DataType = inferScalarType inSchema expr
                        Nullable = true
                    })

            { Fields = fields }

        | LogicalPlan.WithColumns(input, cols) ->
            let inSchema = inferPlanSchema input
            let newColsMap = cols |> Map.ofList

            let updatedFields =
                inSchema.Fields
                |> List.map (fun f ->
                    match Map.tryFind f.Name newColsMap with
                    | Some expr ->
                        { f with
                            DataType = inferScalarType inSchema expr
                        }
                    | None -> f)

            let existingNames = inSchema.ColumnNames |> Set.ofList

            let brandNewFields =
                cols
                |> List.filter (fun (n, _) -> not (existingNames.Contains n))
                |> List.map (fun (n, e) ->
                    {
                        Name = n
                        DataType = inferScalarType inSchema e
                        Nullable = true
                    })

            {
                Fields = updatedFields @ brandNewFields
            }

        | LogicalPlan.Join(left, right, _, _, how, suffixOpt, _, _) ->
            match how with
            | JoinKind.Semi
            | JoinKind.Anti -> inferPlanSchema left
            | _ ->
                let leftSchema = inferPlanSchema left
                let rightSchema = inferPlanSchema right
                let leftNames = leftSchema.ColumnNames |> Set.ofList
                let suffix = defaultArg suffixOpt LinqConstants.DefaultRightSuffix

                let rightFields =
                    rightSchema.Fields
                    |> List.map (fun f ->
                        if leftNames.Contains f.Name then
                            { f with
                                Name = f.Name + suffix
                            }
                        else
                            f)

                {
                    Fields = leftSchema.Fields @ rightFields
                }

        | LogicalPlan.JoinAsOf(left, right, spec) ->
            let leftSchema = inferPlanSchema left
            let rightSchema = inferPlanSchema right
            let leftNames = leftSchema.ColumnNames |> Set.ofList
            let suffix = defaultArg spec.Suffix LinqConstants.DefaultRightSuffix

            let rightFields =
                rightSchema.Fields
                |> List.map (fun f ->
                    if leftNames.Contains f.Name then
                        { f with
                            Name = f.Name + suffix
                        }
                    else
                        f)

            {
                Fields = leftSchema.Fields @ rightFields
            }

        | LogicalPlan.GroupBy(input, keys, aggs, _, _) ->
            let inSchema = inferPlanSchema input

            let keyFields =
                keys
                |> List.choose (function
                    | ScalarExpr.Column name ->
                        Some
                            {
                                Name = name
                                DataType = inSchema.TryFindField name |> Option.map (fun f -> f.DataType) |> Option.defaultValue LogicalType.Unknown
                                Nullable = true
                            }
                    | ScalarExpr.Alias(inner, name) ->
                        Some
                            {
                                Name = name
                                DataType = inferScalarType inSchema inner
                                Nullable = true
                            }
                    | other ->
                        Some
                            {
                                Name = sprintf "key_%d" (other.GetHashCode())
                                DataType = inferScalarType inSchema other
                                Nullable = true
                            })

            let aggFields =
                aggs
                |> List.map (fun (name, expr) ->
                    {
                        Name = name
                        DataType = inferScalarType inSchema expr
                        Nullable = true
                    })

            {
                Fields = keyFields @ aggFields
            }

        | LogicalPlan.GroupingSets(input, spec) ->
            let inSchema = inferPlanSchema input
            let keyMap = spec.KeyAliases |> Map.ofList
            let aggMap = spec.Aggs |> Map.ofList
            let fields =
                spec.OutputColumns
                |> List.map (fun name ->
                    let dt =
                        match keyMap.TryFind name with
                        | Some e -> inferScalarType inSchema e
                        | None ->
                            match aggMap.TryFind name with
                            | Some e -> inferScalarType inSchema e
                            | None -> LogicalTypes.Int32
                    {
                        Name = name
                        DataType = dt
                        Nullable = true
                    })
            { Fields = fields }

        | LogicalPlan.Union(left, _, _) -> inferPlanSchema left

        | LogicalPlan.Unpivot(input, idx, _, varName, valName) ->
            let inSchema = inferPlanSchema input
            let idxSet = Set.ofList idx

            let preservedFields =
                inSchema.Fields |> List.filter (fun f -> idxSet.Contains f.Name)

            let varField =
                {
                    Name = varName
                    DataType = LogicalTypes.String
                    Nullable = false
                }

            let valField =
                {
                    Name = valName
                    DataType = LogicalType.Unknown
                    Nullable = true
                }

            {
                Fields = preservedFields @ [ varField; valField ]
            }

        | LogicalPlan.WithRowIndex(input, name, _) ->
            let inSchema = inferPlanSchema input

            let ridField =
                {
                    Name = name
                    DataType = LogicalTypes.UInt32
                    Nullable = false
                }

            {
                Fields = ridField :: inSchema.Fields
            }

        | LogicalPlan.Rename(input, existingNames, newNames) ->
            let inSchema = inferPlanSchema input
            let renameMap = List.zip existingNames newNames |> Map.ofList
            let fields =
                inSchema.Fields
                |> List.map (fun f ->
                    match Map.tryFind f.Name renameMap with
                    | Some newName -> { f with Name = newName }
                    | None -> f)
            { Fields = fields }

        | LogicalPlan.HorizontalConcat inputs ->
            match inputs with
            | head :: tail ->
                let headSchema = inferPlanSchema head
                tail |> List.fold (fun (acc: LogicalSchema) inp -> acc.Merge(inferPlanSchema inp)) headSchema
            | [] -> LogicalSchema.Empty
