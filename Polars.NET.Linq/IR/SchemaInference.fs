namespace Polars.NET.Linq.IR

open System
open Polars.NET.Core
open Polars.NET.Linq

/// In-memory schema and data type inference for LogicalPlan and ScalarExpr
module internal SchemaInference =

    // =========================================================================
    // 1. Scalar Type Inference
    // =========================================================================

    let rec inferScalarType (schema: LogicalSchema) (expr: ScalarExpr) : PlDataType =
        match expr with
        | ScalarExpr.Column name ->
            match schema.TryFindField name with
            | Some field -> field.DataType
            | None -> PlDataType.Unknown

        | ScalarExpr.Literal litVal -> litVal.DataType

        | ScalarExpr.Cast(_, targetType, _) -> targetType

        | ScalarExpr.Unary(op, inner) ->
            match op with
            | UnaryOp.Not
            | UnaryOp.IsNull
            | UnaryOp.IsNotNull -> PlDataType.Boolean
            | UnaryOp.Negate
            | UnaryOp.Abs -> inferScalarType schema inner

        | ScalarExpr.Binary(op, left, right) ->
            match op with
            | BinaryOp.Eq
            | BinaryOp.NotEq
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
            | BinaryOp.RegexMatch -> PlDataType.Boolean

            | BinaryOp.StringConcat -> PlDataType.String

            | BinaryOp.Add
            | BinaryOp.Subtract
            | BinaryOp.Multiply
            | BinaryOp.Divide
            | BinaryOp.Modulo
            | BinaryOp.FloorDivide
            | BinaryOp.Power ->
                let leftType = inferScalarType schema left
                let rightType = inferScalarType schema right

                if leftType = PlDataType.Float64 || rightType = PlDataType.Float64 then
                    PlDataType.Float64
                elif leftType = PlDataType.Float32 || rightType = PlDataType.Float32 then
                    PlDataType.Float32
                elif leftType = PlDataType.Int64 || rightType = PlDataType.Int64 then
                    PlDataType.Int64
                elif leftType = PlDataType.Int32 || rightType = PlDataType.Int32 then
                    PlDataType.Int32
                elif leftType <> PlDataType.Unknown then
                    leftType
                else
                    rightType

        | ScalarExpr.Function(fn, args, returnType) ->
            if returnType <> PlDataType.Unknown then
                returnType
            else
                match fn with
                | ScalarFunction.BitLeftShift
                | ScalarFunction.BitRightShift ->
                    match args with
                    | left :: _ -> inferScalarType schema left
                    | [] -> PlDataType.Unknown
                | _ -> returnType

        | ScalarExpr.Aggregation(kind, innerOpt, _) ->
            match kind with
            | AggKind.Count
            | AggKind.Len -> PlDataType.Int64
            | AggKind.Mean -> PlDataType.Float64
            | AggKind.Median -> PlDataType.Float64
            | AggKind.Std _
            | AggKind.Var _ -> PlDataType.Float64
            | AggKind.Sum
            | AggKind.Min
            | AggKind.Max
            | AggKind.First
            | AggKind.Last
            | AggKind.Quantile _
            | AggKind.List ->
                match innerOpt with
                | Some inner -> inferScalarType schema inner
                | None -> PlDataType.Unknown

        | ScalarExpr.Window(inner, _, _) -> inferScalarType schema inner

        | ScalarExpr.Conditional(_, ifTrue, _) -> inferScalarType schema ifTrue

        | ScalarExpr.Coalesce exprs ->
            match exprs with
            | first :: _ -> inferScalarType schema first
            | [] -> PlDataType.Unknown

        | ScalarExpr.ItemAccess _ -> PlDataType.Unknown
        | ScalarExpr.Slice _ -> PlDataType.Unknown

        | ScalarExpr.Subquery(_, kind) ->
            match kind with
            | SubqueryKind.Exists _
            | SubqueryKind.InSubquery _ -> PlDataType.Boolean
            | SubqueryKind.ScalarScalar -> PlDataType.Unknown

        | ScalarExpr.Alias(inner, _) -> inferScalarType schema inner
        | ScalarExpr.Opaque _ -> PlDataType.Unknown

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
        | LogicalPlan.Distinct(input, _, _)
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
            | PlJoinType.Semi
            | PlJoinType.Anti -> inferPlanSchema left
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
                                DataType = inSchema.TryFindField name |> Option.map (fun f -> f.DataType) |> Option.defaultValue PlDataType.Unknown
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

        | LogicalPlan.Union(left, _, _) -> inferPlanSchema left

        | LogicalPlan.Unpivot(input, idx, _, varName, valName) ->
            let inSchema = inferPlanSchema input
            let idxSet = Set.ofList idx

            let preservedFields =
                inSchema.Fields |> List.filter (fun f -> idxSet.Contains f.Name)

            let varField =
                {
                    Name = varName
                    DataType = PlDataType.String
                    Nullable = false
                }

            let valField =
                {
                    Name = valName
                    DataType = PlDataType.Unknown
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
                    DataType = PlDataType.UInt32
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
