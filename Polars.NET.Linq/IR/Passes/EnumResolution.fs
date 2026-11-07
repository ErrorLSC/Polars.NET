namespace Polars.NET.Linq.IR

open System
open Polars.NET.Core.Helpers

/// Resolves EnumConvert nodes against the types of the columns they read.
///
/// A .NET enum is stored either as a Polars Enum column (categories in underlying-value order) or as an integer
/// column of its underlying values (data written elsewhere, or a [Flags] enum). The translator does not know which,
/// so it emits EnumConvert nodes; this pass picks the form from the inferred type of each converted expression:
/// - ToValue of an Enum maps categories to values; of anything else it is the expression itself.
/// - FromValue gives an Enum: a literal becomes its category, any other expression maps values to categories.
///   An enum literal compared with, or chosen alongside, a non-Enum expression stays its underlying value.
/// - ToName of an Enum is its string form; of a value it maps values to names, keeping an undefined value's number.
/// An expression of unknown type is taken to hold underlying values, as before columns carried types.
module internal EnumResolution =

    let private isEnumType (t: LogicalType) =
        match t with
        | LogicalType.Enum _ -> true
        | _ -> false

    let private categoryLiterals (mapping: EnumMapping) =
        mapping.Categories |> List.map LiteralValue.String

    let private enumType (mapping: EnumMapping) = LogicalType.Enum mapping.Categories

    /// The value, failing the query on an undefined member with the message ingestion raises
    /// (EnumCategories), rather than the engine's unmapped-value error.
    let private requireDefined (mapping: EnumMapping) (value: ScalarExpr) : ScalarExpr =
        ScalarExpr.Function(
            ScalarFunction.RequireListed(
                mapping.Values,
                $"Value '{{}}' is not a defined member of enum {mapping.Name}; a Polars Enum column holds only defined members."
            ),
            [ value ],
            mapping.Underlying
        )

    /// The Enum holding the underlying value `value` (FromValue in its Enum form).
    let private enumOfValue (mapping: EnumMapping) (value: ScalarExpr) : ScalarExpr =
        match value with
        | ScalarExpr.Literal(LiteralValue.Null _) -> ScalarExpr.Literal(LiteralValue.Null(enumType mapping))
        | ScalarExpr.Literal literal when List.contains literal mapping.Values ->
            let category = mapping.Categories.[List.findIndex ((=) literal) mapping.Values]
            ScalarExpr.Cast(ScalarExpr.Literal(LiteralValue.String category), enumType mapping, true)
        | _ ->
            ScalarExpr.Function(
                ScalarFunction.ReplaceStrict(mapping.Values, categoryLiterals mapping),
                [ requireDefined mapping value ],
                enumType mapping
            )

    /// The underlying value of an Enum expression (ToValue in its Enum form).
    let private valueOfEnum (mapping: EnumMapping) (enumExpr: ScalarExpr) : ScalarExpr =
        ScalarExpr.Function(
            ScalarFunction.ReplaceStrict(categoryLiterals mapping, mapping.Values),
            [ ScalarExpr.Cast(enumExpr, LogicalTypes.String, true) ],
            mapping.Underlying
        )

    /// An enum constant awaiting resolution: FromValue of a literal.
    let private (|EnumLiteral|_|) (expr: ScalarExpr) =
        match expr with
        | ScalarExpr.EnumConvert(EnumConversion.FromValue, (ScalarExpr.Literal _ as literal), mapping) ->
            Some(literal, mapping)
        | _ -> None

    /// Whether an Enum-typed expression has exactly the categories of `mapping`, so that its Enum ordering is the
    /// ordering of the mapped values.
    let private hasCategoriesOf (mapping: EnumMapping) (t: LogicalType) =
        match t with
        | LogicalType.Enum categories -> categories = mapping.Categories
        | _ -> false

    let private isComparison (op: BinaryOp) =
        match op with
        | BinaryOp.Eq
        | BinaryOp.NotEq
        | BinaryOp.EqMissing
        | BinaryOp.NotEqMissing
        | BinaryOp.Lt
        | BinaryOp.LtEq
        | BinaryOp.Gt
        | BinaryOp.GtEq -> true
        | _ -> false

    let private mirror (op: BinaryOp) =
        match op with
        | BinaryOp.Lt -> BinaryOp.Gt
        | BinaryOp.LtEq -> BinaryOp.GtEq
        | BinaryOp.Gt -> BinaryOp.Lt
        | BinaryOp.GtEq -> BinaryOp.LtEq
        | other -> other

    /// Resolves the EnumConvert nodes of `expr`, whose columns are those of `schema`.
    let rec resolveScalar (schema: LogicalSchema) (expr: ScalarExpr) : ScalarExpr =
        let resolve = resolveScalar schema
        let typeOf = SchemaInference.inferScalarType schema

        /// Resolves an enum literal next to an already resolved sibling: an Enum literal beside an Enum, else its value.
        let besides (sibling: ScalarExpr) (literal: ScalarExpr) (mapping: EnumMapping) =
            if isEnumType (typeOf sibling) then enumOfValue mapping literal else literal

        let resolvePair (a: ScalarExpr) (b: ScalarExpr) =
            match a, b with
            | EnumLiteral(literal, mapping), EnumLiteral _ -> enumOfValue mapping literal, resolve b
            | EnumLiteral(literal, mapping), other ->
                let other' = resolve other
                besides other' literal mapping, other'
            | other, EnumLiteral(literal, mapping) ->
                let other' = resolve other
                other', besides other' literal mapping
            | _ -> resolve a, resolve b

        match expr with
        | ScalarExpr.EnumConvert(EnumConversion.ToValue, ScalarExpr.EnumConvert(EnumConversion.FromValue, value, _), _) ->
            resolve value

        | ScalarExpr.EnumConvert(conversion, inner, mapping) ->
            let inner' = resolve inner
            let innerIsEnum = isEnumType (typeOf inner')

            match conversion with
            | EnumConversion.ToValue -> if innerIsEnum then valueOfEnum mapping inner' else inner'
            | EnumConversion.FromValue -> if innerIsEnum then inner' else enumOfValue mapping inner'
            | EnumConversion.ToName ->
                if innerIsEnum then
                    ScalarExpr.Cast(inner', LogicalTypes.String, true)
                else
                    ScalarExpr.Function(
                        ScalarFunction.ReplaceStrict(mapping.Values, categoryLiterals mapping),
                        [ inner'; ScalarExpr.Cast(inner', LogicalTypes.String, true) ],
                        LogicalTypes.String
                    )

        // Comparing the value of an Enum whose categories are the enum's members with a member's value is
        // comparing the Enum with that member, as categories follow value order.
        | ScalarExpr.Binary(op, ScalarExpr.EnumConvert(EnumConversion.ToValue, enumExpr, mapping), ScalarExpr.Literal literal)
            when isComparison op && List.contains literal mapping.Values ->
            let enumExpr' = resolve enumExpr
            if hasCategoriesOf mapping (typeOf enumExpr') then
                ScalarExpr.Binary(op, enumExpr', enumOfValue mapping (ScalarExpr.Literal literal))
            else
                ScalarExpr.Binary(op, resolve (ScalarExpr.EnumConvert(EnumConversion.ToValue, enumExpr, mapping)), ScalarExpr.Literal literal)

        | ScalarExpr.Binary(op, (ScalarExpr.Literal _ as literal), (ScalarExpr.EnumConvert(EnumConversion.ToValue, _, _) as enumValue))
            when isComparison op ->
            match resolve (ScalarExpr.Binary(mirror op, enumValue, literal)) with
            | ScalarExpr.Binary(_, left, right) -> ScalarExpr.Binary(op, right, left)
            | other -> other

        | ScalarExpr.Binary(op, left, right) ->
            let left', right' = resolvePair left right
            ScalarExpr.Binary(op, left', right')

        | ScalarExpr.Conditional(test, ifTrue, ifFalse) ->
            let ifTrue', ifFalse' = resolvePair ifTrue ifFalse
            ScalarExpr.Conditional(resolve test, ifTrue', ifFalse')

        | ScalarExpr.Coalesce exprs ->
            let resolvedOthers =
                exprs
                |> List.choose (function
                    | EnumLiteral _ -> None
                    | other -> Some(resolve other))
            let anyEnum = resolvedOthers |> List.exists (typeOf >> isEnumType)
            exprs
            |> List.map (function
                | EnumLiteral(literal, mapping) ->
                    if anyEnum || resolvedOthers.IsEmpty then enumOfValue mapping literal else literal
                | other -> resolve other)
            |> ScalarExpr.Coalesce

        | ScalarExpr.InList(inner, source, negated, nullsEqual) ->
            let inner' = resolve inner
            let source' =
                match source with
                | InListSource.Collection(values, elementType) when
                    (tryEnumMapping elementType).IsSome && not (isEnumType (typeOf inner'))
                    ->
                    let underlying = Enum.GetUnderlyingType(PolarsTypeHelper.UnwrapCoreType elementType)
                    let items = values |> Seq.cast<obj> |> Seq.toArray
                    let numbers = Array.CreateInstance(typedefof<Nullable<_>>.MakeGenericType underlying, items.Length)
                    items
                    |> Array.iteri (fun i item ->
                        if not (isNull item) then
                            numbers.SetValue(Convert.ChangeType(item, underlying), i))
                    InListSource.Collection(numbers, numbers.GetType().GetElementType())
                | other -> other
            ScalarExpr.InList(inner', source', negated, nullsEqual)

        | ScalarExpr.Column _
        | ScalarExpr.Literal _ -> expr
        | ScalarExpr.Unary(op, inner) -> ScalarExpr.Unary(op, resolve inner)
        | ScalarExpr.Cast(inner, t, strict) -> ScalarExpr.Cast(resolve inner, t, strict)
        | ScalarExpr.Function(fn, args, t) -> ScalarExpr.Function(fn, args |> List.map resolve, t)
        | ScalarExpr.Aggregation(kind, inner, distinct) -> ScalarExpr.Aggregation(kind, inner |> Option.map resolve, distinct)
        | ScalarExpr.Window(inner, partitionBy, orderBy) ->
            ScalarExpr.Window(
                resolve inner,
                partitionBy |> List.map resolve,
                orderBy |> List.map (fun o -> { o with Expr = resolve o.Expr })
            )
        | ScalarExpr.SortBy(inner, orderBy) ->
            ScalarExpr.SortBy(resolve inner, orderBy |> List.map (fun o -> { o with Expr = resolve o.Expr }))
        | ScalarExpr.ItemAccess(target, index) -> ScalarExpr.ItemAccess(resolve target, resolve index)
        | ScalarExpr.Slice(target, offset, length) -> ScalarExpr.Slice(resolve target, offset, length)
        | ScalarExpr.Subquery(plan, kind) ->
            let kind' =
                match kind with
                | SubqueryKind.InSubquery(target, negated) -> SubqueryKind.InSubquery(resolve target, negated)
                | other -> other
            ScalarExpr.Subquery(resolvePlan plan, kind')
        | ScalarExpr.Alias(inner, alias) -> ScalarExpr.Alias(resolve inner, alias)
        | ScalarExpr.Udf(name, args, t, invoker) -> ScalarExpr.Udf(name, args |> List.map resolve, t, invoker)

    /// Resolves `expr`, whose value is stored into a column of `columnType` (an assignment), against `schema`: an
    /// enum constant takes the column's representation, its category for an Enum column, else its value.
    and resolveAssignment (schema: LogicalSchema) (columnType: LogicalType) (expr: ScalarExpr) : ScalarExpr =
        match expr with
        | EnumLiteral(literal, mapping) -> if isEnumType columnType then enumOfValue mapping literal else literal
        | _ -> resolveScalar schema expr

    /// Resolves the EnumConvert nodes of every expression in `plan`, each against the schema of the rows it reads.
    and resolvePlan (plan: LogicalPlan) : LogicalPlan =
        let plan' = LogicalPlanOps.mapInputs resolvePlan plan
        let schemaOf = SchemaInference.inferPlanSchema

        match plan' with
        | LogicalPlan.Join(left, right, leftOn, rightOn, how, suffix, coalesce, nullsEqual) ->
            LogicalPlan.Join(
                left,
                right,
                leftOn |> List.map (resolveScalar (schemaOf left)),
                rightOn |> List.map (resolveScalar (schemaOf right)),
                how,
                suffix,
                coalesce,
                nullsEqual
            )
        | LogicalPlan.JoinAsOf(left, right, spec) ->
            let leftSchema = schemaOf left
            let rightSchema = schemaOf right
            LogicalPlan.JoinAsOf(
                left,
                right,
                { spec with
                    LeftOn = resolveScalar leftSchema spec.LeftOn
                    RightOn = resolveScalar rightSchema spec.RightOn
                    LeftBy = spec.LeftBy |> List.map (resolveScalar leftSchema)
                    RightBy = spec.RightBy |> List.map (resolveScalar rightSchema)
                }
            )
        | _ ->
            let inputSchema =
                match LogicalPlanOps.getInputs plan' with
                | [] -> LogicalSchema.Empty
                | first :: rest -> rest |> List.fold (fun (acc: LogicalSchema) p -> acc.Merge(schemaOf p)) (schemaOf first)
            LogicalPlanOps.mapExpressions (resolveScalar inputSchema) plan'
