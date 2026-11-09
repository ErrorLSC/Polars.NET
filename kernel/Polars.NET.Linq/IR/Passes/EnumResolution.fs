namespace Polars.NET.Linq.IR

open System
open Polars.NET.Core.Helpers

/// Resolves the expressions whose translation depends on the types of the columns they read: EnumConvert nodes,
/// the mean of decimals (a decimal Sum / Count, as .NET's Average of decimals), and the checked sums of integers
/// (OverflowException, as .NET's Sum of int / long and Average of long).
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

    /// The checked sum of an integer type .NET sums (Enumerable.Sum of int / long): the type an exact sum is taken
    /// in, and that type's literals of the result type's range.
    let private checkedSumOf (t: LogicalType) : (LogicalType * LiteralValue * LiteralValue) option =
        match t with
        | LogicalType.Arrow(:? Apache.Arrow.Types.Int32Type) ->
            Some(LogicalTypes.Int64, LiteralValue.Int64(int64 Int32.MinValue), LiteralValue.Int64(int64 Int32.MaxValue))
        | LogicalType.Arrow(:? Apache.Arrow.Types.Int64Type) ->
            Some(LogicalTypes.decimalOf 38 0, LiteralValue.Decimal(decimal Int64.MinValue), LiteralValue.Decimal(decimal Int64.MaxValue))
        | _ -> None

    /// Whether a value of type `t` widened to a 64-bit integer cannot overflow a sum of fewer than 2^32 of them.
    let private isNarrowInteger (t: LogicalType) =
        match t with
        | LogicalType.Arrow(:? Apache.Arrow.Types.Int32Type)
        | LogicalType.Arrow(:? Apache.Arrow.Types.Int16Type)
        | LogicalType.Arrow(:? Apache.Arrow.Types.Int8Type)
        | LogicalType.Arrow(:? Apache.Arrow.Types.UInt32Type)
        | LogicalType.Arrow(:? Apache.Arrow.Types.UInt16Type)
        | LogicalType.Arrow(:? Apache.Arrow.Types.UInt8Type) -> true
        | _ -> false

    /// Whether a long expression only takes the values of literals below 2^31 in magnitude (`test ? 1L : 0L`).
    let rec private isSmallLongLiterals (expr: ScalarExpr) =
        match expr with
        | ScalarExpr.Literal(LiteralValue.Int64 value) -> abs value < (1L <<< 31)
        | ScalarExpr.Conditional(_, whenTrue, whenFalse) -> isSmallLongLiterals whenTrue && isSmallLongLiterals whenFalse
        | ScalarExpr.Alias(inner, _) -> isSmallLongLiterals inner
        | _ -> false

    /// Whether the sum of a column of this type, widened to 64 bits, cannot overflow.
    let private widensWithoutOverflow (t: LogicalType) =
        match t with
        | LogicalType.Arrow(:? Apache.Arrow.Types.BooleanType) -> true
        | other -> isNarrowInteger other

    let private overflow = { Kind = QueryErrorKind.Overflow; Message = "" }

    /// Resolves the EnumConvert nodes of `expr`, whose columns are those of `schema`. With `grouped`, the expression's
    /// aggregations are computed per group (a GroupBy's aggregations, a window), otherwise over all the rows.
    let rec resolveScalarIn (grouped: bool) (schema: LogicalSchema) (expr: ScalarExpr) : ScalarExpr =
        let resolve = resolveScalarIn grouped schema
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
        // A checked sum resolved before (a plan resolved again under a scalar terminal).
        | ScalarExpr.Function(ScalarFunction.AssertCount check, _, _) when check.TooMany = overflow -> expr
        | ScalarExpr.Function(fn, args, t) -> ScalarExpr.Function(fn, args |> List.map resolve, t)
        // .NET Average of decimals is a decimal; the engine's mean of decimals is a float. Sum / count keeps it
        // decimal (18 fractional digits), and no values divide by a null count rather than by zero.
        | ScalarExpr.Aggregation(AggKind.Mean, Some inner, false) when
            (match typeOf inner with
             | LogicalType.Arrow(:? Apache.Arrow.Types.Decimal128Type) -> true
             | _ -> false)
            ->
            let inner' = resolve inner
            let count = ScalarExpr.Aggregation(AggKind.Count, Some inner', false)
            let nullIfZero =
                ScalarExpr.Conditional(
                    ScalarExpr.Binary(BinaryOp.Eq, count, ScalarExpr.Literal(LiteralValue.UInt32 0u)),
                    ScalarExpr.Literal(LiteralValue.Null LogicalTypes.Decimal),
                    ScalarExpr.Cast(count, LogicalTypes.Decimal, false))
            ScalarExpr.Binary(
                BinaryOp.Divide,
                ScalarExpr.Cast(ScalarExpr.Aggregation(AggKind.Sum, Some inner', false), LogicalTypes.Decimal, false),
                nullIfZero)
        // .NET sums integers in checked arithmetic: Sum of int / long, and the long sum behind Average of long,
        // throw OverflowException where the engine wraps. The sum is taken exactly in a wider type and checked
        // against the range of the summed type (a strict cast without assertions: the query still fails).
        // A sum of narrower integers or of booleans (a count) widened to 64 bits cannot overflow, which also leaves the
        // rewritten sum alone. Neither can a long sum whose values are all small literals, below 2^32 rows.
        | ScalarExpr.Aggregation((AggKind.Sum | AggKind.Mean) as kind, Some inner, distinct) when
            (match inner with
             | ScalarExpr.Cast(narrow, LogicalType.Arrow(:? Apache.Arrow.Types.Int64Type), _) ->
                 not (widensWithoutOverflow (typeOf narrow))
             | _ -> true)
            && not (kind = AggKind.Sum && isSmallLongLiterals inner)
            && match kind, typeOf inner with
               | AggKind.Sum, t -> (checkedSumOf t).IsSome
               | _, LogicalType.Arrow(:? Apache.Arrow.Types.Int64Type) -> true
               | _ -> false
            ->
            let inner' = resolve inner
            let summedType = typeOf inner'
            let wide, min, max = (checkedSumOf summedType).Value
            match kind with
            // Over all the rows, the checked sum is one aggregation (a single-pass backend kernel); per group it is
            // composed below from plain aggregations, which the engine computes for all the groups at once.
            // The operand is cast to its inferred type, which the engine's may not be (a count, a narrower column).
            | AggKind.Sum when not grouped ->
                ScalarExpr.Aggregation(AggKind.CheckedSum, Some(ScalarExpr.Cast(inner', summedType, false)), distinct)
            | _ ->
                let wideSum = ScalarExpr.Aggregation(AggKind.Sum, Some(ScalarExpr.Cast(inner', wide, false)), distinct)
                let assertions =
                    CompilationScope.TryCurrent |> Option.forall (fun scope -> scope.Backend.SupportsAssertions)
                match kind with
                | AggKind.Sum when not assertions -> ScalarExpr.Cast(wideSum, summedType, true)
                | AggKind.Mean when not assertions -> ScalarExpr.Aggregation(kind, Some inner', distinct)
                | _ ->
                    let isLong =
                        match summedType with
                        | LogicalType.Arrow(:? Apache.Arrow.Types.Int64Type) -> true
                        | _ -> false
                    let outside (total: ScalarExpr) (low: LiteralValue) (high: LiteralValue) =
                        ScalarExpr.Conditional(
                            ScalarExpr.Binary(
                                BinaryOp.Or,
                                ScalarExpr.Binary(BinaryOp.Lt, total, ScalarExpr.Literal low),
                                ScalarExpr.Binary(BinaryOp.Gt, total, ScalarExpr.Literal high)),
                            ScalarExpr.Literal(LiteralValue.Int64 1L),
                            ScalarExpr.Literal(LiteralValue.Int64 0L))
                    // A long sum is checked without a 128-bit sum (the engine's decimal sum is about 50x slower):
                    // x = hi * 2^32 + lo with 0 <= lo < 2^32, and neither part's Int64 sum can overflow below 2^31
                    // rows. The exact total is T * 2^32 + R with T = sum(hi) + sum(lo) // 2^32 and 0 <= R < 2^32,
                    // so it fits a long exactly when T fits an int; the engine's wrapping sum is then exact.
                    // A distinct sum cannot be split (the parts' distinct values are not the values'), so it keeps
                    // the wide sum.
                    let overflowed =
                        if isLong && not distinct then
                            // The operand is cast to Int64: its inferred type is the CLR type's, which the engine's
                            // may not be (a count, a narrower integer column), and the halves must be Int64 arithmetic.
                            let x = ScalarExpr.Cast(inner', LogicalTypes.Int64, false)
                            let two32 = ScalarExpr.Literal(LiteralValue.Int64 4294967296L)
                            let hi = ScalarExpr.Binary(BinaryOp.FloorDivide, x, two32)
                            let lo = ScalarExpr.Binary(BinaryOp.Subtract, x, ScalarExpr.Binary(BinaryOp.Multiply, hi, two32))
                            let sumOf e = ScalarExpr.Aggregation(AggKind.Sum, Some e, false)
                            let t = ScalarExpr.Binary(BinaryOp.Add, sumOf hi, ScalarExpr.Binary(BinaryOp.FloorDivide, sumOf lo, two32))
                            outside t (LiteralValue.Int64(int64 Int32.MinValue)) (LiteralValue.Int64(int64 Int32.MaxValue))
                        else
                            outside wideSum min max
                    let value =
                        match kind with
                        | AggKind.Sum when isLong && not distinct ->
                            ScalarExpr.Aggregation(AggKind.Sum, Some(ScalarExpr.Cast(inner', LogicalTypes.Int64, false)), false)
                        | AggKind.Sum -> ScalarExpr.Cast(wideSum, summedType, false)
                        | _ -> ScalarExpr.Aggregation(kind, Some inner', distinct)
                    ScalarExpr.Function(
                        ScalarFunction.AssertCount { Min = 0L; Max = Some 0L; TooFew = overflow; TooMany = overflow },
                        [ overflowed; value ],
                        typeOf value)
        | ScalarExpr.Aggregation(kind, inner, distinct) -> ScalarExpr.Aggregation(kind, inner |> Option.map resolve, distinct)
        | ScalarExpr.Window(inner, partitionBy, orderBy) ->
            ScalarExpr.Window(
                resolveScalarIn true schema inner,
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
    /// Resolves `expr` as resolveScalarIn, its aggregations taken per group: the form valid in any context.
    and resolveScalar (schema: LogicalSchema) (expr: ScalarExpr) : ScalarExpr = resolveScalarIn true schema expr

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
            // A GroupBy's and a GroupingSets' expressions aggregate per group; any other node's, over its rows.
            let grouped =
                match plan' with
                | LogicalPlan.GroupBy _
                | LogicalPlan.GroupingSets _ -> true
                | _ -> false
            LogicalPlanOps.mapExpressions (resolveScalarIn grouped inputSchema) plan'
