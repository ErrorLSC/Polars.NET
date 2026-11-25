namespace Kaminari.Samidare.IR

open System
open Kaminari.Sazanami.Marshalling

/// Resolves the expressions whose translation depends on the types of the columns they read: EnumConvert nodes,
/// the mean of decimals (a decimal Sum / Count, as .NET's Average of decimals), the checked sums of integers
/// (OverflowException, as .NET's Sum of int / long and Average of long), and the conversions between decimals and
/// floating values (DecimalToFloating, FloatingToDecimal: .NET 11's, on every runtime).
///
/// A .NET enum is stored as an engine Enum column (categories in underlying-value order), as a column of its member
/// names (strings or a dictionary of strings: files, JSON, partition values, categoricals) or as an integer column of
/// its underlying values (data written elsewhere, or a [Flags] enum). The translator does not know which, so it emits
/// EnumConvert nodes; this pass picks the form from the inferred type of each converted expression:
/// - ToValue of an Enum maps categories to values, of names maps names to values; of anything else it is the
///   expression itself.
/// - FromValue gives an Enum: a literal becomes its category, any other expression maps values to categories.
///   An enum literal compared with, or chosen alongside, an expression of names becomes its member name, alongside a
///   value expression it stays its underlying value.
/// - ToName of an Enum is its string form, of names the canonical name of an alias; of a value it maps values to
///   names, keeping an undefined value's number.
/// - A comparison of a member's value with a constant compares the names: equality with the constant's names, an
///   ordering with the set of names whose values satisfy it, so that readers filter (and prune) by names.
/// A column of names holds member names exactly (EnumCategories): any other text is not a value of the enum.
/// An expression of unknown type is taken to hold underlying values, as before columns carried types.
module internal EnumResolution =

    /// How an expression of an enum is stored.
    [<RequireQualifiedAccess>]
    type private Layout =
        /// An engine Enum.
        | Categories
        /// Member names: strings or a dictionary of strings.
        | Names
        /// Underlying values.
        | Values

    let private isText (t: Apache.Arrow.Types.IArrowType) =
        match t with
        | :? Apache.Arrow.Types.StringType
        | :? Apache.Arrow.Types.LargeStringType
        | :? Apache.Arrow.Types.StringViewType -> true
        | _ -> false

    let private layoutOf (t: LogicalType) =
        match t with
        | LogicalType.Enum _ -> Layout.Categories
        | LogicalType.Arrow text when isText text -> Layout.Names
        | LogicalType.Arrow(:? Apache.Arrow.Types.DictionaryType as d) when isText d.ValueType -> Layout.Names
        | _ -> Layout.Values

    let private isEnumType (t: LogicalType) = layoutOf t = Layout.Categories

    /// The integer of an integer literal.
    let private integerOf (literal: LiteralValue) : decimal option =
        match literal with
        | LiteralValue.Int8 v -> Some(decimal v)
        | LiteralValue.Int16 v -> Some(decimal v)
        | LiteralValue.Int32 v -> Some(decimal v)
        | LiteralValue.Int64 v -> Some(decimal v)
        | LiteralValue.UInt8 v -> Some(decimal v)
        | LiteralValue.UInt16 v -> Some(decimal v)
        | LiteralValue.UInt32 v -> Some(decimal v)
        | LiteralValue.UInt64 v -> Some(decimal v)
        | _ -> None

    /// The range of an integer type.
    let private integerRange (t: LogicalType) : (decimal * decimal) option =
        match t with
        | LogicalType.Arrow(:? Apache.Arrow.Types.Int8Type) -> Some(decimal SByte.MinValue, decimal SByte.MaxValue)
        | LogicalType.Arrow(:? Apache.Arrow.Types.Int16Type) -> Some(decimal Int16.MinValue, decimal Int16.MaxValue)
        | LogicalType.Arrow(:? Apache.Arrow.Types.Int32Type) -> Some(decimal Int32.MinValue, decimal Int32.MaxValue)
        | LogicalType.Arrow(:? Apache.Arrow.Types.Int64Type) -> Some(decimal Int64.MinValue, decimal Int64.MaxValue)
        | LogicalType.Arrow(:? Apache.Arrow.Types.UInt8Type) -> Some(0m, decimal Byte.MaxValue)
        | LogicalType.Arrow(:? Apache.Arrow.Types.UInt16Type) -> Some(0m, decimal UInt16.MaxValue)
        | LogicalType.Arrow(:? Apache.Arrow.Types.UInt32Type) -> Some(0m, decimal UInt32.MaxValue)
        | LogicalType.Arrow(:? Apache.Arrow.Types.UInt64Type) -> Some(0m, decimal UInt64.MaxValue)
        | _ -> None

    /// Every member of the enum (aliases too) with its value.
    let private members (mapping: EnumMapping) : (string * decimal) list =
        List.zip mapping.Categories mapping.Values @ mapping.Aliases
        |> List.choose (fun (name, value) -> integerOf value |> Option.map (fun n -> name, n))

    /// The value of a member: `(int)e` and a C# enum comparison, also widened to a type that holds every member's
    /// value (`(int)b` of a byte enum, which C# compares as int).
    let private (|EnumValue|_|) (expr: ScalarExpr) =
        match expr with
        | ScalarExpr.EnumConvert(EnumConversion.ToValue, enumExpr, mapping) -> Some(enumExpr, mapping)
        | ScalarExpr.Cast(ScalarExpr.EnumConvert(EnumConversion.ToValue, enumExpr, mapping), t, _) ->
            match integerRange t with
            | Some(low, high) when members mapping |> List.forall (fun (_, n) -> low <= n && n <= high) -> Some(enumExpr, mapping)
            | _ -> None
        | _ -> None

    let private categoryLiterals (mapping: EnumMapping) =
        mapping.Categories |> List.map LiteralValue.String

    let private enumType (mapping: EnumMapping) = LogicalType.Enum mapping.Categories

    /// The value, failing the query on an undefined member with the message ingestion raises
    /// (EnumCategories), rather than the engine's unmapped-value error.
    let private requireDefined (mapping: EnumMapping) (value: ScalarExpr) : ScalarExpr =
        ScalarExpr.Function(
            ScalarFunction.RequireListed(
                mapping.Values,
                $"Value '{{}}' is not a defined member of enum {mapping.Name}; an Enum column holds only defined members."
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

    /// An expression of names as strings (a dictionary's values).
    let private asText (t: LogicalType) (namesExpr: ScalarExpr) =
        match t with
        | LogicalType.Arrow text when isText text -> namesExpr
        | _ -> ScalarExpr.Cast(namesExpr, LogicalTypes.String, true)

    /// The underlying value of an expression of names (ToValue in its names form); a text no member names fails.
    let private valueOfNames (mapping: EnumMapping) (t: LogicalType) (namesExpr: ScalarExpr) : ScalarExpr =
        let all = List.zip mapping.Categories mapping.Values @ mapping.Aliases
        ScalarExpr.Function(
            ScalarFunction.ReplaceStrict(all |> List.map (fst >> LiteralValue.String), all |> List.map snd),
            [ asText t namesExpr ],
            mapping.Underlying
        )

    /// The member name of an enum constant beside an expression of names; a value no member names fails the query
    /// as it does in an Enum column.
    let private nameOfValue (mapping: EnumMapping) (literal: ScalarExpr) : ScalarExpr =
        match literal with
        | ScalarExpr.Literal(LiteralValue.Null _) -> ScalarExpr.Literal(LiteralValue.Null LogicalTypes.String)
        | ScalarExpr.Literal value when List.contains value mapping.Values ->
            ScalarExpr.Literal(LiteralValue.String mapping.Categories.[List.findIndex ((=) value) mapping.Values])
        | _ -> ScalarExpr.Cast(enumOfValue mapping literal, LogicalTypes.String, true)

    /// The comparison `value op k` of the value of an expression of names with the integer `k`, as a comparison of
    /// the names (null for a null name, as the comparison of values): equality with the names of `k` (none: a text no
    /// member is named, a number), an ordering as membership in the names whose values satisfy it.
    let private compareNames (op: BinaryOp) (namesExpr: ScalarExpr) (mapping: EnumMapping) (k: decimal) : ScalarExpr =
        let text (s: string) = ScalarExpr.Literal(LiteralValue.String s)
        // No member name is a number: an identifier does not start with a digit or a sign.
        let noName = text (k.ToString(Globalization.CultureInfo.InvariantCulture))
        let all = members mapping
        match op with
        | BinaryOp.Eq
        | BinaryOp.NotEq
        | BinaryOp.EqMissing
        | BinaryOp.NotEqMissing ->
            let joiner =
                match op with
                | BinaryOp.Eq
                | BinaryOp.EqMissing -> BinaryOp.Or
                | _ -> BinaryOp.And
            match all |> List.filter (fun (_, n) -> n = k) |> List.map fst with
            | [] -> ScalarExpr.Binary(op, namesExpr, noName)
            | names -> names |> List.map (fun name -> ScalarExpr.Binary(op, namesExpr, text name)) |> List.reduce (fun a b -> ScalarExpr.Binary(joiner, a, b))
        | _ ->
            let satisfies (n: decimal) =
                match op with
                | BinaryOp.Lt -> n < k
                | BinaryOp.LtEq -> n <= k
                | BinaryOp.Gt -> n > k
                | _ -> n >= k
            let inside, outside = all |> List.partition (snd >> satisfies)
            let names (list: (string * decimal) list) = InListSource.Collection(list |> List.map fst |> Array.ofList, typeof<string>)
            match inside, outside with
            | [], _ -> ScalarExpr.Binary(BinaryOp.Eq, namesExpr, noName)
            | _, [] -> ScalarExpr.Binary(BinaryOp.NotEq, namesExpr, noName)
            | _ when inside.Length <= outside.Length -> ScalarExpr.InList(namesExpr, names inside, false, false)
            | _ -> ScalarExpr.InList(namesExpr, names outside, true, false)

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

        /// Resolves an enum literal next to an already resolved sibling: an Enum literal beside an Enum, a name beside
        /// names, else its value.
        let besides (sibling: ScalarExpr) (literal: ScalarExpr) (mapping: EnumMapping) =
            match layoutOf (typeOf sibling) with
            | Layout.Categories -> enumOfValue mapping literal
            | Layout.Names -> nameOfValue mapping literal
            | Layout.Values -> literal

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
            // The name of (and whether there is a member of) an enum made from a value is the value's: an undefined
            // value has its number (no member), where an Enum could not hold it.
            let inner', layout =
                match conversion, inner with
                | (EnumConversion.ToName | EnumConversion.IsDefined), ScalarExpr.EnumConvert(EnumConversion.FromValue, value, _) ->
                    resolve value, Layout.Values
                | _ ->
                    let inner' = resolve inner
                    inner', layoutOf (typeOf inner')
            let innerType = typeOf inner'

            match conversion, layout with
            | EnumConversion.ToValue, Layout.Categories -> valueOfEnum mapping inner'
            | EnumConversion.ToValue, Layout.Names -> valueOfNames mapping innerType inner'
            | EnumConversion.ToValue, Layout.Values -> inner'
            | EnumConversion.FromValue, Layout.Categories -> inner'
            | EnumConversion.FromValue, _ -> enumOfValue mapping inner'
            | EnumConversion.ToName, Layout.Categories -> ScalarExpr.Cast(inner', LogicalTypes.String, true)
            // An alias reads as its value's member: the name .NET gives that value.
            | EnumConversion.ToName, Layout.Names when mapping.Aliases.IsEmpty -> asText innerType inner'
            | EnumConversion.ToName, Layout.Names ->
                let text = asText innerType inner'
                let canonical =
                    mapping.Aliases
                    |> List.map (fun (_, value) -> LiteralValue.String mapping.Categories.[List.findIndex ((=) value) mapping.Values])
                ScalarExpr.Function(
                    ScalarFunction.ReplaceStrict(mapping.Aliases |> List.map (fst >> LiteralValue.String), canonical),
                    [ text; text ],
                    LogicalTypes.String
                )
            // A member: any value of an Enum of the enum's categories; else a member's name or value.
            | EnumConversion.IsDefined, Layout.Categories when hasCategoriesOf mapping innerType -> ScalarExpr.Unary(UnaryOp.IsNotNull, inner')
            | EnumConversion.IsDefined, (Layout.Categories | Layout.Names) ->
                let names = members mapping |> List.map fst |> Array.ofList
                ScalarExpr.InList(asText innerType inner', InListSource.Collection(names, typeof<string>), false, false)
            | EnumConversion.IsDefined, Layout.Values ->
                let values = mapping.Values |> List.map (fun v -> v.BoxedValue) |> Array.ofList
                let array = Array.CreateInstance((if values.Length = 0 then typeof<int> else values[0].GetType()), values.Length)
                values |> Array.iteri (fun i v -> array.SetValue(v, i))
                ScalarExpr.InList(inner', InListSource.Collection(array, array.GetType().GetElementType()), false, false)
            // Ordered by the underlying values: an Enum of the enum's categories and values are, names are not.
            | EnumConversion.Ordered, Layout.Names -> valueOfNames mapping innerType inner'
            | EnumConversion.Ordered, Layout.Categories when not (hasCategoriesOf mapping innerType) -> valueOfEnum mapping inner'
            | EnumConversion.Ordered, _ -> inner'
            | EnumConversion.ToName, Layout.Values ->
                ScalarExpr.Function(
                    ScalarFunction.ReplaceStrict(mapping.Values, categoryLiterals mapping),
                    [ inner'; ScalarExpr.Cast(inner', LogicalTypes.String, true) ],
                    LogicalTypes.String
                )

        // Comparing the value of an Enum whose categories are the enum's members with a member's value is
        // comparing the Enum with that member, as categories follow value order. Comparing the value of names with
        // a constant compares the names (compareNames).
        // A comparison with null compares the names with null (a value is null where its name is).
        | ScalarExpr.Binary(op, (EnumValue(enumExpr, mapping) as enumValue), ScalarExpr.Literal literal) when
            isComparison op
            && (match literal with
                | LiteralValue.Null _ -> true
                | other -> (integerOf other).IsSome)
            ->
            let enumExpr' = resolve enumExpr
            let enumType = typeOf enumExpr'
            match layoutOf enumType, integerOf literal with
            | Layout.Names, None -> ScalarExpr.Binary(op, enumExpr', ScalarExpr.Literal(LiteralValue.Null LogicalTypes.String))
            | Layout.Names, Some k -> compareNames op enumExpr' mapping k
            | Layout.Categories, Some k when hasCategoriesOf mapping enumType ->
                match mapping.Values |> List.tryFind (fun v -> integerOf v = Some k) with
                | Some value -> ScalarExpr.Binary(op, enumExpr', enumOfValue mapping (ScalarExpr.Literal value))
                | None -> ScalarExpr.Binary(op, resolve enumValue, ScalarExpr.Literal literal)
            | _ -> ScalarExpr.Binary(op, resolve enumValue, ScalarExpr.Literal literal)

        // `k op value` is `value (mirror op) k` (resolved as such: its form may be another comparison, or none).
        | ScalarExpr.Binary(op, (ScalarExpr.Literal _ as literal), (EnumValue _ as enumValue))
            when isComparison op ->
            resolve (ScalarExpr.Binary(mirror op, enumValue, literal))

        // An enum compared with an enum constant (F# compares enums without converting them to their values).
        | ScalarExpr.Binary(op, other, EnumLiteral(ScalarExpr.Literal literal, mapping)) when isComparison op ->
            let other' = resolve other
            match layoutOf (typeOf other'), integerOf literal with
            | Layout.Names, Some k -> compareNames op other' mapping k
            | Layout.Names, None -> ScalarExpr.Binary(op, other', ScalarExpr.Literal(LiteralValue.Null LogicalTypes.String))
            | Layout.Categories, Some k when (mapping.Values |> List.exists (fun v -> integerOf v = Some k)) |> not ->
                // No member has the value: compared as values (an Enum cannot hold it).
                ScalarExpr.Binary(op, valueOfEnum mapping other', ScalarExpr.Literal literal)
            | _ -> ScalarExpr.Binary(op, other', besides other' (ScalarExpr.Literal literal) mapping)
        | ScalarExpr.Binary(op, (EnumLiteral _ as literal), other) when isComparison op ->
            resolve (ScalarExpr.Binary(mirror op, other, literal))

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
            let layouts = resolvedOthers |> List.map (typeOf >> layoutOf)
            exprs
            |> List.map (function
                | EnumLiteral(literal, mapping) ->
                    if List.contains Layout.Categories layouts || resolvedOthers.IsEmpty then enumOfValue mapping literal
                    elif List.contains Layout.Names layouts then nameOfValue mapping literal
                    else literal
                | other -> resolve other)
            |> ScalarExpr.Coalesce

        | ScalarExpr.InList(inner, source, negated, nullsEqual) ->
            let inner' = resolve inner
            let source' =
                match source with
                // Over names, the names of the collection's members (aliases too); a value no member names is in no row.
                | InListSource.Collection(values, elementType) when
                    (tryEnumMapping elementType).IsSome && layoutOf (typeOf inner') = Layout.Names
                    ->
                    let all = members (tryEnumMapping elementType).Value
                    let names =
                        values
                        |> Seq.cast<obj>
                        |> Seq.collect (fun item ->
                            if isNull item then
                                Seq.singleton (null: string)
                            else
                                let n = Convert.ToDecimal item
                                all |> Seq.filter (fun (_, value) -> value = n) |> Seq.map fst)
                        |> Seq.toArray
                    InListSource.Collection(names, typeof<string>)
                | InListSource.Collection(values, elementType) when
                    (tryEnumMapping elementType).IsSome && not (isEnumType (typeOf inner'))
                    ->
                    let underlying = Enum.GetUnderlyingType(ClrTypeHelper.UnwrapCoreType elementType)
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
        // A member's value is null where the member is, whatever its layout.
        | ScalarExpr.Unary((UnaryOp.IsNull | UnaryOp.IsNotNull) as op, EnumValue(enumExpr, _)) -> ScalarExpr.Unary(op, resolve enumExpr)
        | ScalarExpr.Unary(op, inner) -> ScalarExpr.Unary(op, resolve inner)
        // A decimal as a float or double, and a float or double as a decimal, convert as .NET 11 does, which an
        // engine's cast does not: correctly rounded, and from the exact binary value.
        | ScalarExpr.Cast(inner, t, strict) ->
            let inner' = resolve inner
            match typeOf inner', t with
            | LogicalType.Arrow(:? Apache.Arrow.Types.Decimal128Type),
              LogicalType.Arrow(:? Apache.Arrow.Types.FloatType | :? Apache.Arrow.Types.DoubleType) ->
                ScalarExpr.Function(ScalarFunction.DecimalToFloating, [ inner' ], t)
            | LogicalType.Arrow(:? Apache.Arrow.Types.FloatType | :? Apache.Arrow.Types.DoubleType),
              LogicalType.Arrow(:? Apache.Arrow.Types.Decimal128Type) ->
                ScalarExpr.Function(ScalarFunction.FloatingToDecimal, [ inner' ], t)
            | _ -> ScalarExpr.Cast(inner', t, strict)
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
        // against the range of the summed type (a strict cast without assertions: the query still fails; an
        // Average is then the engine's mean).
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
            let assertions = CompilationScope.TryCurrent |> Option.forall (fun scope -> scope.Backend.SupportsAssertions)
            // Per group: one exact sum in the wider type, narrowed back to the summed type with the overflow check.
            // One aggregation: an engine evaluates the aggregations of a group-by apart (no common ones), so a check
            // beside the value would sum twice. A long operand is cast to Int64 first (its inferred type is the CLR
            // type's, which the engine's may not be: a count, a narrower integer), which also marks the sum as one of
            // integers for a backend that sums them in a native 128-bit type.
            let narrowedSum () =
                let operand =
                    match summedType with
                    | LogicalType.Arrow(:? Apache.Arrow.Types.Int64Type) ->
                        ScalarExpr.Cast(ScalarExpr.Cast(inner', LogicalTypes.Int64, false), wide, false)
                    | _ -> ScalarExpr.Cast(inner', wide, false)
                ScalarExpr.Function(ScalarFunction.CheckedNarrow, [ ScalarExpr.Aggregation(AggKind.Sum, Some operand, distinct) ], summedType)
            // Over all the rows, the checked sum is one aggregation (a single-pass backend kernel); per group it is the
            // narrowed exact sum, which the engine computes for all the groups at once.
            // The operand is cast to its inferred type, which the engine's may not be (a count, a narrower column).
            let checkedSum () =
                if grouped then narrowedSum ()
                else ScalarExpr.Aggregation(AggKind.CheckedSum, Some(ScalarExpr.Cast(inner', summedType, false)), distinct)
            match kind with
            | AggKind.Sum when not grouped -> checkedSum ()
            | AggKind.Sum when not assertions ->
                ScalarExpr.Cast(ScalarExpr.Aggregation(AggKind.Sum, Some(ScalarExpr.Cast(inner', wide, false)), distinct), summedType, true)
            | AggKind.Sum -> narrowedSum ()
            | _ when not assertions -> ScalarExpr.Aggregation(kind, Some inner', distinct)
            // Average of long, as .NET: the checked long sum, as a double, over the count of values (no values: null).
            | _ when not distinct ->
                let count = ScalarExpr.Aggregation(AggKind.Count, Some inner', false)
                let nullIfZero =
                    ScalarExpr.Conditional(
                        ScalarExpr.Binary(BinaryOp.Eq, count, ScalarExpr.Literal(LiteralValue.UInt32 0u)),
                        ScalarExpr.Literal(LiteralValue.Null LogicalTypes.Float64),
                        ScalarExpr.Cast(count, LogicalTypes.Float64, false))
                ScalarExpr.Binary(BinaryOp.Divide, ScalarExpr.Cast(checkedSum (), LogicalTypes.Float64, false), nullIfZero)
            // Average of distinct longs: the engine's mean, its exact sum checked beside it (an engine's count of
            // distinct values counts null as one).
            | _ ->
                let wideSum = ScalarExpr.Aggregation(AggKind.Sum, Some(ScalarExpr.Cast(inner', wide, false)), distinct)
                let overflowed =
                    ScalarExpr.Conditional(
                        ScalarExpr.Binary(
                            BinaryOp.Or,
                            ScalarExpr.Binary(BinaryOp.Lt, wideSum, ScalarExpr.Literal min),
                            ScalarExpr.Binary(BinaryOp.Gt, wideSum, ScalarExpr.Literal max)),
                        ScalarExpr.Literal(LiteralValue.Int64 1L),
                        ScalarExpr.Literal(LiteralValue.Int64 0L))
                let value = ScalarExpr.Aggregation(kind, Some inner', distinct)
                ScalarExpr.Function(
                    ScalarFunction.AssertCount { Min = 0L; Max = Some 0L; TooFew = overflow; TooMany = overflow },
                    [ overflowed; value ],
                    typeOf value)
        // The least / greatest of names is the name of the least / greatest value (an engine's min_by / max_by: a
        // function over the rows that gives one value, which an aggregation context keeps as one).
        | ScalarExpr.Aggregation((AggKind.Min | AggKind.Max) as kind, Some(ScalarExpr.EnumConvert(EnumConversion.Ordered, inner, mapping)), distinct) ->
            let inner' = resolve inner
            let innerType = typeOf inner'
            match layoutOf innerType with
            | Layout.Names ->
                let by = if kind = AggKind.Min then ScalarFunction.MinBy else ScalarFunction.MaxBy
                ScalarExpr.Function(by, [ inner'; valueOfNames mapping innerType inner' ], innerType)
            | _ -> ScalarExpr.Aggregation(kind, Some inner', distinct)
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
        | ScalarExpr.Subquery(scope, plan, kind) ->
            let kind' =
                match kind with
                | SubqueryKind.In(target, nullsEqual) -> SubqueryKind.In(resolve target, nullsEqual)
                | other -> other
            ScalarExpr.Subquery(scope, resolvePlan plan, kind')
        | ScalarExpr.OuterColumn _ -> expr
        | ScalarExpr.Alias(inner, alias) -> ScalarExpr.Alias(resolve inner, alias)
        | ScalarExpr.Udf(name, args, t, invoker) -> ScalarExpr.Udf(name, args |> List.map resolve, t, invoker)

    /// Resolves `expr`, whose value is stored into a column of `columnType` (an assignment), against `schema`: an
    /// enum constant takes the column's representation, its category for an Enum column, else its value.
    /// Resolves `expr` as resolveScalarIn, its aggregations taken per group: the form valid in any context.
    and resolveScalar (schema: LogicalSchema) (expr: ScalarExpr) : ScalarExpr = resolveScalarIn true schema expr

    and resolveAssignment (schema: LogicalSchema) (columnType: LogicalType) (expr: ScalarExpr) : ScalarExpr =
        match expr with
        | EnumLiteral(literal, mapping) ->
            match layoutOf columnType with
            | Layout.Categories -> enumOfValue mapping literal
            | Layout.Names -> nameOfValue mapping literal
            | Layout.Values -> literal
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
