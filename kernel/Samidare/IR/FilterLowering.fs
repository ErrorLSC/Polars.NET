namespace Kaminari.Samidare.IR

open System
open Apache.Arrow
open Apache.Arrow.Types
open Kaminari.Sazanami.Expressions
open Kaminari.Sazanami.Filters

/// A row filter lowered for a reader: the program to evaluate on its columns, and whether it is the whole filter.
type internal LoweredFilter =
    {
        /// The conjuncts that could be lowered, or None when none could.
        Program: FilterProgram option
        /// Whether every conjunct was lowered: the program's rows are then exactly the filter's (else a superset).
        Exact: bool
    }

/// Lowers a row filter of the IR (semantics in SEMANTICS.md) into a FilterProgram over the columns of an Arrow
/// schema, for readers that filter rows themselves (late materialization). Each top-level conjunct is lowered whole or
/// dropped (the rows then a superset: LoweredFilter.Exact is false); inside a conjunct nothing is approximated.
/// Literals are mapped onto the columns' stored values by LiteralConversion; where a literal falls between stored
/// values or outside a column's range, the comparison is rewritten to the same outcome on every row. Comparisons of
/// computed values (arithmetic, casts, a column with another) lower onto the expression kernel (ValueNode): arithmetic
/// only between operands of one type (the frontend casts; SEMANTICS.md, Arithmetic), a literal operand of that type.
[<RequireQualifiedAccess>]
module internal FilterLowering =

    /// A column a leaf reads: its path from a top-level column, its type (a dictionary's), its value type.
    type private Leaf =
        {
            Path: string list
            Type: IArrowType
            Values: IArrowType
        }

    let private valueType (t: IArrowType) : IArrowType =
        match t with
        | :? DictionaryType as d -> d.ValueType
        | t -> t

    /// The columns of the program being built; a conjunct that does not lower takes its columns back.
    type private Columns() =
        let list = ResizeArray<FilterColumn>()

        member _.Count = list.Count

        member _.Index(leaf: Leaf) : int =
            let path = Array.ofList leaf.Path
            match list.FindIndex(fun c -> c.Path = path) with
            | -1 ->
                list.Add(FilterColumn(path, leaf.Type))
                list.Count - 1
            | i -> i

        member _.TrimTo(count: int) = list.RemoveRange(count, list.Count - count)

        member _.All: FilterColumn[] = list.ToArray()

    /// The schema field an expression names: a top-level column, or a struct field reached through StructFieldByName.
    let rec private field (schema: Schema) (expr: ScalarExpr) : (string list * IArrowType) option =
        match expr with
        | ScalarExpr.Column name ->
            match schema.GetFieldByName name with
            | null -> None
            | f -> Some([ name ], f.DataType)
        | ScalarExpr.Function(ScalarFunction.StructFieldByName, [ parent; ScalarExpr.Literal(LiteralValue.String name) ], _) ->
            match field schema parent with
            | Some(path, (:? StructType as s)) ->
                s.Fields
                |> Seq.tryFind (fun f -> f.Name = name)
                |> Option.map (fun f -> path @ [ name ], f.DataType)
            | _ -> None
        | ScalarExpr.Alias(inner, _) -> field schema inner
        | _ -> None

    /// The flat column an operand reads, seeing through casts that keep every value.
    let private leaf (schema: Schema) (expr: ScalarExpr) : Leaf option =
        let direct e =
            match field schema e with
            | Some(path, t) when not (valueType t :? NestedType) -> Some { Path = path; Type = t; Values = valueType t }
            | _ -> None
        match expr with
        | ScalarExpr.Cast(inner, LogicalType.Arrow target, _) ->
            direct inner |> Option.filter (fun l -> FilterShapes.castKeepsValues l.Values target)
        | _ -> direct expr

    // Nodes as FilterNode (F# does not upcast the nested record types by itself).
    let private compareNode (column: int, op: CompareOp, value: StoredScalar) = FilterNode.Compare(column, op, value) :> FilterNode
    let private inListNode (column: int, values: StoredScalar[]) = FilterNode.InList(column, values) :> FilterNode
    let private isNullNode (column: int) = FilterNode.IsNull column :> FilterNode
    let private isNanNode (column: int) = FilterNode.IsNan column :> FilterNode
    let private booleanNode (column: int) = FilterNode.BooleanColumn column :> FilterNode
    let private constantNode (value: Nullable<bool>) = FilterNode.Constant value :> FilterNode
    let private andNode (operands: FilterNode[]) = FilterNode.And operands :> FilterNode
    let private orNode (operands: FilterNode[]) = FilterNode.Or operands :> FilterNode
    let private notNode (operand: FilterNode) = FilterNode.Not operand :> FilterNode
    let private coalesceNode (operands: FilterNode[]) = FilterNode.Coalesce operands :> FilterNode

    let private nullConstant () : FilterNode = constantNode (Nullable())

    /// False on every value, null on nulls (a comparison no value satisfies).
    let private never (column: int) : FilterNode =
        andNode [| isNullNode column; nullConstant () |]

    /// True on every value, null on nulls (a comparison every value satisfies).
    let private always (column: int) : FilterNode =
        orNode [| notNode (isNullNode column); nullConstant () |]

    let private i128 (v: int64) : Int128 = Int128.op_Implicit v

    /// The stored values of an integer-like column.
    let private integerRange (t: IArrowType) : (Int128 * Int128) option =
        match t with
        | :? Int8Type -> Some(i128 (int64 SByte.MinValue), i128 (int64 SByte.MaxValue))
        | :? UInt8Type -> Some(Int128.Zero, i128 (int64 Byte.MaxValue))
        | :? Int16Type -> Some(i128 (int64 Int16.MinValue), i128 (int64 Int16.MaxValue))
        | :? UInt16Type -> Some(Int128.Zero, i128 (int64 UInt16.MaxValue))
        | :? Int32Type
        | :? Date32Type
        | :? Time32Type -> Some(i128 (int64 Int32.MinValue), i128 (int64 Int32.MaxValue))
        | :? UInt32Type -> Some(Int128.Zero, i128 (int64 UInt32.MaxValue))
        | :? Int64Type
        | :? Date64Type
        | :? Time64Type
        | :? TimestampType
        | :? DurationType -> Some(i128 Int64.MinValue, i128 Int64.MaxValue)
        | :? UInt64Type -> Some(Int128.Zero, Int128.op_Implicit UInt64.MaxValue)
        | :? Decimal128Type -> Some(Int128.MinValue, Int128.MaxValue)
        | :? BooleanType -> Some(Int128.Zero, Int128.One)
        | _ -> None

    /// `column op value` where the value lies strictly between two stored values `below` and `above`.
    let private between (column: int) (op: CompareOp) (below: StoredScalar) (above: StoredScalar) : FilterNode =
        match op with
        | CompareOp.Eq -> never column
        | CompareOp.NotEq -> always column
        | CompareOp.Lt
        | CompareOp.LtEq -> compareNode(column, CompareOp.LtEq, below)
        | _ -> compareNode(column, CompareOp.GtEq, above)

    /// `column op value` for an integer value, folded where it lies outside the column's values.
    let private compareInteger (column: int) (lo: Int128, hi: Int128) (op: CompareOp) (value: Int128) : FilterNode =
        if value < lo then
            match op with
            | CompareOp.Eq
            | CompareOp.Lt
            | CompareOp.LtEq -> never column
            | _ -> always column
        elif value > hi then
            match op with
            | CompareOp.Eq
            | CompareOp.Gt
            | CompareOp.GtEq -> never column
            | _ -> always column
        else
            compareNode(column, op, StoredScalar.Integer value)

    /// `column op value` for a floating value: as it is where the column's width holds it, else between its neighbours.
    let private compareFloat (column: int) (values: IArrowType) (op: CompareOp) (value: float) : FilterNode option =
        let inline exact () = Some(compareNode(column, op, StoredScalar.Float value))
        let inline neighbours (below: float) (above: float) =
            Some(between column op (StoredScalar.Float below) (StoredScalar.Float above))
        match values with
        | :? DoubleType -> exact ()
        | :? FloatType ->
            let f = float32 value
            if Double.IsNaN value || float f = value then exact ()
            elif float f < value then neighbours (float f) (float (MathF.BitIncrement f))
            else neighbours (float (MathF.BitDecrement f)) (float f)
        | :? HalfFloatType ->
            let h: Half = Half.op_Explicit value
            if Double.IsNaN value || float h = value then exact ()
            elif float h < value then neighbours (float h) (float (Half.BitIncrement h))
            else neighbours (float (Half.BitDecrement h)) (float h)
        | _ -> None

    /// `column op literal` for a literal of the column's domain (not null).
    let private compareLiteral (columns: Columns) (leaf: Leaf) (op: CompareOp) (literal: LiteralValue) : FilterNode option =
        match LiteralConversion.ofArrowType leaf.Values with
        | None -> None
        | Some domain ->
            match LiteralConversion.tryConvert domain literal with
            | None -> None
            | Some(StoredLiteral.Bytes bytes) -> Some(compareNode(columns.Index leaf, op, StoredScalar.Bytes bytes))
            | Some(StoredLiteral.Floating value) -> compareFloat (columns.Index leaf) leaf.Values op value
            | Some(StoredLiteral.Integer(value, half)) ->
                match integerRange leaf.Values with
                | None -> None
                | Some range ->
                    let column = columns.Index leaf
                    if not half then
                        Some(compareInteger column range op value)
                    else
                        // Strictly between value and value + 1.
                        match op with
                        | CompareOp.Eq -> Some(never column)
                        | CompareOp.NotEq -> Some(always column)
                        | CompareOp.Lt
                        | CompareOp.LtEq -> Some(compareInteger column range CompareOp.LtEq value)
                        | _ -> Some(compareInteger column range CompareOp.GtEq (value + Int128.One))

    /// A list member as the column stores it, or ValueNone when no stored value equals it (it then matches nothing).
    let private member' (leaf: Leaf) (stored: StoredLiteral) : StoredScalar voption =
        match stored with
        | StoredLiteral.Bytes bytes -> ValueSome(StoredScalar.Bytes bytes)
        | StoredLiteral.Integer(value, false) ->
            match integerRange leaf.Values with
            | Some(lo, hi) when value >= lo && value <= hi -> ValueSome(StoredScalar.Integer value)
            | _ -> ValueNone
        | StoredLiteral.Integer(_, true) -> ValueNone
        | StoredLiteral.Floating value ->
            let fits =
                Double.IsNaN value
                || match leaf.Values with
                   | :? DoubleType -> true
                   | :? FloatType -> float (float32 value) = value
                   | :? HalfFloatType -> float (Half.op_Explicit value: Half) = value
                   | _ -> false
            if fits then ValueSome(StoredScalar.Float value) else ValueNone

    let private compareOp (op: BinaryOp) : CompareOp voption =
        match op with
        | BinaryOp.Eq -> ValueSome CompareOp.Eq
        | BinaryOp.NotEq -> ValueSome CompareOp.NotEq
        | BinaryOp.Lt -> ValueSome CompareOp.Lt
        | BinaryOp.LtEq -> ValueSome CompareOp.LtEq
        | BinaryOp.Gt -> ValueSome CompareOp.Gt
        | BinaryOp.GtEq -> ValueSome CompareOp.GtEq
        | _ -> ValueNone

    let private flip (op: CompareOp) : CompareOp =
        match op with
        | CompareOp.Lt -> CompareOp.Gt
        | CompareOp.LtEq -> CompareOp.GtEq
        | CompareOp.Gt -> CompareOp.Lt
        | CompareOp.GtEq -> CompareOp.LtEq
        | op -> op

    /// The type a value expression is computed in: a column's value type, arithmetic's operands' (one type, or a
    /// literal beside a typed operand), a cast's target; Untyped for a literal (it takes the type it meets); No where
    /// the expression does not lower.
    [<RequireQualifiedAccess>]
    type private Typing =
        | Of of IArrowType
        | Untyped
        | No

    let private arithmeticOp (op: BinaryOp) : ArithmeticOp voption =
        match op with
        | BinaryOp.Add -> ValueSome ArithmeticOp.Add
        | BinaryOp.Subtract -> ValueSome ArithmeticOp.Subtract
        | BinaryOp.Multiply -> ValueSome ArithmeticOp.Multiply
        | BinaryOp.Modulo -> ValueSome ArithmeticOp.Remainder
        // .NET integer division (truncated) and floating division; the engine's integer Divide is not lowered.
        | BinaryOp.TruncatedDivide
        | BinaryOp.Divide -> ValueSome ArithmeticOp.Divide
        | _ -> ValueNone

    /// Whether a cast from `source` to `target` lowers: one type to itself, integers and Float32 / Float64 among
    /// themselves but floating to integer (a non-strict cast wraps or rounds as .NET's unchecked conversion; a strict
    /// one only where it keeps every value, or an integer to a floating type, which never fails).
    let private castLowers (source: IArrowType) (target: IArrowType) (strict: bool) : bool =
        ValueTypes.Same(source, target)
        || ValueTypes.IsArithmetic source
           && ValueTypes.IsArithmetic target
           && not (ValueTypes.IsFloating source && not (ValueTypes.IsFloating target))
           && (not strict || FilterShapes.castKeepsValues source target || not (ValueTypes.IsFloating source) && ValueTypes.IsFloating target)

    let rec private typing (schema: Schema) (expr: ScalarExpr) : Typing =
        match expr with
        | ScalarExpr.Alias(inner, _) -> typing schema inner
        | ScalarExpr.Literal(LiteralValue.Null _) -> Typing.No
        | ScalarExpr.Literal _ -> Typing.Untyped
        | ScalarExpr.Column _
        | ScalarExpr.Function(ScalarFunction.StructFieldByName, _, _) ->
            match field schema expr with
            | Some(_, t) when not (valueType t :? NestedType) -> Typing.Of(valueType t)
            | _ -> Typing.No
        | ScalarExpr.Binary(op, left, right) when (arithmeticOp op).IsSome ->
            match typing schema left, typing schema right with
            | Typing.Of a, Typing.Of b when ValueTypes.Same(a, b) && ValueTypes.IsArithmetic a -> Typing.Of a
            | Typing.Of a, Typing.Untyped
            | Typing.Untyped, Typing.Of a when ValueTypes.IsArithmetic a -> Typing.Of a
            | _ -> Typing.No
        | ScalarExpr.Unary(UnaryOp.Negate, operand) ->
            match typing schema operand with
            | Typing.Of t when ValueTypes.IsSigned t -> Typing.Of t
            | _ -> Typing.No
        | ScalarExpr.Cast(operand, LogicalType.Arrow target, strict) ->
            match typing schema operand with
            | Typing.Of source when castLowers source target strict -> Typing.Of(valueType target)
            | _ -> Typing.No
        | _ -> Typing.No

    /// A literal as a value of `t` exactly (the integer in range, the floating value held by the width), or None.
    let private exactScalar (t: IArrowType) (literal: LiteralValue) : StoredScalar option =
        match LiteralConversion.ofArrowType t with
        | None -> None
        | Some domain ->
            match LiteralConversion.tryConvert domain literal with
            | Some(StoredLiteral.Integer(value, false)) ->
                match integerRange t with
                | Some(lo, hi) when value >= lo && value <= hi -> Some(StoredScalar.Integer value)
                | _ -> None
            | Some(StoredLiteral.Floating value) ->
                let fits =
                    Double.IsNaN value
                    || match t with
                       | :? DoubleType -> true
                       | :? FloatType -> float (float32 value) = value
                       | _ -> false
                if fits then Some(StoredScalar.Float value) else None
            | _ -> None

    /// A value expression computed in type `t` (its typing): columns, literals of `t` (beside an operand of `t`),
    /// arithmetic, negation and casts.
    let rec private lowerValue (columns: Columns) (schema: Schema) (t: IArrowType) (expr: ScalarExpr) : ValueNode option =
        match expr with
        | ScalarExpr.Alias(inner, _) -> lowerValue columns schema t inner
        | ScalarExpr.Literal literal ->
            // An operand of arithmetic is of the arithmetic's type (no promotion: the frontend casts).
            if literal.DataType = LogicalType.Arrow t then exactScalar t literal |> Option.map (fun s -> ValueNode.Scalar s :> ValueNode) else None
        | ScalarExpr.Column _
        | ScalarExpr.Function(ScalarFunction.StructFieldByName, _, _) ->
            match field schema expr with
            | Some(path, ft) when ValueTypes.Same(ft, t) ->
                Some(ValueNode.Column(columns.Index { Path = path; Type = ft; Values = valueType ft }) :> ValueNode)
            | _ -> None
        | ScalarExpr.Binary(op, left, right) ->
            match arithmeticOp op with
            | ValueNone -> None
            | ValueSome arithmetic ->
                let fits =
                    match op with
                    | BinaryOp.TruncatedDivide -> not (ValueTypes.IsFloating t)
                    | BinaryOp.Divide -> ValueTypes.IsFloating t
                    | _ -> true
                match fits, lowerValue columns schema t left, lowerValue columns schema t right with
                | true, Some l, Some r -> Some(ValueNode.Arithmetic(arithmetic, l, r) :> ValueNode)
                | _ -> None
        | ScalarExpr.Unary(UnaryOp.Negate, operand) when ValueTypes.IsSigned t ->
            lowerValue columns schema t operand |> Option.map (fun v -> ValueNode.Negate v :> ValueNode)
        | ScalarExpr.Cast(operand, LogicalType.Arrow target, strict) when ValueTypes.Same(target, t) ->
            match typing schema operand with
            | Typing.Of source when castLowers source target strict ->
                lowerValue columns schema source operand
                |> Option.map (fun v -> if ValueTypes.Same(source, target) then v else ValueNode.Cast(v, valueType target) :> ValueNode)
            | _ -> None
        | _ -> None

    /// The type two typed values of different types are compared in, holding every value of both: the narrowest
    /// integer type holding both integer types (none for Int64 against UInt64), Float64 for Float32 against Float64.
    let private commonType (a: IArrowType) (b: IArrowType) : IArrowType option =
        if ValueTypes.Same(a, b) then
            Some a
        else
            [ Int8Type.Default :> IArrowType; UInt8Type.Default; Int16Type.Default; UInt16Type.Default; Int32Type.Default; UInt32Type.Default
              Int64Type.Default; UInt64Type.Default; FloatType.Default; DoubleType.Default ]
            |> List.tryFind (fun c -> FilterShapes.castKeepsValues a c && FilterShapes.castKeepsValues b c)

    /// Whether two byte types compare by their bytes: strings with strings, binaries with binaries (any layout).
    let private bytesAlike (a: IArrowType) (b: IArrowType) : bool =
        let stringy (t: IArrowType) = t :? StringType || t :? StringViewType
        let binary (t: IArrowType) = t :? BinaryType || t :? BinaryViewType || (t :? FixedSizeBinaryType && ValueTypes.IsBytes t)
        stringy a && stringy b || binary a && binary b

    /// `left op right` over computed values (CompareOp; `nullsEqual` with Eq), or None.
    let private compareValues (columns: Columns) (schema: Schema) (c: CompareOp) (nullsEqual: bool) (left: ScalarExpr) (right: ScalarExpr) : FilterNode option =
        let node (l: ValueNode) (r: ValueNode) = FilterNode.CompareValues(l, c, r, nullsEqual) :> FilterNode
        match typing schema left, typing schema right with
        | Typing.Of a, Typing.Of b when (a :? BooleanType && b :? BooleanType) || ValueTypes.IsBytes a && ValueTypes.IsBytes b ->
            // Booleans and bytes: column with column.
            if (a :? BooleanType) || bytesAlike a b then
                match lowerValue columns schema a left, lowerValue columns schema b right with
                | Some(:? ValueNode.Column as l), Some(:? ValueNode.Column as r) -> Some(node l r)
                | _ -> None
            else None
        | Typing.Of a, Typing.Of b when ValueTypes.IsNumeric a && ValueTypes.IsNumeric b ->
            match commonType a b with
            | None -> None
            | Some t ->
                let side (own: IArrowType) (e: ScalarExpr) =
                    lowerValue columns schema own e
                    |> Option.map (fun v -> if ValueTypes.Same(own, t) then v else ValueNode.Cast(v, t) :> ValueNode)
                match side a left, side b right with
                | Some l, Some r -> Some(node l r)
                | _ -> None
        | Typing.Of t, Typing.Untyped when ValueTypes.IsNumeric t ->
            match right with
            | ScalarExpr.Literal literal ->
                match lowerValue columns schema t left, exactScalar t literal with
                | Some l, Some s -> Some(node l (ValueNode.Scalar s))
                | _ -> None
            | _ -> None
        | Typing.Untyped, Typing.Of t when ValueTypes.IsNumeric t ->
            match left with
            | ScalarExpr.Literal literal ->
                match exactScalar t literal, lowerValue columns schema t right with
                | Some s, Some r -> Some(node (ValueNode.Scalar s) r)
                | _ -> None
            | _ -> None
        | _ -> None

    let rec private lowerNode (columns: Columns) (schema: Schema) (expr: ScalarExpr) : FilterNode option =
        let all (exprs: ScalarExpr list) =
            let nodes = exprs |> List.map (lowerNode columns schema)
            if nodes |> List.forall Option.isSome then Some(nodes |> List.map Option.get |> Array.ofList) else None
        match expr with
        | ScalarExpr.Alias(inner, _) -> lowerNode columns schema inner
        | ScalarExpr.Literal(LiteralValue.Bool b) -> Some(constantNode (Nullable b))
        | ScalarExpr.Literal(LiteralValue.Null _) -> Some(nullConstant ())
        | ScalarExpr.Binary(BinaryOp.And, _, _) -> all (FilterShapes.conjuncts expr) |> Option.map andNode
        | ScalarExpr.Binary(BinaryOp.Or, _, _) -> all (FilterShapes.disjuncts expr) |> Option.map orNode
        | ScalarExpr.Coalesce exprs when not exprs.IsEmpty -> all exprs |> Option.map coalesceNode
        | ScalarExpr.Unary(UnaryOp.Not, inner) -> lowerNode columns schema inner |> Option.map notNode
        | ScalarExpr.Unary((UnaryOp.IsNull | UnaryOp.IsNotNull) as op, operand) ->
            let test =
                match leaf schema operand with
                | Some l -> Some(isNullNode (columns.Index l))
                | None ->
                    // A computed number: null where an operand is, or an integer division by zero.
                    match typing schema operand with
                    | Typing.Of t when ValueTypes.IsNumeric t ->
                        lowerValue columns schema t operand |> Option.map (fun v -> FilterNode.ValueIsNull v :> FilterNode)
                    | _ -> None
            if op = UnaryOp.IsNull then test else test |> Option.map notNode
        | ScalarExpr.Unary(UnaryOp.IsNan, operand) ->
            match leaf schema operand with
            | Some l when LiteralConversion.ofArrowType l.Values = Some ValueDomain.Floating -> Some(isNanNode(columns.Index l))
            | Some _ -> None
            | None ->
                // A computed floating value: NaN is the value equal to NaN in the IR's order (null stays null).
                match typing schema operand with
                | Typing.Of t when ValueTypes.IsFloating t ->
                    lowerValue columns schema t operand
                    |> Option.map (fun v -> FilterNode.CompareValues(v, CompareOp.Eq, ValueNode.Scalar(StoredScalar.Float nan), false) :> FilterNode)
                | _ -> None
        | ScalarExpr.Column _
        | ScalarExpr.Function(ScalarFunction.StructFieldByName, _, _) ->
            match leaf schema expr with
            | Some l when (l.Values :? BooleanType) -> Some(booleanNode(columns.Index l))
            | _ -> None
        | ScalarExpr.Binary(BinaryOp.StartsWith, operand, ScalarExpr.Literal prefix) ->
            stringTest columns schema operand prefix FilterNode.StartsWith
        | ScalarExpr.Binary(BinaryOp.EndsWith, operand, ScalarExpr.Literal suffix) ->
            stringTest columns schema operand suffix FilterNode.EndsWith
        // .NET Contains (the translator's literal search; BinaryOp.Contains is a pattern for the engine).
        | ScalarExpr.Function(ScalarFunction.StrContains, [ operand; ScalarExpr.Literal pattern; ScalarExpr.Literal(LiteralValue.Bool true) ], _) ->
            stringTest columns schema operand pattern FilterNode.Contains
        | ScalarExpr.Binary(op, left, right) -> comparison columns schema op left right
        | ScalarExpr.InList(target, InListSource.Literals values, negated, nullsEqual) ->
            inList columns schema target values negated nullsEqual
        | _ -> None

    /// A test of a string column's bytes against a literal's UTF-8 bytes (ordinal, as the engine's string functions;
    /// valid UTF-8 matches by bytes exactly where it matches by characters); null against a null literal.
    /// `node` makes the test (a node constructor), widened to a FilterNode here.
    and private stringTest<'Node when 'Node :> FilterNode>
        (columns: Columns) (schema: Schema) (operand: ScalarExpr) (literal: LiteralValue) (node: int * byte[] -> 'Node) : FilterNode option =
        match leaf schema operand, literal with
        | Some l, LiteralValue.Null _ when LiteralConversion.ofArrowType l.Values = Some ValueDomain.String -> Some(nullConstant ())
        | Some l, LiteralValue.String s when LiteralConversion.ofArrowType l.Values = Some ValueDomain.String ->
            Some(node (columns.Index l, Text.Encoding.UTF8.GetBytes s) :> FilterNode)
        | _ -> None

    /// A comparison of a column with a literal (either side).
    and private comparison (columns: Columns) (schema: Schema) (op: BinaryOp) (left: ScalarExpr) (right: ScalarExpr) : FilterNode option =
        let split =
            match left, right with
            | _, ScalarExpr.Literal literal -> leaf schema left |> Option.map (fun l -> l, literal, false)
            | ScalarExpr.Literal literal, _ -> leaf schema right |> Option.map (fun l -> l, literal, true)
            | _ -> None
        match split with
        | None ->
            // Computed values: arithmetic, casts, a column with another.
            let literalSide = (match left, right with ScalarExpr.Literal _, _ | _, ScalarExpr.Literal _ -> true | _ -> false)
            match op with
            | BinaryOp.EqMissing
            | BinaryOp.NotEqMissing ->
                // Two-valued; beside a (non-null) literal a null value is simply unequal.
                let equal =
                    if literalSide then
                        compareValues columns schema CompareOp.Eq false left right
                        |> Option.map (fun eq -> coalesceNode [| eq; constantNode (Nullable false) |])
                    else
                        compareValues columns schema CompareOp.Eq true left right
                if op = BinaryOp.EqMissing then equal else equal |> Option.map notNode
            | _ ->
                match compareOp op with
                | ValueSome c -> compareValues columns schema c false left right
                | ValueNone -> None
        | Some(l, literal, flipped) ->
            match op with
            | BinaryOp.EqMissing
            | BinaryOp.NotEqMissing ->
                // Two-valued: null equals null and differs from every value.
                let equal =
                    match literal with
                    | LiteralValue.Null _ -> Some(isNullNode (columns.Index l))
                    | _ ->
                        compareLiteral columns l CompareOp.Eq literal
                        |> Option.map (fun eq -> coalesceNode [| eq; constantNode (Nullable false) |])
                if op = BinaryOp.EqMissing then equal else equal |> Option.map notNode
            | _ ->
                match compareOp op with
                | ValueNone -> None
                | ValueSome c ->
                    match literal with
                    | LiteralValue.Null _ -> Some(nullConstant ()) // a comparison with null is null
                    | _ -> compareLiteral columns l (if flipped then flip c else c) literal

    /// `x IN (values)`: without nullsEqual (SQL) null on a null x, and on a miss when the list holds null; with it
    /// (.NET Contains) a null x is in a list holding null, and the outcome is never null.
    and private inList (columns: Columns) (schema: Schema) target (values: LiteralValue list) negated nullsEqual : FilterNode option =
        match leaf schema target with
        | None ->
            // A computed number: its members as values of its type (one that is none matches nothing).
            let isNull v = match v with LiteralValue.Null _ -> true | _ -> false
            let hasNull = values |> List.exists isNull
            match typing schema target with
            | Typing.Of t when ValueTypes.IsNumeric t && not (nullsEqual && hasNull) ->
                lowerValue columns schema t target
                |> Option.map (fun v ->
                    let members = values |> List.filter (isNull >> not) |> List.choose (exactScalar t) |> Array.ofList
                    let any = FilterNode.ValueInList(v, members) :> FilterNode
                    let node =
                        match nullsEqual, hasNull with
                        | false, false -> any
                        | false, true -> orNode [| any; nullConstant () |]
                        | _ -> coalesceNode [| any; constantNode (Nullable false) |]
                    if negated then notNode node else node)
            | _ -> None
        | Some l ->
            match LiteralConversion.ofArrowType l.Values with
            | None -> None
            | Some domain ->
                let isNull v = match v with LiteralValue.Null _ -> true | _ -> false
                let hasNull = values |> List.exists isNull
                let stored = values |> List.filter (isNull >> not) |> List.map (LiteralConversion.tryConvert domain)
                if stored |> List.exists Option.isNone then
                    None
                else
                    let column = columns.Index l
                    let members = stored |> List.choose (fun s -> match member' l s.Value with ValueSome m -> Some m | ValueNone -> None)
                    let any = if members.IsEmpty then never column else inListNode(column, Array.ofList members)
                    let falseNode = constantNode (Nullable false)
                    let node =
                        match nullsEqual, hasNull with
                        | false, false -> any
                        | false, true -> orNode [| any; nullConstant () |]
                        | true, false -> coalesceNode [| any; falseNode |]
                        | true, true -> orNode [| coalesceNode [| any; falseNode |]; isNullNode column |]
                    Some(if negated then notNode node else node)

    /// Lowers a filter over the columns of `schema` (top-level names; struct fields by StructFieldByName).
    let lower (schema: Schema) (filter: ScalarExpr) : LoweredFilter =
        let columns = Columns()
        let kept = ResizeArray<FilterNode>()
        let mutable exact = true
        for conjunct in FilterShapes.conjuncts (FilterShapes.normalize filter) do
            let mark = columns.Count
            let lowered =
                match lowerNode columns schema conjunct with
                | Some node ->
                    try
                        FilterProgram.Create(columns.All, node) |> ignore
                        Some node
                    with :? ArgumentException ->
                        None
                | None -> None
            match lowered with
            | Some node -> kept.Add node
            | None ->
                columns.TrimTo mark
                exact <- false
        let program =
            match kept.Count with
            | 0 -> None
            | 1 -> Some(FilterProgram.Create(columns.All, kept[0]))
            | _ -> Some(FilterProgram.Create(columns.All, andNode (kept.ToArray())))
        { Program = program; Exact = exact }
