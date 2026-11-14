namespace Kaminari.Samidare.IR

open System
open Apache.Arrow
open Apache.Arrow.Types
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
/// values or outside a column's range, the comparison is rewritten to the same outcome on every row.
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
    let private compareNode (column: int, op: CompareOp, value: FilterScalar) = FilterNode.Compare(column, op, value) :> FilterNode
    let private inListNode (column: int, values: FilterScalar[]) = FilterNode.InList(column, values) :> FilterNode
    let private startsWithNode (column: int, prefix: byte[]) = FilterNode.StartsWith(column, prefix) :> FilterNode
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
    let private between (column: int) (op: CompareOp) (below: FilterScalar) (above: FilterScalar) : FilterNode =
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
            compareNode(column, op, FilterScalar.Integer value)

    /// `column op value` for a floating value: as it is where the column's width holds it, else between its neighbours.
    let private compareFloat (column: int) (values: IArrowType) (op: CompareOp) (value: float) : FilterNode option =
        let inline exact () = Some(compareNode(column, op, FilterScalar.Float value))
        let inline neighbours (below: float) (above: float) =
            Some(between column op (FilterScalar.Float below) (FilterScalar.Float above))
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
            | Some(StoredLiteral.Bytes bytes) -> Some(compareNode(columns.Index leaf, op, FilterScalar.Bytes bytes))
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
    let private member' (leaf: Leaf) (stored: StoredLiteral) : FilterScalar voption =
        match stored with
        | StoredLiteral.Bytes bytes -> ValueSome(FilterScalar.Bytes bytes)
        | StoredLiteral.Integer(value, false) ->
            match integerRange leaf.Values with
            | Some(lo, hi) when value >= lo && value <= hi -> ValueSome(FilterScalar.Integer value)
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
            if fits then ValueSome(FilterScalar.Float value) else ValueNone

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
        | ScalarExpr.Unary(UnaryOp.IsNull, operand) ->
            leaf schema operand |> Option.map (fun l -> isNullNode (columns.Index l))
        | ScalarExpr.Unary(UnaryOp.IsNotNull, operand) ->
            leaf schema operand |> Option.map (fun l -> notNode (isNullNode (columns.Index l)))
        | ScalarExpr.Unary(UnaryOp.IsNan, operand) ->
            match leaf schema operand with
            | Some l when LiteralConversion.ofArrowType l.Values = Some ValueDomain.Floating -> Some(isNanNode(columns.Index l))
            | _ -> None
        | ScalarExpr.Column _
        | ScalarExpr.Function(ScalarFunction.StructFieldByName, _, _) ->
            match leaf schema expr with
            | Some l when (l.Values :? BooleanType) -> Some(booleanNode(columns.Index l))
            | _ -> None
        | ScalarExpr.Binary(BinaryOp.StartsWith, operand, ScalarExpr.Literal prefix) ->
            match leaf schema operand, prefix with
            | Some l, LiteralValue.Null _ when LiteralConversion.ofArrowType l.Values = Some ValueDomain.String -> Some(nullConstant ())
            | Some l, LiteralValue.String s when LiteralConversion.ofArrowType l.Values = Some ValueDomain.String ->
                Some(startsWithNode(columns.Index l, Text.Encoding.UTF8.GetBytes s))
            | _ -> None
        | ScalarExpr.Binary(op, left, right) -> comparison columns schema op left right
        | ScalarExpr.InList(target, InListSource.Literals values, negated, nullsEqual) ->
            inList columns schema target values negated nullsEqual
        | _ -> None

    /// A comparison of a column with a literal (either side).
    and private comparison (columns: Columns) (schema: Schema) (op: BinaryOp) (left: ScalarExpr) (right: ScalarExpr) : FilterNode option =
        let split =
            match left, right with
            | _, ScalarExpr.Literal literal -> leaf schema left |> Option.map (fun l -> l, literal, false)
            | ScalarExpr.Literal literal, _ -> leaf schema right |> Option.map (fun l -> l, literal, true)
            | _ -> None
        match split with
        | None -> None
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
        | None -> None
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
        for conjunct in FilterShapes.conjuncts filter do
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
