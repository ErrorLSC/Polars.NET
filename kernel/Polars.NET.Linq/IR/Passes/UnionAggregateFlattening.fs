namespace Polars.NET.Linq.IR.Passes

open Polars.NET.Linq.IR

/// Merges a grouping of a UNION ALL of groupings on the same keys into one grouping of the union of their rows.
///
/// A query without FULL JOIN combines two per-key summaries by concatenating them and grouping again:
/// `SELECT k, SUM(a), SUM(b) FROM (SELECT k, SUM(x) a, 0 b FROM f GROUP BY k UNION ALL SELECT k, 0, SUM(y) FROM g GROUP BY k) GROUP BY k`.
/// Every row is then hashed twice, once per level, which costs most when the keys are nearly unique. One grouping
/// suffices: `SELECT k, SUM(x), SUM(y) FROM (SELECT k, x, NULL y FROM f UNION ALL SELECT k, NULL, y FROM g) GROUP BY k`.
///
/// Groups keep their order of first appearance, since the first row of a group in a branch is also the first row of
/// that branch's inner group. Only aggregates that merge exactly are rewritten, and any other leaves the plan alone:
/// - An outer Sum of inner Sums, over integers or decimals. Floating sums round differently in another order.
/// - An outer Sum whose branches are `COALESCE(SUM(x), 0)` or the literal 0. A branch's 0 adds nothing, so the
///   branch passes NULL, and the COALESCE moves outside so a group still sums to 0 rather than NULL.
/// - An outer Min or Max of the same inner aggregate or of a non-null literal, which every row of the group repeats.
module internal UnionAggregateFlattening =

    type private Branch =
        { Input: LogicalPlan
          Schema: LogicalSchema
          Keys: Map<string, ScalarExpr>
          Aggs: Map<string, ScalarExpr>
          Columns: string list }

    /// What one branch feeds an outer Sum.
    type private SumPart =
        | Plain of ScalarExpr
        | Guarded of argument: ScalarExpr * zero: ScalarExpr
        | Zero of ScalarExpr

    let rec private stripAlias (expr: ScalarExpr) =
        match expr with
        | ScalarExpr.Alias(inner, _) -> stripAlias inner
        | other -> other

    let private outputName (expr: ScalarExpr) =
        match expr with
        | ScalarExpr.Column name
        | ScalarExpr.Alias(_, name) -> Some name
        | _ -> None

    let private isZero (value: LiteralValue) =
        match value with
        | LiteralValue.Int8 v -> v = 0y
        | LiteralValue.Int16 v -> v = 0s
        | LiteralValue.Int32 v -> v = 0
        | LiteralValue.Int64 v -> v = 0L
        | LiteralValue.UInt8 v -> v = 0uy
        | LiteralValue.UInt16 v -> v = 0us
        | LiteralValue.UInt32 v -> v = 0u
        | LiteralValue.UInt64 v -> v = 0UL
        | LiteralValue.Decimal v -> v = 0m
        | _ -> false

    let rec private unionBranches (plan: LogicalPlan) =
        match plan with
        | LogicalPlan.Union(left, right, true) -> unionBranches left @ unionBranches right
        | other -> [ other ]

    let private tryBranch (plan: LogicalPlan) =
        match plan with
        | LogicalPlan.GroupBy(input, keys, aggs, None, _) when not keys.IsEmpty ->
            let names = keys |> List.map outputName
            let rowLocal = keys |> List.forall (ScalarExprOps.dependsOnOtherRows >> not)
            if rowLocal && names |> List.forall Option.isSome then
                let names = names |> List.map Option.get
                Some
                    { Input = input
                      Schema = SchemaInference.inferPlanSchema input
                      Keys = List.zip names (keys |> List.map stripAlias) |> Map.ofList
                      Aggs = aggs |> List.map (fun (name, expr) -> name, stripAlias expr) |> Map.ofList
                      Columns = names @ (aggs |> List.map fst) }
            else
                None
        | _ -> None

    let private trySumPart (inner: ScalarExpr) =
        match inner with
        | ScalarExpr.Aggregation(AggKind.Sum, Some argument, false) -> Some(Plain argument)
        | ScalarExpr.Coalesce [ ScalarExpr.Aggregation(AggKind.Sum, Some argument, false); ScalarExpr.Literal value as zero ] when isZero value ->
            Some(Guarded(argument, zero))
        | ScalarExpr.Literal value as zero when isZero value -> Some(Zero zero)
        | _ -> None

    /// True when the arguments read each row alone and share one known type.
    let private consistent (arguments: (Branch * ScalarExpr) list) =
        let types =
            arguments
            |> List.map (fun (branch, argument) -> SchemaInference.inferScalarType branch.Schema argument)
            |> List.distinct
        match types with
        | [ LogicalType.Unknown ] -> false
        | [ _ ] -> arguments |> List.forall (snd >> ScalarExprOps.dependsOnOtherRows >> not)
        | _ -> false

    let private tryFlattenSum (branches: Branch list) (column: string) =
        let parts = branches |> List.map (fun branch -> branch.Aggs.TryFind column |> Option.bind trySumPart)
        if parts |> List.exists Option.isNone then
            None
        else
            let parts = parts |> List.map Option.get
            let contributing =
                List.zip branches parts
                |> List.choose (fun (branch, part) ->
                    match part with
                    | Plain argument
                    | Guarded(argument, _) -> Some(branch, argument)
                    | Zero _ -> None)
            let exact = contributing |> List.forall (fun (branch, argument) -> EagerAggregation.isExactNumber branch.Schema argument)
            if contributing.IsEmpty || not exact || not (consistent contributing) then
                None
            else
                let argumentType =
                    let branch, argument = contributing.Head
                    SchemaInference.inferScalarType branch.Schema argument
                let arguments =
                    parts
                    |> List.map (function
                        | Plain argument
                        | Guarded(argument, _) -> argument
                        | Zero _ -> ScalarExpr.Literal(LiteralValue.Null argumentType))
                let isPlain = function Plain _ -> true | _ -> false
                if parts |> List.forall isPlain then
                    Some(arguments, id)
                elif parts |> List.exists isPlain then
                    // A plain Sum may be NULL where a branch's 0 is not, so the two do not merge on every backend.
                    None
                else
                    let zero =
                        parts
                        |> List.pick (function
                            | Guarded(_, zero)
                            | Zero zero -> Some zero
                            | Plain _ -> None)
                    Some(arguments, (fun sum -> ScalarExpr.Coalesce [ sum; zero ]))

    let private tryFlattenMinMax (branches: Branch list) (kind: AggKind) (column: string) =
        let argumentOf (branch: Branch) =
            match branch.Aggs.TryFind column with
            | Some(ScalarExpr.Aggregation(inner, Some argument, false)) when inner = kind -> Some argument
            | Some(ScalarExpr.Literal value as literal) when (match value with LiteralValue.Null _ -> false | _ -> true) -> Some literal
            | _ -> None
        let arguments = branches |> List.map argumentOf
        if arguments |> List.exists Option.isNone then
            None
        else
            let arguments = arguments |> List.map Option.get
            if consistent (List.zip branches arguments) then Some(arguments, id) else None

    let private tryFlatten (input: LogicalPlan) (keys: ScalarExpr list) (aggs: (string * ScalarExpr) list) maintainOrder =
        let branches = unionBranches input |> List.map tryBranch
        if branches.Length < 2 || branches |> List.exists Option.isNone then
            None
        else
            let branches = branches |> List.map Option.get
            let outerKeys =
                keys
                |> List.map (fun key ->
                    match stripAlias key with
                    | ScalarExpr.Column name -> Some name
                    | _ -> None)
            let sameColumns = branches |> List.forall (fun branch -> branch.Columns = branches.Head.Columns)
            if not sameColumns || outerKeys |> List.exists Option.isNone then
                None
            else
                let outerKeys = outerKeys |> List.map Option.get
                let isEveryKey (branch: Branch) = Set.ofList outerKeys = (branch.Keys |> Map.toList |> List.map fst |> Set.ofList)
                if not (branches |> List.forall isEveryKey) then
                    None
                else
                    let flattened =
                        aggs
                        |> List.map (fun (_, expr) ->
                            match stripAlias expr with
                            | ScalarExpr.Aggregation(AggKind.Sum, Some(ScalarExpr.Column column), false) ->
                                tryFlattenSum branches column |> Option.map (fun (arguments, finish) -> AggKind.Sum, arguments, finish)
                            | ScalarExpr.Aggregation((AggKind.Min | AggKind.Max as kind), Some(ScalarExpr.Column column), false) ->
                                tryFlattenMinMax branches kind column |> Option.map (fun (arguments, finish) -> kind, arguments, finish)
                            | _ -> None)
                    if flattened.IsEmpty || flattened |> List.exists Option.isNone then
                        None
                    else
                        let flattened = flattened |> List.map Option.get
                        let argumentName index = $"__fu{index}"
                        let projected =
                            branches
                            |> List.mapi (fun branchIndex branch ->
                                let keyColumns = outerKeys |> List.map (fun name -> name, branch.Keys.[name])
                                let argumentColumns =
                                    flattened
                                    |> List.mapi (fun index (_, arguments, _) -> argumentName index, arguments.[branchIndex])
                                LogicalPlan.Project(branch.Input, keyColumns @ argumentColumns))
                        let union = projected |> List.reduce (fun left right -> LogicalPlan.Union(left, right, true))
                        let originalSchema = SchemaInference.inferPlanSchema input
                        let unionSchema = SchemaInference.inferPlanSchema union
                        let finalAggs =
                            List.zip aggs flattened
                            |> List.mapi (fun index ((name, original), (kind, _, finish)) ->
                                let final = finish (ScalarExpr.Aggregation(kind, Some(ScalarExpr.Column(argumentName index)), false))
                                let originalType = SchemaInference.inferScalarType originalSchema original
                                // The union's first branch set the column's type; the arguments may be narrower.
                                if originalType = LogicalType.Unknown || originalType = SchemaInference.inferScalarType unionSchema final then
                                    name, final
                                else
                                    name, ScalarExpr.Cast(final, originalType, true))
                        Some(LogicalPlan.GroupBy(union, keys, finalAggs, None, maintainOrder))

    let rec apply (plan: LogicalPlan) : LogicalPlan =
        match LogicalPlanOps.mapInputs apply plan with
        | LogicalPlan.GroupBy((LogicalPlan.Union(_, _, true) as input), keys, aggs, None, maintainOrder) as grouped ->
            tryFlatten input keys aggs maintainOrder |> Option.defaultValue grouped
        | other -> other
