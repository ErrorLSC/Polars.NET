namespace Polars.NET.Linq

open System
open System.Collections.Generic
open System.Linq.Expressions
open System.Reflection
open System.Linq

/// Represents an extracted query parameter with its name, declared type, and runtime value.
[<NoEquality; NoComparison>]
type internal ExtractedParameter =
    { Name: string; Type: Type; Value: obj }

/// Represents the result of partial evaluation and parameter extraction.
[<NoEquality; NoComparison>]
type internal ExtractionResult =
    {
        /// The normalized expression tree where dynamic parameters have been replaced with parameter placeholders (@p0, @p1, ...).
        NormalizedExpression: Expression
        /// The dictionary of extracted parameter values keyed by parameter name.
        Parameters: Dictionary<string, ExtractedParameter>
    }

/// Performs client-side partial evaluation to fold closure references
/// and local computations into ConstantExpressions or extracted query parameters before AST translation.
module internal PartialEvaluator =

    /// Checks if a type is a compiler-generated closure class (e.g. <>c__DisplayClass, FSharpFunc).
    let private isCompilerGenerated (t: Type) : bool =
        if isNull t then
            false
        else
            t.IsDefined(typeof<System.Runtime.CompilerServices.CompilerGeneratedAttribute>, false)
            || t.Name.Contains "DisplayClass"
            || t.Name.Contains "<>c"
            || t.Name.Contains "_Closure$__"

    /// Evaluates a member access, constant, array init, or simple conversion through reflection fast path without compiling a dynamic method.
    let rec internal tryFastEvaluate (expr: Expression) : obj option =
        match expr with
        | null -> Some null
        | :? ConstantExpression as ce -> Some ce.Value
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Quote ->
            tryFastEvaluate ue.Operand
        | :? UnaryExpression as ue when
            (ue.NodeType = ExpressionType.Convert || ue.NodeType = ExpressionType.ConvertChecked)
            && isNull ue.Method ->
            match tryFastEvaluate ue.Operand with
            | Some v when isNull v -> Some null
            | Some v when ue.Type.IsInstanceOfType v -> Some v
            | Some v ->
                let targetType =
                    match Nullable.GetUnderlyingType ue.Type with
                    | null -> ue.Type
                    | u -> u

                try
                    if targetType.IsEnum then
                        Some(Enum.ToObject(targetType, v))
                    else
                        Some(Convert.ChangeType(v, targetType))
                with _ ->
                    None
            | None -> None
        | :? MemberExpression as me ->
            if isNull me.Expression then
                // Static member access (e.g. DateTime.Now or static config field)
                match me.Member with
                | :? FieldInfo as fi -> Some(fi.GetValue(null))
                | :? PropertyInfo as pi when pi.CanRead && pi.GetIndexParameters().Length = 0 -> Some(pi.GetValue(null))
                | _ -> None
            else
                match tryFastEvaluate me.Expression with
                | Some targetVal when not (isNull targetVal) ->
                    match me.Member with
                    | :? FieldInfo as fi -> Some(fi.GetValue(targetVal))
                    | :? PropertyInfo as pi when pi.CanRead && pi.GetIndexParameters().Length = 0 ->
                        Some(pi.GetValue(targetVal))
                    | _ -> None
                | _ -> None
        | :? NewArrayExpression as nae when nae.NodeType = ExpressionType.NewArrayInit ->
            let elemType = nae.Type.GetElementType()
            let arr = Array.CreateInstance(elemType, nae.Expressions.Count)

            let rec fill i =
                if i >= nae.Expressions.Count then
                    Some(box arr)
                else
                    match tryFastEvaluate nae.Expressions.[i] with
                    | Some v ->
                        arr.SetValue(v, i)
                        fill (i + 1)
                    | None -> None

            fill 0
        | _ -> None

    /// Evaluates a closed expression subtree to its boxed value.
    /// Uses reflection fast-path for compiler-generated closures to avoid Expression.Compile overhead.
    let internal evaluateSubtree (expr: Expression) : obj =
        match tryFastEvaluate expr with
        | Some value -> value
        | None ->
            let boxed = Expression.Convert(expr, typeof<obj>)
            Expression.Lambda<Func<obj>>(boxed).Compile().Invoke()

    /// Recursively checks if an expression tree references closure variables, external fields/properties,
    /// or method calls (i.e. dynamic parameters rather than pure inlined literal constants).
    let rec private containsDynamicReference (expr: Expression) : bool =
        match expr with
        | null -> false
        | :? ConstantExpression as ce ->
            if isNull ce.Value then
                false
            else
                isCompilerGenerated (ce.Value.GetType())
        | :? MemberExpression -> true
        | :? MethodCallExpression -> true
        | :? NewExpression as ne -> ne.Arguments |> Seq.exists containsDynamicReference
        | :? MemberInitExpression as mie ->
            containsDynamicReference mie.NewExpression
            || mie.Bindings
               |> Seq.exists (function
                   | :? MemberAssignment as ma -> containsDynamicReference ma.Expression
                   | _ -> false)
        | :? UnaryExpression as ue -> containsDynamicReference ue.Operand
        | :? BinaryExpression as be -> containsDynamicReference be.Left || containsDynamicReference be.Right
        | :? ConditionalExpression as ce ->
            containsDynamicReference ce.Test
            || containsDynamicReference ce.IfTrue
            || containsDynamicReference ce.IfFalse
        | _ -> false

    /// Phase 1: Traverses the AST to nominate all subtrees independent of query parameters.
    type private Nominator(canBeEvaluated: Expression -> bool) =
        inherit ExpressionVisitor()
        let candidates = HashSet<Expression>()
        let mutable cannotBeEvaluated = false

        member _.Nominate(expr: Expression) : HashSet<Expression> =
            candidates.Clear()
            cannotBeEvaluated <- false
            ignore (base.Visit expr)
            candidates

        override this.Visit(node: Expression) =
            if isNull node then
                null
            else
                let saveCannotBeEvaluated = cannotBeEvaluated
                cannotBeEvaluated <- false
                ignore (base.Visit node)

                if not cannotBeEvaluated then
                    if canBeEvaluated node then
                        candidates.Add node |> ignore
                    else
                        cannotBeEvaluated <- true

                cannotBeEvaluated <- cannotBeEvaluated || saveCannotBeEvaluated
                node

        /// Overrides MemberInit visiting: prevents the underlying NewExpression node from being
        /// nominated as an independent candidate when the parent MemberInit depends on query parameters.
        override this.VisitMemberInit(node: MemberInitExpression) =
            // 1. Visit arguments of NewExpression directly so nested closures inside constructor arguments
            // are nominated, without nominating the NewExpression container itself.
            for arg in node.NewExpression.Arguments do
                ignore (this.Visit arg)

            // 2. Visit member bindings
            for binding in node.Bindings do
                match binding with
                | :? MemberAssignment as ma -> ignore (this.Visit ma.Expression)
                | :? MemberMemberBinding as mmb ->
                    for b in mmb.Bindings do
                        match b with
                        | :? MemberAssignment as a -> ignore (this.Visit a.Expression)
                        | _ -> ()
                | :? MemberListBinding as mlb ->
                    for init in mlb.Initializers do
                        for arg in init.Arguments do
                            ignore (this.Visit arg)
                | _ -> ()

            node :> Expression

    /// Phase 2: Rewrites maximal nominated subtrees into constant literals (for legacy eval).
    type private SubtreeReplacer(candidates: HashSet<Expression>) =
        inherit ExpressionVisitor()

        override this.Visit(node: Expression) =
            if isNull node then
                null
            elif candidates.Contains node then
                try
                    let value = evaluateSubtree node
                    Expression.Constant(value, node.Type) :> Expression
                with _ ->
                    // Fall back to original AST if evaluation throws (e.g. null dereference)
                    base.Visit node
            else
                base.Visit node

    /// Phase 2 (Parameterized): Rewrites dynamic nominated subtrees into ParameterExpressions,
    /// and static constant subtrees into ConstantExpressions.
    type private ParameterExtractingReplacer(candidates: HashSet<Expression>) =
        inherit ExpressionVisitor()
        let parameters = Dictionary<string, ExtractedParameter>()
        let mutable counter = 0

        member _.Parameters = parameters

        override this.Visit(node: Expression) =
            if isNull node then
                null
            elif candidates.Contains node then
                try
                    let value = evaluateSubtree node

                    if containsDynamicReference node then
                        let paramName = sprintf "@p%d" counter
                        counter <- counter + 1

                        parameters.[paramName] <-
                            {
                                Name = paramName
                                Type = node.Type
                                Value = value
                            }

                        Expression.Parameter(node.Type, paramName) :> Expression
                    else
                        Expression.Constant(value, node.Type) :> Expression
                with _ ->
                    base.Visit node
            else
                base.Visit node

    let private defaultCanBeEvaluated (node: Expression) : bool =
        node.NodeType <> ExpressionType.Parameter
        && node.NodeType <> ExpressionType.Lambda
        && not (typeof<IQueryable>.IsAssignableFrom node.Type)

    /// Evaluates all closure variables and local computations within the expression tree,
    /// replacing them with ConstantExpression literals.
    let eval (expr: Expression) : Expression =
        if isNull expr then
            null
        else
            let nominator = Nominator defaultCanBeEvaluated
            let candidates = nominator.Nominate expr
            let replacer = SubtreeReplacer candidates
            replacer.Visit expr

    /// Extracts all dynamic closure variables and local computations as parameterized placeholders (@p0, @p1, ...),
    /// leaving pure static constants intact, and returning both the normalized AST and parameter mapping.
    let extract (expr: Expression) : ExtractionResult =
        if isNull expr then
            {
                NormalizedExpression = null
                Parameters = Dictionary<string, ExtractedParameter>()
            }
        else
            let nominator = Nominator defaultCanBeEvaluated
            let candidates = nominator.Nominate expr
            let replacer = ParameterExtractingReplacer candidates
            let normalized = replacer.Visit expr

            {
                NormalizedExpression = normalized
                Parameters = replacer.Parameters
            }

    /// Substitutes extracted query parameters in a normalized AST with concrete ConstantExpressions.
    let substitute (expr: Expression) (paramMap: IReadOnlyDictionary<string, obj>) : Expression =
        if isNull expr || isNull paramMap || paramMap.Count = 0 then
            expr
        else
            let visitor =
                { new ExpressionVisitor() with
                    override _.VisitParameter(p: ParameterExpression) =
                        match paramMap.TryGetValue p.Name with
                        | true, value -> Expression.Constant(value, p.Type) :> Expression
                        | false, _ -> p :> Expression
                }

            visitor.Visit expr

    /// Substitutes extracted query parameters using an ExtractedParameter dictionary.
    let substituteExtracted
        (expr: Expression)
        (paramMap: IReadOnlyDictionary<string, ExtractedParameter>)
        : Expression =
        if isNull expr || isNull paramMap || paramMap.Count = 0 then
            expr
        else
            let visitor =
                { new ExpressionVisitor() with
                    override _.VisitParameter(p: ParameterExpression) =
                        match paramMap.TryGetValue p.Name with
                        | true, param -> Expression.Constant(param.Value, param.Type) :> Expression
                        | false, _ -> p :> Expression
                }

            visitor.Visit expr
