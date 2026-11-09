namespace Polars.NET.Linq

open System
open System.Linq
open System.Linq.Expressions
open Kaminari.Sazanami.Helpers
open Kaminari.Samidare

/// Upcasts of a query's elements: `Select(x => (object)x)`, `Select(x => (IFoo)x)`, `Cast<object>()`.
///
/// Rows are materialized as the element type the query declares at that point, which after an upcast is a type
/// the rows only conform to (object, an interface, a base class), while the actual values have a more specific
/// type. Such a query is executed as a narrowed query: the upcasts are removed and the element-wise operators
/// after them are rewritten over the narrower type (their parameter `x` becomes `(Declared)y`). The narrowed
/// query's elements are then converted (boxed or upcast) to the declared element type, as LINQ to Objects does.
module internal ElementUpcast =

    let private elementTypeOf (expr: Expression) =
        ClrTypeHelper.TryGetEnumerableElementType expr.Type |> Option.ofObj

    /// Whether `target` is a reference supertype of `source` (a reference upcast or a boxing conversion), not an
    /// identity, a lift to Nullable or a value-changing conversion.
    let private isUpcast (source: Type) (target: Type) =
        source <> target && not target.IsValueType && target.IsAssignableFrom source

    /// `body` with any conversions to supertypes of its operand removed.
    let rec private stripBodyUpcast (body: Expression) =
        match body with
        | :? UnaryExpression as u when
            (u.NodeType = ExpressionType.Convert || u.NodeType = ExpressionType.TypeAs) && isUpcast u.Operand.Type u.Type
            ->
            stripBodyUpcast u.Operand
        | other -> other

    /// `lambda` over elements of the narrower type `narrow`: its element parameter is read as `(declared)y`.
    let private overNarrowerElement (lambda: LambdaExpression) (narrow: Type) : LambdaExpression =
        let old = lambda.Parameters.[0]
        let replacement = Expression.Parameter(narrow, old.Name)
        let asDeclared = Expression.Convert(replacement, old.Type) :> Expression
        let body =
            { new ExpressionVisitor() with
                override _.VisitParameter(p) = if obj.ReferenceEquals(p, old) then asDeclared else base.VisitParameter p
            }.Visit(lambda.Body)
        Expression.Lambda(body, replacement :: (lambda.Parameters |> Seq.skip 1 |> List.ofSeq))

    /// Operators that act on each element without depending on its static type beyond their lambdas: they can run
    /// over a narrower element type with their lambdas rewritten.
    let private elementWise =
        set [ "Where"; "OrderBy"; "OrderByDescending"; "ThenBy"; "ThenByDescending"; "Take"; "Skip"; "TakeLast"
              "SkipLast"; "TakeWhile"; "SkipWhile"; "Distinct"; "Reverse" ]

    /// The query rewritten over a narrower element type than it declares, or None when it is not an upcast one.
    let rec tryStrip (expr: Expression) : Expression option =
        let continueFrom (inner: Expression) = tryStrip inner |> Option.defaultValue inner |> Some

        match expr with
        | MethodCall(m, null, [ source; CleanLambda selector ]) when
            m.DeclaringType = typeof<Queryable> && m.Name = "Select" && selector.Parameters.Count = 1
            ->
            // The source narrowed first, then the selector's own upcast removed.
            let source', selector' =
                match tryStrip source with
                | Some narrowed -> narrowed, overNarrowerElement selector (elementTypeOf narrowed).Value
                | None -> source, selector

            let body = stripBodyUpcast selector'.Body
            if obj.ReferenceEquals(source', source) && obj.ReferenceEquals(body, selector.Body) then
                None
            elif body :? ParameterExpression && obj.ReferenceEquals(body, selector'.Parameters.[0]) then
                // Only an upcast of the element: the select disappears.
                Some source'
            else
                let select = m.GetGenericMethodDefinition().MakeGenericMethod(selector'.Parameters.[0].Type, body.Type)
                Some(Expression.Call(select, source', Expression.Quote(Expression.Lambda(body, selector'.Parameters))))

        | MethodCall(m, null, [ source ]) when m.DeclaringType = typeof<Queryable> && m.Name = "Cast" ->
            match elementTypeOf source with
            | Some sourceType when isUpcast sourceType (m.GetGenericArguments().[0]) || sourceType = m.GetGenericArguments().[0] ->
                continueFrom source
            | _ -> None

        | MethodCall(m, null, source :: args) when
            m.DeclaringType = typeof<Queryable>
            && elementWise.Contains m.Name
            && m.IsGenericMethod
            && args |> List.forall (fun a -> a.Type = typeof<int> || (match a with CleanLambda l -> l.Parameters.Count = 1 | _ -> false))
            ->
            tryStrip source
            |> Option.map (fun narrowed ->
                let narrow = (elementTypeOf narrowed).Value
                let args' =
                    args
                    |> List.map (fun a ->
                        match a with
                        | CleanLambda l -> Expression.Quote(overNarrowerElement l narrow) :> Expression
                        | other -> other)
                let typeArgs =
                    m.GetGenericArguments()
                    |> Array.mapi (fun i t -> if i = 0 then narrow else t)
                Expression.Call(m.GetGenericMethodDefinition().MakeGenericMethod typeArgs, narrowed :: args') :> Expression)

        | _ -> None

    /// For a terminal that returns one element of its source (First, Single, Last, ElementAt and their OrDefault
    /// forms without a default value) over an upcast source: the same terminal over the narrowed source, whose
    /// result is then converted to the declared one. None for any other expression. An OrDefault terminal over
    /// value-type elements reads them as nullable, so that an empty source still gives null.
    let tryStripTerminal (expr: Expression) : Expression option =
        match expr with
        | MethodCall(m, null, source :: rest) when
            m.DeclaringType = typeof<Queryable>
            && m.IsGenericMethod
            && m.GetGenericArguments().Length = 1
            && m.ReturnType = m.GetGenericArguments().[0]
            && (rest |> List.forall (fun a -> a.Type = typeof<int> || a.Type = typeof<Index>))
            ->
            tryStrip source
            |> Option.bind (fun inner ->
                elementTypeOf inner
                |> Option.filter (fun rowType -> m.ReturnType.IsAssignableFrom rowType)
                |> Option.map (fun rowType ->
                    let inner', rowType' =
                        if m.Name.EndsWith("OrDefault", StringComparison.Ordinal)
                           && rowType.IsValueType
                           && isNull (Nullable.GetUnderlyingType rowType) then
                            let nullable = typedefof<Nullable<_>>.MakeGenericType rowType
                            let p = Expression.Parameter(rowType, "x")
                            let select =
                                typeof<Queryable>.GetMethods()
                                |> Array.find (fun mi ->
                                    mi.Name = "Select"
                                    && (mi.GetParameters().[1].ParameterType.GetGenericArguments().[0].GetGenericArguments().Length = 2))
                                |> fun mi -> mi.MakeGenericMethod(rowType, nullable)
                            let lambda = Expression.Lambda(Expression.Convert(p, nullable), p)
                            Expression.Call(select, inner, Expression.Quote lambda) :> Expression, nullable
                        else
                            inner, rowType
                    let terminal = m.GetGenericMethodDefinition().MakeGenericMethod rowType'
                    Expression.Call(terminal, inner' :: rest) :> Expression))
        | _ -> None
