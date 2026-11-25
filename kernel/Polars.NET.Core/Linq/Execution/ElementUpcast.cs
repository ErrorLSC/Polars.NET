// The LINQ provider builds generic code at run time (operators over element types known only then): its public entry
// points carry RequiresDynamicCode / RequiresUnreferencedCode, so the analyzer's warnings inside it are silenced here.
#pragma warning disable IL2026, IL2060, IL2067, IL2070, IL2072, IL2075, IL2080, IL2087, IL2090, IL2091, IL3050, IL3051

using System.Collections.Frozen;
using System.Linq.Expressions;
using Kaminari.Sazanami.Marshalling;

namespace Polars.NET.Core.Linq.Execution;

/// <summary>
/// Upcasts of a query's elements: <c>Select(x => (object)x)</c>, <c>Select(x => (IFoo)x)</c>, <c>Cast&lt;object&gt;()</c>.
/// Rows are materialized as the element type the query declares at that point, which after an upcast is a type the rows
/// only conform to (object, an interface, a base class), while the actual values have a more specific type. Such a query
/// is executed as a narrowed query: the upcasts are removed and the element-wise operators after them are rewritten over
/// the narrower type (their parameter <c>x</c> becomes <c>(Declared)y</c>). The narrowed query's elements are then
/// converted (boxed or upcast) to the declared element type, as LINQ to Objects does.
/// </summary>
internal static class ElementUpcast
{
    private static Type? ElementTypeOf(Expression expr) => ClrTypeHelper.TryGetEnumerableElementType(expr.Type);

    /// <summary>
    /// Whether <paramref name="target"/> is a reference supertype of <paramref name="source"/> (a reference upcast or a
    /// boxing conversion), not an identity, a lift to Nullable or a value-changing conversion.
    /// </summary>
    private static bool IsUpcast(Type source, Type target) => source != target && !target.IsValueType && target.IsAssignableFrom(source);

    /// <summary>The lambda under its quotes and conversions, or null when there is none.</summary>
    private static LambdaExpression? CleanLambda(Expression? expr) => expr switch
    {
        LambdaExpression l => l,
        UnaryExpression { NodeType: ExpressionType.Quote or ExpressionType.Convert or ExpressionType.ConvertChecked } u => CleanLambda(u.Operand),
        _ => null,
    };

    /// <summary><paramref name="body"/> with any conversions to supertypes of its operand removed.</summary>
    private static Expression StripBodyUpcast(Expression body) =>
        body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.TypeAs } u && IsUpcast(u.Operand.Type, u.Type)
            ? StripBodyUpcast(u.Operand)
            : body;

    private sealed class ParameterReplacer(ParameterExpression old, Expression replacement) : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node) => ReferenceEquals(node, old) ? replacement : base.VisitParameter(node);
    }

    /// <summary><paramref name="lambda"/> over elements of the narrower type <paramref name="narrow"/>: its element parameter is read as <c>(declared)y</c>.</summary>
    private static LambdaExpression OverNarrowerElement(LambdaExpression lambda, Type narrow)
    {
        var old = lambda.Parameters[0];
        var replacement = Expression.Parameter(narrow, old.Name);
        var body = new ParameterReplacer(old, Expression.Convert(replacement, old.Type)).Visit(lambda.Body);
        return Expression.Lambda(body, [replacement, .. lambda.Parameters.Skip(1)]);
    }

    /// <summary>
    /// Operators that act on each element without depending on its static type beyond their lambdas: they can run over a
    /// narrower element type with their lambdas rewritten.
    /// </summary>
    private static readonly FrozenSet<string> ElementWise = FrozenSet.ToFrozenSet(
    [
        "Where", "OrderBy", "OrderByDescending", "ThenBy", "ThenByDescending", "Take", "Skip", "TakeLast", "SkipLast", "TakeWhile",
        "SkipWhile", "Distinct", "Reverse",
    ]);

    /// <summary>The query rewritten over a narrower element type than it declares, or null when it is not an upcast one.</summary>
    public static Expression? TryStrip(Expression expr)
    {
        if (expr is not MethodCallExpression { Object: null } call || call.Method.DeclaringType != typeof(Queryable)) return null;
        var m = call.Method;
        var args = call.Arguments;

        if (m.Name == "Select" && args.Count == 2 && CleanLambda(args[1]) is { Parameters.Count: 1 } selector)
        {
            var source = args[0];
            // The source narrowed first, then the selector's own upcast removed.
            var (narrowedSource, narrowedSelector) = TryStrip(source) is { } narrowed
                ? (narrowed, OverNarrowerElement(selector, ElementTypeOf(narrowed)!))
                : (source, selector);
            var body = StripBodyUpcast(narrowedSelector.Body);
            if (ReferenceEquals(narrowedSource, source) && ReferenceEquals(body, selector.Body)) return null;
            // Only an upcast of the element: the select disappears.
            if (body is ParameterExpression && ReferenceEquals(body, narrowedSelector.Parameters[0])) return narrowedSource;
            var select = m.GetGenericMethodDefinition().MakeGenericMethod(narrowedSelector.Parameters[0].Type, body.Type);
            return Expression.Call(select, narrowedSource, Expression.Quote(Expression.Lambda(body, narrowedSelector.Parameters)));
        }

        if (m.Name == "Cast" && args.Count == 1)
        {
            var target = m.GetGenericArguments()[0];
            return ElementTypeOf(args[0]) is { } sourceType && (IsUpcast(sourceType, target) || sourceType == target)
                ? TryStrip(args[0]) ?? args[0]
                : null;
        }

        if (ElementWise.Contains(m.Name) && m.IsGenericMethod && args.Count >= 1
            && args.Skip(1).All(a => a.Type == typeof(int) || CleanLambda(a) is { Parameters.Count: 1 }))
        {
            if (TryStrip(args[0]) is not { } narrowed) return null;
            var narrow = ElementTypeOf(narrowed)!;
            Expression[] rewritten = [.. args.Skip(1).Select(a => CleanLambda(a) is { } l ? Expression.Quote(OverNarrowerElement(l, narrow)) : a)];
            Type[] typeArgs = [.. m.GetGenericArguments().Select((t, i) => i == 0 ? narrow : t)];
            return Expression.Call(m.GetGenericMethodDefinition().MakeGenericMethod(typeArgs), [narrowed, .. rewritten]);
        }
        return null;
    }

    /// <summary>
    /// For a terminal that returns one element of its source (First, Single, Last, ElementAt and their OrDefault forms
    /// without a default value) over an upcast source: the same terminal over the narrowed source, whose result is then
    /// converted to the declared one. Null for any other expression. An OrDefault terminal over value-type elements reads
    /// them as nullable, so that an empty source still gives null.
    /// </summary>
    public static Expression? TryStripTerminal(Expression expr)
    {
        if (expr is not MethodCallExpression { Object: null } call
            || call.Arguments.Count < 1
            || call.Method.DeclaringType != typeof(Queryable)
            || !call.Method.IsGenericMethod
            || call.Method.GetGenericArguments().Length != 1
            || call.Method.ReturnType != call.Method.GetGenericArguments()[0]
            || !call.Arguments.Skip(1).All(a => a.Type == typeof(int) || a.Type == typeof(Index)))
            return null;
        var m = call.Method;
        if (TryStrip(call.Arguments[0]) is not { } inner || ElementTypeOf(inner) is not { } rowType || !m.ReturnType.IsAssignableFrom(rowType)) return null;
        var (source, sourceRowType) = (inner, rowType);
        if (m.Name.EndsWith("OrDefault", StringComparison.Ordinal) && rowType.IsValueType && Nullable.GetUnderlyingType(rowType) is null)
        {
            var nullable = typeof(Nullable<>).MakeGenericType(rowType);
            var p = Expression.Parameter(rowType, "x");
            var select = typeof(Queryable).GetMethods()
                .First(mi => mi.Name == "Select" && mi.GetParameters()[1].ParameterType.GetGenericArguments()[0].GetGenericArguments().Length == 2)
                .MakeGenericMethod(rowType, nullable);
            source = Expression.Call(select, inner, Expression.Quote(Expression.Lambda(Expression.Convert(p, nullable), p)));
            sourceRowType = nullable;
        }
        var terminal = m.GetGenericMethodDefinition().MakeGenericMethod(sourceRowType);
        return Expression.Call(terminal, [source, .. call.Arguments.Skip(1)]);
    }
}
