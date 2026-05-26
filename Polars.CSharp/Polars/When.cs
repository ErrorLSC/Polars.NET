#pragma warning disable 1591
namespace Polars.CSharp;

public readonly partial struct Polars
{
    /// <summary>
    /// Start a `when-then-otherwise` expression.
    /// </summary>
    public static When When(IntoExprColumn condition)
        => new(condition.Consume());
}

public class When
{
    internal readonly Expr _condition;

    internal When(Expr condition)
    {
        _condition = condition;
    }

    public Then Then(IntoExprColumn statement)
    {
        return new Then(_condition, statement.Consume());
    }
}

public class Then
{
    internal readonly List<Expr> _conditions = [];
    internal readonly List<Expr> _statements = [];

    internal Then(Expr condition, Expr statement)
    {
        _conditions.Add(condition);
        _statements.Add(statement);
    }

    public ChainedWhen When(IntoExprColumn condition)
        => new (this, condition.Consume());
    

    /// <summary>
    /// Define a default for the `when-then-otherwise` expression.
    /// </summary>
    public Expr Otherwise(IntoExprColumn statement)
    {
        Expr otherwiseExpr = statement.Consume();

        for (int i = _conditions.Count - 1; i >= 0; i--)
        {
            var cond = _conditions[i];
            var truthy = _statements[i];

            otherwiseExpr = Expr.Ternary(cond, truthy, otherwiseExpr);
        }

        return otherwiseExpr;
    }
}

public class ChainedWhen
{
    internal readonly Then _parent;
    internal readonly Expr _condition;

    internal ChainedWhen(Then parent, Expr condition)
    {
        _parent = parent;
        _condition = condition;
    }

    public Then Then(IntoExprColumn statement)
    {
        _parent._conditions.Add(_condition);
        _parent._statements.Add(statement.Consume());
        
        return _parent;
    }
}