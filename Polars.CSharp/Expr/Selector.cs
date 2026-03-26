using Polars.NET.Core;
#pragma warning disable CS1591 
namespace Polars.CSharp;

/// <summary>
/// Represents a column selector strategy (e.g., All, Exclude).
/// convertable to Expr.
/// </summary>
public class Selector : IDisposable
{
    /// <summary>
    /// The safety handle of Selector
    /// </summary>
    public SelectorHandle Handle { get; private set; }

    internal Selector(SelectorHandle handle)
    {
        Handle = handle;
    }

    internal SelectorHandle CloneHandle() => PolarsWrapper.CloneSelector(Handle);
    
    public Selector Clone() => new(CloneHandle());

    // --- Int ---
    public static Expr operator *(Selector s, int other) => s.ToExpr() * other;
    public static Expr operator +(Selector s, int other) => s.ToExpr() + other;
    public static Expr operator -(Selector s, int other) => s.ToExpr() - other;
    public static Expr operator /(Selector s, int other) => s.ToExpr() / other;
    public static Expr operator %(Selector s, int other) => s.ToExpr() % other;
    public static Expr operator >(Selector s, int other) => s.ToExpr() > other;
    public static Expr operator <(Selector s, int other) => s.ToExpr() < other;
    public static Expr operator >=(Selector s, int other) => s.ToExpr() >= other;
    public static Expr operator <=(Selector s, int other) => s.ToExpr() <= other;
    public static Expr operator ==(Selector s, int other) => s.ToExpr() == other;
    public static Expr operator !=(Selector s, int other) => s.ToExpr() != other;
    public static Expr operator ^(Selector s, int v) => s.ToExpr() ^ v;
    // --- Long ---
    public static Expr operator *(Selector s, long other) => s.ToExpr() * other;
    public static Expr operator +(Selector s, long other) => s.ToExpr() + other;
    public static Expr operator -(Selector s, long other) => s.ToExpr() - other;
    public static Expr operator /(Selector s, long other) => s.ToExpr() / other;
    public static Expr operator %(Selector s, long other) => s.ToExpr() % other;
    public static Expr operator >(Selector s, long other) => s.ToExpr() > other;
    public static Expr operator <(Selector s, long other) => s.ToExpr() < other;
    public static Expr operator >=(Selector s, long other) => s.ToExpr() >= other;
    public static Expr operator <=(Selector s, long other) => s.ToExpr() <= other;
    public static Expr operator ==(Selector s, long other) => s.ToExpr() == other;
    public static Expr operator !=(Selector s, long other) => s.ToExpr() != other;
    public static Expr operator ^(Selector s, long v) => s.ToExpr() ^ v;
    // --- Double ---
    public static Expr operator *(Selector s, double other) => s.ToExpr() * other;
    public static Expr operator +(Selector s, double other) => s.ToExpr() + other;
    public static Expr operator -(Selector s, double other) => s.ToExpr() - other;
    public static Expr operator /(Selector s, double other) => s.ToExpr() / other;
    public static Expr operator %(Selector s, double other) => s.ToExpr() % other;
    public static Expr operator >(Selector s, double other) => s.ToExpr() > other;
    public static Expr operator <(Selector s, double other) => s.ToExpr() < other;
    public static Expr operator >=(Selector s, double other) => s.ToExpr() >= other;
    public static Expr operator <=(Selector s, double other) => s.ToExpr() <= other;
    public static Expr operator ==(Selector s, double other) => s.ToExpr() == other;
    public static Expr operator !=(Selector s, double other) => s.ToExpr() != other;
    // --- Float ---
    public static Expr operator *(Selector s, float other) => s.ToExpr() * other;
    public static Expr operator +(Selector s, float other) => s.ToExpr() + other;
    public static Expr operator -(Selector s, float other) => s.ToExpr() - other;
    public static Expr operator /(Selector s, float other) => s.ToExpr() / other;
    public static Expr operator %(Selector s, float other) => s.ToExpr() % other;
    public static Expr operator >(Selector s, float other) => s.ToExpr() > other;
    public static Expr operator <(Selector s, float other) => s.ToExpr() < other;
    public static Expr operator >=(Selector s, float other) => s.ToExpr() >= other;
    public static Expr operator <=(Selector s, float other) => s.ToExpr() <= other;
    public static Expr operator ==(Selector s, float other) => s.ToExpr() == other;
    public static Expr operator !=(Selector s, float other) => s.ToExpr() != other;

    // --- String ---
    public static Expr operator +(Selector s, string v) => s.ToExpr() + v;
    public static Expr operator ==(Selector s, string v) => s.ToExpr() == v;
    public static Expr operator !=(Selector s, string v) => s.ToExpr() != v;

    //Boolean
    public static Expr operator &(Selector s, bool v) => s.ToExpr() & v;
    public static Expr operator |(Selector s, bool v) => s.ToExpr() | v;
    public static Expr operator ^(Selector s, bool v) => s.ToExpr() ^ v;

    // Reverse Ops (int,double,float string * Selector)
    public static Expr operator *(int other, Selector s) => Polars.Lit(other) * s.ToExpr();
    public static Expr operator +(int other, Selector s) => Polars.Lit(other) + s.ToExpr();
    public static Expr operator -(int other, Selector s) => Polars.Lit(other) - s.ToExpr();
    public static Expr operator /(int other, Selector s) => Polars.Lit(other) / s.ToExpr();
    public static Expr operator %(int other, Selector s) => Polars.Lit(other) * s.ToExpr();
    public static Expr operator >(int other, Selector s) => Polars.Lit(other) > s.ToExpr();
    public static Expr operator <(int other, Selector s) => Polars.Lit(other) < s.ToExpr();
    public static Expr operator >=(int other, Selector s) => Polars.Lit(other) >= s.ToExpr();
    public static Expr operator <=(int other, Selector s) => Polars.Lit(other) <= s.ToExpr();
    public static Expr operator ==(int other, Selector s) => Polars.Lit(other) == s.ToExpr();
    public static Expr operator !=(int other, Selector s) => Polars.Lit(other) != s.ToExpr();
    public static Expr operator ^(int v, Selector s) => v ^ s.ToExpr();

    public static Expr operator *(long other, Selector s) => Polars.Lit(other) * s.ToExpr();
    public static Expr operator +(long other, Selector s) => Polars.Lit(other) + s.ToExpr();
    public static Expr operator -(long other, Selector s) => Polars.Lit(other) - s.ToExpr();
    public static Expr operator /(long other, Selector s) => Polars.Lit(other) / s.ToExpr();
    public static Expr operator %(long other, Selector s) => Polars.Lit(other) * s.ToExpr();
    public static Expr operator >(long other, Selector s) => Polars.Lit(other) > s.ToExpr();
    public static Expr operator <(long other, Selector s) => Polars.Lit(other) < s.ToExpr();
    public static Expr operator >=(long other, Selector s) => Polars.Lit(other) >= s.ToExpr();
    public static Expr operator <=(long other, Selector s) => Polars.Lit(other) <= s.ToExpr();
    public static Expr operator ==(long other, Selector s) => Polars.Lit(other) == s.ToExpr();
    public static Expr operator !=(long other, Selector s) => Polars.Lit(other) != s.ToExpr();
    public static Expr operator ^(long v, Selector s) => v ^ s.ToExpr();

    public static Expr operator *(double other, Selector s) => Polars.Lit(other) * s.ToExpr();
    public static Expr operator +(double other, Selector s) => Polars.Lit(other) + s.ToExpr();
    public static Expr operator -(double other, Selector s) => Polars.Lit(other) - s.ToExpr();
    public static Expr operator /(double other, Selector s) => Polars.Lit(other) / s.ToExpr();
    public static Expr operator %(double other, Selector s) => Polars.Lit(other) * s.ToExpr();
    public static Expr operator >(double other, Selector s) => Polars.Lit(other) > s.ToExpr();
    public static Expr operator <(double other, Selector s) => Polars.Lit(other) < s.ToExpr();
    public static Expr operator >=(double other, Selector s) => Polars.Lit(other) >= s.ToExpr();
    public static Expr operator <=(double other, Selector s) => Polars.Lit(other) <= s.ToExpr();
    public static Expr operator ==(double other, Selector s) => Polars.Lit(other) == s.ToExpr();
    public static Expr operator !=(double other, Selector s) => Polars.Lit(other) != s.ToExpr();

    public static Expr operator *(float other, Selector s) => Polars.Lit(other) * s.ToExpr();
    public static Expr operator +(float other, Selector s) => Polars.Lit(other) + s.ToExpr();
    public static Expr operator -(float other, Selector s) => Polars.Lit(other) - s.ToExpr();
    public static Expr operator /(float other, Selector s) => Polars.Lit(other) / s.ToExpr();
    public static Expr operator %(float other, Selector s) => Polars.Lit(other) * s.ToExpr();
    public static Expr operator >(float other, Selector s) => Polars.Lit(other) > s.ToExpr();
    public static Expr operator <(float other, Selector s) => Polars.Lit(other) < s.ToExpr();
    public static Expr operator >=(float other, Selector s) => Polars.Lit(other) >= s.ToExpr();
    public static Expr operator <=(float other, Selector s) => Polars.Lit(other) <= s.ToExpr();
    public static Expr operator ==(float other, Selector s) => Polars.Lit(other) == s.ToExpr();
    public static Expr operator !=(float other, Selector s) => Polars.Lit(other) != s.ToExpr();

    public static Expr operator +(string other, Selector s) => Polars.Lit(other) + s.ToExpr();
    public static Expr operator ==(string other, Selector s) => Polars.Lit(other) == s.ToExpr();
    public static Expr operator !=(string other, Selector s) => Polars.Lit(other) != s.ToExpr();

    public static Expr operator &(bool v, Selector s) => v & s.ToExpr();
    public static Expr operator |(bool v, Selector s) => v | s.ToExpr();
    public static Expr operator ^(bool v, Selector s) => v ^ s.ToExpr();

    /// <summary>
    /// Exclude columns by name.
    /// </summary>
    public Selector Exclude(params string[] names)
    {
        var newHandle = PolarsWrapper.SelectorExclude(Handle, names);
        return new Selector(newHandle);
    }
    /// <summary>
    /// Select columns by name.
    /// </summary>
    public static Selector Cols(params string[] names)
    {
        var newHandle = PolarsWrapper.SelectorCols(names);
        return new Selector(newHandle);
    }
    /// <summary>
    /// Select columns by name.
    /// </summary>
    public static Selector Col(params string[] names)
        => Cols(names);
    /// <summary>
    /// Convert the selector to an Expression.
    /// This allows using selectors inside Select(), WithColumns(), etc.
    /// </summary>
    public Expr ToExpr()
    {
        var exprHandle = PolarsWrapper.SelectorToExpr(Handle);
        return new Expr(exprHandle);
    }

    // --- Set Operations ( &, |, !, - ) ---

    public static Selector operator &(Selector left, Selector right)
        => new(PolarsWrapper.SelectorAnd(left.CloneHandle(), right.CloneHandle()));

    public static Selector operator |(Selector left, Selector right)
        => new(PolarsWrapper.SelectorOr(left.CloneHandle(), right.CloneHandle()));

    public static Selector operator !(Selector s)
        => new(PolarsWrapper.SelectorNot(s.CloneHandle()));

    // Difference: A - B
    public static Selector operator -(Selector left, Selector right) => left & (!right);
    
    /// <summary>
    /// Implicitly convert Selector to Expr.
    /// This is the magic that allows df.Select(Polars.All())
    /// </summary>
    public static implicit operator Expr(Selector selector) => selector.ToExpr();
    public override bool Equals(object? obj) => base.Equals(obj);

    public override int GetHashCode() => base.GetHashCode();
    /// <summary>
    /// Dispose unused Selector Handle
    /// </summary>
    public void Dispose()
    {
        Handle?.Dispose();
        GC.SuppressFinalize(this); 
    }
}
