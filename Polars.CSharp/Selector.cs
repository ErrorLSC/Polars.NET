using Polars.NET.Core;
#pragma warning disable CS1591 // 缺少对公共可见类型或成员的 XML 注释
namespace Polars.CSharp
{
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

        internal SelectorHandle CloneHandle()
        {
            return PolarsWrapper.CloneSelector(Handle);
        }
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

        // --- String (常用) ---
        // 字符串只支持 + (拼接) 和 比较
        public static Expr operator +(Selector s, string v) => s.ToExpr() + v;
        public static Expr operator ==(Selector s, string v) => s.ToExpr() == v;
        public static Expr operator !=(Selector s, string v) => s.ToExpr() != v;

        //Boolean
        public static Expr operator &(Selector s, bool v) => s.ToExpr() & v;
        public static Expr operator |(Selector s, bool v) => s.ToExpr() | v;
        public static Expr operator ^(Selector s, bool v) => s.ToExpr() ^ v;

        // 反向操作 (int,double,float string * Selector)
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
            // 注意：Wrapper 里的 SelectorExclude 会调用 TransferOwnership
            // 这意味着当前的 Handle 被 Rust 拿走了，我们得到一个新的 Handle
            // 所以这里不仅是 fluent API，还是 ownership passing
            var newHandle = PolarsWrapper.SelectorExclude(Handle, names);
            return new Selector(newHandle);
        }

        /// <summary>
        /// Convert the selector to an Expression.
        /// This allows using selectors inside Select(), WithColumns(), etc.
        /// </summary>
        public Expr ToExpr()
        {
            // Wrapper 里的 SelectorToExpr 也会 TransferOwnership
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
        public static Selector operator -(Selector left, Selector right)
        {
            // 实现为: Left & (!Right)
            return left & (!right);
        }
        /// <summary>
        /// Implicitly convert Selector to Expr.
        /// This is the magic that allows df.Select(Polars.All())
        /// </summary>
        public static implicit operator Expr(Selector selector) => selector.ToExpr();
        public override bool Equals(object? obj)
        {
            // 保持默认的引用比较 (Reference Equality)
            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            // 保持默认的 HashCode
            return base.GetHashCode();
        }
        /// <summary>
        /// Dispose unused Selector Handle
        /// </summary>
        public void Dispose()
        {
            Handle?.Dispose();
        }
    }
}