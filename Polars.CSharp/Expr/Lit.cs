using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// A Polars Expr
/// </summary>
public partial class Expr : IDisposable
{
    internal static Expr MakeLit(object val)
    {
        if (val is Expr e) return e;

        if (val is Selector s) return s.ToExpr();

        return val switch
        {
            // --- Integer ---
            int i => new Expr(PolarsWrapper.Lit(i)),
            uint ui => new Expr(PolarsWrapper.Lit(ui)),
            long l => new Expr(PolarsWrapper.Lit(l)),
            ulong ul => new Expr(PolarsWrapper.Lit(ul)),
            short sh => new Expr(PolarsWrapper.Lit(sh)),
            ushort ush => new Expr(PolarsWrapper.Lit(ush)),
            byte by => new Expr(PolarsWrapper.Lit(by)),
            sbyte sb => new Expr(PolarsWrapper.Lit(sb)),
            Int128 i128 => new Expr(PolarsWrapper.Lit(i128)),

            // --- Float ---
            double d => new Expr(PolarsWrapper.Lit(d)),
            float f => new Expr(PolarsWrapper.Lit(f)),
            Half h => new Expr(PolarsWrapper.Lit(h)),

            // --- String and Boolean ---
            string str => new Expr(PolarsWrapper.Lit(str)),
            bool b => new Expr(PolarsWrapper.Lit(b)),

            // --- Time ---
            DateTime dt => new Expr(PolarsWrapper.Lit(dt)),
            DateTimeOffset dtos => new Expr(PolarsWrapper.Lit(dtos)),
            DateOnly date => new Expr(PolarsWrapper.Lit(date)),
            TimeOnly time => new Expr(PolarsWrapper.Lit(time)),
            TimeSpan ts => new Expr(PolarsWrapper.Lit(ts)),

            // --- Decimal ---
            decimal dec => Polars.Lit(dec),

            // --- Null ---
            null => new Expr(PolarsWrapper.LitNull()),

            // --- Exception ---
            _ => throw new NotSupportedException($"Unsupported literal type: {val.GetType().Name}")
        };
    }

}