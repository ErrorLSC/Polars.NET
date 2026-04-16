#pragma warning disable 1591
using System.Text;
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

/// <summary>
/// Operations on binary columns. Access via <see cref="Expr.Bin"/>.
/// </summary>
public readonly struct BinaryOps
{
    private readonly Expr _expr;
    internal BinaryOps(Expr expr) { _expr = expr; }

    private Expr Wrap(Func<ExprHandle, ExprHandle> op)
        => new(op(_expr.CloneHandle()));

    // Constant scaling factors for high performance
    private const double BytesInKb = 1024.0;
    private const double BytesInMb = 1048576.0;         // 1024^2
    private const double BytesInGb = 1073741824.0;      // 1024^3
    private const double BytesInTb = 1099511627776.0;   // 1024^4
    /// <summary>
    /// Computes the size of the binary data in the specified unit.
    /// The default unit is Bytes. Scaling to other units will result in a Float64 expression.
    /// </summary>
    /// <param name="unit">The unit to scale the binary size to.</param>
    /// <returns>A new Polars Expression representing the scaled size.</returns>
    public Expr Size(SizeUnit unit = SizeUnit.Bytes)
    {
        // 1. Get the base expression (returns size in raw bytes as UInt32/UInt64)
        Expr byteSizeExpr = Wrap(PolarsWrapper.BinSizeBytes);

        // 2. Apply scaling logic directly into the Polars AST using C# 8.0+ switch expression
        return unit switch
        {
            SizeUnit.Bytes     => byteSizeExpr,
            SizeUnit.Kilobytes => byteSizeExpr / BytesInKb,
            SizeUnit.Megabytes => byteSizeExpr / BytesInMb,
            SizeUnit.Gigabytes => byteSizeExpr / BytesInGb,
            SizeUnit.Terabytes => byteSizeExpr / BytesInTb,
            _ => throw new ArgumentOutOfRangeException(nameof(unit), unit, "Unsupported SizeUnit mapping.")
        };
    }
    /// <summary>
    /// Check if binaries in Series contain a binary substring.
    /// </summary>
    /// <param name="literal">The binary substring to look for</param>
    /// <returns>Expression/Series of data type Boolean.</returns>
    public Expr Contains(BytesOrExpr literal) => new(PolarsWrapper.BinContains(_expr.CloneHandle(),literal.Expression.CloneHandle()));
    /// <summary>
    /// Check if values start with a binary substring.
    /// </summary>
    /// <param name="prefix">Prefix substring.</param>
    /// <returns>Expression/Series of data type Boolean.</returns>
    public Expr StartsWith(BytesOrExpr prefix) => new(PolarsWrapper.BinStartsWith(_expr.CloneHandle(),prefix.Expression.CloneHandle()));
    /// <summary>
    /// Check if values end with a binary substring.
    /// </summary>
    /// <param name="suffix">Suffix substring.</param>
    /// <returns>Expression/Series of data type Boolean.</returns>
    public Expr EndsWith(BytesOrExpr suffix) => new(PolarsWrapper.BinEndsWith(_expr.CloneHandle(),suffix.Expression.CloneHandle()));
    /// <summary>
    /// Take the first n bytes of the binary values.
    /// </summary>
    /// <param name="n">Length of the slice (integer or expression). Negative indexing is supported</param>
    /// <returns>Expression/Series of data type Binary</returns>
    public Expr Head(IntOrExpr? n=null)
    {
        Expr nExpr = n?.Expression ?? Pl.Lit(5);
        return new(PolarsWrapper.BinHead(_expr.CloneHandle(),nExpr.CloneHandle()));  
    } 
    /// <summary>
    /// Take the last n bytes of the binary values.
    /// </summary>
    /// <param name="n">Length of the slice (integer or expression). Negative indexing is supported</param>
    /// <returns>Expression/Series of data type Binary</returns>
    public Expr Tail(IntOrExpr? n=null)
    {
        Expr nExpr = n?.Expression ?? Pl.Lit(5);
        return new(PolarsWrapper.BinTail(_expr.CloneHandle(),nExpr.CloneHandle()));  
    } 
    /// <summary>
    /// Encode a value using the provided encoding.
    /// </summary>
    /// <param name="encoding">The encoding to use.</param>
    /// <returns>Expression/Series of data type Binary.</returns>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    public Expr Encode(TransferEncoding encoding) => encoding switch
    {
        TransferEncoding.Base64 => new Expr(PolarsWrapper.BinBase64Encode(_expr.CloneHandle())),
        TransferEncoding.Hex    => new Expr(PolarsWrapper.BinHexEncode(_expr.CloneHandle())),
        
        _ => throw new ArgumentOutOfRangeException(nameof(encoding), encoding, "Unsupported transfer encoding.")
    };
    /// <summary>
    /// Decode values using the provided encoding.
    /// </summary>
    /// <param name="encoding">The encoding to use.</param>
    /// <param name="strict">Raise an error if the underlying value cannot be decoded, otherwise mask out with a null value.</param>
    /// <returns>Expression/Series of data type Binary.</returns>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    public Expr Decode(TransferEncoding encoding,bool strict=true) => encoding switch
    {
        TransferEncoding.Base64 => new Expr(PolarsWrapper.BinBase64Decode(_expr.CloneHandle(),strict)),
        TransferEncoding.Hex    => new Expr(PolarsWrapper.BinHexDecode(_expr.CloneHandle(),strict)),
        
        _ => throw new ArgumentOutOfRangeException(nameof(encoding), encoding, "Unsupported transfer encoding.")
    };
    /// <summary>
    /// Interpret bytes as another type.Supported types are numerical or temporal dtypes, or an Array of these dtypes.
    /// </summary>
    /// <param name="dtype">Which type to interpret binary column into.</param>
    /// <param name="endianness">Which endianness to use when interpreting bytes, by default “little”.</param>
    /// <returns>Expression/Series of data type dtype. Note that rows of the binary array where the length does not match the size in bytes of the output array (number of items * byte size of item) will become NULL.</returns>
    public Expr Reinterpret(IntoDataTypeExpr dtype, Endianness endianness = Endianness.Little)
    {
        bool isLittle = endianness == Endianness.Little;
        
        return new(PolarsWrapper.BinReinterpret(_expr.CloneHandle(),dtype.Consume().Handle,isLittle));  
    }
    /// <summary>
    /// Slice the binary values.
    /// </summary>
    /// <param name="offset">Start index. Negative indexing is supported.</param>
    /// <param name="length">Length of the slice. If set to None (default), the slice is taken to the end of the value.</param>
    /// <returns>Expression/Series of data type Binary.</returns>
    public Expr Slice(IntOrExpr offset,IntOrExpr? length)
    {
        Expr offsetExpr = offset.Expression;

        Expr lengthExpr = length?.Expression ?? Pl.LitNull();

        return new(PolarsWrapper.BinSlice(_expr.CloneHandle(),offsetExpr.CloneHandle(),lengthExpr.CloneHandle()));
    }
    
}

/// <summary>
/// Represents either a byte array literal or a Polars Expression.
/// Used extensively in the Binary namespace to simulate Into[Expr}.
/// </summary>
public readonly struct BytesOrExpr
{
    internal readonly Expr Expression;

    private BytesOrExpr(Expr expr) 
    {
        Expression = expr;
    }

    public static implicit operator BytesOrExpr(byte[] value) 
        => new(Pl.LitBinary(value));

    public static implicit operator BytesOrExpr(Expr expr) 
        => new(expr);

    public static implicit operator BytesOrExpr(string value) 
        => new(Pl.Lit(Encoding.UTF8.GetBytes(value)));

    public static implicit operator BytesOrExpr(Series series) 
        => new(Pl.Lit(series));

}