using Pl = Polars.CSharp.Polars;
namespace Polars.CSharp;

/// <summary>
/// Wrapper for Binary operations on a Series.
/// </summary>
public readonly struct SeriesBinaryOps
{
    private readonly Series _series;
    internal SeriesBinaryOps(Series series) { _series = series; }
    private Series Apply(Func<Expr, Expr> op) 
        => _series.ApplyExpr(op(Pl.Col(_series.Name)));
    /// <inheritdoc cref="BinaryOps.Size(SizeUnit)"/>
    public Series Size(SizeUnit unit = SizeUnit.Bytes) => Apply(e => e.Bin.Size(unit));
    /// <inheritdoc cref="BinaryOps.Contains"/>
    public Series Contains(BytesOrExpr literal) => Apply(e => e.Bin.Contains(literal));
    /// <inheritdoc cref="BinaryOps.StartsWith"/>
    public Series StartsWith(BytesOrExpr prefix) => Apply(e => e.Bin.StartsWith(prefix));
    /// <inheritdoc cref="BinaryOps.EndsWith"/>
    public Series EndsWith(BytesOrExpr suffix) => Apply(e => e.Bin.EndsWith(suffix));
    /// <inheritdoc cref="BinaryOps.Head"/>
    public Series Head(int n=5) => Apply(e => e.Bin.Head(n));
    /// <inheritdoc cref="BinaryOps.Tail"/>
    public Series Tail(int n=5) => Apply(e => e.Bin.Tail(n));
    /// <inheritdoc cref="BinaryOps.Encode"/>
    public Series Encode(TransferEncoding encoding) => Apply(e => e.Bin.Encode(encoding));
    /// <inheritdoc cref="BinaryOps.Decode"/>
    public Series Decode(TransferEncoding encoding,bool strict=true) => Apply(e => e.Bin.Decode(encoding,strict));
    /// <inheritdoc cref="BinaryOps.Reinterpret"/>
    public Series Reinterpret(IntoDataTypeExpr dtype, Endianness endianness = Endianness.Little) 
        => Apply(e => e.Bin.Reinterpret(dtype,endianness));
    /// <inheritdoc cref="BinaryOps.Reinterpret"/>
    public Series Slice(int offset, int? length=null) 
        => Apply(e => e.Bin.Slice(offset,length));
}