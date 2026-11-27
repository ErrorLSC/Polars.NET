using Polars.NET.Core.Wrappers;

namespace Polars.CSharp;

/// <summary>
/// The casts a scan may apply when a file's types differ from the schema it is read with (the given schema, or the
/// first file's). Every cast is off by default: a mismatch raises.
/// <para>Unstable in the engine: the set of casts may change in a minor release.</para>
/// </summary>
public sealed record ScanCastOptions
{
    /// <summary>Casts from integer columns.</summary>
    public IntegerCasts Integers { get; init; }

    /// <summary>Casts from float columns.</summary>
    public FloatCasts Floats { get; init; }

    /// <summary>Casts from datetime columns.</summary>
    public DatetimeCasts Datetimes { get; init; }

    /// <summary>Struct fields of the schema that a file's struct lacks are filled with nulls (instead of raising).</summary>
    public bool InsertMissingStructFields { get; init; }

    /// <summary>Struct fields of a file that the schema does not name are dropped (instead of raising).</summary>
    public bool IgnoreExtraStructFields { get; init; }

    /// <summary>A categorical column read where the schema says String is cast to String.</summary>
    public bool CategoricalToString { get; init; }

    internal PlScanCastFlags ToNative()
    {
        var flags = PlScanCastFlags.None;
        if (Integers.HasFlag(IntegerCasts.Upcast)) flags |= PlScanCastFlags.IntegerUpcast;
        if (Integers.HasFlag(IntegerCasts.ToFloat)) flags |= PlScanCastFlags.IntegerToFloat;
        if (Floats.HasFlag(FloatCasts.Upcast)) flags |= PlScanCastFlags.FloatUpcast;
        if (Floats.HasFlag(FloatCasts.Downcast)) flags |= PlScanCastFlags.FloatDowncast;
        if (Datetimes.HasFlag(DatetimeCasts.NanosecondDowncast)) flags |= PlScanCastFlags.DatetimeNanosecondDowncast;
        if (Datetimes.HasFlag(DatetimeCasts.MicrosecondDowncast)) flags |= PlScanCastFlags.DatetimeMicrosecondDowncast;
        if (Datetimes.HasFlag(DatetimeCasts.MillisecondUpcast)) flags |= PlScanCastFlags.DatetimeMillisecondUpcast;
        if (Datetimes.HasFlag(DatetimeCasts.MicrosecondUpcast)) flags |= PlScanCastFlags.DatetimeMicrosecondUpcast;
        if (Datetimes.HasFlag(DatetimeCasts.ConvertTimeZone)) flags |= PlScanCastFlags.DatetimeConvertTimeZone;
        if (InsertMissingStructFields) flags |= PlScanCastFlags.InsertMissingStructFields;
        if (IgnoreExtraStructFields) flags |= PlScanCastFlags.IgnoreExtraStructFields;
        if (CategoricalToString) flags |= PlScanCastFlags.CategoricalToString;
        return flags;
    }
}

/// <summary>Casts a scan may apply to integer columns.</summary>
[Flags]
public enum IntegerCasts
{
    /// <summary>A type mismatch raises.</summary>
    None = 0,
    /// <summary>Lossless widening to a larger integer type (and widening a decimal's precision at the same scale).</summary>
    Upcast = 1,
    /// <summary>Integers may be read as floats.</summary>
    ToFloat = 2,
}

/// <summary>Casts a scan may apply to float columns.</summary>
[Flags]
public enum FloatCasts
{
    /// <summary>A type mismatch raises.</summary>
    None = 0,
    /// <summary><c>Float32</c> may be read as <c>Float64</c>.</summary>
    Upcast = 1,
    /// <summary><c>Float64</c> may be read as <c>Float32</c>.</summary>
    Downcast = 2,
}

/// <summary>Casts a scan may apply to datetime columns.</summary>
[Flags]
public enum DatetimeCasts
{
    /// <summary>A type mismatch raises.</summary>
    None = 0,
    /// <summary>Nanoseconds may be cut to any coarser unit (as Spark's INT96 timestamps need).</summary>
    NanosecondDowncast = 1,
    /// <summary>Microseconds may be cut to milliseconds.</summary>
    MicrosecondDowncast = 2,
    /// <summary>Microseconds may be read as nanoseconds.</summary>
    MicrosecondUpcast = 4,
    /// <summary>Milliseconds may be read as microseconds or nanoseconds.</summary>
    MillisecondUpcast = 8,
    /// <summary>Datetimes may be converted to the schema's time zone.</summary>
    ConvertTimeZone = 16,
    /// <summary>Every coarsening: <see cref="NanosecondDowncast"/> and <see cref="MicrosecondDowncast"/>.</summary>
    Downcast = NanosecondDowncast | MicrosecondDowncast,
    /// <summary>Every refinement: <see cref="MillisecondUpcast"/> and <see cref="MicrosecondUpcast"/>.</summary>
    Upcast = MillisecondUpcast | MicrosecondUpcast,
}
