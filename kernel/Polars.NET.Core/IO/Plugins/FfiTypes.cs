using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow.C;
using CArrowArrayStream = Kaminari.Sazanami.Arrow.Interop.CArrowArrayStream;

namespace Polars.NET.Core.IO.Plugins;

// The repr(C) types of polars-io-ext-ffi's common.rs, file_reader_v1.rs and lazyframe_resolver_v1.rs. A `repr(C, u8)`
// enum (FfiOption<T>, FfiResult<T>) is a tag byte and a union aligned to its largest member: C# has no generic explicit
// layout, so each instantiation the contracts use has its own struct. pl_io_plugin_layout reports the engine's sizes
// and offsets; IoPluginLayout checks these against them.

/// <summary>Borrowed UTF-8 text, valid for the call it is passed to. Empty text may have any pointer.</summary>
[StructLayout(LayoutKind.Sequential)]
internal unsafe struct FfiStr
{
    public byte* Ptr;
    public nuint Len;

    public readonly ReadOnlySpan<byte> Bytes => Len == 0 ? [] : new(Ptr, checked((int)Len));

    public readonly string Text => Len == 0 ? "" : Encoding.UTF8.GetString(Bytes);
}

/// <summary>Owned bytes: whoever ends up with it calls <see cref="Release"/> with <see cref="Owner"/>.</summary>
[StructLayout(LayoutKind.Sequential)]
internal unsafe struct FfiBuf
{
    public byte* Ptr;
    public nuint Len;
    public void* Owner;
    public delegate* unmanaged[Cdecl]<void*, void> Release;

    /// <summary>The text as UTF-8 in a native block of its own (freed by <see cref="FfiBlock.Free"/>).</summary>
    public static FfiBuf From(string text)
    {
        int length = Encoding.UTF8.GetByteCount(text);
        byte* block = (byte*)NativeMemory.Alloc((nuint)Math.Max(1, length));
        Encoding.UTF8.GetBytes(text, new Span<byte>(block, length));
        return new() { Ptr = block, Len = (nuint)length, Owner = block, Release = &FfiBlock.Free };
    }

    /// <summary>The bytes copied into a native block of their own (freed by <see cref="FfiBlock.Free"/>).</summary>
    public static FfiBuf From(ReadOnlySpan<byte> bytes)
    {
        byte* block = (byte*)NativeMemory.Alloc((nuint)Math.Max(1, bytes.Length));
        bytes.CopyTo(new Span<byte>(block, bytes.Length));
        return new() { Ptr = block, Len = (nuint)bytes.Length, Owner = block, Release = &FfiBlock.Free };
    }
}

/// <summary>A borrowed array, valid for the call it is passed to. An empty one may have any pointer.</summary>
[StructLayout(LayoutKind.Sequential)]
internal unsafe struct FfiSlice
{
    public void* Ptr;
    public nuint Len;

    public readonly ReadOnlySpan<T> Items<T>() where T : unmanaged => Len == 0 ? [] : new(Ptr, checked((int)Len));
}

/// <summary>An owned array: one release frees it and its elements.</summary>
[StructLayout(LayoutKind.Sequential)]
internal unsafe struct FfiVec
{
    public void* Ptr;
    public nuint Len;
    public void* Owner;
    public delegate* unmanaged[Cdecl]<void*, void> Release;

    /// <summary>The texts as one native block: the FfiBufs (without releases of their own), then their bytes.</summary>
    public static FfiVec From(IReadOnlyList<string> texts)
    {
        nuint headers = (nuint)texts.Count * (nuint)sizeof(FfiBuf);
        nuint total = headers;
        foreach (string text in texts) total += (nuint)Encoding.UTF8.GetByteCount(text);
        byte* block = (byte*)NativeMemory.Alloc(Math.Max(1, total));
        var items = (FfiBuf*)block;
        byte* bytes = block + headers;
        for (int i = 0; i < texts.Count; i++)
        {
            int length = Encoding.UTF8.GetByteCount(texts[i]);
            Encoding.UTF8.GetBytes(texts[i], new Span<byte>(bytes, length));
            items[i] = new() { Ptr = bytes, Len = (nuint)length };
            bytes += length;
        }
        return new() { Ptr = block, Len = (nuint)texts.Count, Owner = block, Release = &FfiBlock.Free };
    }

    /// <summary>The values as one native block.</summary>
    public static FfiVec From(ReadOnlySpan<ulong> values)
    {
        ulong* block = (ulong*)NativeMemory.Alloc((nuint)Math.Max(1, values.Length) * sizeof(ulong));
        values.CopyTo(new Span<ulong>(block, values.Length));
        return new() { Ptr = block, Len = (nuint)values.Length, Owner = block, Release = &FfiBlock.Free };
    }
}

internal static unsafe class FfiBlock
{
    /// <summary>The release of every buffer and array the bridge hands out: one native block each.</summary>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    public static void Free(void* owner) => NativeMemory.Free(owner);
}

[StructLayout(LayoutKind.Sequential)]
internal struct FfiError
{
    public uint Kind;
    public FfiBuf Message;
}

/// <summary>FfiErrorKind values the bridge reports.</summary>
internal static class FfiErrorKind
{
    public const uint Other = 0;
    public const uint NotImplemented = 3;
    public const uint InvalidArgument = 5;
    public const uint Compute = 17;
}

internal static class FfiTag
{
    public const byte None = 0;
    public const byte Some = 1;
    public const byte Ok = 0;
    public const byte Err = 1;
}

[StructLayout(LayoutKind.Explicit)]
internal struct FfiOptionU64
{
    [FieldOffset(0)] public byte Tag;
    [FieldOffset(8)] public ulong Value;

    public readonly ulong? Get => Tag == FfiTag.Some ? Value : null;

    public static FfiOptionU64 From(ulong? value) => value is { } v ? new() { Tag = FfiTag.Some, Value = v } : default;
}

[StructLayout(LayoutKind.Explicit)]
internal struct FfiOptionStr
{
    [FieldOffset(0)] public byte Tag;
    [FieldOffset(8)] public FfiStr Value;

    public readonly string? Get => Tag == FfiTag.Some ? Value.Text : null;
}

[StructLayout(LayoutKind.Explicit)]
internal struct FfiOptionSlice
{
    [FieldOffset(0)] public byte Tag;
    [FieldOffset(8)] public FfiSlice Value;
}

[StructLayout(LayoutKind.Explicit)]
internal struct FfiOptionBuf
{
    [FieldOffset(0)] public byte Tag;
    [FieldOffset(8)] public FfiBuf Value;

    public static FfiOptionBuf From(string? text) => text is null ? default : new() { Tag = FfiTag.Some, Value = FfiBuf.From(text) };
}

[StructLayout(LayoutKind.Explicit)]
internal struct FfiResultHandle
{
    [FieldOffset(0)] public byte Tag;
    [FieldOffset(8)] public nint Ok;
    [FieldOffset(8)] public FfiError Err;
}

[StructLayout(LayoutKind.Explicit)]
internal struct FfiResultBuf
{
    [FieldOffset(0)] public byte Tag;
    [FieldOffset(8)] public FfiBuf Ok;
    [FieldOffset(8)] public FfiError Err;
}

[StructLayout(LayoutKind.Explicit)]
internal struct FfiResultU64
{
    [FieldOffset(0)] public byte Tag;
    [FieldOffset(8)] public ulong Ok;
    [FieldOffset(8)] public FfiError Err;
}

[StructLayout(LayoutKind.Explicit)]
internal struct FfiResultUnit
{
    [FieldOffset(0)] public byte Tag;
    [FieldOffset(8)] public FfiError Err;
}

[StructLayout(LayoutKind.Explicit)]
internal struct FfiResultCapabilities
{
    [FieldOffset(0)] public byte Tag;
    [FieldOffset(8)] public FfiCapabilities Ok;
    [FieldOffset(8)] public FfiError Err;
}

[StructLayout(LayoutKind.Explicit)]
internal struct FfiResultSchema
{
    [FieldOffset(0)] public byte Tag;
    [FieldOffset(8)] public CArrowSchema Ok;
    [FieldOffset(8)] public FfiError Err;
}

[StructLayout(LayoutKind.Explicit)]
internal struct FfiResultStream
{
    [FieldOffset(0)] public byte Tag;
    [FieldOffset(8)] public CArrowArrayStream Ok;
    [FieldOffset(8)] public FfiError Err;
}

[StructLayout(LayoutKind.Explicit)]
internal struct FfiResultBufVec
{
    [FieldOffset(0)] public byte Tag;
    [FieldOffset(8)] public FfiVec Ok;
    [FieldOffset(8)] public FfiError Err;
}

[StructLayout(LayoutKind.Explicit)]
internal struct FfiResultResolved
{
    [FieldOffset(0)] public byte Tag;
    [FieldOffset(8)] public FfiResolved Ok;
    [FieldOffset(8)] public FfiError Err;
}

/// <summary>file_reader_v1::Capabilities.</summary>
[StructLayout(LayoutKind.Sequential)]
internal struct FfiCapabilities
{
    public uint Flags;
    public FfiOptionU64 MaxConcurrentScans;
    public FfiOptionU64 PreferredRemotePreInit;

    public const uint Unordered = 1 << 0;
    public const uint SourcesRemote = 1 << 1;
}

/// <summary>file_reader_v1::BeginReadArgs: the columns (FfiStr) to read, never empty.</summary>
[StructLayout(LayoutKind.Sequential)]
internal struct FfiBeginReadArgs
{
    public FfiSlice Columns;
    public byte Verbose;
}

/// <summary>lazyframe_resolver_v1::ResolveArgs.</summary>
[StructLayout(LayoutKind.Sequential)]
internal struct FfiResolveArgs
{
    public FfiOptionSlice Projection;
    public FfiOptionU64 Limit;
    public FfiSlice Filters;
    public FfiSlice FilterColumns;
    public FfiOptionU64 FilterDropColumnsIdx;
    public FfiOptionStr ExistingVersionKey;
    public FfiOptionU64 TargetNSources;
    public byte Verbose;
}

/// <summary>lazyframe_resolver_v1::ResolvedScan.</summary>
[StructLayout(LayoutKind.Sequential)]
internal struct FfiResolvedScan
{
    public CArrowSchema Schema;
    public FfiVec Sources;
    public FfiBuf ReaderPackage;
    public nint Reader;
    public FfiVec AppliedFilters;
    public FfiOptionU64 RowEstimate;
}

[StructLayout(LayoutKind.Explicit)]
internal struct FfiOptionResolvedScan
{
    [FieldOffset(0)] public byte Tag;
    [FieldOffset(8)] public FfiResolvedScan Value;
}

/// <summary>lazyframe_resolver_v1::Resolved.</summary>
[StructLayout(LayoutKind.Sequential)]
internal struct FfiResolved
{
    public FfiOptionResolvedScan Scan;
    public FfiOptionBuf VersionKey;
}

/// <summary>file_reader_v1::Plugin: the entry points, in the contract's order.</summary>
[StructLayout(LayoutKind.Sequential)]
internal unsafe struct FileReaderContract
{
    public delegate* unmanaged[Cdecl]<FfiSlice, FfiResultHandle> BuilderDeserialize;
    public delegate* unmanaged[Cdecl]<nint, FfiResultBuf> BuilderSerialize;
    public delegate* unmanaged[Cdecl]<nint, nint> BuilderClone;
    public delegate* unmanaged[Cdecl]<nint, void> BuilderRelease;
    public delegate* unmanaged[Cdecl]<nint, FfiBuf> BuilderName;
    public delegate* unmanaged[Cdecl]<nint, ulong> BuilderFingerprint;
    public delegate* unmanaged[Cdecl]<nint, FfiResultBufVec> BuilderExplain;
    public delegate* unmanaged[Cdecl]<nint, FfiResultCapabilities> BuilderCapabilities;
    public delegate* unmanaged[Cdecl]<nint, FfiResultHandle> ScanNew;
    public delegate* unmanaged[Cdecl]<nint, void> ScanRelease;
    public delegate* unmanaged[Cdecl]<nint, FfiStr, FfiResultHandle> ReaderNew;
    public delegate* unmanaged[Cdecl]<nint, void> ReaderRelease;
    public delegate* unmanaged[Cdecl]<nint, FfiResultSchema> ReaderSchema;
    public delegate* unmanaged[Cdecl]<nint, FfiResultU64> ReaderNRows;
    public delegate* unmanaged[Cdecl]<nint, FfiBeginReadArgs*, FfiResultStream> ReaderBeginRead;
    public delegate* unmanaged[Cdecl]<nint, void> ReaderCancel;
    public delegate* unmanaged[Cdecl]<nint, FfiResultUnit> ReaderTakeError;
}

/// <summary>lazyframe_resolver_v1::Plugin: the entry points, in the contract's order.</summary>
[StructLayout(LayoutKind.Sequential)]
internal unsafe struct ResolverContract
{
    public delegate* unmanaged[Cdecl]<FfiSlice, FfiResultHandle> ResolverDeserialize;
    public delegate* unmanaged[Cdecl]<nint, FfiResultBuf> ResolverSerialize;
    public delegate* unmanaged[Cdecl]<nint, nint> ResolverClone;
    public delegate* unmanaged[Cdecl]<nint, void> ResolverRelease;
    public delegate* unmanaged[Cdecl]<nint, FfiBuf> ResolverName;
    public delegate* unmanaged[Cdecl]<nint, ulong> ResolverFingerprint;
    public delegate* unmanaged[Cdecl]<nint, FfiResultSchema> ResolverSchema;
    public delegate* unmanaged[Cdecl]<nint, FfiResolveArgs*, FfiResultResolved> ResolverResolve;
}
