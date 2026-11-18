using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Collections;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Core;
using Kaminari.Sazanami.Arrow.TypeSystem;
using Kaminari.Sazanami.Json;
using Kaminari.Sazanami.Marshalling;

namespace Kaminari.Sazanami.Data.Mapping;

internal sealed class FixedSizeBinaryToGuidAccessor : TypedColumnAccessor<Guid>,
    ITypedAccessor<Guid>, ITypedAccessor<Guid?>, IBinarySpanAccessor
{
    private FixedSizeBinaryArray? _array;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(Guid);

    public override void SetBatch(IArrowArray array) => _array = (FixedSizeBinaryArray)array;

    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> GetBytesSpan(int index) => _array!.GetBytes(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override Guid GetTypedValue(int index) => new(_array!.GetBytes(index), bigEndian: true);

    Guid ITypedAccessor<Guid>.GetTypedValue(int index) => GetTypedValue(index);
    Guid? ITypedAccessor<Guid?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
}

internal sealed class FixedSizeBinaryAccessor : TypedColumnAccessor<byte[]>,
    ITypedAccessor<byte[]>,
    ITypedAccessor<Guid>, ITypedAccessor<Guid?>,
    IBinarySpanAccessor
{
    private FixedSizeBinaryArray? _array;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(byte[]);
    public override void SetBatch(IArrowArray array) => _array = (FixedSizeBinaryArray)array;
    public override bool IsNull(int index) => _array!.IsNull(index);
    public override byte[] GetTypedValue(int index) => _array!.GetBytes(index).ToArray();

    Guid ITypedAccessor<Guid>.GetTypedValue(int index) => new(_array!.GetBytes(index), bigEndian: true);
    Guid? ITypedAccessor<Guid?>.GetTypedValue(int index) => IsNull(index) ? null : new Guid(_array!.GetBytes(index), bigEndian: true);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> GetBytesSpan(int index) => _array!.GetBytes(index);
    byte[] ITypedAccessor<byte[]>.GetTypedValue(int index) => GetTypedValue(index);
}

internal sealed class BinaryToGuidAccessor : TypedColumnAccessor<Guid>,
    ITypedAccessor<Guid>, ITypedAccessor<Guid?>, IBinarySpanAccessor
{
    private IArrowArray? _array;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(Guid);
    public override void SetBatch(IArrowArray array) => _array = array;
    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> GetBytesSpan(int index)
    {
        if (_array is BinaryViewArray bv) return bv.GetBytes(index);
        if (_array is LargeBinaryArray lba) return lba.GetBytes(index);
        if (_array is BinaryArray ba) return ba.GetBytes(index);
        throw new InvalidCastException($"Expected Binary-like array, got {_array?.GetType()}");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override Guid GetTypedValue(int index)
    {
        ReadOnlySpan<byte> bytes = GetBytesSpan(index);
        if (bytes.Length != 16)
        {
            throw new InvalidDataException($"Cannot convert binary of length {bytes.Length} to Guid.");
        }
        return new Guid(bytes);
    }

    Guid ITypedAccessor<Guid>.GetTypedValue(int index) => GetTypedValue(index);
    Guid? ITypedAccessor<Guid?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
}

internal sealed unsafe class StringViewAccessor : TypedColumnAccessor<string>,
    IBinarySpanAccessor,
    ITypedAccessor<string>,
    ITypedAccessor<char>,
    ITypedAccessor<char?>
{
    [StructLayout(LayoutKind.Sequential, Size = 16)]
    private readonly struct ArrowStringView { }

    private IArrowArray? _array;
    private StringViewArray? _viewArray;
    private byte* _viewsPtr;
    private MemoryHandle[]? _dataBufferHandles;
    private nint[]? _dataBufferPtrs;

    // Decoded strings by their 16-byte view, direct mapped. Within one array, equal views are equal strings: an
    // inlined view holds the whole string, a referencing view its buffer and offset. Rows gathered from the same
    // source row (a dimension's strings repeated by a join, a filtered or sorted column) share their views, so
    // repeated values decode and allocate once. Cleared per array, since buffer indices are per array.
    // A column whose values hardly repeat (ids, free text) stops using the cache after a probation of
    // lookups with few hits, so it pays only for the probation.
    private const int StringCacheBits = 10;
    private const int CacheProbation = 4096;
    private ulong[]? _cacheKeys;
    private string?[]? _cacheValues;
    private bool _cacheEnabled;
    private int _cacheLookups;
    private int _cacheHits;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(string);
    public override bool UsesFastNullBitmap => true;

    public override void SetBatch(IArrowArray array)
    {
        ReleaseDataBuffers();
        _array = array;
        if (array is StringViewArray viewArray)
        {
            if (_cacheValues is null)
            {
                _cacheKeys = new ulong[2 << StringCacheBits];
                _cacheValues = new string?[1 << StringCacheBits];
            }
            else
            {
                System.Array.Clear(_cacheValues);
            }
            _cacheEnabled = true;
            _cacheLookups = 0;
            _cacheHits = 0;
            _viewArray = viewArray;
            ArrowStringView* rawViews = BindPrimitiveBuffer<ArrowStringView>(viewArray, 1);
            _viewsPtr = (byte*)rawViews;
            int totalBuffers = viewArray.Data.Buffers.Length;
            int dataBufferCount = totalBuffers - 2;
            if (dataBufferCount > 0)
            {
                _dataBufferHandles = new MemoryHandle[dataBufferCount];
                _dataBufferPtrs = new nint[dataBufferCount];
                for (int i = 0; i < dataBufferCount; i++)
                {
                    var buf = viewArray.Data.Buffers[i + 2];
                    if (!buf.IsEmpty)
                    {
                        _dataBufferPtrs[i] = (nint)GetBufferPointer(buf.Memory, ref _dataBufferHandles[i]);
                    }
                }
            }
        }
        else
        {
            BindNullBitmap(array);
            _viewArray = null;
            _viewsPtr = null;
        }
    }

    private void ReleaseDataBuffers()
    {
        if (_dataBufferHandles != null)
        {
            for (int i = 0; i < _dataBufferHandles.Length; i++)
            {
                _dataBufferHandles[i].Dispose();
            }
            _dataBufferHandles = null;
            _dataBufferPtrs = null;
        }
    }

    public override void Dispose()
    {
        ReleaseDataBuffers();
        base.Dispose();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override string GetTypedValue(int index)
    {
        if (_viewsPtr != null)
        {
            byte* viewPtr = _viewsPtr + ((nint)index << 4);
            int length = *(int*)viewPtr;
            if (length == 0) return string.Empty;
            if (!_cacheEnabled) return DecodeView(viewPtr, length);

            ulong k0 = *(ulong*)viewPtr;
            ulong k1 = *(ulong*)(viewPtr + 8);
            if (length < 12)
            {
                // Only the inlined string's own bytes identify it.
                if (length < 4)
                {
                    k0 &= (1UL << (32 + length * 8)) - 1;
                    k1 = 0;
                }
                else
                {
                    k1 &= (1UL << ((length - 4) * 8)) - 1;
                }
            }

            int slot = (int)(((k0 ^ (k1 * 0x9E3779B97F4A7C15UL)) * 0xBF58476D1CE4E5B9UL) >> (64 - StringCacheBits));
            string? cached = _cacheValues![slot];
            if (cached is not null && _cacheKeys![2 * slot] == k0 && _cacheKeys[2 * slot + 1] == k1)
            {
                _cacheHits++;
                return cached;
            }

            if (++_cacheLookups - _cacheHits >= CacheProbation && _cacheHits * 8 < _cacheLookups)
            {
                _cacheEnabled = false;
            }

            string value = DecodeView(viewPtr, length);
            _cacheKeys![2 * slot] = k0;
            _cacheKeys[2 * slot + 1] = k1;
            _cacheValues[slot] = value;
            return value;
        }

        if (_viewArray != null) return _viewArray.GetString(index);
        if (_array is LargeStringArray lsa) return lsa.GetString(index);
        if (_array is StringArray sa) return sa.GetString(index);

        throw new InvalidCastException($"Expected String or StringView, got {_array?.GetType()}");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string DecodeView(byte* viewPtr, int length) =>
        length <= 12
            ? Encoding.UTF8.GetString(viewPtr + 4, length)
            : Encoding.UTF8.GetString((byte*)_dataBufferPtrs![*(int*)(viewPtr + 8)] + *(int*)(viewPtr + 12), length);

    char ITypedAccessor<char>.GetTypedValue(int index)
    {
        string s = GetTypedValue(index);
        return string.IsNullOrEmpty(s) ? '\0' : s[0];
    }

    char? ITypedAccessor<char?>.GetTypedValue(int index)
    {
        if (IsNullFast(index)) return null;
        string s = GetTypedValue(index);
        return string.IsNullOrEmpty(s) ? '\0' : s[0];
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> GetBytesSpan(int index)
    {
        if (_viewsPtr != null)
        {
            byte* viewPtr = _viewsPtr + ((nint)index << 4);
            int length = *(int*)viewPtr;
            if (length == 0) return [];
            if (length <= 12)
            {
                return new ReadOnlySpan<byte>(viewPtr + 4, length);
            }
            int bufIdx = *(int*)(viewPtr + 8);
            int bufOffset = *(int*)(viewPtr + 12);
            byte* dataPtr = (byte*)_dataBufferPtrs![bufIdx] + bufOffset;
            return new ReadOnlySpan<byte>(dataPtr, length);
        }

        if (_viewArray != null) return _viewArray.GetBytes(index);
        if (_array is LargeStringArray lsa) return lsa.GetBytes(index);
        if (_array is StringArray sa) return sa.GetBytes(index);

        throw new InvalidCastException($"Expected String or StringView, got {_array?.GetType()}");
    }
}

internal sealed class BinaryAccessor : TypedColumnAccessor<byte[]>, IBinarySpanAccessor,
    ITypedAccessor<byte[]>
{
    private IArrowArray? _array;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(byte[]);

    public override void SetBatch(IArrowArray array) => _array = array;

    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> GetBytesSpan(int index)
    {
        if (_array is BinaryArray ba) return ba.GetBytes(index);
        if (_array is LargeBinaryArray lba) return lba.GetBytes(index);
        if (_array is BinaryViewArray bv) return bv.GetBytes(index);

        throw new InvalidCastException($"Expected BinaryArray, got {_array?.GetType()}");
    }

    public override byte[] GetTypedValue(int index) => GetBytesSpan(index).ToArray();
}

// --- Magic Time Types ---
