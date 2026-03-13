using Microsoft.FSharp.Core;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Polars.NET.Core.Helpers;

public static unsafe class FSharpHelper
{
    // ========================================================================
    // 1. ValueOption<T> (struct) -> Values + Validity
    // ========================================================================
    
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (T[] values, byte[]? validity) UnzipValueOption<T>(ReadOnlySpan<FSharpValueOption<T>> data)
        where T : unmanaged
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<T>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        // Get ref to the beginning of the ReadOnlySpan (No longer bound to array)
        ref FSharpValueOption<T> srcRef = ref MemoryMarshal.GetReference(data);
        
        // Get ref to the beginning of the destination array (Replaces fixed T* pDst)
        ref T dstRef = ref MemoryMarshal.GetArrayDataReference(values);

        // Unroll 8
        int i = 0; 
        int limit = len - 8;
        int totalLen = len; 

        // No fixed block needed anymore!
        for (; i <= limit; i += 8)
        {
            HandleVOptionItem(i,     ref srcRef, ref dstRef, ref validity, ref validRef, totalLen);
            HandleVOptionItem(i + 1, ref srcRef, ref dstRef, ref validity, ref validRef, totalLen);
            HandleVOptionItem(i + 2, ref srcRef, ref dstRef, ref validity, ref validRef, totalLen);
            HandleVOptionItem(i + 3, ref srcRef, ref dstRef, ref validity, ref validRef, totalLen);
            HandleVOptionItem(i + 4, ref srcRef, ref dstRef, ref validity, ref validRef, totalLen);
            HandleVOptionItem(i + 5, ref srcRef, ref dstRef, ref validity, ref validRef, totalLen);
            HandleVOptionItem(i + 6, ref srcRef, ref dstRef, ref validity, ref validRef, totalLen);
            HandleVOptionItem(i + 7, ref srcRef, ref dstRef, ref validity, ref validRef, totalLen);
        }
        
        for (; i < len; i++)
        {
            HandleVOptionItem(i, ref srcRef, ref dstRef, ref validity, ref validRef, totalLen);
        }
        
        return (values, validity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (T[] values, byte[]? validity) UnzipValueOption<T>(FSharpValueOption<T>[] data)
        where T : unmanaged
        => UnzipValueOption<T>(new ReadOnlySpan<FSharpValueOption<T>>(data));


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void HandleVOptionItem<T>(
        int i, 
        ref FSharpValueOption<T> srcBase, 
        ref T dstBase, // Changed from T* pDst to ref T dstBase
        ref byte[]? validity, 
        ref byte validRef,
        int totalLen) where T : unmanaged
    {
        ref FSharpValueOption<T> item = ref Unsafe.Add(ref srcBase, i);
        
        // F# ValueOption Tag: 1 = ValueSome, 0 = ValueNone
        if (item.Tag == FSharpValueOption<T>.Tags.ValueSome)
        {
            // Replaced pDst[i] with Unsafe.Add
            Unsafe.Add(ref dstBase, i) = item.Value;

            if (validity != null)
            {
                // Unsafe.IsNullRef check is fast
                if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                
                ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                target |= (byte)(1 << (i & 7));
            }
        }
        else
        {
            if (validity == null)
            {
                InitValidity(ref validity, ref validRef, i, totalLen);
            }
            
            // Replaced pDst[i] with Unsafe.Add
            Unsafe.Add(ref dstBase, i) = default;
        }
    }

    // ========================================================================
    // 2. Option<T> (class) -> Values + Validity
    // ========================================================================
    // F# Option is ref type (class) Array is object[] (ptr array)。
    
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (T[] values, byte[]? validity) UnzipOption<T>(ReadOnlySpan<FSharpOption<T>> data)
        where T : unmanaged
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<T>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();
        
        // Get ref to the beginning of the ReadOnlySpan
        ref FSharpOption<T> srcRef = ref MemoryMarshal.GetReference(data);
        
        // Get ref to the beginning of the destination array (Replaces fixed T* pDst)
        ref T dstRef = ref MemoryMarshal.GetArrayDataReference(values);

        // No fixed block needed anymore!
        int i = 0;
        int limit = len - 4;

        for (; i <= limit; i += 4)
        {
            HandleOptionItem(i,     ref srcRef, ref dstRef, ref validity, ref validRef, len);
            HandleOptionItem(i + 1, ref srcRef, ref dstRef, ref validity, ref validRef, len);
            HandleOptionItem(i + 2, ref srcRef, ref dstRef, ref validity, ref validRef, len);
            HandleOptionItem(i + 3, ref srcRef, ref dstRef, ref validity, ref validRef, len);
        }

        for (; i < len; i++)
        {
            HandleOptionItem(i, ref srcRef, ref dstRef, ref validity, ref validRef, len);
        }
        
        return (values, validity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (T[] values, byte[]? validity) UnzipOption<T>(FSharpOption<T>[] data)
        where T : unmanaged
        => UnzipOption<T>(new ReadOnlySpan<FSharpOption<T>>(data));

    // T* pDst => ref T dstBase
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void HandleOptionItem<T>(
        int i,
        ref FSharpOption<T> srcBase,
        ref T dstBase, // Changed from T* pDst to ref T dstBase
        ref byte[]? validity,
        ref byte validRef,
        int totalLen) where T : unmanaged
    {
        // Get ref
        FSharpOption<T> item = Unsafe.Add(ref srcBase, i);
        
        // item != null means Some
        if (item != null)
        {
            // Replaced pDst[i] with Unsafe.Add
            Unsafe.Add(ref dstBase, i) = item.Value;
            
            if (validity != null) 
            {
                if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                target |= (byte)(1 << (i & 7));
            }
        }
        else
        {
            // None (null pointer)
            if (validity == null) 
            {
                 InitValidity(ref validity, ref validRef, i, totalLen);
            }
            
            // Replaced pDst[i] with Unsafe.Add
            Unsafe.Add(ref dstBase, i) = default;
        }
    }
    
    // ========================================================================
    // Validity Initialization Helper (Backfill Logic)
    // ========================================================================
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void InitValidity(ref byte[]? validity, ref byte validRef, int currentIdx, int totalLen)
    {
        // Calc Bitmap Total Bytes
        int byteLen = (totalLen + 7) >> 3;
        validity = new byte[byteLen];
        validRef = ref MemoryMarshal.GetArrayDataReference(validity);
        
        // Fill all Valid byte (0xFF)
        int fullBytes = currentIdx >> 3;
        if (fullBytes > 0)
        {
            Unsafe.InitBlock(ref validRef, 0xFF, (uint)fullBytes);
        }

        // Fill valid bit before current byte 
        // Index 18. 18 & 7 = 2 (010). means this byte 0, 1 bite is Valid， 2 is Null
        // Mask = (1 << 2) - 1 = 3 (00000011)
        int remainingBits = currentIdx & 7;
        if (remainingBits > 0)
        {
            ref byte target = ref Unsafe.Add(ref validRef, fullBytes);
            target = (byte)((1 << remainingBits) - 1);
        }
    }
    // ========================================================================
    // 3. Boolean Packing (Option/VOption -> Bitmaps directly)
    // ========================================================================

    /// <summary>
    /// Pack ReadOnlySpan<FSharpValueOption<bool>> directly to Arrow Bitmaps (Values + Validity)
    /// Zero-Allocation intermediate.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (byte[] values, byte[]? validity) PackValueOptionBool(ReadOnlySpan<FSharpValueOption<bool>> data)
    {
        int len = data.Length;
        int byteLen = (len + 7) >> 3;
        
        var values = new byte[byteLen];
        byte[]? validity = null; // Lazy init

        // Pointers
        ref byte valuesRef = ref MemoryMarshal.GetArrayDataReference(values);
        ref byte validRef = ref Unsafe.NullRef<byte>(); // Placeholder
        
        ref FSharpValueOption<bool> srcRef = ref MemoryMarshal.GetReference(data);

        for (int i = 0; i < len; i++)
        {
            ref FSharpValueOption<bool> item = ref Unsafe.Add(ref srcRef, i);

            // Tag: 1 = Some, 0 = None
            if (item.Tag == FSharpValueOption<bool>.Tags.ValueSome)
            {
                // Is True?
                if (item.Value)
                {
                    // Set Value Bit
                    ref byte targetVal = ref Unsafe.Add(ref valuesRef, i >> 3);
                    targetVal |= (byte)(1 << (i & 7));
                }
                
                // Set Validity Bit
                if (validity != null)
                {
                    if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                    ref byte targetValid = ref Unsafe.Add(ref validRef, i >> 3);
                    targetValid |= (byte)(1 << (i & 7));
                }
            }
            else
            {
                // None -> Handle Validity Initialization
                if (validity == null)
                {
                    InitValidity(ref validity, ref validRef, i, len);
                }
                // Value bit remains 0 (default)
            }
        }
        return (values, validity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (byte[] values, byte[]? validity) PackValueOptionBool(FSharpValueOption<bool>[] data)
        => PackValueOptionBool(new ReadOnlySpan<FSharpValueOption<bool>>(data));
    
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (byte[] values, byte[]? validity) PackOptionBool(ReadOnlySpan<FSharpOption<bool>> data)
    {
        int len = data.Length;
        int byteLen = (len + 7) >> 3;

        var values = new byte[byteLen];
        byte[]? validity = null;

        ref byte valuesRef = ref MemoryMarshal.GetArrayDataReference(values);
        ref byte validRef = ref Unsafe.NullRef<byte>();
        
        ref FSharpOption<bool> srcRef = ref MemoryMarshal.GetReference(data);

        for (int i = 0; i < len; i++)
        {
            FSharpOption<bool> item = Unsafe.Add(ref srcRef, i);

            if (item != null) // Some
            {
                if (item.Value)
                {
                    ref byte targetVal = ref Unsafe.Add(ref valuesRef, i >> 3);
                    targetVal |= (byte)(1 << (i & 7));
                }

                if (validity != null)
                {
                    if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                    ref byte targetValid = ref Unsafe.Add(ref validRef, i >> 3);
                    targetValid |= (byte)(1 << (i & 7));
                }
            }
            else // None
            {
                if (validity == null)
                {
                    InitValidity(ref validity, ref validRef, i, len);
                }
            }
        }
        return (values, validity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (byte[] values, byte[]? validity) PackOptionBool(FSharpOption<bool>[] data)
        => PackOptionBool(new ReadOnlySpan<FSharpOption<bool>>(data));

    // ========================================================================
    // 4. String Unwrapping (Option/VOption -> string[])
    // ========================================================================

    /// <summary>
    /// Unwrap FSharpOption<string>[] -> string[] (null for None)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static string?[] UnwrapOptionString(ReadOnlySpan<FSharpOption<string>> data)
    {
        int len = data.Length;
        var result = new string?[len];
        
        ref FSharpOption<string> srcRef = ref MemoryMarshal.GetReference(data);
        ref string? dstRef = ref MemoryMarshal.GetArrayDataReference(result);

        for (int i = 0; i < len; i++)
        {
            // Load pointer
            FSharpOption<string> item = Unsafe.Add(ref srcRef, i);
            
            // If item is not null, take Value, else null
            // F# Option<RefType> implementation: Value property holds the ref
            Unsafe.Add(ref dstRef, i) = item?.Value;
        }
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string?[] UnwrapOptionString(FSharpOption<string>[] data)
        => UnwrapOptionString(new ReadOnlySpan<FSharpOption<string>>(data));

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static string?[] UnwrapValueOptionString(ReadOnlySpan<FSharpValueOption<string>> data)
    {
        int len = data.Length;
        var result = new string?[len];

        ref FSharpValueOption<string> srcRef = ref MemoryMarshal.GetReference(data);
        ref string? dstRef = ref MemoryMarshal.GetArrayDataReference(result);

        for (int i = 0; i < len; i++)
        {
            ref FSharpValueOption<string> item = ref Unsafe.Add(ref srcRef, i);
            
            if (item.Tag == FSharpValueOption<string>.Tags.ValueSome)
            {
                Unsafe.Add(ref dstRef, i) = item.Value;
            }
            else
            {
                Unsafe.Add(ref dstRef, i) = null;
            }
        }
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string?[] UnwrapValueOptionString(FSharpValueOption<string>[] data)
        => UnwrapValueOptionString(new ReadOnlySpan<FSharpValueOption<string>>(data));

    // ========================================================================
    // DateTime Support (Option/VOption -> Int64[] Microseconds)
    // ========================================================================

    /// <summary>
    /// Pack FSharpOption<DateTime>[] directly to Microseconds[]
    /// (Zero-Allocation, Pointer-based Unzip)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (long[] values, byte[]? validity) UnzipOptionDateTimeToUs(ReadOnlySpan<FSharpOption<DateTime>> data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        long mask = 0x3FFFFFFFFFFFFFFF; 
        long epoch = 621355968000000000; 

        ref FSharpOption<DateTime> srcRef = ref MemoryMarshal.GetReference(data);
        ref long dstRef = ref MemoryMarshal.GetArrayDataReference(values);

        for (int i = 0; i < len; i++)
        {
            FSharpOption<DateTime> item = Unsafe.Add(ref srcRef, i);

            if (item != null)
            {
                long ticks = item.Value.Ticks & mask;
                Unsafe.Add(ref dstRef, i) = (ticks - epoch) / 10;

                if (validity != null)
                {
                    if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                    ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                    target |= (byte)(1 << (i & 7));
                }
            }
            else
            {
                if (validity == null)
                {
                    InitValidity(ref validity, ref validRef, i, len);
                }
                Unsafe.Add(ref dstRef, i) = 0;
            }
        }
        
        return (values, validity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (long[] values, byte[]? validity) UnzipOptionDateTimeToUs(FSharpOption<DateTime>[] data)
        => UnzipOptionDateTimeToUs(new ReadOnlySpan<FSharpOption<DateTime>>(data));

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (long[] values, byte[]? validity) UnzipValueOptionDateTimeToUs(ReadOnlySpan<FSharpValueOption<DateTime>> data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        long mask = 0x3FFFFFFFFFFFFFFF; 
        long epoch = 621355968000000000; 

        ref FSharpValueOption<DateTime> srcRef = ref MemoryMarshal.GetReference(data);
        ref long dstRef = ref MemoryMarshal.GetArrayDataReference(values);

        for (int i = 0; i < len; i++)
        {
            ref FSharpValueOption<DateTime> item = ref Unsafe.Add(ref srcRef, i);
            
            if (item.Tag == FSharpValueOption<DateTime>.Tags.ValueSome)
            {
                long ticks = item.Value.Ticks & mask;
                Unsafe.Add(ref dstRef, i) = (ticks - epoch) / 10;
                
                if (validity != null)
                {
                    if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                    ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                    target |= (byte)(1 << (i & 7));
                }
            }
            else
            {
                if (validity == null)
                {
                    InitValidity(ref validity, ref validRef, i, len);
                }
                Unsafe.Add(ref dstRef, i) = 0;
            }
        }
        
        return (values, validity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (long[] values, byte[]? validity) UnzipValueOptionDateTimeToUs(FSharpValueOption<DateTime>[] data)
        => UnzipValueOptionDateTimeToUs(new ReadOnlySpan<FSharpValueOption<DateTime>>(data));
    // ========================================================================
    // DateOnly Support (Option/VOption -> Int32[] Days)
    // ========================================================================
    
    /// <summary>
    /// Pack FSharpOption<DateOnly>[] directly to Int32[] (Days since 1970-01-01)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (int[] values, byte[]? validity) UnzipOptionDateOnlyToInt32(ReadOnlySpan<FSharpOption<DateOnly>> data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<int>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        int epochShift = 719162; 

        ref FSharpOption<DateOnly> srcRef = ref MemoryMarshal.GetReference(data);
        ref int dstRef = ref MemoryMarshal.GetArrayDataReference(values);

        for (int i = 0; i < len; i++)
        {
            FSharpOption<DateOnly> item = Unsafe.Add(ref srcRef, i);

            if (item != null)
            {
                // Item.Value is DateOnly struct -> .DayNumber is int
                Unsafe.Add(ref dstRef, i) = item.Value.DayNumber - epochShift;

                if (validity != null)
                {
                    if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                    ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                    target |= (byte)(1 << (i & 7));
                }
            }
            else
            {
                if (validity == null)
                {
                    InitValidity(ref validity, ref validRef, i, len);
                }
                Unsafe.Add(ref dstRef, i) = 0;
            }
        }
        
        return (values, validity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (int[] values, byte[]? validity) UnzipOptionDateOnlyToInt32(FSharpOption<DateOnly>[] data)
        => UnzipOptionDateOnlyToInt32(new ReadOnlySpan<FSharpOption<DateOnly>>(data));


    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (int[] values, byte[]? validity) UnzipValueOptionDateOnlyToInt32(ReadOnlySpan<FSharpValueOption<DateOnly>> data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<int>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        int epochShift = 719162;

        ref FSharpValueOption<DateOnly> srcRef = ref MemoryMarshal.GetReference(data);
        ref int dstRef = ref MemoryMarshal.GetArrayDataReference(values);

        for (int i = 0; i < len; i++)
        {
            ref FSharpValueOption<DateOnly> item = ref Unsafe.Add(ref srcRef, i);

            if (item.Tag == FSharpValueOption<DateOnly>.Tags.ValueSome)
            {
                Unsafe.Add(ref dstRef, i) = item.Value.DayNumber - epochShift;

                if (validity != null)
                {
                    if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                    ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                    target |= (byte)(1 << (i & 7));
                }
            }
            else
            {
                if (validity == null)
                {
                    InitValidity(ref validity, ref validRef, i, len);
                }
                Unsafe.Add(ref dstRef, i) = 0;
            }
        }
        
        return (values, validity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (int[] values, byte[]? validity) UnzipValueOptionDateOnlyToInt32(FSharpValueOption<DateOnly>[] data)
        => UnzipValueOptionDateOnlyToInt32(new ReadOnlySpan<FSharpValueOption<DateOnly>>(data));

    // ========================================================================
    // DateTimeOffset Support (Option/VOption -> UTC Microseconds)
    // ========================================================================

    /// <summary>
    /// Pack FSharpOption<DateTimeOffset>[] directly to UTC Microseconds[]
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (long[] values, byte[]? validity) UnzipOptionDateTimeOffsetToUs(ReadOnlySpan<FSharpOption<DateTimeOffset>> data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        long epoch = 621355968000000000; 

        ref FSharpOption<DateTimeOffset> srcRef = ref MemoryMarshal.GetReference(data);
        ref long dstRef = ref MemoryMarshal.GetArrayDataReference(values);

        for (int i = 0; i < len; i++)
        {
            // Load reference (pointer)
            FSharpOption<DateTimeOffset> item = Unsafe.Add(ref srcRef, i);

            if (item != null)
            {
                // Access .Value.UtcTicks directly
                long utcTicks = item.Value.UtcTicks;
                Unsafe.Add(ref dstRef, i) = (utcTicks - epoch) / 10;

                if (validity != null)
                {
                    if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                    ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                    target |= (byte)(1 << (i & 7));
                }
            }
            else
            {
                if (validity == null)
                {
                    InitValidity(ref validity, ref validRef, i, len);
                }
                Unsafe.Add(ref dstRef, i) = 0;
            }
        }
        
        return (values, validity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (long[] values, byte[]? validity) UnzipOptionDateTimeOffsetToUs(FSharpOption<DateTimeOffset>[] data)
        => UnzipOptionDateTimeOffsetToUs(new ReadOnlySpan<FSharpOption<DateTimeOffset>>(data));

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (long[] values, byte[]? validity) UnzipValueOptionDateTimeOffsetToUs(ReadOnlySpan<FSharpValueOption<DateTimeOffset>> data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        long epoch = 621355968000000000; 

        ref FSharpValueOption<DateTimeOffset> srcRef = ref MemoryMarshal.GetReference(data);
        ref long dstRef = ref MemoryMarshal.GetArrayDataReference(values);

        for (int i = 0; i < len; i++)
        {
            ref FSharpValueOption<DateTimeOffset> item = ref Unsafe.Add(ref srcRef, i);

            if (item.Tag == FSharpValueOption<DateTimeOffset>.Tags.ValueSome)
            {
                long utcTicks = item.Value.UtcTicks;
                Unsafe.Add(ref dstRef, i) = (utcTicks - epoch) / 10;

                if (validity != null)
                {
                    if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                    ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                    target |= (byte)(1 << (i & 7));
                }
            }
            else
            {
                if (validity == null)
                {
                    InitValidity(ref validity, ref validRef, i, len);
                }
                Unsafe.Add(ref dstRef, i) = 0;
            }
        }
        
        return (values, validity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (long[] values, byte[]? validity) UnzipValueOptionDateTimeOffsetToUs(FSharpValueOption<DateTimeOffset>[] data)
        => UnzipValueOptionDateTimeOffsetToUs(new ReadOnlySpan<FSharpValueOption<DateTimeOffset>>(data));

    // ========================================================================
    // TimeOnly Support (Option/VOption -> Int64[] Nanoseconds)
    // ========================================================================

    /// <summary>
    /// Pack FSharpOption<TimeOnly>[] directly to Int64[] (Nanoseconds)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (long[] values, byte[]? validity) UnzipOptionTimeOnlyToNs(ReadOnlySpan<FSharpOption<TimeOnly>> data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        long multiplier = 100; // Ticks(100ns) -> ns

        ref FSharpOption<TimeOnly> srcRef = ref MemoryMarshal.GetReference(data);
        ref long dstRef = ref MemoryMarshal.GetArrayDataReference(values);

        for (int i = 0; i < len; i++)
        {
            FSharpOption<TimeOnly> item = Unsafe.Add(ref srcRef, i);

            if (item != null)
            {
                // item.Value is TimeOnly struct -> .Ticks is long
                Unsafe.Add(ref dstRef, i) = item.Value.Ticks * multiplier;

                if (validity != null)
                {
                    if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                    ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                    target |= (byte)(1 << (i & 7));
                }
            }
            else
            {
                if (validity == null)
                {
                    InitValidity(ref validity, ref validRef, i, len);
                }
                Unsafe.Add(ref dstRef, i) = 0;
            }
        }
        
        return (values, validity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (long[] values, byte[]? validity) UnzipOptionTimeOnlyToNs(FSharpOption<TimeOnly>[] data)
        => UnzipOptionTimeOnlyToNs(new ReadOnlySpan<FSharpOption<TimeOnly>>(data));

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (long[] values, byte[]? validity) UnzipValueOptionTimeOnlyToNs(ReadOnlySpan<FSharpValueOption<TimeOnly>> data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        long multiplier = 100;

        ref FSharpValueOption<TimeOnly> srcRef = ref MemoryMarshal.GetReference(data);
        ref long dstRef = ref MemoryMarshal.GetArrayDataReference(values);

        for (int i = 0; i < len; i++)
        {
            ref FSharpValueOption<TimeOnly> item = ref Unsafe.Add(ref srcRef, i);

            if (item.Tag == FSharpValueOption<TimeOnly>.Tags.ValueSome)
            {
                Unsafe.Add(ref dstRef, i) = item.Value.Ticks * multiplier;

                if (validity != null)
                {
                    if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                    ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                    target |= (byte)(1 << (i & 7));
                }
            }
            else
            {
                if (validity == null)
                {
                    InitValidity(ref validity, ref validRef, i, len);
                }
                Unsafe.Add(ref dstRef, i) = 0;
            }
        }
        
        return (values, validity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (long[] values, byte[]? validity) UnzipValueOptionTimeOnlyToNs(FSharpValueOption<TimeOnly>[] data)
        => UnzipValueOptionTimeOnlyToNs(new ReadOnlySpan<FSharpValueOption<TimeOnly>>(data));

    // ========================================================================
    // TimeSpan Support (Option/VOption -> Int64[] Microseconds)
    // ========================================================================

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (long[] values, byte[]? validity) UnzipOptionTimeSpanToUs(ReadOnlySpan<FSharpOption<TimeSpan>> data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        ref FSharpOption<TimeSpan> srcRef = ref MemoryMarshal.GetReference(data);
        ref long dstRef = ref MemoryMarshal.GetArrayDataReference(values);

        for (int i = 0; i < len; i++)
        {
            FSharpOption<TimeSpan> item = Unsafe.Add(ref srcRef, i);

            if (item != null)
            {
                Unsafe.Add(ref dstRef, i) = item.Value.Ticks / 10;

                if (validity != null)
                {
                    if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                    ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                    target |= (byte)(1 << (i & 7));
                }
            }
            else
            {
                if (validity == null) InitValidity(ref validity, ref validRef, i, len);
                Unsafe.Add(ref dstRef, i) = 0;
            }
        }
        return (values, validity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (long[] values, byte[]? validity) UnzipOptionTimeSpanToUs(FSharpOption<TimeSpan>[] data)
        => UnzipOptionTimeSpanToUs(new ReadOnlySpan<FSharpOption<TimeSpan>>(data));

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (long[] values, byte[]? validity) UnzipValueOptionTimeSpanToUs(ReadOnlySpan<FSharpValueOption<TimeSpan>> data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        ref FSharpValueOption<TimeSpan> srcRef = ref MemoryMarshal.GetReference(data);
        ref long dstRef = ref MemoryMarshal.GetArrayDataReference(values);

        for (int i = 0; i < len; i++)
        {
            ref FSharpValueOption<TimeSpan> item = ref Unsafe.Add(ref srcRef, i);

            if (item.Tag == FSharpValueOption<TimeSpan>.Tags.ValueSome)
            {
                Unsafe.Add(ref dstRef, i) = item.Value.Ticks / 10;

                if (validity != null)
                {
                    if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                    ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                    target |= (byte)(1 << (i & 7));
                }
            }
            else
            {
                if (validity == null) InitValidity(ref validity, ref validRef, i, len);
                Unsafe.Add(ref dstRef, i) = 0;
            }
        }
        return (values, validity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (long[] values, byte[]? validity) UnzipValueOptionTimeSpanToUs(FSharpValueOption<TimeSpan>[] data)
        => UnzipValueOptionTimeSpanToUs(new ReadOnlySpan<FSharpValueOption<TimeSpan>>(data));
    // ========================================================================
    // Decimal Support (Option/VOption -> decimal?[])
    // ========================================================================

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static decimal?[] UnwrapOptionDecimal(ReadOnlySpan<FSharpOption<decimal>> data)
    {
        int len = data.Length;
        var result = new decimal?[len];

        ref FSharpOption<decimal> srcRef = ref MemoryMarshal.GetReference(data);
        ref decimal? dstRef = ref MemoryMarshal.GetArrayDataReference(result);

        for (int i = 0; i < len; i++)
        {
            FSharpOption<decimal> item = Unsafe.Add(ref srcRef, i);
            Unsafe.Add(ref dstRef, i) = item?.Value;
        }
        return result;
    }
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static decimal?[] UnwrapValueOptionDecimal(ReadOnlySpan<FSharpValueOption<decimal>> data)
    {
        int len = data.Length;
        var result = new decimal?[len];

        ref FSharpValueOption<decimal> srcRef = ref MemoryMarshal.GetReference(data);
        ref decimal? dstRef = ref MemoryMarshal.GetArrayDataReference(result);

        for (int i = 0; i < len; i++)
        {
            ref FSharpValueOption<decimal> item = ref Unsafe.Add(ref srcRef, i);

            if (item.Tag == FSharpValueOption<decimal>.Tags.ValueSome)
            {
                Unsafe.Add(ref dstRef, i) = item.Value;
            }
            else
            {
                Unsafe.Add(ref dstRef, i) = null;
            }
        }
        return result;
    }
}