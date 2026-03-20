namespace Polars.NET.Core.Helpers;

public static partial class ArrayHelper
{
    // =========================================================================
    // Layout Compatibility Flags
    // =========================================================================
    internal static readonly bool IsInt8LayoutCompatible;  // stride 2
    internal static readonly bool IsInt16LayoutCompatible; // stride 4
    internal static readonly bool IsInt32LayoutCompatible; // stride 8
    internal static readonly bool IsInt64LayoutCompatible; // stride 16
    // Int128 Special Flags
    internal static readonly bool IsInt128TypeA; // [Value(16), Bool(1), Pad(15)]
    internal static readonly bool IsInt128TypeB; // [Bool(1), Pad(15), Value(16)] <--- Linux .NET Default
    // internal static readonly bool Int128NeedsSwap; // True if [Hi, Lo] mismatch
    /// <summary>
    /// Check Int32 Memory Layout
    /// </summary>
    static unsafe ArrayHelper()
    {
        // ---------------------------------------------------------
        // 1. Check Int8 (sbyte?) Layout
        // ---------------------------------------------------------
        // Expected Layout : [Bool(1B), Val(1B)] -> Value Offset = 1
        // ---------------------------------------------------------
        try
        {
            sbyte?[] probe = [0x7F]; // MaxValue
            fixed (sbyte?* p = probe)
            {
                byte* b = (byte*)p;
                // Bool @0, Value @1
                bool boolAt0 = *b == 1; 
                bool valAt1 = *(b + 1) == 0x7F;

                IsInt8LayoutCompatible = boolAt0 && valAt1 && (sizeof(sbyte?) == 2);
            }
        }
        catch { IsInt8LayoutCompatible = false; }
        // ---------------------------------------------------------
        // 2. Check Int16 (short?) Layout
        // ---------------------------------------------------------
        // Expected Layout: [Bool(1B), Pad(1B), Val(2B)] -> Value Offset = 2
        // ---------------------------------------------------------
        try
        {
            short?[] probe = [0x1234];
            fixed (short?* p = probe)
            {
                byte* b = (byte*)p;
                // Bool @0, Value @2
                bool boolAt0 = *b == 1;
                bool valAt2 = *(short*)(b + 2) == 0x1234;

                IsInt16LayoutCompatible = boolAt0 && valAt2 && (sizeof(short?) == 4);
            }
        }
        catch { IsInt16LayoutCompatible = false; }
        // Check Int32 Memory Layout [Bool, Pad, Pad, Pad, Int, Int, Int, Int]
        try
        {
            int?[] testProbe = [0x12345678]; // HasValue=True, Value=0x12345678
            fixed (int?* ptr = testProbe)
            {
                byte* bPtr = (byte*)ptr;
                bool hasValueAt0 = *bPtr == 1;
                bool valueAt4 = *(int*)(bPtr + 4) == 0x12345678;

                if (hasValueAt0 && valueAt4 && sizeof(int?) == 8)
                {
                    IsInt32LayoutCompatible = true;
                }
                else
                {
                    IsInt32LayoutCompatible = false;
                }
            }
        }
        catch
        {
            IsInt32LayoutCompatible = false;
        }
        // ---------------------------------------------------------
        // Check Int64 (long?) Layout
        // ---------------------------------------------------------
        // Expected Layout: [Bool(1B), Pad(7B), Val(8B)] -> Value Offset = 8
        // ---------------------------------------------------------
        try
        {
            long?[] probe = [0x1234567890ABCDEF];
            fixed (long?* p = probe)
            {
                byte* b = (byte*)p;
                // Bool @0, Value @8
                bool boolAt0 = *b == 1;
                bool valAt8 = *(long*)(b + 8) == 0x1234567890ABCDEF;

                IsInt64LayoutCompatible = boolAt0 && valAt8 && (sizeof(long?) == 16);
            }
        }
        catch { IsInt64LayoutCompatible = false; }
        // Check Int128 Layout
        try {
            Int128 val = Int128.MaxValue;
            Int128?[] probe = [val];

            fixed (Int128?* p = probe)
            {
                byte* b = (byte*)p;
                
                // Check Type A: Value @ 0, Bool @ 16
                bool valAt0 = *(Int128*)b == val;
                bool boolAt16 = *(b + 16) == 1;
                
                if (valAt0 && boolAt16)
                {
                    IsInt128TypeA = true;
                }
                else
                {
                    // Check Type B: Bool @ 0, Value @ 16
                    // Bool @ 0，Value @ 16
                    bool boolAt0 = *b == 1;
                    bool valAt16 = *(Int128*)(b + 16) == val;

                    if (boolAt0 && valAt16)
                    {
                        IsInt128TypeB = true;
                    }
                }
            }
        }
        catch { /* Ignore */ }

    //     // 3. Detect Int128 Internal Layout (High/Low Swap Check)
    //     try
    //     {
    //         Int128 val = 1; 
            
    //         unsafe
    //         {
    //             byte* p = (byte*)&val;
    //             bool isLittleEndian = (*p == 1);
                
    //             Int128NeedsSwap = !isLittleEndian; 
                
    //         }
    //     }
    //     catch { }
    }
}