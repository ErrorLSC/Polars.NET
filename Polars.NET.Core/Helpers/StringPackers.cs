using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Unicode; 

namespace Polars.NET.Core.Helpers;

[StructLayout(LayoutKind.Explicit, Size = 16)]
public unsafe struct ArrowStringView
{
    [FieldOffset(0)] public int Length;
    [FieldOffset(4)] public fixed byte InlineData[12];
    [FieldOffset(4)] public fixed byte Prefix[4];
    [FieldOffset(8)] public int BufferIndex;
    [FieldOffset(12)] public int Offset;
}

public static unsafe class StringPacker
{
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (ArrowStringView[] views, byte[]? dataBuffer, byte[]? validity) PackStringView(string?[] data)
    {
        int len = data.Length;
        
        // 【神之一手】：提前分配 Views 数组，它不仅是返回值，更是我们的“零成本缓存器”！
        var views = GC.AllocateUninitializedArray<ArrowStringView>(len);
        
        long totalDataSize = 0;
        bool hasNull = false;

        // Pass 1: 计算并“完美白嫖” views[i].Length 缓存
        fixed (ArrowStringView* pViews = views)
        {
            // 批量清零，保证 Arrow 内存规范无脏数据
            Unsafe.InitBlock(pViews, 0, (uint)(len * sizeof(ArrowStringView)));

            for (int i = 0; i < len; i++)
            {
                string? s = data[i];
                if (s != null)
                {
                    // 依然使用底层最快的 GetByteCount 计算所需容量
                    int byteCount = Encoding.UTF8.GetByteCount(s);
                    
                    // 【缓存生效】：直接写进结果视图里，第二遍不用算了！
                    pViews[i].Length = byteCount; 
                    
                    if (byteCount > 12)
                    {
                        totalDataSize += byteCount;
                    }
                }
                else
                {
                    hasNull = true;
                }
            }
        }

        byte[]? dataBuffer = totalDataSize > 0 
            ? GC.AllocateUninitializedArray<byte>((int)totalDataSize) 
            : null;

        byte[]? validity = null;
        if (hasNull)
        {
            int validLen = (len + 7) >> 3;
            validity = new byte[validLen];
            Array.Fill(validity, (byte)0xFF); 
        }

        // Pass 2: 极速填充 (使用 .NET 8 Utf8.FromUtf16)
        int currentDataOffset = 0;
        fixed (ArrowStringView* pViews = views)
        fixed (byte* pDataBuffer = dataBuffer) 
        fixed (byte* pValid = validity)
        {
            for (int i = 0; i < len; i++)
            {
                string? s = data[i];

                // 优化 1：Guard Clause 拦截 null，代码瞬间拉平！
                if (s is null)
                {
                    if (pValid != null)
                    {
                        pValid[i >> 3] &= (byte)~(1 << (i & 7));
                    }
                    continue; // 直接下一条，干脆利落
                }

                // 优化 2：直接读取 Pass 1 的缓存，0 额外开销
                int byteCount = pViews[i].Length;

                if (byteCount <= 12)
                {
                    // 极速内联路径 (.NET 8 SIMD)
                    if (byteCount > 0)
                    {
                        Utf8.FromUtf16(
                            s, 
                            new Span<byte>(pViews[i].InlineData, 12), 
                            out _, out _, 
                            replaceInvalidSequences: false
                        );
                    }
                }
                else
                {
                    // 长字符串分配路径
                    byte* targetDataPtr = pDataBuffer + currentDataOffset;
                    
                    Utf8.FromUtf16(
                        s, 
                        new Span<byte>(targetDataPtr, byteCount), 
                        out _, out _, 
                        replaceInvalidSequences: false
                    );
                    
                    Unsafe.CopyBlock(pViews[i].Prefix, targetDataPtr, 4);
                    
                    pViews[i].BufferIndex = 0; 
                    pViews[i].Offset = currentDataOffset;
                    
                    currentDataOffset += byteCount;
                }
            }
        }

        return (views, dataBuffer, validity);
    }
}