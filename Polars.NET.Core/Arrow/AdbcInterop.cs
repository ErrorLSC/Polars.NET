using System;
using System.Reflection;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.C;

namespace Polars.NET.Core.Arrow;

public static unsafe class AdbcInterop
{
    // ==========================================
    // 静态缓存区：爆破后留下的永久通道
    // ==========================================
    private static FieldInfo? s_nativeStatementField;
    private static FieldInfo? s_driverField;
    private static FieldInfo? s_nativeDriverField;
    private static FieldInfo? s_bindStreamField; // 💥 新增：专门用来爆破不可访问的函数指针
    /// <summary>
    /// Safely wraps the Wormhole injection, ADBC execution, and memory cleanup into a single atomic operation.
    /// </summary>
    public static UpdateResult ExecuteIngest(AdbcStatement statement, DataFrameHandle dfHandle)
    {
        IntPtr rawStreamPtr = IntPtr.Zero;
        try
        {
            // 1. Export the C pointer from Rust
            rawStreamPtr = ArrowStreamInterop.ExportToNativeCStream(dfHandle);

            // 2. Fire the Wormhole: Bypass C# ADBC restrictions and inject the pointer directly
            Inject(statement, rawStreamPtr);

            // 3. Pull the trigger: C++ engine pulls the data and returns the result
            return statement.ExecuteUpdate();
        }
        finally
        {
            // 4. Clean up the 40-byte C struct shell safely
            // (The actual data buffers are managed by Rust and freed via the ADBC release callback)
            if (rawStreamPtr != IntPtr.Zero)
            {
                unsafe
                {
                    Apache.Arrow.C.CArrowArrayStream.Free((Apache.Arrow.C.CArrowArrayStream*)rawStreamPtr);
                }
            }
        }
    }
    private static void EnsureInitialized(AdbcStatement stmt)
    {
        if (s_nativeStatementField != null) return;

        const BindingFlags BF = BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.IgnoreCase;
        
        var stmtType = stmt.GetType(); 

        s_nativeStatementField = stmtType.GetField("_nativeStatement", BF)
            ?? throw new InvalidOperationException("Wormhole: 找不到 _nativeStatement 字段。");

        s_driverField = stmtType.GetField("_driver", BF)
            ?? throw new InvalidOperationException("Wormhole: 找不到 _driver 字段。");

        object driverObj = s_driverField.GetValue(stmt)!;
        foreach (var f in driverObj.GetType().GetFields(BF))
        {
            if (f.FieldType.Name.Contains("CAdbcDriver"))
            {
                s_nativeDriverField = f;
                break;
            }
        }

        if (s_nativeDriverField == null)
            throw new InvalidOperationException("Wormhole: 找不到底层 CAdbcDriver 结构体。");

        // 💥 提前安放炸药：锁定那个“不可访问”的函数指针字段
        s_bindStreamField = typeof(CAdbcDriver).GetField("StatementBindStream", BF)
            ?? throw new InvalidOperationException("Wormhole: 找不到 StatementBindStream 函数指针！");
    }

    private static void Inject(AdbcStatement stmt, IntPtr rawStreamPtr)
    {
        EnsureInitialized(stmt);

        var stream = (Apache.Arrow.C.CArrowArrayStream*)rawStreamPtr;

        CAdbcStatement cStmt = (CAdbcStatement)s_nativeStatementField!.GetValue(stmt)!;
        object driverObj = s_driverField!.GetValue(stmt)!;

        // 提取 CAdbcDriver 结构体
        CAdbcDriver cDriver;
        if (s_nativeDriverField!.FieldType.IsPointer) {
            cDriver = *(CAdbcDriver*)Pointer.Unbox(s_nativeDriverField.GetValue(driverObj)!);
        } else {
            cDriver = (CAdbcDriver)s_nativeDriverField.GetValue(driverObj)!;
        }

        // 💥 轰炸时刻：无视保护级别，强行从结构体里抠出底层指针！
        object bindVal = s_bindStreamField!.GetValue(cDriver)!;
        void* ptr = bindVal switch
        {
            IntPtr i => (void*)i,
            UIntPtr u => (void*)u,
            _ => (void*)Convert.ToInt64(bindVal)
        };

        // 强转为可执行的非托管函数委托
        var bind = (delegate* unmanaged<CAdbcStatement*, Apache.Arrow.C.CArrowArrayStream*, CAdbcError*, AdbcStatusCode>)ptr;

        // 扣动扳机
        CAdbcError err = default;
        var status = bind(&cStmt, stream, &err);

        if (status != AdbcStatusCode.Success)
        {
            throw new Exception($"ADBC Inject failed! 底层拒收，错误码: {status}");
        }

        s_nativeStatementField.SetValue(stmt, cStmt);
    }
}