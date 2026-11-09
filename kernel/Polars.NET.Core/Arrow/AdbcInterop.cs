using System.Reflection;
using System.Runtime.CompilerServices;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.C;

namespace Polars.NET.Core.Arrow;

public static unsafe class AdbcInterop
{
    private static FieldInfo? s_nativeDriverField;
    private static FieldInfo? s_bindStreamField;

    private static int s_nativeStatementOffset = -1;
    private static int s_driverOffset = -1;

    private class RawDataScanner { public byte Data; }

    public static UpdateResult ExecuteIngest(AdbcStatement statement, DataFrameHandle dfHandle)
    {
        IntPtr rawStreamPtr = IntPtr.Zero;
        try
        {
            rawStreamPtr = ArrowStreamInterop.ExportToNativeCStream(dfHandle);
            Inject(statement, rawStreamPtr);
            return statement.ExecuteUpdate();
        }
        finally
        {
            if (rawStreamPtr != IntPtr.Zero)
            {
                Apache.Arrow.C.CArrowArrayStream.Free((Apache.Arrow.C.CArrowArrayStream*)rawStreamPtr);
            }
        }
    }

    private static void EnsureInitialized(AdbcStatement stmt)
    {
        if (s_nativeStatementOffset != -1) return;

        const BindingFlags BF = BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.IgnoreCase;
        var stmtType = stmt.GetType();

        var nativeStatementField = stmtType.GetField("_nativeStatement", BF)
            ?? throw new InvalidOperationException("Wormhole: Failed to find _nativeStatement");

        var driverField = stmtType.GetField("_driver", BF)
            ?? throw new InvalidOperationException("Wormhole: Failed to find _driver");

        // ==========================================
        // Sniff _nativeStatement _driver offset
        // ==========================================
        object boxedStmtStruct = nativeStatementField.GetValue(stmt)!;
        object driverObjRef = driverField.GetValue(stmt)!;

        ref byte stmtPayload = ref Unsafe.As<RawDataScanner>(stmt).Data;
        int structSize = sizeof(CAdbcStatement);

        // Find _nativeStatement
        for (int offset = 0; offset < 128; offset++)
        {
            ref byte candidate = ref Unsafe.Add(ref stmtPayload, offset);
            ref byte target = ref Unsafe.As<RawDataScanner>(boxedStmtStruct).Data;

            bool match = true;
            for (int i = 0; i < structSize; i++)
            {
                if (Unsafe.Add(ref candidate, i) != Unsafe.Add(ref target, i)) { match = false; break; }
            }
            if (match) { s_nativeStatementOffset = offset; break; }
        }

        // Find _driver
        for (int offset = 0; offset < 128; offset++)
        {
            ref object candidateObj = ref Unsafe.As<byte, object>(ref Unsafe.Add(ref stmtPayload, offset));
            if (ReferenceEquals(candidateObj, driverObjRef))
            {
                s_driverOffset = offset;
                break;
            }
        }

        if (s_nativeStatementOffset == -1 || s_driverOffset == -1)
            throw new Exception("Wormhole: Offset sniffing failed for AdbcStatement!");

        foreach (var f in driverObjRef.GetType().GetFields(BF))
        {
            if (f.FieldType.Name.Contains("CAdbcDriver")) { s_nativeDriverField = f; break; }
        }
        s_bindStreamField = typeof(CAdbcDriver).GetField("StatementBindStream", BF);
    }

    private static void Inject(AdbcStatement stmt, IntPtr rawStreamPtr)
    {
        EnsureInitialized(stmt);
        var stream = (Apache.Arrow.C.CArrowArrayStream*)rawStreamPtr;

        // Get statement object first mem address
        ref byte payloadStart = ref Unsafe.As<RawDataScanner>(stmt).Data;

        // Get _driver ref
        ref object driverObj = ref Unsafe.As<byte, object>(ref Unsafe.Add(ref payloadStart, s_driverOffset));

        // Get _nativeStatement ref
        ref CAdbcStatement cStmtRef = ref Unsafe.As<byte, CAdbcStatement>(ref Unsafe.Add(ref payloadStart, s_nativeStatementOffset));

        CAdbcDriver cDriver;
        if (s_nativeDriverField!.FieldType.IsPointer)
        {
            cDriver = *(CAdbcDriver*)Pointer.Unbox(s_nativeDriverField.GetValue(driverObj)!);
        }
        else
        {
            cDriver = (CAdbcDriver)s_nativeDriverField.GetValue(driverObj)!;
        }

        object bindVal = s_bindStreamField!.GetValue(cDriver)!;
        void* ptr = bindVal switch
        {
            IntPtr i => (void*)i,
            UIntPtr u => (void*)u,
            _ => (void*)Convert.ToInt64(bindVal)
        };

        var bind = (delegate* unmanaged<CAdbcStatement*, Apache.Arrow.C.CArrowArrayStream*, CAdbcError*, AdbcStatusCode>)ptr;
        CAdbcError err = default;

        // Set ref as pointer
        fixed (CAdbcStatement* pStmt = &cStmtRef)
        {
            var status = bind(pStmt, stream, &err);
            if (status != AdbcStatusCode.Success)
            {
                throw new Exception($"ADBC Inject failed! ErrorCode: {status}");
            }
        }
    }
}