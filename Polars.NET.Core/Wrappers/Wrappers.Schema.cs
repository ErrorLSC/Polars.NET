using System.Runtime.InteropServices;
using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public static partial class PolarsWrapper
{
    /// <summary>
    /// Create Blank Schema
    /// </summary>
    public static SchemaHandle SchemaCreate()
    {
        return NewSchema([], []);
    }

    /// <summary>
    /// Build Schema from name and dtype
    /// </summary>
    public static SchemaHandle NewSchema(string[] names, DataTypeHandle[] types)
    {
        if (names.Length != types.Length)
            throw new ArgumentException("Names and Types must have same length");

        var typePtrs = HandlesToPtrs(types);

        return UseUtf8StringArray(names, (namePtrs) => 
        {
            return ErrorHelper.Check(
                NativeBindings.pl_schema_new(namePtrs, typePtrs, (UIntPtr)names.Length)
            );
        });
    }
    /// <summary>
    /// Get the length of Schema
    /// </summary>
    public static ulong GetSchemaLen(SchemaHandle schema)
        => NativeBindings.pl_schema_len(schema);
    /// <summary>
    /// Get Schema Field by Index
    /// </summary>
    public static void GetSchemaFieldAt(SchemaHandle schema, ulong index, out string name, out DataTypeHandle typeHandle)
    {
        NativeBindings.pl_schema_get_at_index(
            schema, 
            (UIntPtr)index, 
            out IntPtr namePtr, 
            out var outTypeHandle
        );

        typeHandle = ErrorHelper.Check(outTypeHandle);

        name = ErrorHelper.CheckString(namePtr);
    }
    public static void SchemaAddField(SchemaHandle schema, string name, DataTypeHandle dtype)
    {
        if (schema.IsInvalid) throw new ArgumentException("Schema handle is invalid");
        if (dtype.IsInvalid) throw new ArgumentException("DataType handle is invalid");
        using var handlesLock = new SafeHandleLock<SafeHandle>([schema, dtype]);
        NativeBindings.pl_schema_add_field(
            handlesLock.Pointers[0],
            name,
            handlesLock.Pointers[1]
        );

        ErrorHelper.CheckVoid(); 
    }
}