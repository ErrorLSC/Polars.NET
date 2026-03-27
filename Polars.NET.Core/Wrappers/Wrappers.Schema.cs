using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using Apache.Arrow.Types;
using Polars.NET.Core.Arrow;
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
    {
        bool success = NativeBindings.pl_schema_len(schema,out uint len);
        
        ErrorHelper.CheckBool(success); 
        
        return len;
    }
    /// <summary>
    /// Get Schema Field by Index
    /// </summary>
    public static void GetSchemaFieldAt(SchemaHandle schema, ulong index, out string name, out DataTypeHandle typeHandle)
    {
        bool success = NativeBindings.pl_schema_get_at_index(
                schema, 
                (UIntPtr)index, 
                out IntPtr namePtr, 
                out var outTypeHandle
            );

        ErrorHelper.CheckBool(success); 

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
    /// <summary>
    /// Create a SchemaHandle directly from a .NET Type by leveraging Apache Arrow Type resolution.
    /// </summary>
    public static SchemaHandle NewSchemaFromType(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)]
        Type type)
    {
        ArgumentNullException.ThrowIfNull(type);

        var members = ArrowTypeResolver.GetReadableMembers(type);
        
        var names = new string[members.Length];
        var typeHandles = new DataTypeHandle[members.Length];

        for (int i = 0; i < members.Length; i++)
        {
            var member = members[i];
            names[i] = member.Name;
            
            var memberType = ArrowTypeResolver.GetMemberType(member);
            var arrowType = ArrowTypeResolver.GetArrowTypeFromNetType(memberType);
            
            typeHandles[i] = MapArrowToDataTypeHandle(arrowType);
        }

        return NewSchema(names, typeHandles);
    }

    /// <summary>
    /// Maps an Apache Arrow IArrowType directly to a native Polars DataTypeHandle.
    /// </summary>
    private static DataTypeHandle MapArrowToDataTypeHandle(IArrowType arrowType)
    {
        return arrowType switch
        {
            BooleanType => NewPrimitiveType((int)PlDataType.Boolean),
            Int8Type => NewPrimitiveType((int)PlDataType.Int8),
            Int16Type => NewPrimitiveType((int)PlDataType.Int16),
            Int32Type => NewPrimitiveType((int)PlDataType.Int32),
            Int64Type => NewPrimitiveType((int)PlDataType.Int64),
            UInt8Type => NewPrimitiveType((int)PlDataType.UInt8),
            UInt16Type => NewPrimitiveType((int)PlDataType.UInt16),
            UInt32Type => NewPrimitiveType((int)PlDataType.UInt32),
            UInt64Type => NewPrimitiveType((int)PlDataType.UInt64),
            HalfFloatType => NewPrimitiveType((int)PlDataType.Float16),
            FloatType => NewPrimitiveType((int)PlDataType.Float32),
            DoubleType => NewPrimitiveType((int)PlDataType.Float64),
            Decimal128Type d128 => NewDecimalType(d128.Precision, d128.Scale),
            Decimal256Type d256 => NewDecimalType(d256.Precision, d256.Scale),
            // String & Binary
            StringType or StringViewType or LargeStringType => NewPrimitiveType((int)PlDataType.String),
            BinaryType or BinaryViewType or LargeBinaryType or FixedSizeBinaryType => NewPrimitiveType((int)PlDataType.Binary),
            
            
            // Date & Time
            Date32Type or Date64Type => NewPrimitiveType((int)PlDataType.Date),
            Time32Type or Time64Type => NewPrimitiveType((int)PlDataType.Time),
            
            TimestampType ts => NewDateTimeType(
                ts.Unit switch
                {
                    TimeUnit.Nanosecond => 0,   // ns
                    TimeUnit.Microsecond => 1,  // us
                    TimeUnit.Millisecond => 2,  // ms
                    _ => 1
                }, ts.Timezone),
                
            DurationType dur => NewDurationType(
                dur.Unit switch
                {
                    TimeUnit.Nanosecond => 0,
                    TimeUnit.Microsecond => 1,
                    TimeUnit.Millisecond => 2,
                    _ => 1
                }),
                
            ListType list => NewListType(MapArrowToDataTypeHandle(list.ValueDataType)),
            LargeListType lList => NewListType(MapArrowToDataTypeHandle(lList.ValueDataType)),
            FixedSizeListType fList => NewArrayType(MapArrowToDataTypeHandle(fList.ValueDataType), (uint)fList.ListSize),
            
            StructType str => NewStructType(
                [.. str.Fields.Select(f => f.Name)],
                [.. str.Fields.Select(f => MapArrowToDataTypeHandle(f.DataType))]),
                
            NullType => NewPrimitiveType((int)PlDataType.Null),
            _ => NewPrimitiveType((int)PlDataType.Unknown)
        };
    }
}