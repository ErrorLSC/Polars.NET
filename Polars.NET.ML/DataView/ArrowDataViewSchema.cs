using Apache.Arrow.Types;
using Microsoft.ML.Data; 

namespace Polars.NET.ML.DataView;

/// <summary>
/// Maps Apache Arrow types to ML.NET DataView types.
/// </summary>
internal static class ArrowDataViewMapper
{
    /// <summary>
    /// Recursively resolves an Arrow Type into an ML.NET DataViewType.
    /// </summary>
    public static DataViewType GetDataViewType(IArrowType arrowType)
    {
        return arrowType switch
        {
            // ==========================================
            // Premitives
            // ==========================================
            Int8Type => NumberDataViewType.SByte,
            Int16Type => NumberDataViewType.Int16,
            Int32Type => NumberDataViewType.Int32,
            Int64Type => NumberDataViewType.Int64,
            UInt8Type => NumberDataViewType.Byte,
            UInt16Type => NumberDataViewType.UInt16,
            UInt32Type => NumberDataViewType.UInt32,
            UInt64Type => NumberDataViewType.UInt64,
            FloatType => NumberDataViewType.Single,
            DoubleType => NumberDataViewType.Double,
            BooleanType => BooleanDataViewType.Instance,
            
            // ==========================================
            // String and DateTime
            // ==========================================
            StringType or LargeStringType or StringViewType => TextDataViewType.Instance,
            TimestampType => DateTimeDataViewType.Instance,
            DurationType => TimeSpanDataViewType.Instance,

            // ==========================================
            // Tensors
            // ==========================================
            
            FixedSizeListType fsList => new VectorDataViewType(
                itemType: (PrimitiveDataViewType)GetDataViewType(fsList.ValueDataType), 
                size: fsList.ListSize),
                
            ListType list => new VectorDataViewType(
                itemType: (PrimitiveDataViewType)GetDataViewType(list.ValueDataType), 
                size: 0),

            // ==========================================
            // Categorical (Dictionary)
            // ==========================================
            DictionaryType dict => dict.IndexType switch
            {
                Int8Type or UInt8Type => new KeyDataViewType(typeof(byte), 0),
                Int16Type or UInt16Type => new KeyDataViewType(typeof(ushort), 0),
                Int32Type or UInt32Type => new KeyDataViewType(typeof(uint), 0),
                Int64Type or UInt64Type => new KeyDataViewType(typeof(ulong), 0),
                
                _ => throw new NotSupportedException($"Dictionary index type '{dict.IndexType.Name}' is not supported as ML.NET KeyDataViewType.")
            },
            
            _ => throw new NotSupportedException(
                $"Arrow type '{arrowType.Name}' is currently not supported in ML.NET DataView mapping.")
        };
    }
}