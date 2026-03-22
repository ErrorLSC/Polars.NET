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

            MapType => throw new NotSupportedException(
                "Dynamic Map types cannot be directly consumed by ML.NET pipelines. Please unnest, explode, or convert to Struct/List features using Polars before creating the IDataView."),
            Decimal128Type or Decimal256Type => throw new NotSupportedException(
                "ML.NET trainers operate on float/double. Please cast your Decimal column to Float64 in Polars (e.g., df.Select(Col(\"Price\").Cast(DataType.Float64))) before exporting to IDataView."),
            
            _ => throw new NotSupportedException(
                $"Arrow type '{arrowType.Name}' is currently not supported in ML.NET DataView mapping.")
        };
    }
    /// <summary>
    /// Recursively resolves an ML.NET DataViewType into an Arrow Type.
    /// </summary>
    public static IArrowType GetArrowType(DataViewType dataViewType)
    {
        return dataViewType switch
        {
            // ==========================================
            // Primitives
            // ==========================================
            NumberDataViewType n when n == NumberDataViewType.Single => FloatType.Default,
            NumberDataViewType n when n == NumberDataViewType.Double => DoubleType.Default,
            NumberDataViewType n when n == NumberDataViewType.SByte => Int8Type.Default,
            NumberDataViewType n when n == NumberDataViewType.Int16 => Int16Type.Default,
            NumberDataViewType n when n == NumberDataViewType.Int32 => Int32Type.Default,
            NumberDataViewType n when n == NumberDataViewType.Int64 => Int64Type.Default,
            NumberDataViewType n when n == NumberDataViewType.Byte => UInt8Type.Default,
            NumberDataViewType n when n == NumberDataViewType.UInt16 => UInt16Type.Default,
            NumberDataViewType n when n == NumberDataViewType.UInt32 => UInt32Type.Default,
            NumberDataViewType n when n == NumberDataViewType.UInt64 => UInt64Type.Default,

            // ==========================================
            // Text and Bool
            // ==========================================
            TextDataViewType => StringViewType.Default,
            BooleanDataViewType => BooleanType.Default,

            // ==========================================
            // Time
            // ==========================================
            DateTimeDataViewType => new TimestampType(TimeUnit.Microsecond, timezone: null as string),
            TimeSpanDataViewType => DurationType.FromTimeUnit(TimeUnit.Microsecond),

            // ==========================================
            // Tensors / Vectors
            // ==========================================
            VectorDataViewType v when v.ItemType == NumberDataViewType.Single 
                => new FixedSizeListType(FloatType.Default, v.Size),
                
            VectorDataViewType v when v.ItemType == NumberDataViewType.Int32 
                => new FixedSizeListType(Int32Type.Default, v.Size),

            // ==========================================
            // Fallback
            // ==========================================
            _ => throw new NotSupportedException(
                $"ML.NET DataViewType '{dataViewType.RawType.Name}' is not supported in Arrow mapping.")
        };
    }
}