using System.Buffers;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Variant;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Wrappers;
using ArrowStructType = Apache.Arrow.Types.StructType;
using DeltaArrayType = Kaminari.Seseragi.Lakehouse.Delta.Schema.ArrayType;
using DeltaStructType = Kaminari.Seseragi.Lakehouse.Delta.Schema.StructType;
using DeltaMapType = Kaminari.Seseragi.Lakehouse.Delta.Schema.MapType;

namespace Polars.NET.Core.Delta;

/// <summary>
/// Variant columns of a scanned Delta table rendered as JSON text (<c>variantAsJson</c>): every variant, at any
/// depth (struct fields, array elements, map keys and values), becomes a string, as Spark's <c>to_json</c> renders it
/// (see <see cref="VariantJson"/>); the structs, lists and maps around them keep their shape.
/// </summary>
internal static class VariantColumns
{
    /// <summary>Whether the type holds a variant at any depth.</summary>
    public static bool HasVariant(DeltaType type) => type switch
    {
        PrimitiveType { Name: PrimitiveType.Variant } => true,
        DeltaStructType s => s.Fields.Any(f => HasVariant(f.Type)),
        DeltaArrayType a => HasVariant(a.ElementType),
        DeltaMapType m => HasVariant(m.KeyType) || HasVariant(m.ValueType),
        _ => false,
    };

    /// <summary>The frame (the table's columns under their logical names) with its variant columns as JSON text.</summary>
    public static LazyFrameHandle AsJson(LazyFrameHandle lf, DeltaStructType schema)
    {
        StructField[] fields = [.. schema.Fields.Where(f => HasVariant(f.Type))];
        if (fields.Length == 0) return lf;
        ExprHandle[] exprs = [.. fields.Select(field =>
        {
            using DataTypeHandle output = DeltaKernelScan.EngineType(field.Type, variantAsJson: true);
            DeltaType type = field.Type;
            ExprHandle mapped = PolarsWrapper.MapMany(PolarsWrapper.Col(field.Name), [], arrays => Convert(arrays[0], type), output);
            return PolarsWrapper.Alias(mapped, field.Name);
        })];
        return PolarsWrapper.LazyWithColumns(lf, exprs);
    }

    /// <summary>The array with every variant in it (as the Delta type says where) replaced by its JSON text.</summary>
    private static IArrowArray Convert(IArrowArray array, DeltaType type)
    {
        if (!HasVariant(type)) return array;
        return (type, array) switch
        {
            (PrimitiveType, StructArray variant) => Json(variant),
            (DeltaStructType s, StructArray structs) => Struct(structs, s),
            (DeltaArrayType a, LargeListArray list) => List(list, a.ElementType),
            (DeltaMapType m, LargeListArray list) => List(list, new DeltaStructType(
            [
                new StructField { Name = "key", Type = m.KeyType },
                new StructField { Name = "value", Type = m.ValueType },
            ])),
            _ => throw new PolarsException($"A variant column came back from the engine as {array.Data.DataType.Name}, not as a struct of binaries."),
        };

        // Variants (structs of their value and metadata binaries) as JSON strings. A null variant is a null struct
        // (the binaries under it hold placeholders) and stays null.
        static StringViewArray Json(StructArray variants)
        {
            var type = (ArrowStructType)variants.Data.DataType;
            IArrowArray value = variants.Fields[type.GetFieldIndex("value")];
            IArrowArray metadata = variants.Fields[type.GetFieldIndex("metadata")];
            var builder = new StringViewArray.Builder();
            var text = new ArrayBufferWriter<byte>();
            for (int i = 0; i < variants.Length; i++)
            {
                if (variants.IsNull(i))
                {
                    builder.AppendNull();
                    continue;
                }
                text.ResetWrittenCount();
                VariantJson.Write(Bytes(metadata, i), Bytes(value, i), text);
                builder.Append(text.WrittenSpan);
            }
            return builder.Build();
        }

        static ReadOnlySpan<byte> Bytes(IArrowArray binaries, int index) => binaries switch
        {
            BinaryViewArray view => view.GetBytes(index),
            LargeBinaryArray large => large.GetBytes(index),
            BinaryArray plain => plain.GetBytes(index),
            _ => throw new PolarsException($"A variant's binaries came back from the engine as {binaries.Data.DataType.Name}."),
        };

        // A struct's children are slices of the struct's rows: converted one by one, the struct rebuilt around them.
        static StructArray Struct(StructArray structs, DeltaStructType type)
        {
            var arrowFields = ((ArrowStructType)structs.Data.DataType).Fields;
            var children = new IArrowArray[arrowFields.Count];
            var fields = new Field[arrowFields.Count];
            for (int i = 0; i < children.Length; i++)
            {
                children[i] = Convert(structs.Fields[i], type.Fields[i].Type);
                fields[i] = new Field(arrowFields[i].Name, children[i].Data.DataType, arrowFields[i].IsNullable);
            }
            var (validity, nulls) = Validity(structs);
            return new StructArray(new ArrowStructType(fields), structs.Length, children, validity, nulls);
        }

        // A list's offsets point into its whole values array (not sliced): all of it is converted, the offsets kept.
        static LargeListArray List(LargeListArray list, DeltaType elementType)
        {
            IArrowArray values = Convert(list.Values, elementType);
            var offsets = new ArrowBuffer.Builder<long>(list.Length + 1);
            offsets.Append(list.ValueOffsets);
            var (validity, nulls) = Validity(list);
            var listType = new LargeListType(new Field(((LargeListType)list.Data.DataType).ValueField.Name, values.Data.DataType, true));
            return new LargeListArray(listType, list.Length, offsets.Build(), values, validity, nulls);
        }

        // The validity bitmap of an array from its first row on (the array may be a slice), and its null count.
        static (ArrowBuffer Bitmap, int Nulls) Validity(IArrowArray array)
        {
            if (array.NullCount == 0) return (ArrowBuffer.Empty, 0);
            var bitmap = new ArrowBuffer.BitmapBuilder(array.Length);
            for (int i = 0; i < array.Length; i++) bitmap.Append(!array.IsNull(i));
            return (bitmap.Build(), array.NullCount);
        }
    }
}
