using Apache.Arrow.Types;

namespace Kaminari.Sazanami.Arrow;

/// <summary>Comparisons of Arrow types.</summary>
internal static class ArrowTypes
{
    /// <summary>
    /// Whether two Arrow types hold their values the same way: the type and its parameters, struct fields by name and
    /// order; the nullability of nested fields is not compared.
    /// </summary>
    public static bool SameType(IArrowType a, IArrowType b) => (a, b) switch
    {
        (StructType x, StructType y) => x.Fields.Count == y.Fields.Count && x.Fields.Zip(y.Fields).All(p => p.First.Name == p.Second.Name && SameType(p.First.DataType, p.Second.DataType)),
        (ListType x, ListType y) => SameType(x.ValueDataType, y.ValueDataType),
        (LargeListType x, LargeListType y) => SameType(x.ValueDataType, y.ValueDataType),
        (ListViewType x, ListViewType y) => SameType(x.ValueDataType, y.ValueDataType),
        (LargeListViewType x, LargeListViewType y) => SameType(x.ValueDataType, y.ValueDataType),
        (FixedSizeListType x, FixedSizeListType y) => x.ListSize == y.ListSize && SameType(x.ValueDataType, y.ValueDataType),
        (MapType x, MapType y) => SameType(x.KeyField.DataType, y.KeyField.DataType) && SameType(x.ValueField.DataType, y.ValueField.DataType),
        (RunEndEncodedType x, RunEndEncodedType y) => SameType(x.RunEndsDataType, y.RunEndsDataType) && SameType(x.ValuesDataType, y.ValuesDataType),
        (DictionaryType x, DictionaryType y) => SameType(x.IndexType, y.IndexType) && SameType(x.ValueType, y.ValueType),
        (TimestampType x, TimestampType y) => x.Unit == y.Unit && (x.Timezone ?? "") == (y.Timezone ?? ""),
        (Time32Type x, Time32Type y) => x.Unit == y.Unit,
        (Time64Type x, Time64Type y) => x.Unit == y.Unit,
        (DurationType x, DurationType y) => x.Unit == y.Unit,
        (IntervalType x, IntervalType y) => x.Unit == y.Unit,
        (Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type, _) =>
            a.TypeId == b.TypeId && DecimalParameters(a) == DecimalParameters(b),
        (FixedSizeBinaryType x, FixedSizeBinaryType y) => a.TypeId == b.TypeId && x.ByteWidth == y.ByteWidth,
        _ => a.TypeId == b.TypeId,
    };

    /// <summary>The precision and scale of a decimal type.</summary>
    public static (int Precision, int Scale) DecimalParameters(IArrowType type) => type switch
    {
        Decimal32Type d => (d.Precision, d.Scale),
        Decimal64Type d => (d.Precision, d.Scale),
        Decimal128Type d => (d.Precision, d.Scale),
        Decimal256Type d => (d.Precision, d.Scale),
        _ => throw new ArgumentException($"{type.Name} is not a decimal type.", nameof(type)),
    };
}
