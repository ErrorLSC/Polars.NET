using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Marshalling;
using Microsoft.FSharp.Collections;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Helpers;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Linq.Backend;

/// <summary>Maps IR vocabulary (logical types and relational / function enums) to native Polars counterparts.</summary>
internal static class PolarsVocabulary
{
    /// <summary>Materializes a LogicalType as a native Polars DataTypeHandle (Arrow types via the shared Arrow mapping).</summary>
    public static DataTypeHandle ToDataTypeHandle(LogicalType t)
    {
        switch (t)
        {
            case LogicalType.Arrow arrow:
                return PolarsWrapper.MapArrowToDataTypeHandle(arrow.arrowType);
            case LogicalType.ListOf list:
                return PolarsWrapper.NewListType(ToDataTypeHandle(list.element));
            case LogicalType.StructOf st:
                return PolarsWrapper.NewStructType([.. st.fields.Select(f => f.Item1)], [.. st.fields.Select(f => ToDataTypeHandle(f.Item2))]);
            case LogicalType.Enum e:
                using (var frozen = PolarsWrapper.FrozenCategoriesNew([.. e.categories]))
                    return PolarsWrapper.NewEnumType(frozen);
            default:
                return PolarsWrapper.NewPrimitiveType((int)PlDataType.Unknown);
        }
    }

    /// <summary>
    /// The categories Polars records in the metadata of a nested Enum field: each one prefixed with its UTF-8 byte
    /// length and ';'.
    /// </summary>
    private static FSharpList<string>? TryEnumCategories(Field field)
    {
        if (field.Metadata is null || !field.Metadata.TryGetValue(EnumCategories.EnumMetadataKey, out string? encoded)) return null;
        byte[] bytes = Encoding.UTF8.GetBytes(encoded);
        List<string> categories = [];
        int pos = 0;
        while (pos < bytes.Length)
        {
            int sep = System.Array.IndexOf(bytes, (byte)';', pos);
            int len = int.Parse(Encoding.ASCII.GetString(bytes, pos, sep - pos));
            categories.Add(Encoding.UTF8.GetString(bytes, sep + 1, len));
            pos = sep + 1 + len;
        }
        return ListModule.OfSeq(categories);
    }

    private static bool HasEnum(LogicalType t) => t switch
    {
        LogicalType.Enum => true,
        LogicalType.ListOf list => HasEnum(list.element),
        LogicalType.StructOf st => st.fields.Any(f => HasEnum(f.Item2)),
        _ => false,
    };

    /// <summary>
    /// An Arrow export as an IR type: an Enum nested in a list or struct (field metadata in Arrow) becomes ListOf /
    /// StructOf of LogicalType.Enum; any other type stays Arrow.
    /// </summary>
    private static LogicalType OfArrowType(IArrowType t)
    {
        LogicalType? nested = null;
        switch (t)
        {
            case StructType st:
                var fields = ListModule.OfSeq(st.Fields.Select(f => Tuple.Create(f.Name, OfArrowField(f))));
                if (fields.Any(f => HasEnum(f.Item2))) nested = LogicalType.NewStructOf(fields);
                break;
            case ListType lt:
                var element = OfArrowField(lt.ValueField);
                if (HasEnum(element)) nested = LogicalType.NewListOf(element);
                break;
            case LargeListType llt:
                var largeElement = OfArrowField(llt.ValueField);
                if (HasEnum(largeElement)) nested = LogicalType.NewListOf(largeElement);
                break;
        }
        return nested ?? LogicalType.NewArrow(t);
    }

    private static LogicalType OfArrowField(Field field) =>
        TryEnumCategories(field) is { } categories ? LogicalType.NewEnum(categories) : OfArrowType(field.DataType);

    /// <summary>
    /// The logical type of a native Polars data type: Enum types with their categories, every other type through its
    /// Arrow export, and Unknown for a type Arrow cannot express.
    /// </summary>
    public static LogicalType OfDataTypeHandle(DataTypeHandle dataType)
    {
        if (PolarsWrapper.GetDataTypeKind(dataType) == PlDataType.Enum)
            return LogicalType.NewEnum(ListModule.OfArray(PolarsSchemaExtractor.GetEnumCategories(dataType)));
        try
        {
            return OfArrowType(ArrowFfiBridge.ImportDataType(dataType));
        }
        catch (Exception e) when (e is NotSupportedException or ArgumentException or PolarsException)
        {
            return LogicalType.Unknown;
        }
    }

    public static PlJoinType JoinType(JoinKind kind) => kind.Tag switch
    {
        JoinKind.Tags.Inner => PlJoinType.Inner,
        JoinKind.Tags.Left => PlJoinType.Left,
        JoinKind.Tags.Full => PlJoinType.Outer,
        JoinKind.Tags.Cross => PlJoinType.Cross,
        JoinKind.Tags.Semi => PlJoinType.Semi,
        JoinKind.Tags.Anti => PlJoinType.Anti,
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    /// <summary>
    /// LINQ joins yield rows in outer order, and the matches of one outer row in inner order. A right join reaches the
    /// IR with its sides swapped, so this is also the right join's order. Semi and anti joins filter the left side and
    /// keep its order.
    /// A cross join keeps outer-then-inner order too (the streaming engine would not); a filtered one is lowered
    /// unconstrained, so that the engine can run it as a join on the filter's conditions, and ordered afterwards (see
    /// PolarsPhysicalLowerer.LowerFilteredCrossJoin).
    /// </summary>
    public static PlJoinMaintainOrder JoinMaintainOrder(JoinKind kind) => kind.Tag switch
    {
        JoinKind.Tags.Semi or JoinKind.Tags.Anti => PlJoinMaintainOrder.Left,
        JoinKind.Tags.Inner or JoinKind.Tags.Left or JoinKind.Tags.Full => PlJoinMaintainOrder.LeftRight,
        JoinKind.Tags.Cross => PlJoinMaintainOrder.LeftRight,
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    public static PlJoinCoalesce JoinCoalesce(JoinKeyCoalescing coalescing) => coalescing.Tag switch
    {
        JoinKeyCoalescing.Tags.Default => PlJoinCoalesce.JoinSpecific,
        JoinKeyCoalescing.Tags.Coalesce => PlJoinCoalesce.CoalesceColumns,
        JoinKeyCoalescing.Tags.KeepBoth => PlJoinCoalesce.KeepColumns,
        _ => throw new ArgumentOutOfRangeException(nameof(coalescing)),
    };

    public static PlUniqueKeepStrategy UniqueKeep(DistinctKeep keep) => keep.Tag switch
    {
        DistinctKeep.Tags.First => PlUniqueKeepStrategy.First,
        DistinctKeep.Tags.Last => PlUniqueKeepStrategy.Last,
        DistinctKeep.Tags.Any => PlUniqueKeepStrategy.Any,
        DistinctKeep.Tags.None => PlUniqueKeepStrategy.None,
        _ => throw new ArgumentOutOfRangeException(nameof(keep)),
    };

    public static PlAsofStrategy AsofStrategy(Kaminari.Samidare.IR.AsofStrategy strategy) => strategy.Tag switch
    {
        Kaminari.Samidare.IR.AsofStrategy.Tags.Backward => PlAsofStrategy.Backward,
        Kaminari.Samidare.IR.AsofStrategy.Tags.Forward => PlAsofStrategy.Forward,
        Kaminari.Samidare.IR.AsofStrategy.Tags.Nearest => PlAsofStrategy.Nearest,
        _ => throw new ArgumentOutOfRangeException(nameof(strategy)),
    };

    public static PlRoundMode RoundMode(Kaminari.Samidare.IR.RoundMode mode) => mode.Tag switch
    {
        Kaminari.Samidare.IR.RoundMode.Tags.HalfToEven => PlRoundMode.HalfToEven,
        Kaminari.Samidare.IR.RoundMode.Tags.HalfAwayFromZero => PlRoundMode.HalfAwayFromZero,
        Kaminari.Samidare.IR.RoundMode.Tags.ToZero => PlRoundMode.ToZero,
        _ => throw new ArgumentOutOfRangeException(nameof(mode)),
    };

    public static PlRankMethod RankMethod(Kaminari.Samidare.IR.RankMethod method) => method.Tag switch
    {
        Kaminari.Samidare.IR.RankMethod.Tags.Average => PlRankMethod.Average,
        Kaminari.Samidare.IR.RankMethod.Tags.Min => PlRankMethod.Min,
        Kaminari.Samidare.IR.RankMethod.Tags.Max => PlRankMethod.Max,
        Kaminari.Samidare.IR.RankMethod.Tags.Dense => PlRankMethod.Dense,
        Kaminari.Samidare.IR.RankMethod.Tags.Ordinal => PlRankMethod.Ordinal,
        Kaminari.Samidare.IR.RankMethod.Tags.Random => PlRankMethod.Random,
        _ => throw new ArgumentOutOfRangeException(nameof(method)),
    };

    public static PlInterpolationMethod InterpolationMethod(Kaminari.Samidare.IR.InterpolationMethod method) => method.Tag switch
    {
        Kaminari.Samidare.IR.InterpolationMethod.Tags.Linear => PlInterpolationMethod.Linear,
        Kaminari.Samidare.IR.InterpolationMethod.Tags.Nearest => PlInterpolationMethod.Nearest,
        _ => throw new ArgumentOutOfRangeException(nameof(method)),
    };

    public static PlQuantileMethod QuantileMethod(Kaminari.Samidare.IR.QuantileMethod method) => method.Tag switch
    {
        Kaminari.Samidare.IR.QuantileMethod.Tags.Nearest => PlQuantileMethod.Nearest,
        Kaminari.Samidare.IR.QuantileMethod.Tags.Higher => PlQuantileMethod.Higher,
        Kaminari.Samidare.IR.QuantileMethod.Tags.Lower => PlQuantileMethod.Lower,
        Kaminari.Samidare.IR.QuantileMethod.Tags.Midpoint => PlQuantileMethod.Midpoint,
        Kaminari.Samidare.IR.QuantileMethod.Tags.Linear => PlQuantileMethod.Linear,
        Kaminari.Samidare.IR.QuantileMethod.Tags.Equiprobable => PlQuantileMethod.Equiprobable,
        _ => throw new ArgumentOutOfRangeException(nameof(method)),
    };

    public static PlNullBehavior NullBehavior(Kaminari.Samidare.IR.NullBehavior behavior) => behavior.Tag switch
    {
        Kaminari.Samidare.IR.NullBehavior.Tags.Ignore => PlNullBehavior.Ignore,
        Kaminari.Samidare.IR.NullBehavior.Tags.Drop => PlNullBehavior.Drop,
        _ => throw new ArgumentOutOfRangeException(nameof(behavior)),
    };
}
