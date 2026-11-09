using Apache.Arrow;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Helpers;
using System.Diagnostics.CodeAnalysis;

namespace Kaminari.Sazanami.Data;
// =================================================================================
// 1. Interface
// =================================================================================
public interface IArrowColumnBuffer
{
    void Add(object? value);
    IArrowArray BuildArray();
}

// =================================================================================
// 2. Factory
// =================================================================================
public static class ArrowColumnBufferFactory
{
    public static IArrowColumnBuffer Create([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] Type type,int length)
    {
        if (ClrTypeHelper.UnwrapCoreType(type).IsEnum)
        {
            return new EnumBuffer(type, length);
        }

        var field = ArrowTypeResolver.ResolveField("udf_result", type);

        var builder = ColumnBuilderFactory.Create(field, type,false, length);

        return new BuilderAdapter(builder);
    }

    // =============================================================================
    // Enum values: their member names (a Polars Enum declared as the output type takes them as its
    // categories), or the underlying integers of a [Flags] enum.
    // =============================================================================
    private sealed class EnumBuffer(Type type, int length) : IArrowColumnBuffer
    {
        private readonly Type _enumType = ClrTypeHelper.UnwrapCoreType(type);
        private readonly OptionalKind _optionalKind = ClrTypeHelper.GetOptionalInfo(type).Kind;
        private readonly List<object?> _values = new(length);

        public void Add(object? value) => _values.Add(ClrTypeHelper.UnwrapOptionalValue(value, _optionalKind));

        public IArrowArray BuildArray()
        {
            if (EnumCategories.UsesEnumDataType(_enumType))
            {
                var names = new StringViewArray.Builder();
                foreach (var value in _values)
                {
                    if (value is null) names.AppendNull();
                    else names.Append(EnumCategories.GetCategory(_enumType, value));
                }
                return names.Build();
            }

            var underlying = Enum.GetUnderlyingType(_enumType);
            var buffer = Create(underlying, _values.Count);
            foreach (var value in _values)
            {
                buffer.Add(value is null ? null : Convert.ChangeType(value, underlying));
            }
            return buffer.BuildArray();
        }
    }

    // =============================================================================
    // Adapter: Convert DbToArrowStream.ColumnBuilder to IColumnBuffer
    // =============================================================================
    private class BuilderAdapter(ColumnBuilder internalBuilder) : IArrowColumnBuffer
    {
        public void Add(object? value)
        {
            internalBuilder.AddObject(value);
        }

        public IArrowArray BuildArray()
        {
            return internalBuilder.Build();
        }
    }
}
