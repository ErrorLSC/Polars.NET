using Apache.Arrow;
using Polars.NET.Core.Arrow;

namespace Polars.NET.Core.Data;
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
    public static IArrowColumnBuffer Create(Type type,int length)
    {
        var field = ArrowTypeResolver.ResolveField("udf_result", type);

        var builder = ColumnBuilderFactory.Create(field, type,false, length);

        return new BuilderAdapter(builder);
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
