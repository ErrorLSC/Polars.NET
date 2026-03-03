using Apache.Arrow;
using Polars.NET.Core.Arrow;

namespace Polars.NET.Core.Data;
// =================================================================================
// 1. Interface
// =================================================================================
public interface IColumnBuffer
{
    void Add(object? value);
    IArrowArray BuildArray();
}

// =================================================================================
// 2. Factory
// =================================================================================
public static class ColumnBufferFactory
{
    public static IColumnBuffer Create(Type type,int length)
    {
        var field = ArrowTypeResolver.ResolveField("udf_result", type);
        
        var builder = ColumnBuilderFactory.Create(field, type, length);
        
        return new BuilderAdapter(builder);
    }

    // =============================================================================
    // Adapter: Convert DbToArrowStream.ColumnBuilder to IColumnBuffer
    // =============================================================================
    private class BuilderAdapter(ColumnBuilder internalBuilder) : IColumnBuffer
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
