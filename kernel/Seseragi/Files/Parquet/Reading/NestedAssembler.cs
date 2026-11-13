using System.Buffers;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Seseragi.Files.Parquet.Schema;

namespace Kaminari.Seseragi.Files.Parquet.Reading;

/// <summary>
/// Builds nested columns from Dremel levels: after the leaves of a column read a batch of records (their values in
/// their builders, their levels kept), each struct, list and map node takes its slots, validity and list lengths
/// from the levels of its first leaf (the rules of <see cref="ReadNode"/>). The other leaves must agree: the array
/// lengths are checked once a column is assembled.
/// </summary>
internal static class NestedAssembler
{
    /// <summary>The builder tree of a column, its leaves the readers' builders.</summary>
    public static NativeColumnBuilder CreateBuilder(ReadNode node, IReadOnlyDictionary<ReadNode, LeafReader> readers) => node.Kind switch
    {
        ReadKind.Leaf => readers[node].Builder,
        ReadKind.Struct => new NativeStructBuilder((StructType)node.Field.DataType, [.. node.Children.Select(c => CreateBuilder(c, readers))]),
        ReadKind.FixedSizeList => new NativeFixedSizeListBuilder((FixedSizeListType)node.Field.DataType, CreateBuilder(node.Children[0], readers)),
        _ => new NativeListBuilder(node.Field.DataType, CreateBuilder(node.Children[0], readers)), // lists and maps
    };

    /// <summary>Assembles the batch read by the leaves into the builders of <paramref name="node"/>.</summary>
    /// <exception cref="ParquetFormatException">The leaves disagree, a fixed-size list has another length, or the levels are inconsistent.</exception>
    public static void Assemble(ReadNode node, NativeColumnBuilder builder, IReadOnlyDictionary<ReadNode, LeafReader> readers, string where)
    {
        if (node.Kind == ReadKind.Leaf) return;
        LeafReader source = readers[node.FirstLeaf];
        ReadOnlySpan<byte> def = source.BatchDefinitions, rep = source.BatchRepetitions;
        byte[] mask = ArrayPool<byte>.Shared.Rent(Math.Max(1, def.Length));
        int[] lengths = ArrayPool<int>.Shared.Rent(Math.Max(1, def.Length));
        try
        {
            int slots = 0, nulls = 0;
            switch (node.Kind)
            {
                case ReadKind.Struct:
                    for (int i = 0; i < def.Length; i++)
                    {
                        if (rep[i] > node.SlotRep || def[i] < node.SlotDef) continue;
                        bool valid = def[i] >= node.NullDef;
                        mask[slots++] = valid ? (byte)1 : (byte)0;
                        if (!valid) nulls++;
                    }
                    ((NativeStructBuilder)builder).EndStructs(mask.AsSpan(0, slots));
                    break;
                default: // lists, maps, fixed-size lists
                    for (int i = 0; i < def.Length; i++)
                    {
                        if (rep[i] <= node.SlotRep && def[i] >= node.SlotDef)
                        {
                            bool valid = def[i] >= node.NullDef;
                            mask[slots] = valid ? (byte)1 : (byte)0;
                            if (!valid) nulls++;
                            lengths[slots++] = def[i] >= node.ElementDef ? 1 : 0;
                        }
                        else if (rep[i] == node.ElementRep)
                        {
                            if (slots == 0) throw new ParquetFormatException("an element continues a list that never started.");
                            lengths[slots - 1]++;
                        }
                    }
                    if (node.Kind == ReadKind.FixedSizeList)
                    {
                        for (int k = 0; k < slots; k++)
                        {
                            if (mask[k] != 0 && lengths[k] != node.ListSize)
                                throw new ParquetFormatException($"'{where}' is a fixed-size list of {node.ListSize}, but an entry holds {lengths[k]} items.");
                        }
                        ((NativeFixedSizeListBuilder)builder).AppendSlots(slots, nulls == 0 ? default : mask.AsSpan(0, slots));
                    }
                    else ((NativeListBuilder)builder).AppendLists(lengths.AsSpan(0, slots), nulls == 0 ? default : mask.AsSpan(0, slots));
                    break;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(mask);
            ArrayPool<int>.Shared.Return(lengths);
        }

        for (int c = 0; c < node.Children.Length; c++)
        {
            ReadNode child = node.Children[c];
            NativeColumnBuilder childBuilder = builder switch
            {
                NativeStructBuilder s => s.Children[c],
                NativeListBuilder l => l.Values,
                NativeFixedSizeListBuilder f => f.Values,
                _ => throw new InvalidOperationException(),
            };
            Assemble(child, childBuilder, readers, $"{where}.{child.Field.Name}");
            long expected = builder switch
            {
                NativeStructBuilder s => s.Length,
                NativeListBuilder l => l.ItemCount,
                NativeFixedSizeListBuilder f => f.Length * f.ListSize,
                _ => 0,
            };
            if (childBuilder.Length != expected)
                throw new ParquetFormatException($"The columns under '{where}' disagree: '{child.Field.Name}' holds {childBuilder.Length} entries where {expected} are expected.");
        }
    }
}
