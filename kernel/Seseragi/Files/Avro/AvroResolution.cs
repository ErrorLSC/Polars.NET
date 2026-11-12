using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;

namespace Kaminari.Seseragi.Files.Avro;

/// <summary>How the fields of a reader schema find their counterparts in the writer schema.</summary>
public enum AvroFieldMatching
{
    /// <summary>By field name, as the Avro specification resolves records.</summary>
    ByName,

    /// <summary>By Iceberg's <c>field-id</c> only (no fallback to names), as Iceberg readers do; every reader field
    /// must have an id.</summary>
    ByFieldId,
}

/// <summary>
/// The writer's data cannot be read with the reader schema: the schemas do not match (raised when the file is
/// opened), or a value does not fit the reader's type (raised when it is read: a union branch the reader lacks, an
/// enum symbol without a reader default, a number a lossy promotion would change).
/// </summary>
public sealed class AvroResolutionException(string message) : Exception(message);

/// <summary>
/// Resolves a writer schema against a reader schema (Avro 1.12 "Schema Resolution", with this package's rules):
/// records match fields by name or by field id, never by record name; writer-only fields are skipped; a reader field
/// the writer lacks takes its default, and is an error without one; int → long / float / double, long → float /
/// double, float → double and string ↔ bytes promote (the lossy ones checked per value); decimals may widen their
/// precision at an equal scale; two different logical types are refused. The result is an immutable plan that each
/// decoder set (sequential reader, parallel workers) compiles against its own builders.
/// </summary>
internal static class AvroResolver
{
    /// <summary>The plan for the top-level <paramref name="readerFields"/> (the selected reader columns, in order).</summary>
    public static ResolvedNode ResolveTop(AvroSchema writer, IReadOnlyList<AvroField> readerFields, AvroFieldMatching matching)
    {
        var context = new Context(matching);
        return writer is AvroRecordSchema writerRecord
            ? context.Record(writerRecord, readerFields, "", topLevel: true)
            : throw new AvroResolutionException("The writer schema is not a record, but the reader schema is.");
    }

    /// <summary>The plan for a non-record top level (the single <c>value</c> column).</summary>
    public static ResolvedNode ResolveValue(AvroSchema writer, AvroSchema reader, AvroFieldMatching matching) =>
        writer is AvroRecordSchema
            ? throw new AvroResolutionException("The writer schema is a record, but the reader schema is not.")
            : new ValueRowNode(new Context(matching).Resolve(writer, reader, AvroReadSchema.TopLevelValueColumn));

    private sealed class Context(AvroFieldMatching matching)
    {
        public ResolvedNode Resolve(AvroSchema w, AvroSchema r, string path) => (w, r) switch
        {
            (AvroUnionSchema wu, AvroUnionSchema ru) => UnionToUnion(wu, ru, path),
            (AvroUnionSchema wu, _) => UnionToSingle(wu, r, path),
            (_, AvroUnionSchema ru) => SingleToUnion(w, ru, path),
            _ when IsDecimal(w) && IsDecimal(r) => Decimal(w, r, path),
            (AvroRecordSchema wr, AvroRecordSchema rr) => Record(wr, rr.Fields, path, topLevel: false) switch
            {
                DirectNode => new DirectNode(rr), // the identity reads as the reader record
                var node => node,
            },
            (AvroEnumSchema we, AvroEnumSchema re) => Enum(we, re, path),
            (AvroFixedSchema wf, AvroFixedSchema rf) => wf.Size == rf.Size
                ? Logical(w, r, path)
                : throw Mismatch(path, $"fixed of {wf.Size} bytes cannot be read as fixed of {rf.Size} bytes"),
            (AvroArraySchema wa, AvroArraySchema ra) => Array(wa, ra, path),
            (AvroMapSchema wm, AvroMapSchema rm) => Collapse(new MapNode(Resolve(wm.Values, rm.Values, path + ".value"), rm)),
            _ when w.Type == r.Type && w is AvroPrimitiveSchema => Logical(w, r, path),
            _ when Promotion(w.Type, r.Type) => Promote(w, r, path),
            _ => throw Mismatch(path, $"{Describe(w)} cannot be read as {Describe(r)}"),
        };

        // ---- records ----------------------------------------------------------------------------------------------

        public ResolvedNode Record(AvroRecordSchema writer, IReadOnlyList<AvroField> readerFields, string path, bool topLevel)
        {
            var readerIndex = Index(readerFields, path, "reader");
            Index(writer.Fields, path, "writer"); // duplicates, and a writer without ids in id mode
            var steps = new RecordStep[writer.Fields.Count];
            var bound = new bool[readerFields.Count];
            for (int i = 0; i < writer.Fields.Count; i++)
            {
                AvroField wf = writer.Fields[i];
                if (Key(wf) is { } key && readerIndex.TryGetValue(key, out int at))
                {
                    AvroField rf = readerFields[at];
                    steps[i] = new RecordStep(at, Resolve(wf.Schema, rf.Schema, Join(path, rf.Name)), null);
                    bound[at] = true;
                }
                else
                {
                    steps[i] = new RecordStep(-1, null, wf.Schema); // writer-only: skipped
                }
            }
            List<DefaultStep> defaults = [];
            for (int j = 0; j < readerFields.Count; j++)
            {
                if (bound[j]) continue;
                AvroField rf = readerFields[j];
                string fieldPath = Join(path, rf.Name);
                if (rf.Default is not { } value)
                    throw new AvroResolutionException(
                        $"The reader field '{fieldPath}' is missing from the writer schema and has no default.");
                byte[] bytes;
                try
                {
                    bytes = AvroDefaults.Encode(rf.Schema, value, fieldPath);
                }
                catch (ArgumentException e)
                {
                    throw new AvroResolutionException(e.Message);
                }
                defaults.Add(new DefaultStep(j, rf.Schema, bytes));
            }
            var node = new RecordNode(steps, [.. defaults], readerFields.Count);
            // The identity mapping (writer field i is reader field i, each read as is) decodes as the reader record.
            if (!topLevel && writer.Fields.Count == readerFields.Count && defaults.Count == 0
                && steps.Select((s, i) => s.ReaderIndex == i && s.Node is DirectNode).All(x => x))
                return DirectNode.Identity;
            return node;
        }

        private string? Key(AvroField field) => matching == AvroFieldMatching.ByName
            ? field.Name
            : field.FieldId?.ToString(System.Globalization.CultureInfo.InvariantCulture);

        private Dictionary<string, int> Index(IReadOnlyList<AvroField> fields, string path, string side)
        {
            Dictionary<string, int> index = new(StringComparer.Ordinal);
            for (int i = 0; i < fields.Count; i++)
            {
                if (Key(fields[i]) is not { } key)
                {
                    if (side == "reader")
                        throw new AvroResolutionException($"The reader field '{Join(path, fields[i].Name)}' has no field-id to match by.");
                    continue; // a writer field without an id is never matched
                }
                if (!index.TryAdd(key, i))
                    throw new AvroResolutionException(matching == AvroFieldMatching.ByName
                        ? $"The {side} record '{path}' has two fields named '{key}'."
                        : $"The {side} record '{path}' has two fields with id {key}.");
            }
            if (side == "writer" && matching == AvroFieldMatching.ByFieldId && fields.Count > 0 && index.Count == 0)
                throw new AvroResolutionException($"The writer record '{(path.Length == 0 ? "(top level)" : path)}' has no field ids; it cannot be matched by id.");
            return index;
        }

        // ---- unions -----------------------------------------------------------------------------------------------

        private ResolvedNode UnionToUnion(AvroUnionSchema w, AvroUnionSchema r, string path)
        {
            var shape = AvroReadSchema.ShapeOf(r, out var readerBranches);
            var branches = new BranchPlan[w.Branches.Count];
            bool any = false;
            for (int i = 0; i < w.Branches.Count; i++)
            {
                AvroSchema wb = w.Branches[i];
                if (wb.Type == AvroType.Null)
                {
                    branches[i] = r.NullIndex >= 0 ? BranchPlan.Null : BranchPlan.Error($"'{path}' is null, but the reader's union has no null branch");
                    any |= r.NullIndex >= 0;
                    continue;
                }
                int match = FindBranch(wb, r);
                if (match < 0)
                {
                    branches[i] = BranchPlan.Error($"'{path}' holds {Describe(wb)}, which no branch of the reader's union matches");
                    continue;
                }
                branches[i] = Target(wb, r, match, shape, readerBranches, path);
                any = true;
            }
            if (!any) throw Mismatch(path, "no branch of the writer's union matches the reader's union");
            return new UnionNode(branches, shape);
        }

        private ResolvedNode SingleToUnion(AvroSchema w, AvroUnionSchema r, string path)
        {
            if (w.Type == AvroType.Null)
            {
                return r.NullIndex >= 0
                    ? new FixedBranchNode(BranchPlan.Null, AvroReadSchema.ShapeOf(r, out _))
                    : throw Mismatch(path, "null cannot be read by a union without a null branch");
            }
            int match = FindBranch(w, r);
            if (match < 0) throw Mismatch(path, $"{Describe(w)} matches no branch of the reader's union");
            var shape = AvroReadSchema.ShapeOf(r, out var readerBranches);
            return new FixedBranchNode(Target(w, r, match, shape, readerBranches, path), shape);
        }

        private ResolvedNode UnionToSingle(AvroUnionSchema w, AvroSchema r, string path)
        {
            var branches = new BranchPlan[w.Branches.Count];
            bool any = false;
            for (int i = 0; i < w.Branches.Count; i++)
            {
                AvroSchema wb = w.Branches[i];
                if (r.Type == AvroType.Null && wb.Type == AvroType.Null)
                {
                    branches[i] = BranchPlan.Null;
                    any = true;
                }
                else if (wb.Type != AvroType.Null && Matches(wb, r))
                {
                    branches[i] = new BranchPlan(Resolve(wb, r, path), -1, null);
                    any = true;
                }
                else
                {
                    branches[i] = BranchPlan.Error(wb.Type == AvroType.Null
                        ? $"'{path}' is null, but the reader's type is {Describe(r)}, which cannot be null"
                        : $"'{path}' holds {Describe(wb)}, which cannot be read as {Describe(r)}");
                }
            }
            if (!any) throw Mismatch(path, $"no branch of the writer's union can be read as {Describe(r)}");
            return new UnionNode(branches, AvroReadSchema.UnionShape.Single);
        }

        // Where a writer branch goes in the reader's union, by the shape the reader's union reads as.
        private BranchPlan Target(AvroSchema w, AvroUnionSchema r, int match, AvroReadSchema.UnionShape shape,
            IReadOnlyList<int> readerBranches, string path) => shape switch
        {
            // [int, long] / [float, double] (± null) read as one long / double column.
            AvroReadSchema.UnionShape.PromoteToLong => new BranchPlan(Resolve(w, AvroSchema.Long, path), -1, null),
            AvroReadSchema.UnionShape.PromoteToDouble => new BranchPlan(Resolve(w, AvroSchema.Double, path), -1, null),
            AvroReadSchema.UnionShape.MemberStruct => new BranchPlan(Resolve(w, r.Branches[match], path), IndexOf(readerBranches, match), null),
            _ => new BranchPlan(Resolve(w, r.Branches[match], path), -1, null),
        };

        private static int IndexOf(IReadOnlyList<int> list, int value)
        {
            for (int i = 0; i < list.Count; i++)
                if (list[i] == value) return i;
            return -1;
        }

        /// <summary>The reader branch for a writer type: among named types of the writer's kind, the one with the
        /// same unqualified name first (the one place names count: telling record branches apart), else the first
        /// branch that matches.</summary>
        private static int FindBranch(AvroSchema w, AvroUnionSchema r)
        {
            if (w is AvroNamedSchema wn)
            {
                for (int i = 0; i < r.Branches.Count; i++)
                    if (r.Branches[i] is AvroNamedSchema rn && rn.Type == wn.Type && rn.Name == wn.Name && Matches(w, rn)) return i;
            }
            for (int i = 0; i < r.Branches.Count; i++)
                if (Matches(w, r.Branches[i])) return i;
            return -1;
        }

        /// <summary>Whether the types match at the top (the specification's "match"; names not compared).</summary>
        private static bool Matches(AvroSchema w, AvroSchema r) => (w, r) switch
        {
            (AvroRecordSchema, AvroRecordSchema) or (AvroEnumSchema, AvroEnumSchema)
                or (AvroArraySchema, AvroArraySchema) or (AvroMapSchema, AvroMapSchema) => true,
            (AvroFixedSchema wf, AvroFixedSchema rf) => wf.Size == rf.Size || (IsDecimal(wf) && IsDecimal(rf)),
            _ when IsDecimal(w) && IsDecimal(r) => true,
            (AvroPrimitiveSchema, AvroPrimitiveSchema) => w.Type == r.Type || Promotion(w.Type, r.Type),
            _ => false,
        };

        // ---- primitives, logical types, promotions ----------------------------------------------------------------

        private static bool Promotion(AvroType w, AvroType r) => (w, r) switch
        {
            (AvroType.Int, AvroType.Long or AvroType.Float or AvroType.Double) => true,
            (AvroType.Long, AvroType.Float or AvroType.Double) => true,
            (AvroType.Float, AvroType.Double) => true,
            (AvroType.String, AvroType.Bytes) or (AvroType.Bytes, AvroType.String) => true,
            _ => false,
        };

        private static ResolvedNode Promote(AvroSchema w, AvroSchema r, string path)
        {
            CheckLogical(w, r, path);
            return new PromoteNode(w.Type, r.Type, path);
        }

        // Same underlying encoding: the reader's decoder reads the writer's bytes, unless the meanings differ.
        private static DirectNode Logical(AvroSchema w, AvroSchema r, string path)
        {
            CheckLogical(w, r, path);
            return new DirectNode(r);
        }

        /// <summary>Two valid logical types of different meaning are refused (the value would be reinterpreted);
        /// a plain type on either side is allowed. Timestamps compare by unit and by meaning: an instant
        /// (<c>timestamp-*</c>) or a wall-clock time (<c>local-timestamp-*</c>, or <c>timestamp-*</c> with
        /// <c>adjust-to-utc</c> false).</summary>
        private static void CheckLogical(AvroSchema w, AvroSchema r, string path)
        {
            if (w.Logical == AvroLogicalKind.None || r.Logical == AvroLogicalKind.None) return;
            if (Meaning(w) != Meaning(r))
                throw Mismatch(path, $"{Describe(w)} cannot be read as {Describe(r)}: the logical types differ");
        }

        private static (AvroLogicalKind Kind, bool Wall) Meaning(AvroSchema s) => s.Logical switch
        {
            AvroLogicalKind.TimestampMillis => (AvroLogicalKind.TimestampMillis, s.AdjustToUtc == false),
            AvroLogicalKind.TimestampMicros => (AvroLogicalKind.TimestampMicros, s.AdjustToUtc == false),
            AvroLogicalKind.TimestampNanos => (AvroLogicalKind.TimestampNanos, s.AdjustToUtc == false),
            AvroLogicalKind.LocalTimestampMillis => (AvroLogicalKind.TimestampMillis, true),
            AvroLogicalKind.LocalTimestampMicros => (AvroLogicalKind.TimestampMicros, true),
            AvroLogicalKind.LocalTimestampNanos => (AvroLogicalKind.TimestampNanos, true),
            var kind => (kind, false),
        };

        private static bool IsDecimal(AvroSchema s) => s.Logical == AvroLogicalKind.Decimal;

        // Decimals widen their precision at an equal scale (lossless; Iceberg allows it too), across bytes and fixed.
        private static ResolvedNode Decimal(AvroSchema w, AvroSchema r, string path)
        {
            if (w.Scale != r.Scale || r.Precision < w.Precision)
                throw Mismatch(path, $"decimal({w.Precision}, {w.Scale}) cannot be read as decimal({r.Precision}, {r.Scale}): only the precision may grow");
            bool sameEncoding = w.Type == r.Type && (w is not AvroFixedSchema wf || wf.Size == ((AvroFixedSchema)r).Size);
            if (sameEncoding && w.Precision == r.Precision) return new DirectNode(r);
            if (!AvroReadSchema.IsArrowDecimal(r))
                throw Mismatch(path, $"decimal({r.Precision}, {r.Scale}) is too wide to widen into");
            return new DecimalNode(w is AvroFixedSchema f ? f.Size : -1);
        }

        // ---- enums, arrays --------------------------------------------------------------------------------------

        private static ResolvedNode Enum(AvroEnumSchema w, AvroEnumSchema r, string path)
        {
            if (w.Symbols.SequenceEqual(r.Symbols)) return new DirectNode(r);
            int readerDefault = r.Default is { } d ? IndexOfSymbol(r.Symbols, d) : -1;
            var map = new int[w.Symbols.Count];
            for (int i = 0; i < map.Length; i++)
            {
                int at = IndexOfSymbol(r.Symbols, w.Symbols[i]);
                map[i] = at >= 0 ? at : readerDefault; // -1: an error when the symbol is read
            }
            return new EnumNode(map, r.Symbols, w.Symbols, path);
        }

        private static int IndexOfSymbol(IReadOnlyList<string> symbols, string symbol)
        {
            for (int i = 0; i < symbols.Count; i++)
                if (symbols[i] == symbol) return i;
            return -1;
        }

        private ResolvedNode Array(AvroArraySchema w, AvroArraySchema r, string path)
        {
            int minItemBytes = AvroDecoder.MinEncodedBytes(w.Items, []);
            if (r.Logical == AvroLogicalKind.Map && r.Items is AvroRecordSchema entries)
            {
                // The reader's key is read as its non-null type: a null key in the data is an error, not a null.
                var key = entries.Fields[0];
                AvroField nonNullKey = new(key.Name, AvroArraySchema.MapKey(entries)!, 0) { Properties = key.Properties };
                if (w.Items is not AvroRecordSchema writerEntries)
                    throw Mismatch(path, $"{Describe(w.Items)} items cannot be read as map entries");
                var node = Record(writerEntries, [nonNullKey, entries.Fields[1]], path, topLevel: false);
                // The identity reads with the reader array's own (map) decoder, which refuses null keys too.
                return node is DirectNode ? new DirectNode(r) : new ArrayNode(node, minItemBytes, r);
            }
            return Collapse(new ArrayNode(Resolve(w.Items, r.Items, path + ".item"), minItemBytes, r));
        }

        private static ResolvedNode Collapse(ResolvedNode node) => node switch
        {
            ArrayNode { Items: DirectNode } a => new DirectNode(a.Reader),
            MapNode { Values: DirectNode } m => new DirectNode(m.Reader),
            _ => node,
        };

        private static string Join(string path, string name) => path.Length == 0 ? name : $"{path}.{name}";

        private static string Describe(AvroSchema s) => s switch
        {
            AvroNamedSchema n => $"{s.Type.ToString().ToLowerInvariant()} '{n.FullName}'",
            { Logical: not AvroLogicalKind.None } => $"{s.Type.ToString().ToLowerInvariant()} ({s.Logical})",
            _ => s.Type.ToString().ToLowerInvariant(),
        };

        private static AvroResolutionException Mismatch(string path, string what) =>
            new($"'{(path.Length == 0 ? "(top level)" : path)}': {what}.");
    }
}

// ---- the plan ---------------------------------------------------------------------------------------------------

/// <summary>A node of a resolution plan: compiled into a decoder over a builder of the reader's Arrow type.</summary>
internal abstract class ResolvedNode
{
    public abstract AvroDecoder Compile(NativeColumnBuilder builder);
}

/// <summary>Writer and reader encode alike: decode with the reader schema's own decoder (fast paths included).</summary>
internal sealed class DirectNode(AvroSchema? reader) : ResolvedNode
{
    /// <summary>A record that resolved to the identity mapping; the caller replaces it with the reader record.</summary>
    public static DirectNode Identity { get; } = new(null);

    public AvroSchema Reader => reader ?? throw new InvalidOperationException("An identity record left unreplaced.");

    public override AvroDecoder Compile(NativeColumnBuilder builder) => AvroDecoder.Compile(Reader, builder);
}

internal readonly record struct RecordStep(int ReaderIndex, ResolvedNode? Node, AvroSchema? WriterOnly);

internal readonly record struct DefaultStep(int ReaderIndex, AvroSchema Schema, byte[] Bytes);

/// <summary>A record: writer fields in writer order, each decoded into its reader field or skipped; reader fields the
/// writer lacks replay their encoded default.</summary>
internal sealed class RecordNode(RecordStep[] steps, DefaultStep[] defaults, int readerFieldCount) : ResolvedNode
{
    public override AvroDecoder Compile(NativeColumnBuilder builder)
    {
        var b = (NativeStructBuilder)builder;
        if (b.Children.Length != readerFieldCount) throw new InvalidOperationException("Record builder does not match the reader record.");
        var decoders = new AvroDecoder[steps.Length + defaults.Length];
        for (int i = 0; i < steps.Length; i++)
        {
            var step = steps[i];
            decoders[i] = step.Node is null
                ? new SkipDecoder(AvroSkipper.Compile(step.WriterOnly!))
                : step.Node.Compile(b.Children[step.ReaderIndex]);
        }
        for (int i = 0; i < defaults.Length; i++)
        {
            var d = defaults[i];
            decoders[steps.Length + i] = new ConstantDecoder(AvroDecoder.Compile(d.Schema, b.Children[d.ReaderIndex]), d.Bytes);
        }
        return new RecordDecoder(b, decoders);
    }
}

/// <summary>A non-record top level read into the single <c>value</c> column.</summary>
internal sealed class ValueRowNode(ResolvedNode value) : ResolvedNode
{
    public override AvroDecoder Compile(NativeColumnBuilder builder)
    {
        var root = (NativeStructBuilder)builder;
        return new RecordDecoder(root, [value.Compile(root.Children[0])]);
    }
}

/// <summary>A reader that selects no column: each writer row is skipped.</summary>
internal sealed class SkipRowNode(AvroSchema writer) : ResolvedNode
{
    public override AvroDecoder Compile(NativeColumnBuilder builder) =>
        new RecordDecoder((NativeStructBuilder)builder, [new SkipDecoder(AvroSkipper.Compile(writer))]);
}

internal sealed class PromoteNode(AvroType from, AvroType to, string path) : ResolvedNode
{
    public override AvroDecoder Compile(NativeColumnBuilder b) => (from, to) switch
    {
        (AvroType.Int, AvroType.Long) => new IntAsLongDecoder((NativeFixedWidthBuilder<long>)b),
        (AvroType.Int, AvroType.Float) => new IntAsFloatDecoder((NativeFixedWidthBuilder<float>)b, path),
        (AvroType.Int, AvroType.Double) => new IntAsDoubleDecoder((NativeFixedWidthBuilder<double>)b),
        (AvroType.Long, AvroType.Float) => new LongAsFloatDecoder((NativeFixedWidthBuilder<float>)b, path),
        (AvroType.Long, AvroType.Double) => new LongAsDoubleDecoder((NativeFixedWidthBuilder<double>)b, path),
        (AvroType.Float, AvroType.Double) => new FloatAsDoubleDecoder((NativeFixedWidthBuilder<double>)b),
        (AvroType.String, AvroType.Bytes) => new BinaryDecoder((NativeBinaryBuilder)b),
        (AvroType.Bytes, AvroType.String) => new StringDecoder((NativeBinaryBuilder)b),
        _ => throw new InvalidOperationException($"No promotion from {from} to {to}."),
    };
}

/// <summary>A decimal read into a wider precision: the writer's encoding (fixed size, or -1 for bytes), the reader's
/// builder (which may be Decimal256 where the writer's fits Decimal128).</summary>
internal sealed class DecimalNode(int writerFixedSize) : ResolvedNode
{
    public override AvroDecoder Compile(NativeColumnBuilder b) => AvroDecoder.CompileDecimal(b, writerFixedSize);
}

internal sealed class EnumNode(int[] map, IReadOnlyList<string> readerSymbols, IReadOnlyList<string> writerSymbols, string path) : ResolvedNode
{
    public override AvroDecoder Compile(NativeColumnBuilder b) =>
        new EnumRemapDecoder((NativeDictionaryBuilder)b, readerSymbols, map, writerSymbols, path);
}

internal sealed class ArrayNode(ResolvedNode items, int minItemBytes, AvroArraySchema reader) : ResolvedNode
{
    public ResolvedNode Items { get; } = items;
    public AvroArraySchema Reader { get; } = reader;

    public override AvroDecoder Compile(NativeColumnBuilder b)
    {
        var list = (NativeListBuilder)b;
        return new ArrayDecoder(list, Items.Compile(list.Values), minItemBytes);
    }
}

internal sealed class MapNode(ResolvedNode values, AvroMapSchema reader) : ResolvedNode
{
    public ResolvedNode Values { get; } = values;
    public AvroMapSchema Reader { get; } = reader;

    public override AvroDecoder Compile(NativeColumnBuilder b)
    {
        var list = (NativeListBuilder)b;
        var entries = (NativeStructBuilder)list.Values;
        return new MapDecoder(list, entries, (NativeBinaryBuilder)entries.Children[0],
            Values.Compile(entries.Children[1]));
    }
}

/// <summary>How one writer union branch is read: into the target (Node; Member ≥ 0 for a member-struct union), as
/// null, or as a data error.</summary>
internal sealed record BranchPlan(ResolvedNode? Node, int Member, string? ErrorMessage)
{
    public static BranchPlan Null { get; } = new(null, -1, null);

    public static BranchPlan Error(string message) => new(null, -1, message);
}

/// <summary>A writer union, read by the reader's union shape (or into a single reader type).</summary>
internal sealed class UnionNode(BranchPlan[] branches, AvroReadSchema.UnionShape shape) : ResolvedNode
{
    public override AvroDecoder Compile(NativeColumnBuilder b)
    {
        var decoders = new AvroDecoder?[branches.Length];
        var errors = new string?[branches.Length];
        if (shape == AvroReadSchema.UnionShape.MemberStruct)
        {
            var members = (NativeStructBuilder)b;
            var memberOf = new int[branches.Length];
            for (int i = 0; i < branches.Length; i++)
            {
                var plan = branches[i];
                memberOf[i] = plan.Member;
                errors[i] = plan.ErrorMessage;
                if (plan.Node is not null) decoders[i] = plan.Node.Compile(members.Children[plan.Member]);
            }
            return new ResolvedMemberUnionDecoder(members, decoders, memberOf, errors);
        }
        for (int i = 0; i < branches.Length; i++)
        {
            errors[i] = branches[i].ErrorMessage;
            if (branches[i].Node is { } node) decoders[i] = node.Compile(b);
        }
        return new ResolvedUnionDecoder(b, decoders, errors);
    }
}

/// <summary>A non-union writer value read by a reader union: always the same branch.</summary>
internal sealed class FixedBranchNode(BranchPlan plan, AvroReadSchema.UnionShape shape) : ResolvedNode
{
    public override AvroDecoder Compile(NativeColumnBuilder b)
    {
        if (plan.Node is null) return new NullDecoder(b); // writer null into a nullable reader union
        if (shape == AvroReadSchema.UnionShape.MemberStruct)
        {
            var members = (NativeStructBuilder)b;
            return new FixedMemberDecoder(members, plan.Member, plan.Node.Compile(members.Children[plan.Member]));
        }
        return plan.Node.Compile(b);
    }
}

// ---- decoders the resolution adds ------------------------------------------------------------------------------

internal sealed class SkipDecoder(AvroSkipper skipper) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r) => skipper.Skip(ref r);
}

/// <summary>A reader field the writer lacks: its encoded default, decoded by the reader's decoder for every row.</summary>
internal sealed class ConstantDecoder(AvroDecoder decoder, byte[] bytes) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r)
    {
        var constant = new AvroSpanReader(bytes);
        decoder.Decode(ref constant);
    }
}

internal sealed class IntAsFloatDecoder(NativeFixedWidthBuilder<float> b, string path) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r)
    {
        int value = r.ReadInt();
        float f = value;
        // float → double is exact and double holds every int: compare there, never through an int cast.
        if ((double)f != value) throw Lossy(path, value, "float");
        b.Append(f);
    }

    internal static AvroResolutionException Lossy(string path, long value, string type) =>
        new($"'{path}' holds {value}, which {type} cannot hold exactly.");
}

internal sealed class IntAsDoubleDecoder(NativeFixedWidthBuilder<double> b) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r) => b.Append(r.ReadInt());
}

internal sealed class LongAsFloatDecoder(NativeFixedWidthBuilder<float> b, string path) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r)
    {
        long value = r.ReadLong();
        float f = value;
        if (!LongAsDoubleDecoder.Exact(f, value)) throw IntAsFloatDecoder.Lossy(path, value, "float");
        b.Append(f);
    }
}

internal sealed class LongAsDoubleDecoder(NativeFixedWidthBuilder<double> b, string path) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r)
    {
        long value = r.ReadLong();
        double d = value;
        if (!Exact(d, value)) throw IntAsFloatDecoder.Lossy(path, value, "double");
        b.Append(d);
    }

    /// <summary>Whether <paramref name="d"/> is exactly <paramref name="value"/>. Doubles from longs lie in
    /// [-2^63, 2^63]; 2^63 is out of long's range (the cast would saturate), so it is refused before casting.</summary>
    internal static bool Exact(double d, long value) => d < 9223372036854775808.0 && (long)d == value;
}

internal sealed class EnumRemapDecoder : AvroDecoder
{
    private readonly NativeDictionaryBuilder _b;
    private readonly int[] _map;
    private readonly IReadOnlyList<string> _writerSymbols;
    private readonly string _path;

    public EnumRemapDecoder(NativeDictionaryBuilder b, IReadOnlyList<string> readerSymbols, int[] map,
        IReadOnlyList<string> writerSymbols, string path)
    {
        _b = b;
        _map = map;
        _writerSymbols = writerSymbols;
        _path = path;
        b.Symbols = readerSymbols;
    }

    public override void Decode(ref AvroSpanReader r)
    {
        long index = r.ReadLong();
        if ((ulong)index >= (ulong)_map.Length) throw new AvroFormatException($"Avro enum index {index} is out of range ({_map.Length} symbols).");
        int mapped = _map[index];
        if (mapped < 0)
            throw new AvroResolutionException($"'{_path}' holds the symbol '{_writerSymbols[(int)index]}', which the reader's enum lacks and has no default for.");
        _b.Append(mapped);
    }
}

internal sealed class ResolvedUnionDecoder(NativeColumnBuilder b, AvroDecoder?[] branches, string?[] errors) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r)
    {
        int index = ReadBranch(ref r, branches.Length);
        if (branches[index] is { } branch) branch.Decode(ref r);
        else if (errors[index] is { } error) throw new AvroResolutionException(error + ".");
        else b.AppendNull();
    }
}

internal sealed class ResolvedMemberUnionDecoder(NativeStructBuilder b, AvroDecoder?[] branches, int[] memberOf, string?[] errors) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r)
    {
        int index = ReadBranch(ref r, branches.Length);
        if (errors[index] is { } error) throw new AvroResolutionException(error + ".");
        int member = memberOf[index];
        if (member < 0)
        {
            b.AppendNull();
            return;
        }
        var children = b.Children;
        for (int m = 0; m < children.Length; m++)
        {
            if (m == member) branches[index]!.Decode(ref r);
            else children[m].AppendNull();
        }
        b.EndStruct();
    }
}

/// <summary>A non-union writer value read into one member of a member-struct reader union.</summary>
internal sealed class FixedMemberDecoder(NativeStructBuilder b, int member, AvroDecoder decoder) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r)
    {
        var children = b.Children;
        for (int m = 0; m < children.Length; m++)
        {
            if (m == member) decoder.Decode(ref r);
            else children[m].AppendNull();
        }
        b.EndStruct();
    }
}
