using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using Apache.Arrow;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Core;
using Microsoft.FSharp.Reflection;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Helpers;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Data;

/// <summary>
/// Pre-compiled, schema-bound row hydrator operating over ColumnAccessor[].
/// </summary>
public sealed class CompiledArrowRowMapper<T>
{
    private readonly Func<ColumnAccessor>[] _accessorFactories;
    private readonly int[] _arrowColIndices;

    public Func<ColumnAccessor[], int, T> HydrateRow { get; }

    internal CompiledArrowRowMapper(
        Func<ColumnAccessor>[] accessorFactories,
        int[] arrowColIndices,
        Func<ColumnAccessor[], int, T> hydrateRow)
    {
        _accessorFactories = accessorFactories;
        _arrowColIndices = arrowColIndices;
        HydrateRow = hydrateRow;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ColumnAccessor[] CreateAccessors()
    {
        int len = _accessorFactories.Length;
        if (len == 0) return [];
        var arr = new ColumnAccessor[len];
        for (int i = 0; i < len; i++)
        {
            arr[i] = _accessorFactories[i]();
        }
        return arr;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void BindBatch(ColumnAccessor[] accessors, RecordBatch batch)
    {
        for (int i = 0; i < accessors.Length; i++)
        {
            accessors[i].SetBatch(batch.Column(_arrowColIndices[i]));
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void BindBatch(ColumnAccessor[] accessors, IReadOnlyList<IArrowArray> fields)
    {
        for (int i = 0; i < accessors.Length; i++)
        {
            accessors[i].SetBatch(fields[_arrowColIndices[i]]);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public T HydrateSingleRow(RecordBatch batch, int rowIndex)
    {
        var accessors = CreateAccessors();
        try
        {
            BindBatch(accessors, batch);
            return HydrateRow(accessors, rowIndex);
        }
        finally
        {
            for (int i = 0; i < accessors.Length; i++)
            {
                accessors[i]?.Dispose();
            }
        }
    }
}

/// <summary>
/// Disposable batch cursor holding a single exported Arrow RecordBatch and its bound ColumnAccessor[] array.
/// Provides zero-P/Invoke, zero-boxing row hydration on the hot path.
/// </summary>
public sealed class ArrowBatchCursor<T> : IDisposable
{
    private readonly RecordBatch? _batch;
    private readonly bool _ownsBatch;
    private readonly ColumnAccessor[] _accessors;
    private readonly Func<ColumnAccessor[], int, T> _hydrateRow;
    private bool _disposed;

    public int Length { get; }

    internal ArrowBatchCursor(
        RecordBatch? batch,
        bool ownsBatch,
        ColumnAccessor[] accessors,
        Func<ColumnAccessor[], int, T> hydrateRow,
        int length)
    {
        _batch = batch;
        _ownsBatch = ownsBatch;
        _accessors = accessors;
        _hydrateRow = hydrateRow;
        Length = length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public T ReadRow(int index) => _hydrateRow(_accessors, index);

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        for (int i = 0; i < _accessors.Length; i++)
        {
            _accessors[i]?.Dispose();
        }
        if (_ownsBatch)
        {
            _batch?.Dispose();
        }
    }
}

internal static class ArrowRowMapperHelpers
{
    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    public static TField ThrowNullInNonOptionField<TField>(string fieldName, string colName, int rowIndex)
    {
        throw new InvalidOperationException(
            $"Record field '{fieldName}' does not accept Option, but column '{colName}' contains null at row {rowIndex}.");
    }

    public static TMember ThrowNullInNonNullableMember<TMember>(string memberName, string colName, int rowIndex)
    {
        throw new InvalidOperationException(
            $"Member '{memberName}' of non-nullable type {typeof(TMember).Name} cannot hold the null in column '{colName}' at row {rowIndex}. Declare it as {typeof(TMember).Name}? to receive nulls.");
    }

    public static readonly MethodInfo ThrowNullInNonNullableMemberDef =
        typeof(ArrowRowMapperHelpers).GetMethod(nameof(ThrowNullInNonNullableMember), BindingFlags.Public | BindingFlags.Static)!;

    public static readonly MethodInfo ThrowNullMethodDef =
        typeof(ArrowRowMapperHelpers).GetMethod(nameof(ThrowNullInNonOptionField), BindingFlags.Public | BindingFlags.Static)!;

    public static readonly MethodInfo IsNullMethod =
        typeof(ColumnAccessor).GetMethod(nameof(ColumnAccessor.IsNull), [typeof(int)])!;

    public static readonly MethodInfo IsNullFastMethod =
        typeof(ColumnAccessor).GetMethod(nameof(ColumnAccessor.IsNullFast), [typeof(int)])!;

    public static readonly MethodInfo GetValueMethod =
        typeof(ColumnAccessor).GetMethod(nameof(ColumnAccessor.GetValue), [typeof(int)])!;
}

/// <summary>
/// Unified expression-tree compiler and cache for Arrow RecordBatch row mapping across C# and F#.
/// </summary>
public static class ArrowRowMapper<T>
{
    private static readonly ConcurrentDictionary<string, CompiledArrowRowMapper<T>> Cache = new(StringComparer.Ordinal);

    public static string[] ColumnNames { get; } = CollectRequiredNames(typeof(T));

    private static string ComputeSchemaSignature(Schema schema)
    {
        var fields = schema.FieldsList;
        int count = fields.Count;
        if (count == 0) return string.Empty;

        var sb = new StringBuilder(count * 24);
        for (int i = 0; i < count; i++)
        {
            if (i > 0) sb.Append('|');
            var f = fields[i];
            sb.Append(f.Name).Append(':').Append(f.DataType.TypeId).Append(':');
            switch (f.DataType)
            {
                case Apache.Arrow.Types.TimestampType ts:
                    sb.Append(ts.Unit).Append(':').Append(ts.Timezone ?? string.Empty);
                    break;
                case Apache.Arrow.Types.DurationType dur:
                    sb.Append(dur.Unit);
                    break;
                case Apache.Arrow.Types.Time64Type t64:
                    sb.Append(t64.Unit);
                    break;
                case Apache.Arrow.Types.Time32Type t32:
                    sb.Append(t32.Unit);
                    break;
                case Apache.Arrow.Types.Decimal128Type d128:
                    sb.Append(d128.Precision).Append(',').Append(d128.Scale);
                    break;
                case Apache.Arrow.Types.Decimal256Type d256:
                    sb.Append(d256.Precision).Append(',').Append(d256.Scale);
                    break;
                case Apache.Arrow.Types.ListType lt:
                    sb.Append(lt.ValueDataType.TypeId);
                    break;
                case Apache.Arrow.Types.LargeListType llt:
                    sb.Append(llt.ValueDataType.TypeId);
                    break;
                case Apache.Arrow.Types.FixedSizeListType fslt:
                    sb.Append(fslt.ValueDataType.TypeId).Append(':').Append(fslt.ListSize);
                    break;
                case Apache.Arrow.Types.StructType st:
                    sb.Append(st.Fields.Count);
                    for (int k = 0; k < st.Fields.Count; k++)
                    {
                        sb.Append(',').Append(st.Fields[k].Name).Append(':').Append(st.Fields[k].DataType.TypeId);
                    }
                    break;
                default:
                    sb.Append(f.DataType.Name);
                    break;
            }
        }
        return sb.ToString();
    }

    public static CompiledArrowRowMapper<T> GetOrCreate(Schema schema)
    {
        string signature = ComputeSchemaSignature(schema);
        if (Cache.TryGetValue(signature, out var cached))
        {
            return cached;
        }

        var compiled = BuildMapper(schema);
        return Cache.GetOrAdd(signature, compiled);
    }

    /// <summary>
    /// Exports a DataFrameHandle to an Arrow RecordBatch in a single FFI call and binds a fast row cursor.
    /// </summary>
    public static ArrowBatchCursor<T> CreateCursor(DataFrameHandle dfHandle)
    {
        long height = PolarsWrapper.DataFrameHeight(dfHandle);
        if (height == 0L)
        {
            return new ArrowBatchCursor<T>(null, ownsBatch: false, [], static (_, _) => default!, 0);
        }

        RecordBatch batch = ArrowFfiBridge.ExportDataFrame(dfHandle);
        var mapper = GetOrCreate(batch.Schema);
        var accessors = mapper.CreateAccessors();
        mapper.BindBatch(accessors, batch);
        return new ArrowBatchCursor<T>(batch, ownsBatch: true, accessors, mapper.HydrateRow, batch.Length);
    }

    /// <summary>
    /// Creates a row cursor over an existing Arrow RecordBatch.
    /// </summary>
    public static ArrowBatchCursor<T> CreateCursor(RecordBatch batch, bool ownsBatch = false)
    {
        if (batch.Length == 0)
        {
            return new ArrowBatchCursor<T>(batch, ownsBatch, [], static (_, _) => default!, 0);
        }

        var mapper = GetOrCreate(batch.Schema);
        var accessors = mapper.CreateAccessors();
        mapper.BindBatch(accessors, batch);
        return new ArrowBatchCursor<T>(batch, ownsBatch, accessors, mapper.HydrateRow, batch.Length);
    }

    private static bool IsFSharpOption(Type t)
        => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(FSharpOption<>);

    private static bool IsFSharpValueOption(Type t)
        => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(FSharpValueOption<>);

    private static bool IsFSharpList(Type t)
        => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(FSharpList<>);

    private static bool IsFSharpAnonymousObject(Type t)
        => t.FullName != null &&
           (t.FullName.StartsWith("Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject", StringComparison.Ordinal) ||
            (t.IsGenericType && t.Name.StartsWith("AnonymousObject`", StringComparison.Ordinal)));

    private static bool IsScalarOrEnum(Type t)
        => PolarsTypeHelper.IsScalarType(t) || t == typeof(string) || t.IsEnum;

    private static bool IsScalarLike(Type t)
    {
        if (IsScalarOrEnum(t)) return true;
        if (Nullable.GetUnderlyingType(t) is { } u) return IsScalarLike(u);
        if (IsFSharpOption(t) || IsFSharpValueOption(t)) return IsScalarLike(t.GetGenericArguments()[0]);
        return false;
    }

    private static bool IsCollectionLike(Type t)
    {
        if (t == typeof(string) || t == typeof(byte[])) return false;
        if (t.IsArray) return true;
        if (t.IsGenericType)
        {
            var gDef = t.GetGenericTypeDefinition();
            return gDef == typeof(List<>) ||
                   gDef == typeof(FSharpList<>) ||
                   gDef == typeof(IList<>) ||
                   gDef == typeof(ICollection<>) ||
                   gDef == typeof(IReadOnlyList<>) ||
                   gDef == typeof(IReadOnlyCollection<>) ||
                   gDef == typeof(IEnumerable<>) ||
                   gDef == typeof(IOrderedEnumerable<>);
        }
        return false;
    }

    private static readonly FSharpOption<BindingFlags> AllRecordFlags =
        FSharpOption<BindingFlags>.Some(BindingFlags.Public | BindingFlags.NonPublic);

    private static string[] CollectRequiredNames(Type t)
    {
        if (IsScalarLike(t) || IsCollectionLike(t))
            return [];
        if (FSharpType.IsTuple(t))
            return FSharpType.GetTupleElements(t).SelectMany(CollectRequiredNames).ToArray();
        if (t.IsValueType && t.FullName != null && t.FullName.StartsWith("System.ValueTuple`", StringComparison.Ordinal))
            return t.GetGenericArguments().SelectMany(CollectRequiredNames).ToArray();
        if (FSharpType.IsRecord(t, AllRecordFlags))
            return FSharpType.GetRecordFields(t, AllRecordFlags).Select(f => f.Name).ToArray();
        if (IsFSharpAnonymousObject(t))
            return t.GetProperties(BindingFlags.Public | BindingFlags.Instance).Select(p => p.Name).ToArray();

        var writeable = t.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite)
            .Select(p => p.Name)
            .ToArray();

        return writeable.Length > 0
            ? writeable
            : t.GetProperties(BindingFlags.Public | BindingFlags.Instance).Select(p => p.Name).ToArray();
    }

    private static CompiledArrowRowMapper<T> BuildMapper(Schema schema)
    {
        var targetType = typeof(T);
        var accessorsParam = Expression.Parameter(typeof(ColumnAccessor[]), "accessors");
        var idxParam = Expression.Parameter(typeof(int), "rowIndex");

        var accessorFactories = new List<Func<ColumnAccessor>>();
        var arrowColIndices = new List<int>();
        var slotVars = new List<ParameterExpression>();
        var slotInitExprs = new List<Expression>();
        var slotMap = new Dictionary<(int ColIdx, Type TargetType), (int SlotIdx, ColumnAccessor Sample, ParameterExpression SlotVar)>();
        var firstSlotByCol = new Dictionary<int, (int SlotIdx, ColumnAccessor Sample, ParameterExpression SlotVar)>();

        (int SlotIdx, ColumnAccessor Sample, ParameterExpression SlotVar) GetOrAddSlot(int colIdx, Type memberType)
        {
            var key = (colIdx, memberType);
            if (slotMap.TryGetValue(key, out var existing))
            {
                return existing;
            }

            Field field = schema.GetFieldByIndex(colIdx);
            ColumnAccessor sample = ColumnAccessorFactory.Create(field, memberType);
            int slotIdx = accessorFactories.Count;
            accessorFactories.Add(() => ColumnAccessorFactory.Create(field, memberType));
            arrowColIndices.Add(colIdx);

            Type concreteAccessorType = sample.GetType();
            var slotVar = Expression.Variable(concreteAccessorType, $"slot{slotIdx}");
            slotVars.Add(slotVar);
            slotInitExprs.Add(
                Expression.Assign(
                    slotVar,
                    Expression.Convert(Expression.ArrayIndex(accessorsParam, Expression.Constant(slotIdx)), concreteAccessorType)));

            var entry = (slotIdx, sample, slotVar);
            slotMap[key] = entry;
            if (!firstSlotByCol.ContainsKey(colIdx))
            {
                firstSlotByCol[colIdx] = entry;
            }
            return entry;
        }

        Expression CreateIsNullExpr(int colIdx)
        {
            if (!firstSlotByCol.TryGetValue(colIdx, out var entry))
            {
                var defaultNetType = ArrowTypeResolver.GetNetTypeFromArrowType(schema.GetFieldByIndex(colIdx).DataType);
                entry = GetOrAddSlot(colIdx, defaultNetType);
            }

            var isNullMethod = entry.Sample.UsesFastNullBitmap
                ? ArrowRowMapperHelpers.IsNullFastMethod
                : ArrowRowMapperHelpers.IsNullMethod;
            return Expression.Call(entry.SlotVar, isNullMethod, idxParam);
        }

        Expression CreateRawValueReadExpr(ParameterExpression slotVar, ColumnAccessor sample, Type desiredType)
        {
            var directMethod = sample.GetType().GetMethod(
                nameof(ITypedAccessor<int>.GetTypedValue),
                BindingFlags.Public | BindingFlags.Instance,
                null,
                [typeof(int)],
                null);
            if (directMethod != null && directMethod.ReturnType == desiredType)
            {
                return Expression.Call(slotVar, directMethod, idxParam);
            }

            Type typedInterface = typeof(ITypedAccessor<>).MakeGenericType(desiredType);
            if (typedInterface.IsAssignableFrom(sample.GetType()))
            {
                var typedAccessor = Expression.Convert(slotVar, typedInterface);
                var getMethod = typedInterface.GetMethod(nameof(ITypedAccessor<int>.GetTypedValue))!;
                return Expression.Call(typedAccessor, getMethod, idxParam);
            }

            var callGetValue = Expression.Call(slotVar, ArrowRowMapperHelpers.GetValueMethod, idxParam);
            return Expression.Convert(callGetValue, desiredType);
        }

        Expression CreateReadExpr(int colIdx, Type returnType, string fieldName, bool strictNonOptionNullCheck)
        {
            // 1. FSharpOption<U> or FSharpValueOption<U>
            if (IsFSharpOption(returnType) || IsFSharpValueOption(returnType))
            {
                var (_, sample, slotVar) = GetOrAddSlot(colIdx, returnType);
                return CreateRawValueReadExpr(slotVar, sample, returnType);
            }

            // 2. Nullable<TValue>
            var underlyingNullable = Nullable.GetUnderlyingType(returnType);
            if (underlyingNullable != null)
            {
                var (_, sample, slotVar) = GetOrAddSlot(colIdx, underlyingNullable);
                var isNullMethod = sample.UsesFastNullBitmap
                    ? ArrowRowMapperHelpers.IsNullFastMethod
                    : ArrowRowMapperHelpers.IsNullMethod;
                var isNullCall = Expression.Call(slotVar, isNullMethod, idxParam);

                var rawVal = CreateRawValueReadExpr(slotVar, sample, underlyingNullable);
                var nonNullValExpr = Expression.Convert(rawVal, returnType);

                return Expression.Condition(isNullCall, Expression.Default(returnType), nonNullValExpr);
            }

            // 3. Standard non-nullable value type or reference type
            {
                var (_, sample, slotVar) = GetOrAddSlot(colIdx, returnType);
                var isNullMethod = sample.UsesFastNullBitmap
                    ? ArrowRowMapperHelpers.IsNullFastMethod
                    : ArrowRowMapperHelpers.IsNullMethod;
                var isNullCall = Expression.Call(slotVar, isNullMethod, idxParam);
                var valExpr = CreateRawValueReadExpr(slotVar, sample, returnType);

                Expression onNullExpr;
                if (strictNonOptionNullCheck || returnType.IsValueType)
                {
                    // A null cannot be represented by a non-nullable value type: fail like LINQ to Objects
                    // instead of silently materializing default(T).
                    string colName = schema.GetFieldByIndex(colIdx).Name;
                    var throwDef = strictNonOptionNullCheck
                        ? ArrowRowMapperHelpers.ThrowNullMethodDef
                        : ArrowRowMapperHelpers.ThrowNullInNonNullableMemberDef;
                    onNullExpr = Expression.Call(
                        throwDef.MakeGenericMethod(returnType),
                        Expression.Constant(fieldName, typeof(string)),
                        Expression.Constant(colName, typeof(string)),
                        idxParam);
                }
                else
                {
                    onNullExpr = Expression.Default(returnType);
                }

                return Expression.Condition(isNullCall, onNullExpr, valExpr);
            }
        }

        int fieldCount = schema.FieldsList.Count;
        var colMap = new Dictionary<string, int>(fieldCount, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < fieldCount; i++)
        {
            colMap[schema.GetFieldByIndex(i).Name] = i;
        }

        bool TryFindCol(string propName, string? serializedName, int tupleIndex, out int resolvedColIdx)
        {
            if (tupleIndex == 0)
            {
                if (colMap.TryGetValue($"{propName}_left", out resolvedColIdx)) return true;
                if (colMap.TryGetValue($"{propName}_first", out resolvedColIdx)) return true;
                if (serializedName != null && serializedName != propName)
                {
                    if (colMap.TryGetValue($"{serializedName}_left", out resolvedColIdx)) return true;
                    if (colMap.TryGetValue($"{serializedName}_first", out resolvedColIdx)) return true;
                }
            }
            else if (tupleIndex == 1)
            {
                if (colMap.TryGetValue($"{propName}_second", out resolvedColIdx)) return true;
                if (colMap.TryGetValue($"{propName}_right", out resolvedColIdx)) return true;
                if (serializedName != null && serializedName != propName)
                {
                    if (colMap.TryGetValue($"{serializedName}_second", out resolvedColIdx)) return true;
                    if (colMap.TryGetValue($"{serializedName}_right", out resolvedColIdx)) return true;
                }
            }
            else if (tupleIndex == 2)
            {
                if (colMap.TryGetValue($"{propName}_third", out resolvedColIdx)) return true;
                if (serializedName != null && serializedName != propName)
                {
                    if (colMap.TryGetValue($"{serializedName}_third", out resolvedColIdx)) return true;
                }
            }

            if (colMap.TryGetValue(propName, out resolvedColIdx)) return true;
            if (serializedName != null && serializedName != propName && colMap.TryGetValue(serializedName, out resolvedColIdx))
            {
                return true;
            }

            resolvedColIdx = -1;
            return false;
        }

        Expression BuildSingleObjectExpr(Type objTargetType, int tupleIndex)
        {
            var underlyingNullable = Nullable.GetUnderlyingType(objTargetType);
            bool isOpt = IsFSharpOption(objTargetType);
            bool isVOpt = IsFSharpValueOption(objTargetType);
            bool isNullable = underlyingNullable != null || isOpt || isVOpt || !objTargetType.IsValueType;

            Type actualType = (isOpt || isVOpt)
                ? objTargetType.GetGenericArguments()[0]
                : (underlyingNullable ?? objTargetType);

            var usedColIndices = new List<int>();
            Expression instanceExpr;

            if (FSharpType.IsRecord(actualType, AllRecordFlags))
            {
                var fields = FSharpType.GetRecordFields(actualType, AllRecordFlags);
                var ctors = actualType.GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
                var recordCtor = ctors.FirstOrDefault(c => c.GetParameters().Length == fields.Length) ?? ctors[0];

                var ctorArgs = new Expression[fields.Length];
                for (int i = 0; i < fields.Length; i++)
                {
                    var f = fields[i];
                    var fType = f.PropertyType;
                    bool acceptsNull = IsFSharpOption(fType) ||
                                       IsFSharpValueOption(fType) ||
                                       Nullable.GetUnderlyingType(fType) != null ||
                                       IsFSharpList(fType) ||
                                       fType.IsArray;
                    bool strictNullCheck = !acceptsNull;
                    string serializedName = PolarsTypeHelper.GetSerializedName(f);

                    if (TryFindCol(f.Name, serializedName, tupleIndex, out int colIdx))
                    {
                        usedColIndices.Add(colIdx);
                        ctorArgs[i] = CreateReadExpr(colIdx, fType, f.Name, strictNullCheck);
                    }
                    else if (tupleIndex == 0 && i < fieldCount)
                    {
                        usedColIndices.Add(i);
                        ctorArgs[i] = CreateReadExpr(i, fType, f.Name, strictNullCheck);
                    }
                    else
                    {
                        ctorArgs[i] = Expression.Default(fType);
                    }
                }

                instanceExpr = Expression.New(recordCtor, ctorArgs);
            }
            else
            {
                var ctors = actualType.GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
                var primaryCtor = ctors
                    .Where(c => !(c.GetParameters().Length == 1 && c.GetParameters()[0].ParameterType == actualType)
                                && c.GetParameters().Length > 0)
                    .OrderByDescending(c => c.GetParameters().Length)
                    .FirstOrDefault();

                if (primaryCtor != null)
                {
                    var ctorParams = primaryCtor.GetParameters();
                    var ctorArgs = new Expression[ctorParams.Length];

                    // Check if any parameter matches a column by name
                    bool anyNamedMatch = false;
                    var resolvedParamCols = new int[ctorParams.Length];
                    for (int i = 0; i < ctorParams.Length; i++)
                    {
                        var param = ctorParams[i];
                        var cleanParamName = param.Name?.TrimStart('@') ?? string.Empty;
                        var serializedParamName = PolarsTypeHelper.GetSerializedName(param);

                        if (TryFindCol(cleanParamName, serializedParamName, tupleIndex, out int colIdx))
                        {
                            resolvedParamCols[i] = colIdx;
                            anyNamedMatch = true;
                        }
                        else
                        {
                            var matchingProp = actualType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                .FirstOrDefault(p => string.Equals(p.Name, cleanParamName, StringComparison.OrdinalIgnoreCase));
                            if (matchingProp != null &&
                                TryFindCol(matchingProp.Name, PolarsTypeHelper.GetSerializedName(matchingProp), tupleIndex, out int propColIdx))
                            {
                                resolvedParamCols[i] = propColIdx;
                                anyNamedMatch = true;
                            }
                            else
                            {
                                resolvedParamCols[i] = -1;
                            }
                        }
                    }

                    bool isCompilerGenerated = actualType.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false);

                    for (int i = 0; i < ctorParams.Length; i++)
                    {
                        var param = ctorParams[i];
                        int colIdx = resolvedParamCols[i];
                        if (colIdx == -1 && !anyNamedMatch && isCompilerGenerated && tupleIndex == 0 && i < fieldCount)
                        {
                            colIdx = i;
                        }

                        if (colIdx != -1)
                        {
                            usedColIndices.Add(colIdx);
                            ctorArgs[i] = CreateReadExpr(colIdx, param.ParameterType, param.Name ?? $" arg{i}", strictNonOptionNullCheck: false);
                        }
                        else if (isCompilerGenerated)
                        {
                            // Every member of an anonymous type comes from the projection: a member without a
                            // column means the projection was lost, which must not read as default values.
                            var columns = string.Join(", ", Enumerable.Range(0, fieldCount).Select(c => schema.GetFieldByIndex(c).Name));
                            throw new InvalidOperationException(
                                $"Member '{param.Name}' of {actualType.Name} has no column in the result (columns: {columns}).");
                        }
                        else
                        {
                            ctorArgs[i] = Expression.Default(param.ParameterType);
                        }
                    }

                    instanceExpr = Expression.New(primaryCtor, ctorArgs);
                }
                else
                {
                    var defaultCtor = actualType.GetConstructor(
                        BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance,
                        null,
                        Type.EmptyTypes,
                        null);

                    var instanceVar = Expression.Variable(actualType, "subInstance");
                    Expression createInstanceExpr = defaultCtor != null
                        ? Expression.New(defaultCtor)
                        : Expression.New(actualType);

                    var blockExpressions = new List<Expression> { Expression.Assign(instanceVar, createInstanceExpr) };

                    // 1. Bind Properties (Standard and init-only)
                    var props = actualType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
                    foreach (var prop in props)
                    {
                        if (prop.Name == "EqualityContract" || prop.GetIndexParameters().Length > 0) continue;

                        var setMethod = prop.SetMethod;
                        var backingField = setMethod == null
                            ? actualType.GetField($"<{prop.Name}>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic)
                            : null;
                        if (setMethod == null && (backingField == null || backingField.IsInitOnly))
                            continue;

                        string serializedPropName = PolarsTypeHelper.GetSerializedName(prop);
                        if (TryFindCol(prop.Name, serializedPropName, tupleIndex, out int colIdx))
                        {
                            usedColIndices.Add(colIdx);
                            var readExpr = CreateReadExpr(colIdx, prop.PropertyType, prop.Name, strictNonOptionNullCheck: false);

                            if (setMethod != null)
                            {
                                blockExpressions.Add(Expression.Call(instanceVar, setMethod, readExpr));
                            }
                            else
                            {
                                blockExpressions.Add(Expression.Assign(Expression.Field(instanceVar, backingField!), readExpr));
                            }
                        }
                    }

                    // 2. Bind Public Instance Fields
                    var fields = actualType.GetFields(BindingFlags.Public | BindingFlags.Instance);
                    foreach (var field in fields)
                    {
                        if (field.IsInitOnly || field.IsLiteral) continue;

                        string serializedFieldName = PolarsTypeHelper.GetSerializedName(field);
                        if (TryFindCol(field.Name, serializedFieldName, tupleIndex, out int colIdx))
                        {
                            usedColIndices.Add(colIdx);
                            var readExpr = CreateReadExpr(colIdx, field.FieldType, field.Name, strictNonOptionNullCheck: false);
                            blockExpressions.Add(Expression.Assign(Expression.Field(instanceVar, field), readExpr));
                        }
                    }

                    blockExpressions.Add(instanceVar);
                    instanceExpr = Expression.Block([instanceVar], blockExpressions);
                }
            }

            // Outer Join Null folding: If target accepts null and all mapped columns are null in current row, fold to null/None/ValueNone
            if (isNullable && usedColIndices.Count > 0)
            {
                var distinctCols = usedColIndices.Distinct().ToArray();
                var allNullExpr = distinctCols
                    .Select(CreateIsNullExpr)
                    .Aggregate((acc, e) => Expression.AndAlso(acc, e));

                if (isOpt)
                {
                    var noneExpr = Expression.Default(objTargetType);
                    var someMethod = objTargetType.GetMethod("Some", [actualType])!;
                    var someExpr = Expression.Call(someMethod, instanceExpr);
                    return Expression.Condition(allNullExpr, noneExpr, someExpr);
                }
                if (isVOpt)
                {
                    var vnoneExpr = Expression.Default(objTargetType);
                    var vsomeMethod = objTargetType.GetMethod("NewValueSome", [actualType])!;
                    var vsomeExpr = Expression.Call(vsomeMethod, instanceExpr);
                    return Expression.Condition(allNullExpr, vnoneExpr, vsomeExpr);
                }
                if (underlyingNullable != null)
                {
                    var nullConst = Expression.Constant(null, objTargetType);
                    var valueExpr = Expression.Convert(instanceExpr, objTargetType);
                    return Expression.Condition(allNullExpr, nullConst, valueExpr);
                }
                else
                {
                    var nullConst = Expression.Constant(null, objTargetType);
                    return Expression.Condition(allNullExpr, nullConst, instanceExpr);
                }
            }

            if (isOpt)
            {
                var someMethod = objTargetType.GetMethod("Some", [actualType])!;
                return Expression.Call(someMethod, instanceExpr);
            }
            if (isVOpt)
            {
                var vsomeMethod = objTargetType.GetMethod("NewValueSome", [actualType])!;
                return Expression.Call(vsomeMethod, instanceExpr);
            }
            if (underlyingNullable != null)
            {
                return Expression.Convert(instanceExpr, objTargetType);
            }

            return instanceExpr;
        }

        Expression BuildTupleElementExpr(Type subType, int tupleIdx)
        {
            // An element packed into its own struct column (an entity of a join's tuple row) is read whole.
            if (!IsScalarLike(subType) && !IsCollectionLike(subType)
                && colMap.TryGetValue($"Item{tupleIdx + 1}", out int packedIdx)
                && schema.GetFieldByIndex(packedIdx).DataType is Apache.Arrow.Types.StructType)
            {
                return CreateReadExpr(packedIdx, subType, $"Item{tupleIdx + 1}", strictNonOptionNullCheck: false);
            }

            if (IsScalarLike(subType) || IsCollectionLike(subType))
            {
                var candidateNames = new List<string> { $"Item{tupleIdx + 1}" };
                if (tupleIdx == 0)
                {
                    candidateNames.Add("Outer");
                    candidateNames.Add("Index");
                    candidateNames.Add("First");
                }
                else if (tupleIdx == 1)
                {
                    candidateNames.Add("Inner");
                    candidateNames.Add("Item");
                    candidateNames.Add("Second");
                }
                else if (tupleIdx == 2)
                {
                    candidateNames.Add("Third");
                }

                int colIdx = -1;
                foreach (var name in candidateNames)
                {
                    if (colMap.TryGetValue(name, out int idx))
                    {
                        colIdx = idx;
                        break;
                    }
                }

                if (colIdx == -1 && tupleIdx < fieldCount)
                {
                    colIdx = tupleIdx;
                }

                if (colIdx != -1)
                {
                    return CreateReadExpr(colIdx, subType, candidateNames[0], strictNonOptionNullCheck: false);
                }

                return Expression.Default(subType);
            }

            return BuildSingleObjectExpr(subType, tupleIdx);
        }

        Expression rootExpr;

        // Branch 0: Target is a scalar/enum/option-scalar/collection read from column 0
        if (IsScalarLike(targetType) || IsCollectionLike(targetType))
        {
            if (fieldCount > 0)
            {
                rootExpr = CreateReadExpr(0, targetType, schema.GetFieldByIndex(0).Name, strictNonOptionNullCheck: false);
            }
            else
            {
                rootExpr = Expression.Default(targetType);
            }
        }
        // Branch 1: Tuple composite mapping (F# Tuple, System.ValueTuple, System.Tuple)
        else if (FSharpType.IsTuple(targetType) ||
                 (targetType.FullName != null &&
                  (targetType.FullName.StartsWith("System.ValueTuple`", StringComparison.Ordinal) ||
                   targetType.FullName.StartsWith("System.Tuple`", StringComparison.Ordinal))))
        {
            var elemTypes = FSharpType.IsTuple(targetType)
                ? FSharpType.GetTupleElements(targetType)
                : targetType.GetGenericArguments();

            var tupleCtor = targetType.GetConstructor(elemTypes)
                ?? targetType.GetConstructors(BindingFlags.Public | BindingFlags.Instance).FirstOrDefault()
                ?? throw new NotSupportedException($"Tuple constructor matching arguments could not be resolved for '{targetType.FullName}'.");

            var tupleArgs = new Expression[elemTypes.Length];
            for (int i = 0; i < elemTypes.Length; i++)
            {
                tupleArgs[i] = BuildTupleElementExpr(elemTypes[i], i);
            }

            rootExpr = Expression.New(tupleCtor, tupleArgs);
        }
        // Branch 2: F# AnonymousObject
        else if (IsFSharpAnonymousObject(targetType))
        {
            var ctors = targetType.GetConstructors(BindingFlags.Public | BindingFlags.Instance);
            var primaryCtor = ctors[0];
            var ctorParams = primaryCtor.GetParameters();

            var subArgs = new Expression[ctorParams.Length];
            for (int i = 0; i < ctorParams.Length; i++)
            {
                if (fieldCount > 0)
                {
                    int colIdx = i < fieldCount ? i : 0;
                    subArgs[i] = CreateReadExpr(colIdx, ctorParams[i].ParameterType, ctorParams[i].Name ?? $"Item{i + 1}", strictNonOptionNullCheck: false);
                }
                else
                {
                    subArgs[i] = Expression.Default(ctorParams[i].ParameterType);
                }
            }

            rootExpr = Expression.New(primaryCtor, subArgs);
        }
        // Branch 3: Standard single object, F# record, or C# record/class/struct
        else
        {
            rootExpr = BuildSingleObjectExpr(targetType, 0);
        }

        try
        {
            Expression bodyExpr = slotVars.Count > 0
                ? Expression.Block(slotVars, [.. slotInitExprs, rootExpr])
                : rootExpr;

            var hydrateDelegate = Expression.Lambda<Func<ColumnAccessor[], int, T>>(bodyExpr, accessorsParam, idxParam).Compile();
            return new CompiledArrowRowMapper<T>([.. accessorFactories], [.. arrowColIndices], hydrateDelegate);
        }
        finally
        {
            foreach (var (SlotIdx, Sample, SlotVar) in slotMap.Values)
            {
                Sample.Dispose();
            }
        }
    }
}
