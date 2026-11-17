using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Lakehouse.Tests;

/// <summary>
/// Reads a checkpoint file back (with the kernel's Parquet reader) and renders its values as text, for tests to
/// compare: structs as <c>{name:value,…}</c> (nulls left out), lists and maps as <c>[…]</c>, strings quoted, binary
/// as hex, decimals as their value, dates as days and timestamps as microseconds since the epoch, floats with their
/// sign (<c>-0</c> shows).
/// </summary>
internal static class CheckpointRead
{
    /// <summary>
    /// The rendered value of a top-level column (or of a field below it, by name) at every row; null where the row
    /// holds another action or the field is null.
    /// </summary>
    public static List<string?> Column(byte[] file, string column, params string[] fields)
    {
        using ParquetFile parquet = ParquetFile.Open(file.AsMemory());
        using ParquetReader reader = parquet.OpenReader(new ParquetReadOptions());
        List<string?> values = [];
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch)
            {
                IArrowArray array = batch.Column(batch.Schema.GetFieldIndex(column));
                foreach (string field in fields)
                {
                    var s = (StructArray)array;
                    array = s.Fields[((StructType)s.Data.DataType).GetFieldIndex(field)];
                }
                for (int row = 0; row < batch.Length; row++) values.Add(array.IsNull(row) ? null : Render(array, row));
            }
        }
        return values;
    }

    /// <summary>The names of a top-level struct column's fields.</summary>
    public static List<string> Fields(byte[] file, string column)
    {
        using ParquetFile parquet = ParquetFile.Open(file.AsMemory());
        using ParquetReader reader = parquet.OpenReader(new ParquetReadOptions());
        var type = (StructType)reader.Schema.GetFieldByName(column).DataType;
        return [.. type.Fields.Select(f => f.Name)];
    }

    /// <summary>The value at <paramref name="row"/>, rendered.</summary>
    public static string Render(IArrowArray array, int row)
    {
        if (array.IsNull(row)) return "null";
        return array switch
        {
            StructArray s => "{" + string.Join(",", ((StructType)s.Data.DataType).Fields
                .Select((f, k) => (f.Name, Child: s.Fields[k]))
                .Where(c => !c.Child.IsNull(row))
                .Select(c => $"{c.Name}:{Render(c.Child, row)}")) + "}",
            ListArray l => "[" + string.Join(",", Enumerable.Range(l.ValueOffsets[row], l.ValueOffsets[row + 1] - l.ValueOffsets[row])
                .Select(i => Render(l.Values, i))) + "]",
            LargeListArray l => "[" + string.Join(",", Enumerable.Range((int)l.ValueOffsets[row], (int)(l.ValueOffsets[row + 1] - l.ValueOffsets[row]))
                .Select(i => Render(l.Values, i))) + "]",
            StringArray a => Quote(a.GetString(row)),
            LargeStringArray a => Quote(a.GetString(row)),
            StringViewArray a => Quote(a.GetString(row)),
            BinaryArray a => Convert.ToHexString(a.GetBytes(row)),
            LargeBinaryArray a => Convert.ToHexString(a.GetBytes(row)),
            BinaryViewArray a => Convert.ToHexString(a.GetBytes(row)),
            BooleanArray a => a.GetValue(row)!.Value ? "true" : "false",
            Int8Array a => Number(a.GetValue(row)!.Value),
            Int16Array a => Number(a.GetValue(row)!.Value),
            Int32Array a => Number(a.GetValue(row)!.Value),
            Int64Array a => Number(a.GetValue(row)!.Value),
            Date32Array a => Number(a.Values[row]) + "d",
            TimestampArray a => Number(a.Values[row]) + "us",
            FloatArray a => Float(a.GetValue(row)!.Value),
            DoubleArray a => Float(a.GetValue(row)!.Value),
            Decimal128Array a => a.GetSqlDecimal(row)!.Value.ToString(),
            _ => throw new NotSupportedException($"No rendering of {array.GetType().Name}."),
        };
    }

    private static string Quote(string text) => "\"" + text.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("\"", "\\\"", StringComparison.Ordinal) + "\"";

    private static string Number<T>(T value) where T : IFormattable => value.ToString(null, CultureInfo.InvariantCulture);

    private static string Float(double value) =>
        value == 0 && double.IsNegative(value) ? "-0" : value.ToString("R", CultureInfo.InvariantCulture);
}
