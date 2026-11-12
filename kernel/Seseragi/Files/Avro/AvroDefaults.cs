using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using System.Text.Json;

namespace Kaminari.Seseragi.Files.Avro;

/// <summary>
/// Encodes a field's JSON <c>default</c> as Avro binary for its schema (specification, "Schema Record"): null as
/// nothing, numbers as their encoding, <c>bytes</c> / <c>fixed</c> as strings whose code points 0-255 are the bytes,
/// enums as their symbol, records and maps as objects, arrays as arrays, and a union as the first branch the value
/// matches. Encoding validates the default; reading with a reader schema replays the bytes for a missing field.
/// </summary>
internal static class AvroDefaults
{
    /// <summary>The default value encoded for <paramref name="schema"/>.</summary>
    /// <exception cref="ArgumentException">The value does not match the schema; the message names <paramref name="path"/>.</exception>
    public static byte[] Encode(AvroSchema schema, JsonElement value, string path)
    {
        var buffer = new ArrayBufferWriter<byte>();
        if (!TryWrite(buffer, schema, value, out string? error))
            throw new ArgumentException($"The default of '{path}' ({Trim(value)}) does not match its type: {error}.");
        return buffer.WrittenSpan.ToArray();
    }

    private static string Trim(JsonElement value)
    {
        string text = value.GetRawText();
        return text.Length > 80 ? text[..80] + "…" : text;
    }

    private static bool TryWrite(ArrayBufferWriter<byte> w, AvroSchema schema, JsonElement v, out string? error)
    {
        error = null;
        switch (schema)
        {
            case AvroUnionSchema union:
                // The first branch the value matches (each attempt writes into its own buffer).
                for (int i = 0; i < union.Branches.Count; i++)
                {
                    var attempt = new ArrayBufferWriter<byte>();
                    if (!TryWrite(attempt, union.Branches[i], v, out _)) continue;
                    WriteLong(w, i);
                    w.Write(attempt.WrittenSpan);
                    return true;
                }
                return Fail(out error, "it matches no branch of the union");

            case AvroRecordSchema record:
                if (v.ValueKind != JsonValueKind.Object) return Fail(out error, $"record '{record.FullName}' needs a JSON object");
                foreach (var field in record.Fields)
                {
                    JsonElement fieldValue;
                    if (v.TryGetProperty(field.Name, out var given)) fieldValue = given;
                    else if (field.Default is { } fallback) fieldValue = fallback;
                    else return Fail(out error, $"field '{field.Name}' of record '{record.FullName}' is missing and has no default");
                    if (!TryWrite(w, field.Schema, fieldValue, out string? inner))
                        return Fail(out error, $"field '{field.Name}': {inner}");
                }
                return true;

            case AvroEnumSchema e:
                if (v.ValueKind != JsonValueKind.String) return Fail(out error, $"enum '{e.FullName}' needs a symbol string");
                int symbol = IndexOf(e.Symbols, v.GetString()!);
                if (symbol < 0) return Fail(out error, $"'{v.GetString()}' is not a symbol of enum '{e.FullName}'");
                WriteLong(w, symbol);
                return true;

            case AvroFixedSchema f:
                if (!TryLatin1(v, out byte[]? fixedBytes)) return Fail(out error, $"fixed '{f.FullName}' needs a string of code points 0-255");
                if (fixedBytes.Length != f.Size) return Fail(out error, $"fixed '{f.FullName}' needs {f.Size} bytes, not {fixedBytes.Length}");
                w.Write(fixedBytes);
                return true;

            case AvroArraySchema a:
                if (v.ValueKind != JsonValueKind.Array) return Fail(out error, "an array needs a JSON array");
                if (v.GetArrayLength() > 0)
                {
                    WriteLong(w, v.GetArrayLength());
                    foreach (var item in v.EnumerateArray())
                        if (!TryWrite(w, a.Items, item, out string? inner)) return Fail(out error, $"array item: {inner}");
                }
                WriteLong(w, 0);
                return true;

            case AvroMapSchema m:
                if (v.ValueKind != JsonValueKind.Object) return Fail(out error, "a map needs a JSON object");
                int count = 0;
                foreach (var _ in v.EnumerateObject()) count++;
                if (count > 0)
                {
                    WriteLong(w, count);
                    foreach (var entry in v.EnumerateObject())
                    {
                        WriteString(w, entry.Name);
                        if (!TryWrite(w, m.Values, entry.Value, out string? inner)) return Fail(out error, $"map value '{entry.Name}': {inner}");
                    }
                }
                WriteLong(w, 0);
                return true;

            default:
                return TryWritePrimitive(w, schema.Type, v, out error);
        }
    }

    private static bool TryWritePrimitive(ArrayBufferWriter<byte> w, AvroType type, JsonElement v, out string? error)
    {
        error = null;
        switch (type)
        {
            case AvroType.Null:
                return v.ValueKind == JsonValueKind.Null || Fail(out error, "null needs JSON null");
            case AvroType.Boolean:
                if (v.ValueKind is not (JsonValueKind.True or JsonValueKind.False)) return Fail(out error, "boolean needs true or false");
                w.Write([(byte)(v.GetBoolean() ? 1 : 0)]);
                return true;
            case AvroType.Int:
                if (v.ValueKind != JsonValueKind.Number || !v.TryGetInt32(out int i)) return Fail(out error, "int needs a 32-bit integer");
                WriteLong(w, i);
                return true;
            case AvroType.Long:
                if (v.ValueKind != JsonValueKind.Number || !v.TryGetInt64(out long l)) return Fail(out error, "long needs a 64-bit integer");
                WriteLong(w, l);
                return true;
            case AvroType.Float:
                if (v.ValueKind != JsonValueKind.Number || !v.TryGetDouble(out double fd)) return Fail(out error, "float needs a number");
                BinaryPrimitives.WriteSingleLittleEndian(w.GetSpan(4), (float)fd);
                w.Advance(4);
                return true;
            case AvroType.Double:
                if (v.ValueKind != JsonValueKind.Number || !v.TryGetDouble(out double d)) return Fail(out error, "double needs a number");
                BinaryPrimitives.WriteDoubleLittleEndian(w.GetSpan(8), d);
                w.Advance(8);
                return true;
            case AvroType.Bytes:
                if (!TryLatin1(v, out byte[]? bytes)) return Fail(out error, "bytes needs a string of code points 0-255");
                WriteLong(w, bytes.Length);
                w.Write(bytes);
                return true;
            case AvroType.String:
                if (v.ValueKind != JsonValueKind.String) return Fail(out error, "string needs a JSON string");
                WriteString(w, v.GetString()!);
                return true;
            default:
                return Fail(out error, $"unexpected type {type}");
        }
    }

    private static bool TryLatin1(JsonElement v, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out byte[]? bytes)
    {
        bytes = null;
        if (v.ValueKind != JsonValueKind.String) return false;
        string s = v.GetString()!;
        var result = new byte[s.Length];
        for (int i = 0; i < s.Length; i++)
        {
            if (s[i] > 0xFF) return false;
            result[i] = (byte)s[i];
        }
        bytes = result;
        return true;
    }

    private static int IndexOf(IReadOnlyList<string> symbols, string symbol)
    {
        for (int i = 0; i < symbols.Count; i++)
            if (symbols[i] == symbol) return i;
        return -1;
    }

    private static void WriteString(ArrayBufferWriter<byte> w, string s)
    {
        int count = Encoding.UTF8.GetByteCount(s);
        WriteLong(w, count);
        Encoding.UTF8.GetBytes(s, w.GetSpan(count));
        w.Advance(count);
    }

    private static void WriteLong(ArrayBufferWriter<byte> w, long value)
    {
        ulong n = (ulong)((value << 1) ^ (value >> 63));
        Span<byte> span = w.GetSpan(10);
        int length = 0;
        while (n >= 0x80)
        {
            span[length++] = (byte)(n | 0x80);
            n >>= 7;
        }
        span[length++] = (byte)n;
        w.Advance(length);
    }

    private static bool Fail(out string? error, string message)
    {
        error = message;
        return false;
    }
}
