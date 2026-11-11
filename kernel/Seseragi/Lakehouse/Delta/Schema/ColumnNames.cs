using System.Text;

namespace Kaminari.Seseragi.Lakehouse.Delta.Schema;

/// <summary>
/// Column names as Spark writes them: dotted for nested fields, a part in backticks when it holds characters a bare
/// name cannot (a doubled backtick inside is one backtick).
/// </summary>
public static class ColumnNames
{
    /// <summary>The parts of a dotted column name, backticks taken off.</summary>
    public static IReadOnlyList<string> Split(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        var parts = new List<string>();
        var current = new StringBuilder();
        bool quoted = false;
        for (int i = 0; i < name.Length; i++)
        {
            char c = name[i];
            if (c == '`')
            {
                if (quoted && i + 1 < name.Length && name[i + 1] == '`')
                {
                    current.Append('`');
                    i++;
                }
                else
                {
                    quoted = !quoted;
                }
            }
            else if (c == '.' && !quoted)
            {
                parts.Add(current.ToString());
                current.Clear();
            }
            else
            {
                current.Append(c);
            }
        }
        parts.Add(current.ToString());
        return parts;
    }

    /// <summary>
    /// A dotted name with every part that is not a plain identifier (letters, digits, underscores; not only digits)
    /// in backticks, as Spark's <c>quoteIfNeeded</c> writes table properties such as
    /// <c>delta.dataSkippingStatsColumns</c>.
    /// </summary>
    public static string Quoted(IEnumerable<string> parts)
    {
        ArgumentNullException.ThrowIfNull(parts);
        return string.Join('.', parts.Select(p => IsPlain(p) ? p : Backticked(p)));

        static bool IsPlain(string part) =>
            part.Length > 0 && part.All(c => char.IsAsciiLetterOrDigit(c) || c == '_') && !part.All(char.IsAsciiDigit);
    }

    /// <summary>
    /// A dotted name with only the parts holding a dot (or a backtick) in backticks, as Spark records column paths
    /// in <c>commitInfo</c> (<c>s.`x.y`</c>, <c>we ird</c>).
    /// </summary>
    public static string Display(IEnumerable<string> parts)
    {
        ArgumentNullException.ThrowIfNull(parts);
        return string.Join('.', parts.Select(p => p.Contains('.') || p.Contains('`') ? Backticked(p) : p));
    }

    private static string Backticked(string part) => "`" + part.Replace("`", "``", StringComparison.Ordinal) + "`";
}
