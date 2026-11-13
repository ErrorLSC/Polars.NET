using Kaminari.Seseragi.Files.Excel.Xml;

namespace Kaminari.Seseragi.Files.Excel.Ods;

/// <summary>
/// What the reader needs from an OpenDocument spreadsheet's styles and settings: which table styles hide a sheet
/// (<c>table:display="false"</c> in <c>style:table-properties</c>), which cell styles format time values as elapsed
/// time (their data style, found through <c>style:parent-style-name</c>, is a <c>number:time-style</c> with
/// <c>number:truncate-on-overflow="false"</c>, as <c>[HH]:MM:SS</c> is), and the null date
/// (<c>table:null-date</c>). Fed element by element from content.xml and styles.xml (or a flat document).
/// </summary>
internal sealed class OdsStyles
{
    private readonly HashSet<string> _hiddenTables = new(StringComparer.Ordinal);
    private readonly Dictionary<string, (string? DataStyle, string? Parent)> _cellStyles = new(StringComparer.Ordinal);
    private readonly HashSet<string> _elapsedDataStyles = new(StringComparer.Ordinal);
    private readonly Dictionary<string, bool> _elapsed = new(StringComparer.Ordinal);
    private string? _tableStyle; // the table style whose properties are being read

    /// <summary>The null date is 1904-01-01 (serial numbers count from it, as in Excel's 1904 system).</summary>
    public bool Date1904 { get; private set; }

    /// <summary>Takes in a start or end element of a styles or settings section.</summary>
    public void Observe(ref XmlSpanScanner x)
    {
        if (x.Token == XmlToken.EndElement)
        {
            if (x.Is("style"u8)) _tableStyle = null;
            return;
        }
        if (x.Token != XmlToken.StartElement) return;
        var name = x.LocalName;
        if (name.SequenceEqual("style"u8))
        {
            if (!x.TryGetAttribute("name"u8, out var n) || !x.TryGetAttribute("family"u8, out var family)) return;
            if (family.SequenceEqual("table-cell"u8))
            {
                string? data = x.TryGetAttribute("data-style-name"u8, out var d) ? XmlText.DecodeString(d, false) : null;
                string? parent = x.TryGetAttribute("parent-style-name"u8, out var p) ? XmlText.DecodeString(p, false) : null;
                _cellStyles[XmlText.DecodeString(n, false)] = (data, parent);
            }
            else if (family.SequenceEqual("table"u8) && !x.IsEmptyElement)
            {
                _tableStyle = XmlText.DecodeString(n, false);
            }
        }
        else if (name.SequenceEqual("table-properties"u8))
        {
            if (_tableStyle is not null && x.TryGetAttribute("display"u8, out var display) && !XmlText.ParseBoolean(display))
                _hiddenTables.Add(_tableStyle);
        }
        else if (name.SequenceEqual("time-style"u8))
        {
            if (x.TryGetAttribute("name"u8, out var n) && x.TryGetAttribute("truncate-on-overflow"u8, out var t) && !XmlText.ParseBoolean(t))
                _elapsedDataStyles.Add(XmlText.DecodeString(n, false));
        }
        else if (name.SequenceEqual("null-date"u8))
        {
            Date1904 = x.TryGetAttribute("date-value"u8, out var date) && XmlText.Trim(date).StartsWith("1904-01-01"u8);
        }
    }

    /// <summary>Whether a sheet with this table style is hidden.</summary>
    public bool IsHidden(string? tableStyle) => tableStyle is not null && _hiddenTables.Contains(tableStyle);

    /// <summary>Whether a cell style formats time values as elapsed time (its own data style or its parents').</summary>
    public bool IsElapsed(string? cellStyle)
    {
        if (cellStyle is null) return false;
        if (_elapsed.TryGetValue(cellStyle, out bool known)) return known;
        bool elapsed = false;
        string? style = cellStyle;
        for (int hops = 0; style is not null && hops < 64 && _cellStyles.TryGetValue(style, out var s); hops++)
        {
            if (s.DataStyle is not null)
            {
                elapsed = _elapsedDataStyles.Contains(s.DataStyle);
                break;
            }
            style = s.Parent;
        }
        _elapsed[cellStyle] = elapsed;
        return elapsed;
    }
}
