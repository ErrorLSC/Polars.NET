using System.IO.Compression;
using System.Text;

namespace Seseragi.Files.Tests;

/// <summary>
/// OpenDocument spreadsheets written from markup for the tests: the same <c>office:spreadsheet</c> content (and
/// styles) as a package (.ods: mimetype, manifest, content.xml, styles.xml) or as a flat document (.fods).
/// </summary>
internal static class OdsBuilder
{
    public const string Spreadsheet = "application/vnd.oasis.opendocument.spreadsheet";

    private const string Namespaces = """
        xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0" xmlns:style="urn:oasis:names:tc:opendocument:xmlns:style:1.0" xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0" xmlns:table="urn:oasis:names:tc:opendocument:xmlns:table:1.0" xmlns:draw="urn:oasis:names:tc:opendocument:xmlns:drawing:1.0" xmlns:fo="urn:oasis:names:tc:opendocument:xmlns:xsl-fo-compatible:1.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:number="urn:oasis:names:tc:opendocument:xmlns:datastyle:1.0" xmlns:svg="urn:oasis:names:tc:opendocument:xmlns:svg-compatible:1.0" xmlns:calcext="urn:org:documentfoundation:names:experimental:calc:xmlns:calcext:1.0" office:version="1.3"
        """;

    /// <summary>A spreadsheet as a package or a flat document.</summary>
    /// <param name="spreadsheet">The content of <c>office:spreadsheet</c>: tables and settings.</param>
    /// <param name="automaticStyles">Content of <c>office:automatic-styles</c> (content.xml).</param>
    /// <param name="styles">Content of <c>office:styles</c> (styles.xml).</param>
    public static MemoryStream Build(bool flat, string spreadsheet, string automaticStyles = "", string styles = "", string mediaType = Spreadsheet) =>
        flat ? Flat(spreadsheet, automaticStyles, styles, mediaType) : Package(spreadsheet, automaticStyles, styles, mediaType);

    public static MemoryStream Package(string spreadsheet, string automaticStyles = "", string styles = "", string? mediaType = Spreadsheet,
        bool encrypted = false, bool manifestMediaType = true)
    {
        var stream = new MemoryStream();
        using (var zip = new ZipArchive(stream, ZipArchiveMode.Create, leaveOpen: true))
        {
            if (mediaType is not null) Entry(zip, "mimetype", mediaType, CompressionLevel.NoCompression);
            string encryption = encrypted
                ? """<manifest:encryption-data manifest:checksum-type="SHA1/1K" manifest:checksum="x"><manifest:algorithm manifest:algorithm-name="Blowfish CFB" manifest:initialisation-vector="x"/></manifest:encryption-data>"""
                : "";
            Entry(zip, "META-INF/manifest.xml", $"""
                <?xml version="1.0" encoding="UTF-8"?>
                <manifest:manifest xmlns:manifest="urn:oasis:names:tc:opendocument:xmlns:manifest:1.0" manifest:version="1.3">
                 <manifest:file-entry manifest:full-path="/" manifest:media-type="{(manifestMediaType ? mediaType ?? Spreadsheet : "")}"/>
                 <manifest:file-entry manifest:full-path="content.xml" manifest:media-type="text/xml">{encryption}</manifest:file-entry>
                 <manifest:file-entry manifest:full-path="styles.xml" manifest:media-type="text/xml"/>
                </manifest:manifest>
                """);
            Entry(zip, "content.xml", $"""
                <?xml version="1.0" encoding="UTF-8"?>
                <office:document-content {Namespaces}><office:automatic-styles>{automaticStyles}</office:automatic-styles><office:body><office:spreadsheet>{spreadsheet}</office:spreadsheet></office:body></office:document-content>
                """);
            Entry(zip, "styles.xml", $"""
                <?xml version="1.0" encoding="UTF-8"?>
                <office:document-styles {Namespaces}><office:styles>{styles}</office:styles></office:document-styles>
                """);
        }
        stream.Position = 0;
        return stream;
    }

    public static MemoryStream Flat(string spreadsheet, string automaticStyles = "", string styles = "", string mediaType = Spreadsheet) =>
        new(Encoding.UTF8.GetBytes($"""
            <?xml version="1.0" encoding="UTF-8"?>
            <office:document {Namespaces} office:mimetype="{mediaType}"><office:styles>{styles}</office:styles><office:automatic-styles>{automaticStyles}</office:automatic-styles><office:body><office:spreadsheet>{spreadsheet}</office:spreadsheet></office:body></office:document>
            """));

    /// <summary>A table of rows, each a list of cell markups.</summary>
    public static string Table(string name, params string[] rows) =>
        $"""<table:table table:name="{name}">{string.Concat(rows.Select(r => $"<table:table-row>{r}</table:table-row>"))}</table:table>""";

    public static string Text(string s) => $"""<table:table-cell office:value-type="string"><text:p>{s}</text:p></table:table-cell>""";

    public static string Float(string v) => $"""<table:table-cell office:value-type="float" office:value="{v}"><text:p>{v}</text:p></table:table-cell>""";

    private static void Entry(ZipArchive zip, string name, string content, CompressionLevel level = CompressionLevel.Optimal)
    {
        using var w = new StreamWriter(zip.CreateEntry(name, level).Open(), new UTF8Encoding(false));
        w.Write(content);
    }
}
