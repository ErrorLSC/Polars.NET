using System.Text;
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class SeriesStringOpsTests
{
    [Fact]
    [Trait("Series", "StringBasicOps")]
    public void Test_Series_String_Basic_Ops()
    {
        string[] data = ["hello", "WORLD", "a.b*c?d", "title case", "波拉熊🐻"];
        using Series s = Pl.Series("str_col", data);

        using Series upper = s.Str.ToUppercase();
        Assert.Equal("str_col", upper.Name);
        Assert.Equal(["HELLO", "WORLD", "A.B*C?D", "TITLE CASE", "波拉熊🐻"], upper.ToArray<string>());

        using Series lower = s.Str.ToLowercase();
        Assert.Equal(["hello", "world", "a.b*c?d", "title case", "波拉熊🐻"], lower.ToArray<string>());

        using Series title = s.Str.ToTitlecase();
        Assert.Equal(["Hello", "World", "A.B*C?D", "Title Case", "波拉熊🐻"], title.ToArray<string>());

        using Series escaped = s.Str.EscapeRegex();
        Assert.Equal(["hello", "WORLD", @"a\.b\*c\?d", "title case", "波拉熊🐻"], escaped.ToArray<string>());


        using Series lenBytes = s.Str.LenBytes();
        using Series lenChars = s.Str.LenChars();

        Assert.Equal([5u, 5u, 7u, 10u, 13u], lenBytes.ToArray<uint>());
        Assert.Equal([5u, 5u, 7u, 10u, 4u],lenChars.ToArray<uint>());

        using Series reverse = s.Str.Reverse();
        Assert.Equal(["olleh","DLROW","d?c*b.a","esac eltit","🐻熊拉波"],reverse.ToArray<string>());
    }
    [Fact]
    [Trait("Series", "StringSlice")]
    public void Test_Series_String_Slice()
    {
        string[] data = ["apple", "banana", "cherry", "波拉熊"];
        using Series s = Pl.Series("str_col", data);

        using Series slice1 = s.Str.Slice(1);
        Assert.Equal(["pple", "anana", "herry", "拉熊"], slice1.ToArray<string>());

        using Series slice2 = s.Str.Slice(1, 2);
        Assert.Equal(["pp", "an", "he", "拉熊"], slice2.ToArray<string>());

        using Series slice3 = s.Str.Slice(-2);
        Assert.Equal(["le", "na", "ry", "拉熊"], slice3.ToArray<string>());
    }
    [Fact]
    [Trait("Series", "StringJoin")]
    public void Test_Series_String_Join()
    {
        string[] data = ["a", "b", null, "c"];
        using Series s = Pl.Series("str_col", data);

        using Series join1 = s.Str.Join("-", ignoreNulls: true);
        
        Assert.Equal(1, join1.Length);
        Assert.Equal(["a-b-c"], join1.ToArray<string>());

        using Series join2 = s.Str.Join("-", ignoreNulls: false);
        
        Assert.Equal(1, join2.Length);
        Assert.Equal([null], join2.ToArray<string>());
    }
    [Fact]
    [Trait("Series", "StringReplace")]
    public void Test_Series_String_Replace_And_ReplaceAll()
    {
        string[] data = [
            "hello world", 
            "foo bar foo", 
            "123 abc 123", 
            "波拉熊 波拉熊"
        ];
        using Series s = Pl.Series("str_col", data);

        using Series replaceLiteral = s.Str.Replace("foo", "baz", literal: true, n: 1);
        Assert.Equal([
            "hello world", 
            "baz bar foo", 
            "123 abc 123", 
            "波拉熊 波拉熊"
        ], replaceLiteral.ToArray<string>());

        using Series replaceRegex = s.Str.Replace(@"\d+", "num", literal: false, n: 1);
        Assert.Equal([
            "hello world", 
            "foo bar foo", 
            "num abc 123", 
            "波拉熊 波拉熊"
        ], replaceRegex.ToArray<string>());

        using Series replaceAllLiteral = s.Str.ReplaceAll("波拉", "Pola", literal: true);
        Assert.Equal([
            "hello world", 
            "foo bar foo", 
            "123 abc 123", 
            "Pola熊 Pola熊"
        ], replaceAllLiteral.ToArray<string>());

        using Series replaceAllRegex = s.Str.ReplaceAll(@"\d+", "num", literal: false);
        Assert.Equal([
            "hello world", 
            "foo bar foo", 
            "num abc num", 
            "波拉熊 波拉熊"
        ], replaceAllRegex.ToArray<string>());
    }

    [Fact]
    [Trait("Series", "StringReplaceMany")]
    public void Test_Series_String_ReplaceMany()
    {
        string[] data = [
            "hello world", 
            "apple and banana", 
            "波拉熊 love polars"
        ];
        using Series s = Pl.Series("str_col", data);

        string[] expected = [
            "hi earth", 
            "pear and orange", 
            "PolaBear love Polars.NET"
        ];

        var mapping = new Dictionary<string, string>
        {
            { "hello", "hi" },
            { "world", "earth" },
            { "apple", "pear" },
            { "banana", "orange" },
            { "波拉熊", "PolaBear" },
            { "polars", "Polars.NET" }
        };
        using Series resDict = s.Str.ReplaceMany(mapping);
        Assert.Equal(expected, resDict.ToArray<string>());

        string[] patterns = ["hello", "world", "apple", "banana", "波拉熊", "polars"];
        string[] replaceWith = ["hi", "earth", "pear", "orange", "PolaBear", "Polars.NET"];
        
        using Series resEnum = s.Str.ReplaceMany(patterns, replaceWith);
        Assert.Equal(expected, resEnum.ToArray<string>());

        using Series patSeries = Pl.Series("pat", patterns).Implode();
        using Series valSeries = Pl.Series("val", replaceWith).Implode();
        
        using Series resSeries = s.Str.ReplaceMany(patSeries, valSeries);
        Assert.Equal(expected, resSeries.ToArray<string>());
    }
    [Fact]
    [Trait("Series", "StringExtract")]
    public void Test_Series_String_Extract()
    {
        string[] data = ["user@gmail.com", "admin@yahoo.com", "invalid-email", "波拉熊@polars.rs"];
        using Series s = Pl.Series("emails", data);

        using Series ext1 = s.Str.Extract(@"([^@]+)@([^@]+)");
        Assert.Equal(["user", "admin", null, "波拉熊"], ext1.ToArray<string>());

        using Series ext2 = s.Str.Extract(@"([^@]+)@([^@]+)", groupIndex: 2);
        Assert.Equal(["gmail.com", "yahoo.com", null, "polars.rs"], ext2.ToArray<string>());
    }

    [Fact]
    [Trait("Series", "StringExtractMany")]
    public void Test_Series_String_ExtractMany()
    {
        string[] data = ["I like apple and banana", "I love cherry", "Nothing here", "波拉熊 likes bamboo"];
        using Series s = Pl.Series("text", data);

        string[] patterns = ["apple", "banana", "cherry", "bamboo"];

        using Series resEnum = s.Str.ExtractMany(patterns);
        Assert.Equal(["apple","banana", "cherry", null, "bamboo"], resEnum.Explode().ToArray<string>());

        using Series patSeries = Pl.Series("patterns", patterns);
        using Series resSeries = s.Str.ExtractMany(patSeries.Implode());
        Assert.Equal(["apple","banana", "cherry", null, "bamboo"], resSeries.Explode().ToArray<string>());
    }

    [Fact]
    [Trait("Series", "StringExtractAll")]
    public void Test_Series_String_ExtractAll()
    {
        string[] data = ["apple 123", "banana 456 789", "no numbers", "波拉熊 111"];
        using Series s = Pl.Series("text", data);

        string[] patterns = [@"\d+"];

        using Series resEnum = s.Str.ExtractAll(patterns);
        Assert.Equal(4, resEnum.Length);
        Assert.Equal(DataType.List(DataType.String), resEnum.DataType);

        using Series patSeries = Pl.Series("pat", patterns);
        using Series resSeries = s.Str.ExtractAll(patSeries);
        Assert.Equal(4, resSeries.Length);
    }

    [Fact]
    [Trait("Series", "StringExtractGroups")]
    public void Test_Series_String_ExtractGroups()
    {
        string[] data = [
            "url: https://google.com", 
            "url: ftp://localhost", 
            "invalid_text", 
            "url: http://波拉熊.com"
        ];
        using Series s = Pl.Series("text", data);

        string pattern = @"url: (?<protocol>\w+)://(?<domain>.+)";
        using Series groups = s.Str.ExtractGroups(pattern);

        Assert.Equal(4, groups.Length);
        var protocolSeries = groups.Struct.Field("protocol");
        Assert.Equal(["https","ftp",null,"http"], protocolSeries.ToArray<string>());
    }
    [Fact]
    [Trait("Series", "StringContains")]
    public void Test_Series_String_Contains()
    {
        string[] data = [
            "hello world", 
            "123.456", 
            "admin@polars.rs", 
            "你好 波拉熊",
            null
        ];
        using Series s = Pl.Series("text", data);

        using Series containsLiteral = s.Str.Contains("world", literal: true);
        Assert.Equal([true, false, false, false, null], containsLiteral.ToArray<bool?>());

        using Series containsDot = s.Str.Contains(".", literal: true);
        Assert.Equal([false, true, true, false, null], containsDot.ToArray<bool?>());

        using Series containsRegex = s.Str.Contains(".", literal: false);
        Assert.Equal([true, true, true, true, null], containsRegex.ToArray<bool?>());

        using Series containsChinese = s.Str.Contains("波拉熊");
        Assert.Equal([false, false, false, true, null], containsChinese.ToArray<bool?>());
    }

    [Fact]
    [Trait("Series", "StringContainsAny")]
    public void Test_Series_String_ContainsAny()
    {
        string[] data = [
            "I like apple", 
            "banana is good", 
            "POLARS is extremely fast", 
            "波拉熊 eats bamboo",
            null
        ];
        using Series s = Pl.Series("text", data);

        string[] patterns = ["apple", "polars", "波拉熊"];

        using Series anyCaseSensitive = s.Str.ContainsAny(patterns, asciiCaseInsensitive: false);
        Assert.Equal([true, false, false, true, null], anyCaseSensitive.ToArray<bool?>());

        using Series anyCaseInsensitive = s.Str.ContainsAny(patterns, asciiCaseInsensitive: true);
        Assert.Equal([true, false, true, true, null], anyCaseInsensitive.ToArray<bool?>());

        using Series patSeries = Pl.Series("patterns", patterns);
        using Series anyWithSeries = s.Str.ContainsAny(patSeries, asciiCaseInsensitive: true);
        Assert.Equal([true, false, true, true, null], anyWithSeries.ToArray<bool?>());
    }
    [Fact]
    [Trait("Series", "StringSplit")]
    public void Test_Series_String_Split()
    {
        string[] data = [
            "apple,banana,cherry", 
            "foo-bar", 
            "波拉熊", 
            null
        ];
        using Series s = Pl.Series("text", data);

        using Series split1 = s.Str.Split(",");
        Assert.Equal(4, split1.Length);
        
        using Series splitInclusive = s.Str.Split("-", inclusive: true);
        Assert.Equal(4, splitInclusive.Length);

        using Series splitRegex = s.Str.Split(@"\W+", literal: false);
        Assert.Equal(4, splitRegex.Length);
    }

    [Fact]
    [Trait("Series", "StringSplitN")]
    public void Test_Series_String_SplitN()
    {
        string[] data = [
            "a,b,c,d", 
            "e,f", 
            "g"
        ];
        using Series s = Pl.Series("text", data);

        using Series splitN = s.Str.SplitN(",", n: 3);

        Assert.Equal(3, splitN.Length);
        Assert.Equal(DataTypeKind.Struct,splitN.DataType.Kind);
    }

    [Fact]
    [Trait("Series", "StringSplitExact")]
    public void Test_Series_String_SplitExact()
    {
        string[] data = [
            "1|2|3|4", 
            "5|6|7", 
            "8|9"
        ];
        using Series s = Pl.Series("text", data);

        using Series splitExact1 = s.Str.SplitExact("|", n: 3, inclusive: false);
        
        Assert.Equal(3, splitExact1.Length);
        Assert.Equal(DataTypeKind.Struct,splitExact1.DataType.Kind);

        using Series splitExactInclusive = s.Str.SplitExact("|", n: 2, inclusive: true);
        
        Assert.Equal(3, splitExactInclusive.Length);
        Assert.Equal(DataTypeKind.Struct,splitExactInclusive.DataType.Kind);
    }
    [Fact]
    [Trait("Series", "StringFind")]
    public void Test_Series_String_Find()
    {
        string[] data = [
            "hello world", 
            "foo bar foo", 
            "波拉熊", 
            null
        ];
        using Series s = Pl.Series("text", data);

        using Series findLiteral = s.Str.Find("o", literal: true);
        
        Assert.Equal([4u, 1u, null, null], findLiteral.ToArray<uint?>());
        Assert.Equal(DataTypeKind.UInt32, findLiteral.DataType.Kind); 

        using Series findRegex = s.Str.Find(@"b\w+", literal: false);
        
        Assert.Equal([null, 4u, null, null], findRegex.ToArray<uint?>());
    }

    [Fact]
    [Trait("Series", "StringFindMany")]
    public void Test_Series_String_FindMany()
    {
        string[] data = [
            "I love apple", 
            "banana is good", 
            "POLARS is fast", 
            "波拉熊 eats bamboo",
            null
        ];
        using Series s = Pl.Series("text", data);

        string[] patternsArray = ["apple", "polars", "波拉熊", "bamboo"];
        var patterns = Pl.Lit(patternsArray).Implode();

        using Series findManySensitive = s.Str.FindMany(patterns, asciiCaseInsensitive: false);

        Assert.Equal([7,null,null,0,15,null], findManySensitive.Explode().ToArray<uint?>());

        using Series findManyInsensitive = s.Str.FindMany(patterns, asciiCaseInsensitive: true);

        Assert.Equal([7,null,0,0,15,null], findManyInsensitive.Explode().ToArray<uint?>());
        
        string[] overlapData = ["ababc"];
        using Series sOverlap = Pl.Series("overlap", overlapData);
        var overlapPat = Pl.Lit(["abab", "babc"]).Implode();

        using Series findOverlap = sOverlap.Str.FindMany(overlapPat, leftmost: true);
        Assert.Equal([0], findOverlap.Explode().ToArray<uint?>());
    }
    [Fact]
    [Trait("Series", "StringCountMatches")]
    public void Test_Series_String_CountMatches()
    {
        string[] data = [
            "apple banana apple", 
            "foo bar foo bar foo", 
            "123.456.789", 
            "波拉熊 波拉熊",
            null
        ];
        using Series s = Pl.Series("text", data);

        string[] literalPatterns = [
            "apple",  
            "foo",   
            ".",     
            "波拉熊",  
            "any"     
        ];
        
        using Series countLiteral = s.Str.CountMatches(literalPatterns, literal: true);

        Assert.Equal([2u, 3u, 2u, 2u, null], countLiteral.ToArray<uint?>());
        Assert.Equal(DataTypeKind.UInt32, countLiteral.DataType.Kind); 

        string[] regexPatterns = [
            @"\ba\w+",      
            @"(foo|bar)",  
            @"\d+",        
            @"波拉熊",      
            @".*"         
        ];
        
        using Series patSeries = Pl.Series("patterns", regexPatterns);
        using Series countRegex = s.Str.CountMatches(patSeries, literal: false);

        Assert.Equal([2u, 5u, 3u, 2u, null], countRegex.ToArray<uint?>());
        Assert.Equal(DataType.UInt32, countRegex.DataType);
    }
    [Fact]
    [Trait("Series", "StringStripWhitespace")]
    public void Test_Series_String_StripWhitespace()
    {
        string[] data = [
            "  hello  ", 
            "\tworld\n", 
            " no space ", 
            null
        ];
        using Series s = Pl.Series("text", data);


        using Series stripAll = s.Str.StripChars();
        Assert.Equal(["hello", "world", "no space", null], stripAll.ToArray<string>());

        using Series stripStart = s.Str.StripCharsStart();
        Assert.Equal(["hello  ", "world\n", "no space ", null], stripStart.ToArray<string>());

        using Series stripEnd = s.Str.StripCharsEnd();
        Assert.Equal(["  hello", "\tworld", " no space", null], stripEnd.ToArray<string>());
    }

    [Fact]
    [Trait("Series", "StringStripChars")]
    public void Test_Series_String_StripChars()
    {
        // 构造基础数据
        string[] data = [
            "xxyhelloyyx", 
            "abab-test-baba", 
            "波拉熊波", 
            null
        ];
        using Series s = Pl.Series("text", data);

        using Series stripChars = s.Str.StripChars("xy");
        Assert.Equal(["hello", "abab-test-baba", "波拉熊波", null], stripChars.ToArray<string>());

        using Series stripStart = s.Str.StripCharsStart("ab");
        Assert.Equal(["xxyhelloyyx", "-test-baba", "波拉熊波", null], stripStart.ToArray<string>());

        using Series stripEnd = s.Str.StripCharsEnd("波");
        Assert.Equal(["xxyhelloyyx", "abab-test-baba", "波拉熊", null], stripEnd.ToArray<string>());
    }

    [Fact]
    [Trait("Series", "StringStripPrefixSuffix")]
    public void Test_Series_String_StripPrefixSuffix()
    {
        // 构造基础数据
        string[] data = [
            "admin_user", 
            "super_admin", 
            "error_log.txt", 
            "波拉熊_info.txt", 
            null
        ];
        using Series s = Pl.Series("text", data);

        using Series stripPrefix = s.Str.StripPrefix("admin_");
        
        Assert.Equal(["user", "super_admin", "error_log.txt", "波拉熊_info.txt", null], stripPrefix.ToArray<string>());

        using Series stripSuffix = s.Str.StripSuffix(".txt");

        Assert.Equal(["admin_user", "super_admin", "error_log", "波拉熊_info", null], stripSuffix.ToArray<string>());

        using Expr prefixExpr = Pl.Lit("波拉熊_");
        using Series stripUnicodePrefix = s.Str.StripPrefix(prefixExpr);
        
        Assert.Equal(["admin_user", "super_admin", "error_log.txt", "info.txt", null], stripUnicodePrefix.ToArray<string>());
    }
    [Fact]
    [Trait("Series", "StringAnchors")]
    public void Test_Series_String_StartsWith_EndsWith()
    {
        string[] data = [
            "polars is fast", 
            "pandas is good", 
            "polar bear", 
            "arctic polar"
        ];
        using Series s = Pl.Series("text", data);

        using Series starts = s.Str.StartsWith("pol");
        
        Assert.Equal([true, false, true, false], starts.ToArray<bool>());
        Assert.Equal(DataType.Boolean, starts.DataType); 

        using Series ends = s.Str.EndsWith("ar");
 
        Assert.Equal([false, false, true, true], ends.ToArray<bool>());
        Assert.Equal(DataType.Boolean, ends.DataType);

        using Expr prefixExpr = Pl.Lit("pan");
        using Series startsExpr = s.Str.StartsWith(prefixExpr);
        
        Assert.Equal([false, true, false, false], startsExpr.ToArray<bool>());
    }

    [Fact]
    [Trait("Series", "StringHeadTail")]
    public void Test_Series_String_Head_Tail()
    {
        string[] data = [
            "apple", 
            "banana", 
            "cherry", 
            "波拉熊"
        ];
        using Series s = Pl.Series("text", data);

        using Series head2 = s.Str.Head(2);
 
        Assert.Equal(["ap", "ba", "ch", "波拉"], head2.ToArray<string>());
        Assert.Equal(DataType.String, head2.DataType);

        using Series tail2 = s.Str.Tail(2);
        
        Assert.Equal(["le", "na", "ry", "拉熊"], tail2.ToArray<string>());
        Assert.Equal(DataType.String, tail2.DataType);

        using Expr countExpr = Pl.Lit(3);
        using Series head3 = s.Str.Head(countExpr);
        
        Assert.Equal(["app", "ban", "che", "波拉熊"], head3.ToArray<string>());
    }
    [Fact]
    [Trait("Series", "StringJson")]
    public void Test_Series_String_Json_Ops()
    {
        string[] data = [
            """{"user": "alice", "age": 25, "scores": [100, 90]}""", 
            """{"user": "bob", "age": 30}""", 
            """{"user": "charlie"}""", 
            null
        ];
        using Series s = Pl.Series("json_col", data);

        using Series matchUser = s.Str.JsonPathMatch("$.user");
        
        Assert.Equal(["alice", "bob", "charlie", null], matchUser.ToArray<string>());
        Assert.Equal(DataTypeKind.String, matchUser.DataType.Kind);

        using Series matchScore = s.Str.JsonPathMatch("$.scores[0]");
        
        Assert.Equal(["100", null, null, null], matchScore.ToArray<string>());

        using Expr pathExpr = Pl.Lit("$.age");
        using Series matchAge = s.Str.JsonPathMatch(pathExpr);
        
        Assert.Equal(["25", "30", null, null], matchAge.ToArray<string>());

        using var targetDtype = DataType.Struct([
            ("user", DataType.String),
            ("age", DataType.Int64)
        ]);

        using Series decoded = s.Str.JsonDecode(targetDtype);

        Assert.Equal(4, decoded.Length);
        Assert.Equal(DataTypeKind.Struct, decoded.DataType.Kind);
        decoded.Show();
    }
    [Fact]
    [Trait("Series", "StringPadding")]
    public void Test_Series_String_Padding()
    {
        string[] data = [
            "1", 
            "42", 
            "12345", 
            "波拉熊"
        ];
        using Series s = Pl.Series("text", data);

        using Series zfill = s.Str.Zfill(5);

        Assert.Equal(["00001", "00042", "12345", "波拉熊"], zfill.ToArray<string>());
        Assert.Equal(DataType.String, zfill.DataType); 

        using Series padStart = s.Str.PadStart(5, "*");

        Assert.Equal(["****1", "***42", "12345", "**波拉熊"], padStart.ToArray<string>());
        Assert.Equal(DataTypeKind.String, padStart.DataType.Kind);

  
        using Series padEnd = s.Str.PadEnd(5, "-");

        Assert.Equal(["1----", "42---", "12345", "波拉熊--"], padEnd.ToArray<string>());
        Assert.Equal(DataTypeKind.String, padEnd.DataType.Kind);

        using Expr lenExpr = Pl.Lit(4);
        using Series padExpr = s.Str.PadStart(lenExpr, "_");

        Assert.Equal(["___1", "__42", "12345", "_波拉熊"], padExpr.ToArray<string>());
    }
    [Fact]
    [Trait("Series", "StringEncoding")]
    public void Test_Series_String_Encode_Decode()
    {
        string[] data = [
            "hello", 
            "polars", 
            "波拉熊", 
            null
        ];
        using Series s = Pl.Series("text", data);

        using Series hexEncoded = s.Str.Encode(TransferEncoding.Hex);
        Assert.Equal(DataType.String, hexEncoded.DataType);

        // "hello" -> "68656c6c6f"
        // "polars" -> "706f6c617273"
        // "波拉熊" -> "e6b3a2e68b8ee7868a"

        Assert.Contains(["68656c6c6f", "706f6c617273"], hexEncoded.ToArray<string>());

        using Series hexDecoded = hexEncoded.Str.Decode(TransferEncoding.Hex);

        using Series base64Encoded = s.Str.Encode(TransferEncoding.Base64);
        Assert.Equal(DataTypeKind.String, base64Encoded.DataType.Kind);

        // "hello" -> "aGVsbG8="
        // "polars" -> "cG9sYXJz"
        // "波拉熊" -> "5rOi5ouJ54aK"
        Assert.Equal(["aGVsbG8=", "cG9sYXJz", "5rOi5ouJ54aK", null], base64Encoded.ToArray<string>());

        using Series base64Decoded = base64Encoded.Str.Decode(TransferEncoding.Base64);
    }

    [Fact]
    [Trait("Series", "StringDecodeStrict")]
    public void Test_Series_String_Decode_Strict_Mode()
    {
        string[] invalidData = [
            "invalid_hex_string!!!", 
            "68656c6c6f", 
            null
        ];
        using Series invalidSeries = Pl.Series("invalid_data", invalidData);

        using Series nonStrictDecoded = invalidSeries.Str.Decode(TransferEncoding.Hex, strict: false);

        Assert.Null(nonStrictDecoded[0]);
        Assert.Null(nonStrictDecoded[2]);

        Assert.Throws<PolarsException>(() => 
        {
            invalidSeries.Str.Decode(TransferEncoding.Hex, strict: true);
        });
    }
    [Fact]
    [Trait("Series", "StringTemporalDate")]
    public void Test_Series_String_ToDate()
    {
        string[] data = [
            "2024-01-01", 
            "2024-12-31", 
            "not_a_date", 
            null
        ];
        using Series s = Pl.Series("date_str", data);

        using Series parsedDate = s.Str.ToDate(strict: false);
        
        Assert.Equal(DataTypeKind.Date, parsedDate.DataType.Kind);

        using Series dateAsStr = parsedDate.Cast(DataType.String);
        Assert.Equal(["2024-01-01", "2024-12-31", null, null], dateAsStr.ToArray<string>());

        string[] customData = ["31/12/2024", "01/01/2024"];
        using Series sCustom = Pl.Series("custom_date", customData);

        using Series parsedCustom = sCustom.Str.ToDate(format: "%d/%m/%Y");
        using Series customAsStr = parsedCustom.Cast(DataType.String);
        
        Assert.Equal(["2024-12-31", "2024-01-01"], customAsStr.ToArray<string>());

        Assert.Throws<PolarsException>(() => 
        {
            s.Str.ToDate(strict: true);
        });
    }

    [Fact]
    [Trait("Series", "StringTemporalTime")]
    public void Test_Series_String_ToTime()
    {
        string[] data = [
            "12:30:45", 
            "08:15:00.123",
            "invalid_time", 
            null
        ];
        using Series s = Pl.Series("time_str", data);

        using Series parsedTime = s.Str.ToTime(strict: false);
        
        Assert.Equal(DataTypeKind.Time, parsedTime.DataType.Kind);

        Assert.Equal([new TimeOnly(12,30,45),new TimeOnly(8,15,0,123), null, null], parsedTime.ToArray<TimeOnly?>());

        string[] customData = ["14-30-00", "09-00-00"];
        using Series sCustom = Pl.Series("custom_time", customData);

        using Series parsedCustom = sCustom.Str.ToTime(format: "%H-%M-%S");
        
        Assert.Equal([new TimeOnly(14,30),new TimeOnly(9,0)], parsedCustom.ToArray<TimeOnly>());

        Assert.Throws<PolarsException>(() => 
        {
            s.Str.ToTime(strict: true);
        });
    }
    [Fact]
    [Trait("Series", "StringTemporalDatetime")]
    public void Test_Series_String_ToDatetime()
    {
        string[] data = [
            "2024-01-01 12:30:45", 
            "2024-12-31 23:59:59.123", 
            "invalid_datetime", 
            null
        ];
        using Series s = Pl.Series("datetime_str", data);

        using Series parsedDt = s.Str.ToDatetime(strict: false);
        
        Assert.Equal(DataTypeKind.Datetime, parsedDt.DataType.Kind);
        Assert.Equal([
            new DateTime(2024, 1, 1, 12, 30, 45), 
            new DateTime(2024, 12, 31, 23, 59, 59, 123), 
            null, 
            null
        ], parsedDt.ToArray<DateTime?>());

        string[] customData = ["2024/05/20 14-30-00", "2023/11/11 11-11-11"];
        using Series sCustom = Pl.Series("custom_dt", customData);

        using Series parsedCustom = sCustom.Str.ToDatetime(format: "%Y/%m/%d %H-%M-%S");
        
        Assert.Equal([
            new DateTime(2024, 5, 20, 14, 30, 0), 
            new DateTime(2023, 11, 11, 11, 11, 11)
        ], parsedCustom.ToArray<DateTime>());

        Assert.Throws<PolarsException>(() => 
        {
            s.Str.ToDatetime(strict: true);
        });
    }

    [Fact]
    [Trait("Series", "StringStrptime")]
    public void Test_Series_String_Strptime()
    {
        string[] dateData = ["2024-01-01", "2024-12-31", null];
        using Series sDate = Pl.Series("date_str", dateData);

        using Series parsedDate = sDate.Str.Strptime(DataType.Date, strict: false);
        
        Assert.Equal(DataTypeKind.Date, parsedDate.DataType.Kind);
        Assert.Equal([
            new DateOnly(2024, 1, 1), 
            new DateOnly(2024, 12, 31), 
            null
        ], parsedDate.ToArray<DateOnly?>());

        string[] dtData = ["01-01-2024 12:00", "15-08-2025 08:30"];
        using Series sDt = Pl.Series("dt_str", dtData);

        using Series parsedDt = sDt.Str.Strptime(DataType.Datetime(TimeUnit.Microseconds), format: "%d-%m-%Y %H:%M");
        
        Assert.Equal(DataType.Datetime(TimeUnit.Microseconds), parsedDt.DataType);
        Assert.Equal([
            new DateTime(2024, 1, 1, 12, 0, 0),
            new DateTime(2025, 8, 15, 8, 30, 0)
        ], parsedDt.ToArray<DateTime>());
    }
    [Fact]
    [Trait("Series", "StringNumericDecimal")]
    public void Test_Series_String_ToDecimal()
    {
        string[] data = [
            "123.45", 
            "-9.99", 
            "0.00", 
            null
        ];
        using Series s = Pl.Series("decimal_str", data);

        using Series parsedDec = s.Str.ToDecimal(scale: 2);

        Assert.Equal(DataType.Decimal(38,2), parsedDec.DataType); 

        Assert.Equal([123.45m, -9.99m, 0.00m, null], parsedDec.ToArray<decimal?>());
    }

    [Fact]
    [Trait("Series", "StringNumericInteger")]
    public void Test_Series_String_ToInteger()
    {
        // 构造基础数据
        string[] data = [
            "42", 
            "-100", 
            "0", 
            null
        ];
        using Series s = Pl.Series("int_str", data);

        using Series parsedDefault = s.Str.ToInteger();

        Assert.Equal(DataType.Int64, parsedDefault.DataType);
        Assert.Equal([42L, -100L, 0L, null], parsedDefault.ToArray<long?>());

        string[] hexData = ["1A", "FF", "-10"];
        using Series sHex = Pl.Series("hex_str", hexData);

        using Series parsedHex = sHex.Str.ToInteger(radix: 16);
        Assert.Equal([26L, 255L, -16L], parsedHex.ToArray<long>()); 

        string[] binData = ["1010", "1111"];
        using Series sBin = Pl.Series("bin_str", binData);

        using Expr radixExpr = Pl.Lit(2);
        using Series parsedBin = sBin.Str.ToInteger(radix: radixExpr, dtype: DataType.Int32);
        
        Assert.Equal(DataType.Int32, parsedBin.DataType);

        Assert.Equal([10, 15], parsedBin.ToArray<int>());


        string[] invalidData = ["123", "not_a_number", null];
        using Series sInv = Pl.Series("inv_str", invalidData);

        using Series parsedInv = sInv.Str.ToInteger(strict: false);

        Assert.Equal([123L, null, null], parsedInv.ToArray<long?>());

        Assert.Throws<PolarsException>(() => 
        {
            sInv.Str.ToInteger(strict: true);
        });
    }
    [Fact]
    [Trait("Series", "StringNormalize")]
    public void Test_Series_String_Normalize()
    {
        // 1. "\u00E9"   -> é (NFC len 1)
        // 2. "e\u0301"  -> e (NFD len 2)
        // 3. "\uFB01"   -> ﬁ (len 1)
        // 4. null
        string[] data = [
            "\u00E9", 
            "e\u0301", 
            "\uFB01", 
            null
        ];
        using Series s = Pl.Series("unicode_str", data);

        using Series nfd = s.Str.Normalize(NormalizationForm.FormD);
        
        Assert.Equal(DataTypeKind.String, nfd.DataType.Kind);
        Assert.Equal(["e\u0301", "e\u0301", "\uFB01", null], nfd.ToArray<string>());

        using Series nfdLen = nfd.Str.LenChars();
        Assert.Equal([2u, 2u, 1u, null], nfdLen.ToArray<uint?>());

        using Series nfc = s.Str.Normalize(NormalizationForm.FormC);
        
        Assert.Equal(["\u00E9", "\u00E9", "\uFB01", null], nfc.ToArray<string>());

        using Series nfcLen = nfc.Str.LenChars();
        Assert.Equal([1u, 1u, 1u, null], nfcLen.ToArray<uint?>());

        using Series nfkc = s.Str.Normalize(NormalizationForm.FormKC);

        Assert.Equal(["\u00E9", "\u00E9", "fi", null], nfkc.ToArray<string>());

        using Series nfkcLen = nfkc.Str.LenChars();
        Assert.Equal([1u, 1u, 2u, null], nfkcLen.ToArray<uint?>());
    }
}