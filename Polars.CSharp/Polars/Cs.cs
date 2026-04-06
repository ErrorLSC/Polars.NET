#pragma warning disable 1591
using Polars.NET.Core;
using Polars.NET.Core.Arrow;

namespace Polars.CSharp;

public readonly partial struct Polars
{
    /// <summary>
    /// String matching selectors namespace.
    /// Usage: Cs.StartsWith("A") or Pl.Cs.StartWith("A")
    /// </summary>
    public readonly struct Selectors
    {
        /// <summary>
        /// Select all columns.
        /// </summary>
        public static Selector All() => new(PolarsWrapper.SelectorAll());
        /// <summary>
        /// Select columns.
        /// </summary>
        public static Selector ByName(params string[] columns) => new(PolarsWrapper.SelectorCols(columns));
        /// <summary>
        /// Select columns by their index. 
        /// Usage: Cs.ByIndex(0, 2, 4)
        /// </summary>
        public static Selector ByIndex(params ReadOnlySpan<long> indices) => ByIndex(indices, strict: true);

        /// <summary>
        /// Select columns by their index with strictness control.
        /// </summary>
        public static Selector ByIndex(ReadOnlySpan<long> indices, bool strict)=> new(PolarsWrapper.SelectorByIndex(indices, strict));
        /// <summary>
        /// Select columns by specific DataType.
        /// </summary>
        public static Selector ByDtype(DataType type) 
        {
            var typeKind = type.Kind;
            return new(PolarsWrapper.SelectorByDtype(typeKind.ToNative()));
        }
        /// <summary>
        /// Select columns by specific DataType.
        /// </summary>
        public static Selector ByDtype(Type type) 
        {
            var arrowType = ArrowTypeResolver.GetArrowTypeFromNetType(type);
            var plType = DataType.FromArrowType(arrowType);
            return ByDtype(plType);
        }
        /// <summary>
        /// Select columns by Generic Type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static Selector ByDtype<T>() 
            => ByDtype(typeof(T));
        /// <summary>
        /// Select all columns EXCEPT the specified Selectors.
        /// </summary>
        public static Selector Exclude(params ReadOnlySpan<Selector> selectors) => All().Exclude(selectors);
        /// <summary>
        /// Select all columns EXCEPT the specified Data Types.
        /// </summary>
        public static Selector Exclude(params ReadOnlySpan<DataType> dtypes) => All().Exclude(dtypes);
        /// <summary>
        /// Select all columns EXCEPT the specified Column names.
        /// </summary>
        public static Selector Exclude(params string[] columns) => All().Exclude(columns);
        /// <summary>
        /// Select the first column.
        /// </summary>
        public static Selector First() => ByIndex([0L]);
        /// <summary>
        /// Select the last column.
        /// </summary>
        public static Selector Last() => ByIndex([-1L]);
        /// <summary>
        /// Select all numeric columns (Int, Float, etc.).
        /// </summary>
        public static Selector Numeric()  => new(PolarsWrapper.SelectorNumeric());
        /// <summary>
        /// Select all string/utf8 columns.
        /// </summary>
        public static Selector String() => ByDtype(DataType.String);
        /// <summary>
        /// Select all date columns.
        /// </summary>
        public static Selector Date() => ByDtype(DataType.Date);
        public static Selector Boolean() => ByDtype(DataType.Boolean);
        public static Selector Binary() => ByDtype(DataType.Binary);
        public static Selector Empty() => new(PolarsWrapper.SelectorEmpty());
        public static Selector Integer() => new(PolarsWrapper.SelectorInteger());
        public static Selector UnsignedInteger() => new(PolarsWrapper.SelectorUnsignedInteger());
        public static Selector SignedInteger() => new(PolarsWrapper.SelectorSignedInteger());
        public static Selector Float() => new(PolarsWrapper.SelectorFloat());
        public static Selector Decimal() => new(PolarsWrapper.SelectorDecimal());
        public static Selector Enum() => new(PolarsWrapper.SelectorEnum());
        public static Selector Nested() => new(PolarsWrapper.SelectorNested());
        public static Selector Struct() => new(PolarsWrapper.SelectorStruct());
        public static Selector Temporal() => new(PolarsWrapper.SelectorTemporal());
        /// <summary>
        /// Select list columns. Optionally filter by the inner data type.
        /// Example: Cs.List(Cs.Integer())
        /// </summary>
        public static Selector List(Selector? inner = null) => new(PolarsWrapper.SelectorList(inner?.CloneHandle()));
        /// <summary>
        /// Select array columns. Optionally filter by inner data type and fixed width.
        /// </summary>
        public static Selector Array(Selector? inner = null, long? width = null) => new(PolarsWrapper.SelectorArray(inner?.CloneHandle(), width));
        private static PlTimeUnit GetNativeTimeUnit(TimeUnit? unit)
            => unit.HasValue ? unit.Value.ToNative() : PlTimeUnit.All;
        private static Selector DatetimeInternal(TimeUnit? timeUnit, string? tzString)
            =>new (PolarsWrapper.SelectorDatetime(GetNativeTimeUnit(timeUnit), tzString));
        /// <summary>
        /// Select all datetime columns (both with and without timezones).
        /// </summary>
        public static Selector Datetime(TimeUnit? timeUnit = null) => DatetimeInternal(timeUnit, null); // TimeZoneSet::Any
        /// <summary>
        /// Select ONLY timezone-naive datetime columns (no timezone set).
        /// </summary>
        public static Selector DatetimeNaive(TimeUnit? timeUnit = null) => DatetimeInternal(timeUnit, "");
        /// <summary>
        /// Select ONLY timezone-aware datetime columns (any timezone).
        /// </summary>
        public static Selector DatetimeAware(TimeUnit? timeUnit = null) => DatetimeInternal(timeUnit, "*"); 
        /// <summary>
        /// Select datetime columns matching a specific timezone (e.g., "UTC", "Asia/Shanghai").
        /// </summary>
        public static Selector DatetimeExact(string timeZone, TimeUnit? timeUnit = null) 
        {
            ArgumentException.ThrowIfNullOrEmpty(timeZone);
            return DatetimeInternal(timeUnit, timeZone);
        }
        /// <summary>
        /// Select all duration columns. Optionally match a specific TimeUnit.
        /// </summary>
        public static Selector Duration(TimeUnit? timeUnit = null) => new (PolarsWrapper.SelectorDuration(GetNativeTimeUnit(timeUnit)));
        /// <summary>
        /// Select column whose name starts with given prefix.
        /// </summary>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public static Selector StartsWith(string prefix) => new(PolarsWrapper.SelectorStartsWith(prefix));
        /// <summary>
        /// Select column whose name ends with given suffix.
        /// </summary>
        /// <param name="suffix"></param>
        /// <returns></returns>
        public static Selector EndsWith(string suffix) => new(PolarsWrapper.SelectorEndsWith(suffix));
        /// <summary>
        /// Select column whose name contains given string.
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public static Selector Contains(string str) => new(PolarsWrapper.SelectorContains(str));
        /// <summary>
        /// Select column whose name matches given string.
        /// </summary>
        /// <param name="regex">Regular Expression</param>
        /// <returns></returns>
        public static Selector Matches(string regex) => new(PolarsWrapper.SelectorMatch(regex));
        /// <summary>
        /// Select all columns with alphabetic names.
        /// </summary>
        public static Selector Alpha(bool asciiOnly = false, bool ignoreSpaces = false)
        {
            // asciiOnly ? [a-zA-Z] : \p{L} (Unicode characters)
            string charClass = asciiOnly ? "a-zA-Z" : @"\p{L}";
            if (ignoreSpaces) charClass += " ";
            
            string pattern = $"^[{charClass}]+$";
            return Matches(pattern);
        }
        /// <summary>
        /// <para>[EN] Select columns whose names consist entirely of CJK scripts (Han, Hiragana, Katakana, Hangul).
        /// The 'chinese' option enables \p{Han}, which also includes Japanese Kanji and Korean Hanja.</para>
        /// <para>[ZH] 选择列名完全由中日韩字符（Han / 平假名 / 片假名 / 韩文）组成的列。
        /// 注意：'chinese' 实际匹配 \p{Han}，包含日文汉字与韩文汉字。</para>
        /// <para>[JA] 列名がCJK文字（漢字・ひらがな・カタカナ・ハングル）のみで構成される列を選択します。
        /// ※ 'chinese' は \p{Han}（日本・韓国の漢字を含む）を有効にします。</para>
        /// <para>[KO] 열 이름이 CJK 문자(한자, 히라가나, 가타카나, 한글)로만 구성된 열을 선택합니다.
        /// ※ 'chinese'는 \p{Han}을 의미하며 일본/한국 한자도 포함합니다.</para>
        /// </summary>
        public static Selector CJK(
            bool chinese = true, 
            bool japanese = true, 
            bool korean = true, 
            bool ignoreSpaces = false)
        {
            if (!chinese && !japanese && !korean)
                throw new ArgumentException("At least one CJK script must be enabled.");

            string charClass = "";
            
            if (chinese)  charClass += @"\p{Han}";
            if (japanese) charClass += @"\p{Hiragana}\p{Katakana}";
            if (korean)   charClass += @"\p{Hangul}";
            
            if (ignoreSpaces) charClass += " ";

            string pattern = $"^[{charClass}]+$";
            return Matches(pattern);
        }
        /// <summary>
        /// <para>[EN] Select columns whose names consist of CJK scripts, Unicode digits (\p{N}),
        /// and optionally ASCII/full-width Latin letters.</para>
        /// <para>[ZH] 选择列名由中日韩字符、数字（\p{N}，含全/半角）以及可选英文字母（全/半角）组成的列。</para>
        /// <para>[JA] 列名がCJK文字・数字（\p{N}、全角/半角）および英字（全角/半角）で構成される列を選択します。</para>
        /// <para>[KO] 열 이름이 CJK 문자, 숫자(\p{N}, 전각/반각) 및 영문자(전각/반각)로 구성된 열을 선택합니다.</para>
        /// </summary>
        public static Selector CJKAlphanumeric(
            bool chinese = true, 
            bool japanese = true, 
            bool korean = true, 
            bool includeLetters = true, 
            bool ignoreSpaces = false)
        {
            if (!chinese && !japanese && !korean)
                throw new ArgumentException("At least one CJK script must be enabled.");

            string charClass = @"\p{N}"; 
            
            if (includeLetters) charClass += "a-zA-ZＡ-Ｚａ-ｚ";
            if (chinese)  charClass += @"\p{Han}";
            if (japanese) charClass += @"\p{Hiragana}\p{Katakana}";
            if (korean)   charClass += @"\p{Hangul}";
            
            if (ignoreSpaces) charClass += " ";

            string pattern = $"^[{charClass}]+$";
            return Matches(pattern);
        }

        /// <summary>
        /// Select all columns with alphanumeric names.
        /// </summary>
        public static Selector Alphanumeric(bool asciiOnly = false, bool ignoreSpaces = false)
        {
            // asciiOnly ? [a-zA-Z0-9] : [\p{L}\p{N}]
            string charClass = asciiOnly ? "a-zA-Z0-9" : @"\p{L}\p{N}";
            if (ignoreSpaces) charClass += " ";

            string pattern = $"^[{charClass}]+$";
            return Matches(pattern);
        }
        /// <summary>
        /// Expand a Selector against a DataFrame to get the matched column names.
        /// </summary>
        public static string[] ExpandSelector(DataFrame target, Selector selector)
        {
            ArgumentNullException.ThrowIfNull(target);
            ArgumentNullException.ThrowIfNull(selector);

            using var emptyDf = target.Clear(); 
            
            using var result = emptyDf.Select(selector);
            
            return [.. result.Columns];
        }

        /// <summary>
        /// Expand an Expr against a DataFrame to get the matched column names.
        /// </summary>
        public static string[] ExpandSelector(DataFrame target, Expr expr)
        {
            ArgumentNullException.ThrowIfNull(target);
            ArgumentNullException.ThrowIfNull(expr);

            using var emptyDf = target.Clear();
            using var result = emptyDf.Select(expr);
            return [.. result.Columns];
        }
        public static string[] ExpandSelector(LazyFrame target, Selector selector)
           => [.. target.Select(selector).Schema.Names];
        
    }
}