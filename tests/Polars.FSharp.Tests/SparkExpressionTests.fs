namespace Polars.FSharp.Tests

open System
open Apache.Arrow
open Apache.Arrow.Types
open Xunit
open Kaminari.Samidare
open Kaminari.Samidare.IR
open Kaminari.Samidare.Sql

/// The Spark expression profile (Delta CHECK constraints, generated columns, defaults): how Spark's text is read, and
/// what the binder refuses and why. Values are checked against Spark itself in the conformance suite
/// (SparkExpressionConformance, Spark/spark-expressions.json).
module SparkExpressionTests =

    let private spark text = SqlFormat.expression (SqlParser.parseExpression SqlDialect.SparkExpression text)
    let private literalOf text =
        match (SqlParser.parseExpression SqlDialect.SparkExpression text).Kind with
        | SqlExprKind.Literal l -> l
        | other -> failwithf "not a literal: %A" other

    let private inner = StructType([ Field("X", Int32Type.Default, true); Field("y", StringType.Default, true) ])

    let private columns =
        [ "i", LogicalTypes.Int32
          "l", LogicalTypes.Int64
          "d", LogicalTypes.decimalOf 4 2
          "dd", LogicalTypes.decimalOf 38 10
          "f", LogicalTypes.Float64
          "s", LogicalTypes.String
          "b", LogicalTypes.Boolean
          "dt", LogicalTypes.Date
          "ts", LogicalTypes.DatetimeUtc
          "tn", LogicalTypes.Datetime
          "st", LogicalType.Arrow inner ]

    let private now = DateTimeOffset(2021, 3, 5, 4, 0, 0, TimeSpan.Zero)

    let private bindAs (use': SparkExpressionUse) (zone: string) (text: string) =
        SqlFrontend.BindSparkExpression(text, [ for n, t in columns -> struct (n, t) ], use', zone, now)

    let private bind text = bindAs SparkExpressionUse.Generated "UTC" text

    let private refused (use': SparkExpressionUse) (text: string) =
        Assert.Throws<SqlException>(fun () -> bindAs use' "UTC" text |> ignore).Message

    let private refusedAs text = refused SparkExpressionUse.Generated text

    // ----------------------------------------------------------------------------------------------------------
    // Reading the text
    // ----------------------------------------------------------------------------------------------------------

    [<Fact>]
    let ``Spark's operators read as their SQL meaning`` () =
        Assert.Equal("(a IS NOT DISTINCT FROM b)", spark "a <=> b")
        Assert.Equal("(a = b)", spark "a == b")
        Assert.Equal("((NOT a) AND b)", spark "!a AND b")
        Assert.Equal("((a DIV b) + c)", spark "a div b + c")
        Assert.Equal("(a NOT RLIKE 'x')", spark "a NOT REGEXP 'x'")
        Assert.Equal("(a NOT ILIKE 'x')", spark "a NOT ILIKE 'x'")
        Assert.Equal("(\"a b\" > 0)", spark "`a b` > 0")

    [<Fact>]
    let ``Strings take Spark's quotes and escapes`` () =
        Assert.Equal(SqlLiteral.String "x", literalOf "\"x\"")
        Assert.Equal(SqlLiteral.String "it's", literalOf @"'it\'s'")
        Assert.Equal(SqlLiteral.String "it's", literalOf "'it''s'")
        Assert.Equal(SqlLiteral.String "a\tb\nc", literalOf @"'a\tb\nc'")
        // LIKE's escapes keep their backslash; any other escaped character stands for itself.
        Assert.Equal(SqlLiteral.String @"\%\_q", literalOf @"'\%\_\q'")
        Assert.Equal(SqlLiteral.String "AA\u0000", literalOf @"'\101A\0'")
        Assert.Equal(SqlLiteral.String "\U0001F600", literalOf @"'\U0001F600'")
        Assert.Equal(SqlLiteral.String "ab", literalOf "'a' \"b\"")
        Assert.Equal(SqlLiteral.String @"a\nb", literalOf @"r'a\nb'")
        Assert.Equal(SqlLiteral.Suffixed("10", "L"), literalOf "10L")
        Assert.Equal(SqlLiteral.Suffixed("1.5", "BD"), literalOf "1.5bd")
        Assert.Equal(SqlLiteral.Suffixed("2e3", "D"), literalOf "2e3D")
        Assert.StartsWith("Unterminated string", Assert.Throws<SqlException>(fun () -> spark @"'a\'" |> ignore).Message)

    [<Fact>]
    let ``ISO SQL keeps its own reading`` () =
        let iso text = SqlFormat.expression (SqlParser.parseExpression SqlDialect.Standard text)
        Assert.Equal("\"x\"", iso "\"x\"")
        Assert.Equal(@"'a\n'", iso @"'a\n'")
        let fails text = Assert.Throws<SqlException>(fun () -> iso text |> ignore).Message
        Assert.StartsWith("A number cannot run into a name", fails "10L")
        Assert.StartsWith("Unexpected character '!'", fails "!a")
        Assert.StartsWith("Expected the end of the text", fails "a div b")
        Assert.StartsWith("Expected the end of the text", fails "a RLIKE 'x'")

    [<Fact>]
    let ``Bitwise operators and the :: cast are refused by name`` () =
        let fails text = Assert.Throws<SqlException>(fun () -> spark text |> ignore).Message
        Assert.StartsWith("Bitwise operators are not supported: '&'", fails "a & b")
        Assert.StartsWith("Bitwise operators are not supported: '|'", fails "a | b")
        Assert.StartsWith("The :: cast is not supported", fails "a::int")

    // ----------------------------------------------------------------------------------------------------------
    // Binding
    // ----------------------------------------------------------------------------------------------------------

    [<Fact>]
    let ``Names ignore case, quoted or not; a struct's fields too`` () =
        let v = bind "`I` + L > St.x"
        Assert.Equal<string array>([| "i"; "l"; "st" |], v.Columns)
        let field = bind "st.Y"
        Assert.Equal(SqlTypeClass.String, SqlTypes.classify field.Type)
        match field.Expr with
        | ScalarExpr.Function(ScalarFunction.StructFieldByName, [ ScalarExpr.Column "st"; ScalarExpr.Literal(LiteralValue.String "y") ], _) -> ()
        | other -> Assert.Fail $"%A{other}"
        Assert.StartsWith("No field 'z' in the struct", refusedAs "st.z")
        Assert.Contains("Did you mean i?", refusedAs "ii + 1")

    [<Fact>]
    let ``Raising nodes are guarded where Spark short-circuits`` () =
        let v = bind "CASE WHEN i <> 0 THEN l div i ELSE 0 END"
        let rec divisions (e: ScalarExpr) =
            [ match e with
              | ScalarExpr.Function(ScalarFunction.CheckedDivide _, args, _) -> yield List.last args
              | _ -> ()
              for child in ScalarExprOps.children e do
                  yield! divisions child ]
        match divisions v.Expr with
        | [ guard ] -> Assert.NotEqual(ScalarExprOps.unguarded, guard)
        | other -> Assert.Fail $"expected one division, got %A{other}"

    [<Fact>]
    let ``What Spark converts at run time and the engine cannot is refused`` () =
        Assert.Contains("Spark casts the text at run time", refusedAs "s = 1")
        Assert.Contains("Spark fails the write where the text does not convert", refusedAs "cast(s as int)")
        Assert.Contains("shortest decimal text", refusedAs "cast(f as decimal(5,2))")
        Assert.Contains("as Java does", refusedAs "cast(f as string)")
        Assert.Contains("CAST_INVALID_INPUT", refusedAs "'1.5' = i")
        Assert.StartsWith("DECIMAL(38,10) * DECIMAL(38,10) may need 77 digits", refusedAs "dd * dd")
        Assert.StartsWith("div takes integers or decimals", refusedAs "l div f")
        let inLosAngeles = Assert.Throws<SqlException>(fun () -> bindAs SparkExpressionUse.Generated "America/Los_Angeles" "date_trunc('DAY', ts)" |> ignore)
        Assert.Contains("only in UTC", inLosAngeles.Message)
        Assert.Contains("round() of a floating value to 2 digits", refusedAs "round(f, 2)")
        Assert.StartsWith("TRY_CAST is not supported", refusedAs "try_cast(i as int)")
        // LIKE escapes with a backslash; one before any other character is Spark's error too.
        Assert.StartsWith("The escape character is not allowed to precede 'b'", refusedAs @"s LIKE 'a\\b'")

    [<Fact>]
    let ``RLIKE patterns the engine reads differently from Java are refused`` () =
        Assert.Contains("a group construct", refusedAs "s RLIKE '(?=a)'")
        Assert.Contains(@"uses \1", refusedAs @"s RLIKE '(a)\\1'")
        Assert.Contains("a class inside a class", refusedAs "s RLIKE '[[:alpha:]]'")
        Assert.Contains("a possessive quantifier", refusedAs "s RLIKE 'a++'")
        Assert.Contains("a class intersection", refusedAs "s RLIKE '[a-z&&[^b]]'")
        // Java's ASCII classes and line terminators are spelt out.
        Assert.Equal<Result<string, string>>(Ok @"[0-9]+[^\n\r\x{85}\x{2028}\x{2029}](?:\r\n|[\n\r\x{85}\x{2028}\x{2029}])?$", SparkExpressions.javaRegex @"\d+.$")
        Assert.Equal<Result<string, string>>(Ok @"[a-zA-Z0-9_ ]\x{2f}", SparkExpressions.javaRegex @"[\w ]\/")

    [<Fact>]
    let ``Each use allows what Spark allows there`` () =
        Assert.StartsWith("A CHECK constraint must be a BOOLEAN, not INT", refused SparkExpressionUse.Check "i + 1")
        Assert.StartsWith("current_date() is not allowed in a CHECK constraint", refused SparkExpressionUse.Check "dt < current_date()")
        Assert.StartsWith("current_timestamp() is not allowed in a generated column", refusedAs "current_timestamp")
        Assert.StartsWith("rand() is not allowed in a generated column: it is not deterministic", refusedAs "rand() < 2")
        Assert.StartsWith("Aggregate functions are not allowed in a CHECK constraint", refused SparkExpressionUse.Check "sum(i) > 0")
        Assert.StartsWith("Subqueries are not allowed in a CHECK constraint", refused SparkExpressionUse.Check "i IN (SELECT 1)")
        Assert.StartsWith("Window functions are not allowed", refusedAs "rank() OVER (ORDER BY i)")
        Assert.StartsWith("A column default cannot read a column (i)", refused SparkExpressionUse.Default "i + 1")
        Assert.Contains("Did you mean upper?", refusedAs "uper(s)")
        // A default reads the write's time: the date in the session's time zone.
        let today = bindAs SparkExpressionUse.Default "America/Los_Angeles" "current_date()"
        Assert.Equal(ScalarExpr.Literal(LiteralValue.Date(DateOnly(2021, 3, 4))), today.Expr)
        Assert.Equal<string array>([||], today.Columns)
        Assert.Equal(LogicalTypes.DatetimeUtc, (bindAs SparkExpressionUse.Default "UTC" "current_timestamp").Type)

    [<Fact>]
    let ``An unknown time zone is refused`` () =
        let e = Assert.Throws<ArgumentException>(fun () -> bindAs SparkExpressionUse.Check "Mars/Olympus" "i > 0" |> ignore)
        Assert.Contains("Unknown time zone 'Mars/Olympus'", e.Message)
