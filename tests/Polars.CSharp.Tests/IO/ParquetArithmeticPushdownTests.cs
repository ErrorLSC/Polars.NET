#nullable enable
using System.Linq.Expressions;
using Kaminari.Seseragi.Files.Parquet;
using Polars.CSharp.Linq;

namespace Polars.CSharp.Tests.IO;

/// <summary>
/// Predicates with arithmetic, casts and comparisons between columns, evaluated by <see cref="ParquetReader{T}"/>
/// itself (pushed down exactly) against the rows the Polars LINQ provider keeps for the same expression: the IR's
/// arithmetic (wrapping integers, division and remainder by zero null, MinValue / -1 wrapping, Float32 in Float32) on
/// two engines.
/// </summary>
public sealed class ParquetArithmeticPushdownTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"parquet-arith-{Guid.NewGuid():N}");
    private readonly string _path;

    public ParquetArithmeticPushdownTests()
    {
        Directory.CreateDirectory(_dir);
        _path = Path.Combine(_dir, "rows.parquet");
        var random = new Random(20261119);
        int[] ints = [int.MinValue, int.MinValue + 1, -7, -1, 0, 1, 2, 3, 7, 1000, 70000, int.MaxValue];
        long[] longs = [long.MinValue, -1, 0, 1, 3, 1L << 40, long.MaxValue];
        uint[] uints = [0, 1, 5, 3_000_000_000, uint.MaxValue];
        short[] shorts = [short.MinValue, -3, 0, 2, short.MaxValue];
        double[] doubles = [double.NaN, double.NegativeInfinity, -2.5, -0.0, 0.0, 0.1, 1.5, 1e300, double.PositiveInfinity];
        float[] floats = [float.NaN, -1.5f, -0.0f, 0.0f, 0.1f, 3.0f, 1e30f, float.PositiveInfinity];
        T Pick<T>(T[] pool) => pool[random.Next(pool.Length)];
        Row[] rows = [.. Enumerable.Range(0, 3000).Select(i => new Row(
            i, Pick(ints), Pick(ints), Pick(longs), Pick(uints), Pick(shorts), Pick(doubles), Pick(floats),
            random.Next(5) == 0 ? null : Pick(ints), random.Next(5) == 0 ? null : Pick(longs)))];
        using var df = DataFrame.From(rows);
        df.WriteParquet(_path, statistics: true, rowGroupSize: 500);
    }

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    public sealed record Row(long Id, int I, int J, long L, uint U, short S, double D, float F, int? N, long? M);

    private static readonly Expression<Func<Row, bool>>[] Predicates =
    [
        r => r.I * 3 > r.J,                     // wraps
        r => r.I + 1 < r.I,                     // only MaxValue wraps below itself
        r => r.I / r.J == 0,                    // a zero divisor is null
        r => r.I % r.J == -1,
        r => r.I / -1 == int.MinValue,          // MinValue / -1 wraps
        r => r.I % -1 == 0,
        r => -r.I == r.I,
        r => r.L * r.L > 0,
        r => r.L > r.I,                         // widened
        r => r.U - 5u > 10u,                    // unsigned wraps
        r => r.S * 2 == 4,                      // short promoted to int
        r => (short)(r.I * 2) < 0,              // narrowing wraps
        r => (byte)r.I == 1,
        r => r.D * 2 > r.D,
        r => r.D / 0.0 > 0,
        r => r.D % 1.0 != 0,
        r => r.F * 0.1f > 0.01f,                // Float32 arithmetic
        r => r.F + 1f == r.F,
        r => (double)r.F < r.D,
        r => r.I * 1.5 > 3,                     // to double
        r => (long)r.I * r.L < 0,
        r => r.N + 1 == 1,                      // lifted: null stays null
        r => r.N == r.J,                        // two-valued equality between columns
        r => r.N != r.I,
        r => r.N * 2 > r.M,
        r => r.M % 3 == null,
        r => r.N / r.N == 1,
        r => (r.I + r.J) % 7 == 3 && r.D > 0.0,
        r => r.I < r.J || r.L == 0,
        r => (float)r.D > 1f,                   // Float64 to Float32 rounds
        r => (double)r.U > 1e9,                 // unsigned to double
        r => (float)r.L * 2f > r.F,
    ];

    public static TheoryData<int> PredicateIndexes => [.. Enumerable.Range(0, Predicates.Length)];

    private void AssertSameRows(Expression<Func<Row, bool>> where, string what)
    {
        using var df = DataFrame.ReadParquet(_path);
        List<long> expected = [.. df.AsQueryable<Row>().Where(where).Select(r => r.Id)];
        using var file = ParquetFile.Open(_path);
        using ParquetReader<Row> reader = file.OpenReader(where);
        Assert.True(reader.FiltersExactly, what);
        List<long> actual = [.. reader.ReadRows().Select(r => r.Id)];
        Assert.True(expected.SequenceEqual(actual), $"{what}: Polars keeps {expected.Count} rows, the reader {actual.Count}; first difference at "
            + $"{expected.Except(actual).Concat(actual.Except(expected)).FirstOrDefault()}");
    }

    [Theory]
    [MemberData(nameof(PredicateIndexes))]
    public void APredicate_KeepsTheRowsTheLinqProviderKeeps(int index) => AssertSameRows(Predicates[index], Predicates[index].ToString());

    [Fact]
    public void RandomPredicates_KeepTheRowsTheLinqProviderKeeps()
    {
        var random = new Random(19112026);
        ParameterExpression r = Expression.Parameter(typeof(Row), "r");
        for (int round = 0; round < 150; round++)
        {
            Expression body = RandomComparison(random, r);
            if (random.Next(3) == 0) body = Expression.AndAlso(body, RandomComparison(random, r));
            var where = Expression.Lambda<Func<Row, bool>>(body, r);
            AssertSameRows(where, $"round {round}: {where}");
        }
    }

    private static readonly (string Member, Type Type)[] Numbers =
        [("I", typeof(int)), ("J", typeof(int)), ("L", typeof(long)), ("U", typeof(uint)), ("D", typeof(double)), ("F", typeof(float)), ("N", typeof(int?)), ("M", typeof(long?))];

    /// <summary>A comparison of an arithmetic expression of one type with a constant or another expression of it.</summary>
    private static Expression RandomComparison(Random random, ParameterExpression r)
    {
        var (member, type) = Numbers[random.Next(Numbers.Length)];
        Expression left = RandomArithmetic(random, r, member, type);
        string[] alike = [.. Numbers.Where(n => (Nullable.GetUnderlyingType(n.Type) ?? n.Type) == (Nullable.GetUnderlyingType(type) ?? type)).Select(n => n.Member)];
        Expression right = random.Next(2) == 0 ? Constant(random, type) : RandomArithmetic(random, r, alike[random.Next(alike.Length)], type);
        if (left.Type != right.Type)
        {
            Type lifted = left.Type.IsValueType && Nullable.GetUnderlyingType(left.Type) is null ? typeof(Nullable<>).MakeGenericType(left.Type) : left.Type;
            left = Expression.Convert(left, lifted);
            right = Expression.Convert(right, lifted);
        }
        return random.Next(6) switch
        {
            0 => Expression.Equal(left, right),
            1 => Expression.NotEqual(left, right),
            2 => Expression.LessThan(left, right),
            3 => Expression.LessThanOrEqual(left, right),
            4 => Expression.GreaterThan(left, right),
            _ => Expression.GreaterThanOrEqual(left, right),
        };
    }

    private static Expression RandomArithmetic(Random random, ParameterExpression r, string member, Type type)
    {
        Expression e = Expression.Property(r, member);
        if (e.Type != type) e = Expression.Convert(e, type);
        for (int step = random.Next(1, 3); step > 0; step--)
        {
            Expression operand = Constant(random, type);
            e = random.Next(5) switch
            {
                0 => Expression.Add(e, operand),
                1 => Expression.Subtract(e, operand),
                2 => Expression.Multiply(e, operand),
                3 => Expression.Divide(e, operand),
                _ => Expression.Modulo(e, operand),
            };
        }
        return e;
    }

    private static readonly int[] IntConstants = [-7, -1, 0, 1, 2, 3, 1000, int.MaxValue, int.MinValue];
    private static readonly long[] LongConstants = [-1, 0, 1, 3, 1L << 40, long.MaxValue];
    private static readonly uint[] UIntConstants = [0, 1, 5, 3_000_000_000];
    private static readonly double[] DoubleConstants = [-2.5, 0.0, 0.1, 1.5, 3, double.PositiveInfinity];
    private static readonly float[] FloatConstants = [-1.5f, 0.0f, 0.1f, 3f];

    private static Expression Constant(Random random, Type type)
    {
        Type core = Nullable.GetUnderlyingType(type) ?? type;
        object value = core switch
        {
            _ when core == typeof(int) => (object)IntConstants[random.Next(IntConstants.Length)],
            _ when core == typeof(long) => (object)LongConstants[random.Next(LongConstants.Length)],
            _ when core == typeof(uint) => (object)UIntConstants[random.Next(UIntConstants.Length)],
            _ when core == typeof(double) => (object)DoubleConstants[random.Next(DoubleConstants.Length)],
            _ => (object)FloatConstants[random.Next(FloatConstants.Length)],
        };
        return Expression.Constant(value, type);
    }
}
