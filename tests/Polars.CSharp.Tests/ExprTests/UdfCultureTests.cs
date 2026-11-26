using System.Collections.Concurrent;
using System.Globalization;
using Apache.Arrow;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// A .NET function given to the engine runs in the culture of the thread that built the expression, also when the
/// streaming engine calls it on its own threads (whose culture is the process default).
/// </summary>
public class UdfCultureTests
{
    private const int Rows = 400_000;

    [Fact]
    public void Batch_And_Group_Functions_Run_In_The_Callers_Culture()
    {
        CultureInfo saved = CultureInfo.CurrentCulture, savedUi = CultureInfo.CurrentUICulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("de-DE");
            CultureInfo.CurrentUICulture = new CultureInfo("fr-FR");
            ConcurrentBag<(string Culture, string UiCulture)> seen = [];
            IArrowArray Record(IArrowArray a)
            {
                seen.Add((CultureInfo.CurrentCulture.Name, CultureInfo.CurrentUICulture.Name));
                return a;
            }

            using var df = DataFrame.FromSeries(Series.From("v", [.. Enumerable.Range(0, Rows)]), Series.From("k", [.. Enumerable.Range(0, Rows).Select(i => i % 7)]));
            using (var mapped = df.Lazy().Select(Col("v").MapBatches(Record, DataType.Int32).Alias("m")).Collect())
                Assert.Equal(Rows, mapped.Height);
            Assert.True(seen.Count > 1); // streamed in several batches, on the engine's threads
            using (var grouped = df.Lazy().GroupBy([Col("k")]).Agg(Col("v").MapGroups(Record, DataType.Int32, returnsScalar: false).Alias("m")).Collect())
                Assert.Equal(7, grouped.Height);

            Assert.All(seen, s => Assert.Equal(("de-DE", "fr-FR"), s));
            // The engine's threads are left in their own culture.
            Assert.Equal("de-DE", CultureInfo.CurrentCulture.Name);
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
            CultureInfo.CurrentUICulture = savedUi;
        }
    }
}
