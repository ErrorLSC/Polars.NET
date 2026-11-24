using Apache.Arrow;
using Kaminari.Samidare.IR;

namespace Kaminari.Seseragi.Files.Csv.Reading;

/// <summary>
/// How a dataset's file maps onto the dataset (<see cref="CsvReadOptions.Dataset"/>): its columns, named by its header
/// (or numbered without one), take the types of the dataset's columns of the same name; a column the dataset lacks is
/// an error unless <paramref name="IgnoreExtra"/> (then not read); a dataset's column the file lacks is the dataset's
/// to fill (nulls) or refuse. Once the file's names are known, <paramref name="Resolve"/> gives the columns it reads
/// (names it has) and its row filter (its partition values and missing columns put in).
/// </summary>
/// <param name="Columns">The dataset's columns from files.</param>
/// <param name="IgnoreExtra">Whether a file's columns the dataset lacks are left out (else an error).</param>
/// <param name="InferredFrom">The records of each file the dataset inferred the types from (0 when given): errors suggest overrides then.</param>
/// <param name="Resolve">The columns to read and the filter, from the file's column names.</param>
internal sealed record CsvDatasetColumns(Schema Columns, bool IgnoreExtra, int InferredFrom,
    Func<string[], (IReadOnlyList<string> Columns, ScalarExpr? Filter)> Resolve);
