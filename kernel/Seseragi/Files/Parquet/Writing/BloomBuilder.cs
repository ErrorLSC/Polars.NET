using System.IO.Hashing;
using System.Numerics;
using System.Runtime.ExceptionServices;
using Kaminari.Sazanami.Encodings;
using Kaminari.Sazanami.Filters;
using Kaminari.Sazanami.Ordering;

namespace Kaminari.Seseragi.Files.Parquet.Writing;

/// <summary>
/// The Bloom filter of a column chunk being written: each value hashed as Parquet's filters hash it (XXH64, seed 0, of
/// its PLAIN bytes, byte arrays without their length) as it enters a page, whatever the page's encoding turns out to
/// be; the filter sized for the chunk's distinct values when it is finished, at most for
/// <see cref="ParquetBloomFilterOptions.DistinctValues"/>, cut to <see cref="ParquetBloomFilterOptions.MaxBytes"/>.
/// The distinct hashes are counted exactly, in one of two ways: while they are few, in an open-addressing table that
/// stays in the caches (Sazanami's <see cref="FixedValueDictionary{T}"/>); past <see cref="TableLimit"/>, every hash is
/// appended and the chunk's hashes are sorted and deduplicated at the end by buckets of their top bits (Sazanami's
/// <see cref="HashKeys"/>), then inserted in that order, the order of the filter's blocks. Either way the filter is the
/// same (a set of hashes, bits or-ed in). Large chunks finish on helper threads the writer's budget lends: bucket
/// ranges are independent, and ranges of whole blocks fill apart.
/// </summary>
internal sealed class BloomBuilder(ParquetBloomFilterOptions options, WorkBudget? budget = null)
{
    /// <summary>The distinct hashes counted in a table (beyond, appended and sorted at the end).</summary>
    internal const int TableLimit = 1 << 16;

    /// <summary>The parts a large chunk's buckets are cut into (a power of two: whole blocks per part, see <see cref="Finish"/>).</summary>
    private const int Parts = 16;

    private readonly FixedValueDictionary<ulong> _table = new();
    private ulong[] _hashes = [];
    private ulong[] _bucketed = [];
    private int[]? _starts, _distinct;
    private int _count;
    private bool _appending; // past the table's limit: hashes appended

    /// <summary>The most blocks a filter takes: <see cref="ParquetBloomFilterOptions.MaxBytes"/>, rounded down to a power of two.</summary>
    private int MaxBlocks => (int)BitOperations.RoundUpToPowerOf2((uint)(options.MaxBytes / SplitBlockBloomFilter.BlockBytes) + 1) / 2;

    public void Add(ReadOnlySpan<byte> plain)
    {
        ulong hash = XxHash64.HashToUInt64(plain);
        if (_appending)
        {
            Append(hash);
            return;
        }
        _table.GetOrAdd(hash);
        if (_table.Count <= TableLimit) return;
        // Many distinct values: from now on appended (the table's, distinct, first).
        _appending = true;
        foreach (ulong known in _table.Values) Append(known);
        _table.Clear();
    }

    private void Append(ulong hash)
    {
        if (_count == _hashes.Length) Array.Resize(ref _hashes, Math.Max(2 * TableLimit, checked(_hashes.Length * 2)));
        _hashes[_count++] = hash;
    }

    /// <summary>The chunk's filter bitset; the builder is then ready for the next chunk.</summary>
    public byte[] Finish()
    {
        if (!_appending)
        {
            SplitBlockBloomFilter small = Empty(_table.Count);
            foreach (ulong hash in _table.Values) small.Insert(hash);
            _table.Clear();
            return small.Bitset.ToArray();
        }
        if (_bucketed.Length < _count) _bucketed = new ulong[_hashes.Length];
        int[] starts = _starts ??= new int[HashKeys.Buckets + 1], distinct = _distinct ??= new int[HashKeys.Buckets];
        HashKeys.Bucket(_hashes.AsSpan(0, _count), _bucketed, starts);
        const int PerPart = HashKeys.Buckets / Parts;
        var counts = new int[Parts];
        ulong[] bucketed = _bucketed;
        Run(part => counts[part] = HashKeys.SortDistinct(bucketed, starts, distinct, part * PerPart, (part + 1) * PerPart));
        SplitBlockBloomFilter filter = Empty(counts.Sum());
        // A part's hashes share their top bits, which pick the block: with at least as many blocks as parts, the parts
        // fill blocks of their own.
        if (filter.Bitset.Length / SplitBlockBloomFilter.BlockBytes >= Parts) Run(Insert);
        else for (int part = 0; part < Parts; part++) Insert(part);
        _count = 0;
        _appending = false;
        return filter.Bitset.ToArray();

        void Insert(int part)
        {
            for (int b = part * PerPart; b < (part + 1) * PerPart; b++)
            {
                foreach (ulong hash in bucketed.AsSpan(starts[b], distinct[b])) filter.Insert(hash);
            }
        }
    }

    private SplitBlockBloomFilter Empty(long distinct)
    {
        distinct = Math.Max(1, distinct);
        if (options.DistinctValues is long most) distinct = Math.Min(distinct, most);
        return SplitBlockBloomFilter.Empty(Math.Min(SplitBlockBloomFilter.BlocksFor(distinct, options.FalsePositiveRate), MaxBlocks));
    }

    /// <summary>
    /// Runs the <see cref="Parts"/> parts of <paramref name="work"/> on this thread and on the helpers the budget lends
    /// (a helper that starts late finds none left: only the parts are waited for).
    /// </summary>
    private void Run(Action<int> work)
    {
        if (budget is not { Max: > 1 } lender || _count < 2 * TableLimit)
        {
            for (int part = 0; part < Parts; part++) work(part);
            return;
        }
        int next = -1, completed = 0;
        Exception? failure = null;
        using var done = new ManualResetEventSlim(false); // set by the thread completing the last part, before Wait returns
        for (int helper = 1; helper < Parts && lender.TryEnter(); helper++)
        {
            ThreadPool.UnsafeQueueUserWorkItem<object?>(_ =>
            {
                try
                {
                    Take();
                }
                finally
                {
                    lender.Exit();
                }
            }, null, preferLocal: false);
        }
        Take();
        done.Wait();
        if (failure is not null) ExceptionDispatchInfo.Throw(failure);

        void Take()
        {
            int part;
            while ((part = Interlocked.Increment(ref next)) < Parts)
            {
                try
                {
                    work(part);
                }
                catch (Exception e)
                {
                    Interlocked.CompareExchange(ref failure, e, null);
                }
                if (Interlocked.Increment(ref completed) == Parts) done.Set();
            }
        }
    }
}
