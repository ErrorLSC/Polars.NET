namespace Kaminari.Sazanami.Ordering;

/// <summary>
/// Sorting and deduplicating 64-bit hashes (keys whose top bits spread evenly) by buckets of their top 16 bits: one
/// counting pass and one scatter, then each small bucket sorted and deduplicated in place. Buckets are independent, so
/// ranges of them may be processed side by side; read bucket by bucket, the distinct keys come in ascending order.
/// </summary>
internal static class HashKeys
{
    /// <summary>The number of buckets (the top 16 bits).</summary>
    public const int Buckets = 1 << 16;

    /// <summary>The bucket of a key.</summary>
    public static int BucketOf(ulong key) => (int)(key >> 48);

    /// <summary>
    /// Scatters <paramref name="keys"/> into <paramref name="into"/> (as long) by bucket; <paramref name="starts"/>
    /// (<see cref="Buckets"/> + 1 entries) receives where each bucket starts, its last entry the count.
    /// </summary>
    public static void Bucket(ReadOnlySpan<ulong> keys, Span<ulong> into, Span<int> starts)
    {
        if (into.Length < keys.Length) throw new ArgumentException("The destination is shorter than the keys.", nameof(into));
        if (starts.Length != Buckets + 1) throw new ArgumentException($"The starts take {Buckets + 1} entries.", nameof(starts));
        starts.Clear();
        foreach (ulong key in keys) starts[BucketOf(key) + 1]++;
        for (int b = 1; b < starts.Length; b++) starts[b] += starts[b - 1];
        // The starts serve as cursors (each ends at the next bucket's start), then move back one place.
        foreach (ulong key in keys) into[starts[BucketOf(key)]++] = key;
        for (int b = Buckets; b > 0; b--) starts[b] = starts[b - 1];
        starts[0] = 0;
    }

    /// <summary>
    /// Sorts and deduplicates buckets <paramref name="first"/> to <paramref name="last"/> (excluded) of keys
    /// <see cref="Bucket"/> laid out: each bucket's distinct keys, ascending, at its start, their number in
    /// <paramref name="distinct"/>. Returns the distinct keys of those buckets.
    /// </summary>
    public static int SortDistinct(Span<ulong> bucketed, ReadOnlySpan<int> starts, Span<int> distinct, int first, int last)
    {
        int total = 0;
        for (int b = first; b < last; b++)
        {
            Span<ulong> bucket = bucketed[starts[b]..starts[b + 1]];
            if (bucket.IsEmpty)
            {
                distinct[b] = 0;
                continue;
            }
            if (bucket.Length <= 32) InsertionSort(bucket);
            else bucket.Sort();
            int kept = 1;
            for (int i = 1; i < bucket.Length; i++)
            {
                if (bucket[i] != bucket[kept - 1]) bucket[kept++] = bucket[i];
            }
            distinct[b] = kept;
            total += kept;
        }
        return total;
    }

    /// <summary>Sorts a few keys (the usual bucket) without the general sort's set-up.</summary>
    private static void InsertionSort(Span<ulong> keys)
    {
        for (int i = 1; i < keys.Length; i++)
        {
            ulong key = keys[i];
            int j = i - 1;
            while (j >= 0 && keys[j] > key)
            {
                keys[j + 1] = keys[j];
                j--;
            }
            keys[j + 1] = key;
        }
    }
}
