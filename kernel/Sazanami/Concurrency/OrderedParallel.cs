using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace Kaminari.Sazanami.Concurrency;

/// <summary>
/// Maps the items of a sequence on the thread pool, up to a window of them at once ahead of the consumer, and
/// delivers the results in the sequence's order. The window opens gradually (1, 2, 4, … items as results are taken),
/// so a consumer that stops after the first results has not paid for a full window. Every item taken from the source is handed to <c>map</c>, which
/// owns it from then on (disposes it when it must). A failing <c>map</c> fails the sequence at its item's place; a
/// failing source fails it after the results of the items taken before. Whenever the consumer stops (early, or on a
/// failure) the maps still running are waited for and their results dropped: <c>map</c> must not return results that
/// need disposing.
/// </summary>
internal static class OrderedParallel
{
    public static IEnumerable<TResult> Map<TSource, TResult>(IEnumerable<TSource> source, Func<TSource, TResult> map, int window)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(window);
        return Iterate(source, map, window);

        static IEnumerable<TResult> Iterate(IEnumerable<TSource> source, Func<TSource, TResult> map, int window)
        {
            Queue<Task<TResult>> running = new(window);
            ExceptionDispatchInfo? failed = null;
            using IEnumerator<TSource> items = source.GetEnumerator();
            bool more = true;
            int open = 1;
            try
            {
                while (true)
                {
                    while (more && running.Count < open)
                    {
                        more = Next(items, ref failed);
                        if (more)
                        {
                            TSource item = items.Current;
                            running.Enqueue(Task.Run(() => map(item)));
                        }
                    }
                    if (running.Count == 0) break;
                    yield return running.Dequeue().GetAwaiter().GetResult();
                    open = Math.Min(window, 2 * open);
                }
            }
            finally
            {
                foreach (Task task in running) task.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing).GetAwaiter().GetResult();
            }
            failed?.Throw();
        }

        static bool Next(IEnumerator<TSource> items, ref ExceptionDispatchInfo? failed)
        {
            try
            {
                return items.MoveNext();
            }
            catch (Exception e)
            {
                failed = ExceptionDispatchInfo.Capture(e);
                return false;
            }
        }
    }

    /// <inheritdoc cref="OrderedParallel"/>
    public static IAsyncEnumerable<TResult> MapAsync<TSource, TResult>(IAsyncEnumerable<TSource> source, Func<TSource, TResult> map, int window,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(window);
        return Iterate(source, map, window, cancellationToken);

        static async IAsyncEnumerable<TResult> Iterate(IAsyncEnumerable<TSource> source, Func<TSource, TResult> map, int window,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            Queue<Task<TResult>> running = new(window);
            ExceptionDispatchInfo? failed = null;
            await using IAsyncEnumerator<TSource> items = source.GetAsyncEnumerator(cancellationToken);
            bool more = true;
            int open = 1;
            try
            {
                while (true)
                {
                    while (more && running.Count < open)
                    {
                        (more, failed) = await Next(items).ConfigureAwait(false);
                        if (more)
                        {
                            TSource item = items.Current;
                            running.Enqueue(Task.Run(() => map(item)));
                        }
                    }
                    if (running.Count == 0) break;
                    yield return await running.Dequeue().ConfigureAwait(false);
                    open = Math.Min(window, 2 * open);
                }
            }
            finally
            {
                foreach (Task task in running) await task.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
            failed?.Throw();
        }

        static async ValueTask<(bool, ExceptionDispatchInfo?)> Next(IAsyncEnumerator<TSource> items)
        {
            try
            {
                return (await items.MoveNextAsync().ConfigureAwait(false), null);
            }
            catch (Exception e)
            {
                return (false, ExceptionDispatchInfo.Capture(e));
            }
        }
    }
}
