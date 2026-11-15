using System.Buffers;

namespace Kaminari.Sazanami.Helpers;

/// <summary>
/// A glob over '/'-separated paths, as Polars expands local paths (the <c>glob</c> crate): <c>*</c> any run of
/// characters within a segment, <c>?</c> one character, <c>[abc]</c>, <c>[a-z]</c> one of a set (<c>[!a]</c> or
/// <c>[^a]</c> none of it), <c>**</c> as a whole segment any number of segments (none included). Wildcards match a
/// leading dot (hidden files are not special) and never a '/'; matching is case-sensitive; there are no escapes (a
/// literal <c>*</c> is <c>[*]</c>).
/// </summary>
internal sealed class GlobPattern
{
    private static readonly SearchValues<char> Wildcards = SearchValues.Create("*?[");

    private readonly string[] _segments;

    private GlobPattern(string prefix, string[] segments)
    {
        Prefix = prefix;
        _segments = segments;
        Recursive = segments.Length > 1 || segments is ["**"];
    }

    /// <summary>The literal directories before the first wildcard, ending with '/' (empty when the pattern starts with one).</summary>
    public string Prefix { get; }

    /// <summary>Whether matches may lie below the prefix's directory (more than one segment after it, or <c>**</c>).</summary>
    public bool Recursive { get; }

    /// <summary>Whether <paramref name="path"/> holds a wildcard character (<c>*</c>, <c>?</c>, <c>[</c>).</summary>
    public static bool IsGlob(ReadOnlySpan<char> path) => path.ContainsAny(Wildcards);

    /// <summary>Parses a pattern whose separators are '/'.</summary>
    /// <exception cref="ArgumentException">A <c>**</c> shares its segment with other characters, or a <c>[</c> is not closed.</exception>
    public static GlobPattern Parse(string pattern)
    {
        ArgumentNullException.ThrowIfNull(pattern);
        int first = pattern.AsSpan().IndexOfAny(Wildcards);
        int cut = first < 0 ? pattern.Length : pattern.AsSpan(0, first).LastIndexOf('/') + 1;
        string[] segments = pattern[cut..].Split('/');
        foreach (string segment in segments)
        {
            if (segment != "**" && segment.Contains("**", StringComparison.Ordinal))
                throw new ArgumentException($"In the glob '{pattern}', '**' must be a whole path segment (as in 'a/**/b').", nameof(pattern));
            for (int i = segment.IndexOf('['); i >= 0; i = segment.IndexOf('[', i + 1))
            {
                if (ClassEnd(segment, i) < 0) throw new ArgumentException($"In the glob '{pattern}', a '[' is not closed.", nameof(pattern));
                i = ClassEnd(segment, i);
            }
        }
        return new GlobPattern(pattern[..cut], segments);
    }

    /// <summary>Whether <paramref name="relative"/>, a '/'-separated path below <see cref="Prefix"/>, matches the rest of the pattern.</summary>
    public bool IsMatch(ReadOnlySpan<char> relative)
    {
        // The path's segments as ranges, then segment-wise matching with backtracking over '**'.
        int count = relative.Count('/') + 1;
        Span<Range> parts = count <= 64 ? stackalloc Range[count] : new Range[count];
        relative.Split(parts, '/');
        return Segments(_segments, 0, relative, parts);
    }

    private static bool Segments(string[] pattern, int p, ReadOnlySpan<char> path, ReadOnlySpan<Range> parts)
    {
        while (p < pattern.Length)
        {
            if (pattern[p] == "**")
            {
                // '**' takes none or more segments, but a file is still needed after the last pattern segment.
                if (p == pattern.Length - 1) return parts.Length > 0;
                for (int skip = 0; skip <= parts.Length; skip++)
                {
                    if (Segments(pattern, p + 1, path, parts[skip..])) return true;
                }
                return false;
            }
            if (parts.IsEmpty || !Segment(pattern[p], path[parts[0]])) return false;
            p++;
            parts = parts[1..];
        }
        return parts.IsEmpty;
    }

    /// <summary>Matches one segment: '*' backtracks to the last star tried (linear for most patterns).</summary>
    private static bool Segment(string pattern, ReadOnlySpan<char> text)
    {
        int p = 0, t = 0, star = -1, resume = 0;
        while (t < text.Length)
        {
            if (p < pattern.Length)
            {
                char c = pattern[p];
                if (c == '*')
                {
                    star = p++;
                    resume = t;
                    continue;
                }
                if (c == '?')
                {
                    p++;
                    t++;
                    continue;
                }
                if (c == '[')
                {
                    int end = ClassEnd(pattern, p);
                    if (InClass(pattern.AsSpan(p + 1, end - p - 1), text[t]))
                    {
                        p = end + 1;
                        t++;
                        continue;
                    }
                }
                else if (c == text[t])
                {
                    p++;
                    t++;
                    continue;
                }
            }
            if (star < 0) return false;
            p = star + 1;
            t = ++resume;
        }
        while (p < pattern.Length && pattern[p] == '*') p++;
        return p == pattern.Length;
    }

    /// <summary>Where the class opened at <paramref name="open"/> closes (a ']' right after the opening or its negation is literal); -1 when it does not.</summary>
    private static int ClassEnd(string pattern, int open)
    {
        int i = open + 1;
        if (i < pattern.Length && pattern[i] is '!' or '^') i++;
        if (i < pattern.Length && pattern[i] == ']') i++;
        int close = pattern.IndexOf(']', i);
        return close;
    }

    private static bool InClass(ReadOnlySpan<char> body, char c)
    {
        bool negated = body.Length > 0 && body[0] is '!' or '^';
        if (negated) body = body[1..];
        bool found = false;
        for (int i = 0; i < body.Length; i++)
        {
            if (i + 2 < body.Length && body[i + 1] == '-')
            {
                if (c >= body[i] && c <= body[i + 2]) found = true;
                i += 2;
            }
            else if (body[i] == c) found = true;
        }
        return found != negated;
    }
}
