using System.Runtime.CompilerServices;

namespace Kaminari.Sazanami.Helpers;

/// <summary>
/// Provides terminal-friendly display sanitization helpers for engine outputs.
/// </summary>
public static class TerminalDisplayHelpers
{
    /// <summary>
    /// Sanitizes text for console/terminal display by replacing toxic control characters
    /// (such as NUL, standalone CR, backspace, bell, ESC sequences, etc.) with their
    /// corresponding Unicode Control Pictures (U+2400..U+243F), while preserving standard
    /// newlines (\n, \r\n) and horizontal indentation (\t, spaces).
    /// </summary>
    /// <param name="text">The raw text to be displayed.</param>
    /// <returns>The sanitized string safe for terminal rendering.</returns>
    public static string SanitizeForDisplay(string? text)
    {
        if (string.IsNullOrEmpty(text))
            return text ?? string.Empty;

        ReadOnlySpan<char> span = text.AsSpan();
        bool hasToxic = false;
        for (int i = 0; i < span.Length; i++)
        {
            if (IsToxic(span[i], span, i))
            {
                hasToxic = true;
                break;
            }
        }

        if (!hasToxic)
            return text;

        return string.Create(text.Length, text, static (dest, src) =>
        {
            ReadOnlySpan<char> srcSpan = src.AsSpan();
            for (int i = 0; i < srcSpan.Length; i++)
            {
                char c = srcSpan[i];
                if (c < 0x20)
                {
                    if (c == '\n' || c == '\t')
                    {
                        dest[i] = c;
                    }
                    else if (c == '\r' && i + 1 < srcSpan.Length && srcSpan[i + 1] == '\n')
                    {
                        dest[i] = c;
                    }
                    else
                    {
                        dest[i] = (char)(0x2400 + c);
                    }
                }
                else if (c == 0x7F)
                {
                    dest[i] = '\u2421';
                }
                else
                {
                    dest[i] = c;
                }
            }
        });
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsToxic(char c, ReadOnlySpan<char> span, int index)
    {
        if (c < 0x20)
        {
            if (c == '\n' || c == '\t')
                return false;
            if (c == '\r' && index + 1 < span.Length && span[index + 1] == '\n')
                return false;
            return true;
        }
        return c == 0x7F;
    }
}

