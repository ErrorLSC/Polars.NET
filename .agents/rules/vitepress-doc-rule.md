# VitePress Alert & Markdown Conventions

- Never use `**` bold syntax in VitePress alert titles or headers (e.g. write `> [!TIP] Title Here`, never `> [!TIP] **Title**`). VitePress custom-block-title already applies bold formatting by default; using `**` causes literal asterisks to be displayed in the rendered page.
- Never use raw ```mermaid code blocks in documentation articles unless a dedicated mermaid plugin is explicitly installed. Prefer clean, responsive HTML pipeline cards (`.pl-pipeline`, `.pl-step`, `.pl-arrow`) or Markdown tables.
- Never use emojis in documentation or codebase comments/articles (e.g. no 🚀, 🎯, ❌, etc.). Keep technical documentation clean, professional, and serious. Use standard text or HTML entities (e.g. `&rarr;` for arrows, `[Error]` for errors).

