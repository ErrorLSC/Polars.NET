import { defineConfig } from 'vitepress';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function parseTocYaml(filePath: string, basePath: string) {
  try {
    if (!fs.existsSync(filePath)) {
      console.warn(`[VitePress] TOC file not found: ${filePath}`);
      return [];
    }
    const content = fs.readFileSync(filePath, 'utf-8');
    const lines = content.split(/\r?\n/);
    const result: any[] = [];
    let currentGroup: any = null;

    for (const line of lines) {
      if (line.startsWith('- name:')) {
        const name = line.replace('- name:', '').trim();
        currentGroup = { text: name, collapsed: false, items: [] };
        result.push(currentGroup);
      } else if (line.startsWith('  - name:')) {
        const name = line.replace('  - name:', '').trim();
        currentGroup?.items.push({ text: name });
      } else if (line.startsWith('    href:')) {
        const href = line.replace('    href:', '').trim();
        const lastItem = currentGroup?.items[currentGroup.items.length - 1];
        if (lastItem) {
          const cleanHref = href.endsWith('.md') ? href.slice(0, -3) : href;
          lastItem.link = `${basePath}/${cleanHref}`;
        }
      }
    }
    return result;
  } catch (err) {
    console.error(`[VitePress] Failed to parse ${filePath}:`, err);
    return [];
  }
}

const csharpSidebar = parseTocYaml(
  path.resolve(__dirname, '../api/csharp_generated/toc.yml'),
  '/api/csharp_generated'
);

const fsharpSidebar = parseTocYaml(
  path.resolve(__dirname, '../api/fsharp_generated/toc.yml'),
  '/api/fsharp_generated'
);

const articlesSidebar = [
  {
    text: 'Getting Started',
    collapsed: false,
    items: [
      { text: 'Overview', link: '/articles/' },
      { text: 'Introduction', link: '/articles/intro' },
      { text: 'Quick Start', link: '/articles/quickstart' }
    ]
  },
  {
    text: 'Core Concepts',
    collapsed: false,
    items: [
      { text: 'Expressions & Computations', link: '/articles/expressions' },
      { text: 'LazyFrame & Query Optimization', link: '/articles/lazy' },
      { text: 'Time Series & Analytics', link: '/articles/timeseries' }
    ]
  },
  {
    text: 'Storage & Lakehouse',
    collapsed: false,
    items: [
      { text: 'Delta Lake & ACID Storage', link: '/articles/delta' }
    ]
  },
  {
    text: 'Idioms & Best Practices',
    collapsed: false,
    items: [
      { text: 'C# Idioms', link: '/articles/csharp-idioms' },
      { text: 'F# Idioms', link: '/articles/fsharp-idioms' },
      { text: 'LINQ & Query CE', link: '/articles/linq' }
    ]
  },
  {
    text: 'Platform & Ecosystem',
    collapsed: false,
    items: [
      { text: 'Ecosystem & Native Interop', link: '/articles/interop' }
    ]
  }
];

export default defineConfig({
  base: '/Polars.NET/',
  title: 'Polars.NET',
  description: 'Blazingly Fast DataFrames for .NET',
  lang: 'en-US',
  lastUpdated: true,
  cleanUrls: true,

  head: [
    ['link', { rel: 'icon', href: '/Polars.NET/logo.svg' }],
    ['meta', { name: 'theme-color', content: '#512BD4' }]
  ],

  themeConfig: {
    logo: '/logo.svg',
    siteTitle: 'Polars.NET',

    nav: [
      { text: 'Guide', link: '/articles/intro', activeMatch: '/articles/' },
      { text: 'C# API', link: '/api/csharp_generated/DataFrame', activeMatch: '/api/csharp_generated/' },
      { text: 'F# API', link: '/api/fsharp_generated/pl', activeMatch: '/api/fsharp_generated/' },
      { text: 'LINQ', link: '/articles/linq' },
      { text: 'Interop', link: '/articles/interop' },
      { text: 'Polars', link: 'https://pola.rs/' },
      { text: 'GitHub', link: 'https://github.com/errorlsc/polars.net' }
    ],

    sidebar: {
      '/articles/': articlesSidebar,
      '/api/csharp_generated/': csharpSidebar,
      '/api/fsharp_generated/': fsharpSidebar,
      '/api/': [
        {
          text: 'API Reference',
          items: [
            { text: 'API Overview', link: '/api/' },
            { text: 'C# API Reference', link: '/api/csharp_generated/DataFrame' },
            { text: 'F# API Reference', link: '/api/fsharp_generated/pl' }
          ]
        }
      ]
    },

    socialLinks: [
      { icon: 'github', link: 'https://github.com/errorlsc/polars.net' }
    ],

    search: {
      provider: 'local'
    },

    editLink: {
      pattern: 'https://github.com/errorlsc/polars.net/edit/main/docs/:path',
      text: 'Edit this page on GitHub'
    },

    footer: {
      message: 'Released under the MIT License. Powered by the Polars Rust core & Apache Arrow.',
      copyright: 'Copyright © 2026-present Qinglei Zhang'
    }
  }
});

