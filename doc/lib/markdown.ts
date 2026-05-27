import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { D2 } from '@terrastruct/d2';
import type { TableOfContents } from 'fumadocs-core/toc';
import { toHtml } from 'hast-util-to-html';
import { fromMarkdown } from 'mdast-util-from-markdown';
import { gfmFromMarkdown } from 'mdast-util-gfm';
import { gfm } from 'micromark-extension-gfm';
import { toHast } from 'mdast-util-to-hast';
import GithubSlugger from 'github-slugger';
import type { Code, Root } from 'mdast';
import type { Element, Root as HastRoot } from 'hast';

const docsRoot = path.join(process.cwd(), 'content/docs');
const d2 = new D2();
let d2Queue: Promise<unknown> = Promise.resolve();

type Frontmatter = {
  title?: string;
  description?: string;
  full?: boolean;
};

export type RenderedMarkdown = {
  html: string;
  toc: TableOfContents;
  frontmatter: Frontmatter;
};

function parseFrontmatter(input: string): {
  markdown: string;
  frontmatter: Frontmatter;
} {
  if (!input.startsWith('---\n')) {
    return { markdown: input, frontmatter: {} };
  }

  const end = input.indexOf('\n---', 4);

  if (end === -1) {
    return { markdown: input, frontmatter: {} };
  }

  const frontmatterText = input.slice(4, end).trim();
  const markdown = input.slice(end + 4).replace(/^\n/, '');
  const frontmatter: Frontmatter = {};

  for (const line of frontmatterText.split('\n')) {
    const separator = line.indexOf(':');

    if (separator === -1) {
      continue;
    }

    const key = line.slice(0, separator).trim();
    const value = line.slice(separator + 1).trim().replace(/^["']|["']$/g, '');

    if (key === 'title' || key === 'description') {
      frontmatter[key] = value;
    }

    if (key === 'full') {
      frontmatter.full = value === 'true';
    }
  }

  return { markdown, frontmatter };
}

function addHeadingIds(tree: HastRoot, toc: TableOfContents): void {
  const slugger = new GithubSlugger();

  function textContent(node: HastRoot | Element): string {
    if (!('children' in node)) {
      return '';
    }

    return node.children
      .map((child) => {
        if (child.type === 'text') {
          return child.value;
        }

        if (child.type === 'element') {
          return textContent(child);
        }

        return '';
      })
      .join('');
  }

  function visit(node: HastRoot | Element): void {
    if (node.type === 'element' && /^h[2-4]$/.test(node.tagName)) {
      const depth = Number(node.tagName.slice(1));
      const title = textContent(node).trim();
      const id = slugger.slug(title);

      node.properties ??= {};
      node.properties.id = id;
      toc.push({
        title,
        url: `#${id}`,
        depth,
      });
    }

    if ('children' in node) {
      for (const child of node.children) {
        if (child.type === 'element') {
          visit(child);
        }
      }
    }
  }

  visit(tree);
}

function removeDuplicateTitle(tree: Root): void {
  const first = tree.children[0];

  if (first?.type === 'heading' && first.depth === 1) {
    tree.children.shift();
  }
}

function escapeHtml(value: string): string {
  return value
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}

async function renderD2(code: Code, salt: string): Promise<string> {
  try {
    const { lightSvg, darkSvg } = await enqueueD2Render(async () => {
      const lightResult = await d2.compile({
        fs: {
          index: code.value,
        },
        inputPath: 'index',
        options: {
          layout: 'elk',
          themeID: 0,
          pad: 32,
          noXMLTag: true,
        },
      });
      const darkResult = await d2.compile({
        fs: {
          index: code.value,
        },
        inputPath: 'index',
        options: {
          layout: 'elk',
          themeID: 200,
          darkThemeID: 200,
          pad: 32,
          noXMLTag: true,
        },
      });
      const lightSvg = await d2.render(lightResult.diagram, {
        ...lightResult.renderOptions,
        salt: `${salt}-light`,
        noXMLTag: true,
      });
      const darkSvg = await d2.render(darkResult.diagram, {
        ...darkResult.renderOptions,
        salt: `${salt}-dark`,
        noXMLTag: true,
      });

      return { lightSvg, darkSvg };
    });

    return `<figure class="d2-diagram"><div class="d2-diagram-light">${lightSvg}</div><div class="d2-diagram-dark">${darkSvg}</div></figure>`;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);

    return `<figure class="d2-diagram d2-diagram-error"><figcaption>D2 render error</figcaption><pre><code>${escapeHtml(message)}</code></pre></figure>`;
  }
}

async function enqueueD2Render<T>(task: () => Promise<T>): Promise<T> {
  const next = d2Queue.then(task, task);

  d2Queue = next.catch(() => undefined);

  return next;
}

async function renderD2Blocks(tree: Root): Promise<void> {
  let index = 0;

  for (const [nodeIndex, node] of tree.children.entries()) {
    if (node.type !== 'code' || node.lang !== 'd2') {
      continue;
    }

    tree.children[nodeIndex] = {
      type: 'html',
      value: await renderD2(node, `diagram-${index}`),
    };
    index += 1;
  }
}

export async function renderMarkdown(file: string): Promise<RenderedMarkdown> {
  const raw = await readFile(path.join(docsRoot, file), 'utf8');
  const { markdown, frontmatter } = parseFrontmatter(raw);
  const mdast = fromMarkdown(markdown, {
    extensions: [gfm()],
    mdastExtensions: [gfmFromMarkdown()],
  });

  removeDuplicateTitle(mdast);
  await renderD2Blocks(mdast);

  const toc: TableOfContents = [];
  const hast = toHast(mdast, {
    allowDangerousHtml: true,
  }) as HastRoot;

  addHeadingIds(hast, toc);

  return {
    html: toHtml(hast, {
      allowDangerousHtml: true,
    }),
    toc,
    frontmatter,
  };
}
