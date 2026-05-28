import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { D2 } from '@terrastruct/d2';
import type {
  CompileOptions,
  Connection,
  Diagram,
  Shape,
  Text,
} from '@terrastruct/d2';
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
let d2FontOptions: Promise<D2FontOptions> | undefined;

type D2FontOptions = Record<
  'fontRegular' | 'fontItalic' | 'fontBold' | 'fontSemibold',
  string
>;
type D2ThemePalette = Record<string, string>;

const d2SemanticTokens = {
  textColor: '--d2-token-text-color',
  labelFill: '--d2-token-label-fill',
  shapeFill: '--d2-token-shape-fill',
  shapeStroke: '--d2-token-shape-stroke',
  shapePrimaryAccent: '--d2-token-shape-primary-accent',
  shapeSecondaryAccent: '--d2-token-shape-secondary-accent',
  shapeNeutralAccent: '--d2-token-shape-neutral-accent',
  connectionStroke: '--d2-token-connection-stroke',
  connectionFill: '--d2-token-connection-fill',
};
const d2LightPalette: D2ThemePalette = {
  N1: '#0A0F25',
  N2: '#676C7E',
  N3: '#9499AB',
  N4: '#CFD2DD',
  N5: '#DEE1EB',
  N6: '#EEF1F8',
  N7: '#FFFFFF',
  B1: '#0D32B2',
  B2: '#0D32B2',
  B3: '#E3E9FD',
  B4: '#E3E9FD',
  B5: '#EDF0FD',
  B6: '#F7F8FE',
  AA2: '#4A6FF3',
  AA4: '#EDF0FD',
  AA5: '#F7F8FE',
  AB4: '#EDF0FD',
  AB5: '#F7F8FE',
};
const d2DarkPalette: D2ThemePalette = {
  N1: '#CDD6F4',
  N2: '#BAC2DE',
  N3: '#A6ADC8',
  N4: '#585B70',
  N5: '#45475A',
  N6: '#313244',
  N7: '#1E1E2E',
  B1: '#CBA6F7',
  B2: '#CBA6F7',
  B3: '#6C7086',
  B4: '#585B70',
  B5: '#45475A',
  B6: '#313244',
  AA2: '#F38BA8',
  AA4: '#45475A',
  AA5: '#313244',
  AB4: '#45475A',
  AB5: '#313244',
};

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

async function readD2Font(file: string): Promise<string> {
  return readFile(path.join(process.cwd(), 'assets/fonts', file), 'base64');
}

function getD2FontOptions(): Promise<D2FontOptions> {
  d2FontOptions ??= Promise.all([
    readD2Font('Inter-Regular.ttf'),
    readD2Font('Inter-Bold.ttf'),
    readD2Font('Inter-SemiBold.ttf'),
  ]).then(([fontRegular, fontBold, fontSemibold]) => ({
    fontRegular,
    fontItalic: fontRegular,
    fontBold,
    fontSemibold,
  }));

  return d2FontOptions;
}

function semanticColor(
  token: string,
  fallback: string | undefined,
  palette: D2ThemePalette,
): string {
  const color = fallback ? (palette[fallback] ?? fallback) : undefined;

  return color ? `var(${token}, ${color})` : `var(${token})`;
}

function styleD2Text(
  text: Text | undefined,
  palette: D2ThemePalette,
  options?: { upright?: boolean },
): void {
  if (!text) {
    return;
  }

  if (options?.upright) {
    text.italic = false;
  }

  text.color = semanticColor(d2SemanticTokens.textColor, text.color, palette);

  if (text.labelFill) {
    text.labelFill = semanticColor(
      d2SemanticTokens.labelFill,
      text.labelFill,
      palette,
    );
  }
}

function styleD2Shape(shape: Shape, palette: D2ThemePalette): void {
  shape.borderRadius = Math.max(shape.borderRadius, 12);
  shape.strokeWidth = Math.min(shape.strokeWidth, 1);
  shape.fill = semanticColor(d2SemanticTokens.shapeFill, shape.fill, palette);
  shape.stroke = semanticColor(
    d2SemanticTokens.shapeStroke,
    shape.stroke,
    palette,
  );

  if (shape.primaryAccentColor) {
    shape.primaryAccentColor = semanticColor(
      d2SemanticTokens.shapePrimaryAccent,
      shape.primaryAccentColor,
      palette,
    );
  }

  if (shape.secondaryAccentColor) {
    shape.secondaryAccentColor = semanticColor(
      d2SemanticTokens.shapeSecondaryAccent,
      shape.secondaryAccentColor,
      palette,
    );
  }

  if (shape.neutralAccentColor) {
    shape.neutralAccentColor = semanticColor(
      d2SemanticTokens.shapeNeutralAccent,
      shape.neutralAccentColor,
      palette,
    );
  }

  if ('label' in shape) {
    styleD2Text(shape, palette);
  }

  if ('columns' in shape && Array.isArray(shape.columns)) {
    for (const column of shape.columns) {
      styleD2Text(column.name, palette);
      styleD2Text(column.type, palette);
    }
  }
}

function styleD2Connection(
  connection: Connection,
  palette: D2ThemePalette,
): void {
  connection.stroke = semanticColor(
    d2SemanticTokens.connectionStroke,
    connection.stroke,
    palette,
  );

  if (connection.fill) {
    connection.fill = semanticColor(
      d2SemanticTokens.connectionFill,
      connection.fill,
      palette,
    );
  }

  styleD2Text(connection, palette, { upright: true });
  styleD2Text(connection.srcLabel, palette, { upright: true });
  styleD2Text(connection.dstLabel, palette, { upright: true });
}

function styleD2Diagram(
  diagram: Diagram | undefined,
  palette: D2ThemePalette,
): void {
  if (!diagram) {
    return;
  }

  for (const shape of diagram.shapes) {
    styleD2Shape(shape, palette);
  }

  for (const connection of diagram.connections) {
    styleD2Connection(connection, palette);
  }

  for (const shape of diagram.legend?.shapes ?? []) {
    styleD2Shape(shape, palette);
  }

  for (const connection of diagram.legend?.connections ?? []) {
    styleD2Connection(connection, palette);
  }

  for (const child of [
    ...(diagram.layers ?? []),
    ...(diagram.scenarios ?? []),
    ...(diagram.steps ?? []),
  ]) {
    styleD2Diagram(child, palette);
  }
}

async function renderD2(code: Code, salt: string): Promise<string> {
  try {
    const { lightSvg, darkSvg } = await enqueueD2Render(async () => {
      // The D2 worker JSON-serializes compile requests; Go decodes []byte fields
      // from base64 strings, while Uint8Array serializes into an invalid shape.
      const fontOptions = (await getD2FontOptions()) as unknown as Pick<
        CompileOptions,
        'fontRegular' | 'fontItalic' | 'fontBold' | 'fontSemibold'
      >;
      const lightResult = await d2.compile({
        fs: {
          index: code.value,
        },
        inputPath: 'index',
        options: {
          ...fontOptions,
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
          ...fontOptions,
          layout: 'elk',
          themeID: 200,
          darkThemeID: 200,
          pad: 32,
          noXMLTag: true,
        },
      });
      styleD2Diagram(lightResult.diagram, d2LightPalette);
      styleD2Diagram(darkResult.diagram, d2DarkPalette);
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
