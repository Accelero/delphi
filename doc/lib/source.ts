import type { Root } from 'fumadocs-core/page-tree';

export type DocsPage = {
  slugs: string[];
  url: string;
  file: string;
  data: {
    title: string;
    description?: string;
    full?: boolean;
  };
};

function page(
  slugs: string[],
  file: string,
  data: DocsPage['data'],
): DocsPage {
  const path = slugs.join('/');

  return {
    slugs,
    file,
    url: path.length > 0 ? `/docs/${path}` : '/docs',
    data,
  };
}

const pages = [
  page([], 'index.md', {
    title: 'Delphi Docs',
    description:
      'Architecture, runtime workflows, and operating notes for the Delphi project.',
  }),
  page(['architecture', 'chat-system'], 'architecture/chat-system.md', {
    title: 'Chat System',
    description:
      'Service boundaries, state ownership, and realtime fanout for Delphi chat.',
  }),
  page(
    ['architecture', 'chat-request-flow'],
    'architecture/chat-request-flow.md',
    {
      title: 'Chat Request Flow',
      description:
        'Submit-to-commit, stop, and browser reconciliation flow for chat turns.',
    },
  ),
  page(
    ['architecture', 'chat-failure-analysis'],
    'architecture/chat-failure-analysis.md',
    {
      title: 'Chat Failure Analysis',
      description:
        'Crash behavior, recovery expectations, and invariants for chat services.',
    },
  ),
  page(['architecture', 'alteration'], 'architecture/alteration.md', {
    title: 'Design Notes',
    description:
      'Open design decisions and architecture refinements for Delphi chat.',
  }),
  page(['reference'], 'reference.md', {
    title: 'Source Reference',
    description: 'Generated source documentation entry points for Delphi.',
  }),
  page(['legacy'], 'legacy.md', {
    title: 'Documentation Archive',
    description: 'Historical documentation retained as source material.',
  }),
] satisfies DocsPage[];

const pagesBySlug = new Map(pages.map((item) => [item.slugs.join('/'), item]));

const pageTree: Root = {
  name: 'Delphi Docs',
  children: [
    {
      type: 'page',
      name: 'Overview',
      url: '/docs',
    },
    {
      type: 'folder',
      name: 'Architecture',
      defaultOpen: true,
      children: [
        {
          type: 'page',
          name: 'Chat System',
          url: '/docs/architecture/chat-system',
        },
        {
          type: 'page',
          name: 'Chat Request Flow',
          url: '/docs/architecture/chat-request-flow',
        },
        {
          type: 'page',
          name: 'Chat Failure Analysis',
          url: '/docs/architecture/chat-failure-analysis',
        },
        {
          type: 'page',
          name: 'Design Notes',
          url: '/docs/architecture/alteration',
        },
      ],
    },
    {
      type: 'page',
      name: 'Source Reference',
      url: '/docs/reference',
    },
    {
      type: 'page',
      name: 'Archive',
      url: '/docs/legacy',
    },
  ],
};

export const source = {
  pageTree,
  getPage(slugs?: string[]) {
    return pagesBySlug.get((slugs ?? []).join('/'));
  },
  getPages() {
    return pages;
  },
  generateParams() {
    return pages.map((item) => ({
      slug: item.slugs,
    }));
  },
};
