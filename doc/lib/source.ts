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
    ['architecture', 'chat-migration'],
    'architecture/chat-migration.md',
    {
      title: 'Chat Migration',
      description:
        'Consolidated current state, old-system differences, and remaining work for the chat migration.',
    },
  ),
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
  page(
    ['architecture', 'pg-cutover-upload-pipeline-plan'],
    'architecture/pg-cutover-upload-pipeline-plan.md',
    {
      title: 'PG Cutover Plan',
      description:
        'Historical record of the SurrealDB to Postgres cutover; its upload milestones are superseded.',
    },
  ),
  page(
    ['architecture', 'document-upload'],
    'architecture/document-upload.md',
    {
      title: 'Document Upload and Lifecycle',
      description:
        'Event-sourced document CRUD — API contract, event catalog, concurrency control, upload pipeline, projections, and garbage collection.',
    },
  ),
  page(
    ['document-crud', 'pg-outbox-document-crud'],
    'document-crud/pg-outbox-document-crud.md',
    {
      title: 'PG Outbox Document CRUD',
      description:
        'Selected initial document CRUD design using Postgres transactions, an outbox table, and NATS projection fanout.',
    },
  ),
  page(
    ['document-crud', 'document-crud-pipeline'],
    'document-crud/document-crud-pipeline.md',
    {
      title: 'Document CRUD Pipeline',
      description:
        'Alternative NATS-first document CRUD with PG/S3/Qdrant/NebulaGraph projections.',
    },
  ),
  page(
    ['document-crud', 'document-event-sourcing'],
    'document-crud/document-event-sourcing.md',
    {
      title: 'Document Event Sourcing',
      description:
        'EventStoreDB-backed document source of truth with NATS work fanout.',
    },
  ),
  page(
    ['document-crud', 'document-crud-sync'],
    'document-crud/document-crud-sync.md',
    {
      title: 'Document CRUD Sync Pattern',
      description:
        'Alternative NATS event-first pattern for document CRUD, ingestion, and projections.',
    },
  ),
  page(
    ['document-crud', 'nats-event-first-document-crud'],
    'document-crud/nats-event-first-document-crud.md',
    {
      title: 'NATS Event-First Document CRUD',
      description:
        'Durable NATS command/event publishing for document CRUD.',
    },
  ),
  page(
    ['document-crud', 'nats-projection-flow'],
    'document-crud/nats-projection-flow.md',
    {
      title: 'NATS Projection Flow',
      description:
        'Worker processing, redelivery, dedupe, and crash recovery for document projections.',
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
          name: 'Chat Migration',
          url: '/docs/architecture/chat-migration',
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
          name: 'Document Upload and Lifecycle',
          url: '/docs/architecture/document-upload',
        },
        {
          type: 'page',
          name: 'PG Cutover Plan (historical)',
          url: '/docs/architecture/pg-cutover-upload-pipeline-plan',
        },
        {
          type: 'page',
          name: 'Design Notes',
          url: '/docs/architecture/alteration',
        },
      ],
    },
    {
      type: 'folder',
      name: 'Document CRUD',
      defaultOpen: true,
      children: [
        {
          type: 'page',
          name: 'PG Outbox Document CRUD',
          url: '/docs/document-crud/pg-outbox-document-crud',
        },
        {
          type: 'page',
          name: 'Document CRUD Pipeline',
          url: '/docs/document-crud/document-crud-pipeline',
        },
        {
          type: 'page',
          name: 'Document Event Sourcing',
          url: '/docs/document-crud/document-event-sourcing',
        },
        {
          type: 'page',
          name: 'Document CRUD Sync Pattern',
          url: '/docs/document-crud/document-crud-sync',
        },
        {
          type: 'page',
          name: 'NATS Event-First Document CRUD',
          url: '/docs/document-crud/nats-event-first-document-crud',
        },
        {
          type: 'page',
          name: 'NATS Projection Flow',
          url: '/docs/document-crud/nats-projection-flow',
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
