import { source } from '@/lib/source';
import { renderMarkdown } from '@/lib/markdown';
import {
  DocsBody,
  DocsDescription,
  DocsPage,
  DocsTitle,
} from 'fumadocs-ui/page';
import { notFound } from 'next/navigation';

export default async function Page(props: {
  params: Promise<{ slug?: string[] }>;
}) {
  const params = await props.params;
  const page = source.getPage(params.slug);

  if (!page) {
    notFound();
  }

  const rendered = await renderMarkdown(page.file);

  return (
    <DocsPage toc={rendered.toc} full={page.data.full}>
      <DocsTitle>{rendered.frontmatter.title ?? page.data.title}</DocsTitle>
      <DocsDescription>
        {rendered.frontmatter.description ?? page.data.description}
      </DocsDescription>
      <DocsBody dangerouslySetInnerHTML={{ __html: rendered.html }} />
    </DocsPage>
  );
}

export function generateStaticParams() {
  return source.generateParams();
}

export async function generateMetadata(props: {
  params: Promise<{ slug?: string[] }>;
}) {
  const params = await props.params;
  const page = source.getPage(params.slug);

  if (!page) {
    notFound();
  }

  return {
    title: page.data.title,
    description: page.data.description,
  };
}
