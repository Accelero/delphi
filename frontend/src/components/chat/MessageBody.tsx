import { lazy, Suspense } from "react";
import type { CitationEntry } from "../../lib/types";

const RichMessageBody = lazy(() => import("./RichMessageBody"));

export function MessageBody({
  content,
  streaming,
  citations = []
}: {
  content: string;
  streaming: boolean;
  citations?: CitationEntry[];
}) {
  return (
    <Suspense fallback={<PlainMessageBody content={content} />}>
      <RichMessageBody content={content} streaming={streaming} citations={citations} />
    </Suspense>
  );
}

function PlainMessageBody({ content }: { content: string }) {
  if (!content.trim()) return null;
  return <div className="whitespace-pre-wrap text-[15px] leading-7">{content}</div>;
}
