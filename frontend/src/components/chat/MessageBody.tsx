import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { useSmoothedContent } from "../../hooks/useSmoothedContent";

export function MessageBody({
  content,
  streaming
}: {
  content: string;
  streaming: boolean;
}) {
  const shown = useSmoothedContent(content, streaming);

  return (
    <div className="prose prose-stone max-w-none text-[15px] leading-7">
      <ReactMarkdown remarkPlugins={[remarkGfm]}>{shown}</ReactMarkdown>
    </div>
  );
}
