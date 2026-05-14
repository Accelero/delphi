import { describe, it, expect, vi } from "vitest";
import userEvent from "@testing-library/user-event";

import { render, screen } from "../../../test-utils/render";
import { DocumentCard } from "./DocumentCard";
import type { FeedDocument } from "@/lib/api";

function doc(overrides: Partial<FeedDocument> = {}): FeedDocument {
  return {
    id: "document:abc",
    canonical_id: "test:abc",
    source_type: "arxiv",
    source_uri: "https://arxiv.org/abs/0001.0001",
    authors: ["Alice", "Bob"],
    title: "Hello world",
    summary: "A summary of hello world.",
    ingested_at: new Date().toISOString(),
    content_hash: "h",
    version: 1,
    metadata: {},
    ...overrides,
  };
}

describe("DocumentCard", () => {
  it("renders title, authors, summary", () => {
    render(<DocumentCard item={doc()} isNew={false} onClearNew={() => {}} />);
    expect(screen.getByText("Hello world")).toBeInTheDocument();
    expect(screen.getByText(/Alice, Bob/)).toBeInTheDocument();
    expect(screen.getByText("A summary of hello world.")).toBeInTheDocument();
  });

  it("shows the New badge when isNew=true", () => {
    render(<DocumentCard item={doc()} isNew onClearNew={() => {}} />);
    expect(screen.getByText("New")).toBeInTheDocument();
  });

  it("calls onClearNew on mouseenter when isNew=true", async () => {
    const onClearNew = vi.fn();
    render(<DocumentCard item={doc()} isNew onClearNew={onClearNew} />);
    await userEvent.hover(screen.getByText("Hello world"));
    expect(onClearNew).toHaveBeenCalled();
  });

  it("does not fire onClearNew when isNew=false", async () => {
    const onClearNew = vi.fn();
    render(
      <DocumentCard item={doc()} isNew={false} onClearNew={onClearNew} />,
    );
    await userEvent.hover(screen.getByText("Hello world"));
    expect(onClearNew).not.toHaveBeenCalled();
  });
});
