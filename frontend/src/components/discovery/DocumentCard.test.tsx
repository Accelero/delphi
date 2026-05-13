import { describe, it, expect, vi } from "vitest";
import userEvent from "@testing-library/user-event";

import { render, screen } from "../../../test-utils/render";
import { PaperCard } from "./PaperCard";
import type { FeedDocument } from "@/lib/api";

function doc(overrides: Partial<FeedDocument> = {}): FeedDocument {
  return {
    id: "document:abc",
    canonical_id: "test:abc",
    source_type: "arxiv",
    source_uri: "https://arxiv.org/abs/0001.0001",
    authors: ["Alice", "Bob"],
    title: "Hello world",
    summary: "An abstract about hello world.",
    ingested_at: new Date().toISOString(),
    content_hash: "h",
    version: 1,
    metadata: {},
    read: false,
    ...overrides,
  };
}

describe("PaperCard", () => {
  it("renders title, authors, abstract", () => {
    render(
      <PaperCard
        item={doc()}
        isNew={false}
        onMarkRead={() => {}}
        onMarkUnread={() => {}}
      />,
    );
    expect(screen.getByText("Hello world")).toBeInTheDocument();
    expect(screen.getByText(/Alice, Bob/)).toBeInTheDocument();
    expect(
      screen.getByText("An abstract about hello world."),
    ).toBeInTheDocument();
  });

  it("shows the New badge when isNew=true", () => {
    render(
      <PaperCard
        item={doc()}
        isNew
        onMarkRead={() => {}}
        onMarkUnread={() => {}}
      />,
    );
    expect(screen.getByText("New")).toBeInTheDocument();
  });

  it("shows the Read chip when item.read=true", () => {
    render(
      <PaperCard
        item={doc({ read: true })}
        isNew={false}
        onMarkRead={() => {}}
        onMarkUnread={() => {}}
      />,
    );
    expect(screen.getByText("Read")).toBeInTheDocument();
  });

  it("clicking the card body fires onMarkRead with the doc id", async () => {
    const onMarkRead = vi.fn();
    render(
      <PaperCard
        item={doc()}
        isNew={false}
        onMarkRead={onMarkRead}
        onMarkUnread={() => {}}
      />,
    );
    await userEvent.click(screen.getByText("Hello world"));
    // Title link's `e.stopPropagation()` means clicking the title alone
    // shouldn't fire — but clicking the abstract should.
    await userEvent.click(
      screen.getByText("An abstract about hello world."),
    );
    expect(onMarkRead).toHaveBeenCalledWith("document:abc");
  });

  it("clicking the Read chip fires onMarkUnread (and stops propagation)", async () => {
    const onMarkRead = vi.fn();
    const onMarkUnread = vi.fn();
    render(
      <PaperCard
        item={doc({ read: true })}
        isNew={false}
        onMarkRead={onMarkRead}
        onMarkUnread={onMarkUnread}
      />,
    );
    await userEvent.click(screen.getByText("Read"));
    expect(onMarkUnread).toHaveBeenCalledWith("document:abc");
    expect(onMarkRead).not.toHaveBeenCalled();
  });

  it("does not call onMarkRead when clicking an already-read card", async () => {
    const onMarkRead = vi.fn();
    render(
      <PaperCard
        item={doc({ read: true })}
        isNew={false}
        onMarkRead={onMarkRead}
        onMarkUnread={() => {}}
      />,
    );
    await userEvent.click(
      screen.getByText("An abstract about hello world."),
    );
    expect(onMarkRead).not.toHaveBeenCalled();
  });
});
