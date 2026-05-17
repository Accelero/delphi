/**
 * Two invariants from the chat-streaming redesign:
 *
 *  1. Session isolation — switching sessions mid-stream must not leak
 *     the in-flight assistant tokens (or the optimistic user message)
 *     into the new session's UI. Enforced by `key={sessionId}` on the
 *     <Chat> element so it remounts per session; without the key
 *     `useSessionStream`'s local state survives navigation and
 *     A's stream tee'd into B.
 *
 *  2. Cross-tab fan-out — two tabs viewing the same conversation each
 *     hold their own GET /stream subscription. When tab A submits, the
 *     backend worker streams the assistant reply into the shared
 *     session buffer; tab B sees the same tokens land in its own
 *     <Chat> via its own subscription.
 *
 * Untagged: runs in tier1 and tier2.
 */

import { test, expect } from "@playwright/test";

import { loginViaKeycloak } from "../helpers/login";

function isTier2(baseURL?: string): boolean {
  return !!baseURL && new URL(baseURL).port !== "5173";
}

test("switching sessions mid-stream does not leak messages", async ({
  page,
  baseURL,
}) => {
  if (isTier2(baseURL)) {
    await loginViaKeycloak(page);
  }

  // Start on /corpus; beforeLoad lands us on some existing session.
  await page.goto("/corpus");
  await expect(page).toHaveURL(/\/corpus\/[\w-]+$/, { timeout: 10_000 });
  const newChatButton = page.getByRole("button", { name: /new chat/i });
  await expect(newChatButton).toBeVisible();

  // Mint session A.
  const urlBeforeA = page.url();
  await newChatButton.click();
  await page.waitForURL(
    (u) => /\/corpus\/[\w-]+$/.test(u.pathname) && u.href !== urlBeforeA,
    { timeout: 10_000 },
  );
  const urlA = page.url();

  // Mint session B.
  await newChatButton.click();
  await page.waitForURL(
    (u) => /\/corpus\/[\w-]+$/.test(u.pathname) && u.href !== urlA,
    { timeout: 10_000 },
  );
  const urlB = page.url();
  expect(urlB).not.toBe(urlA);

  // Go back to A and send a unique message there.
  await page.goto(urlA);
  await expect(page).toHaveURL(urlA);
  const messageA = `session-A ${Date.now()} ${Math.random().toString(36).slice(2, 8)}`;
  await page.getByPlaceholder(/type a message/i).fill(messageA);

  // Submit and wait until the POST is in flight, but DO NOT wait for
  // the stream to finish — the whole point is to switch away mid-stream.
  const aMessagesPost = page.waitForRequest(
    (r) =>
      /\/api\/chat\/conversations\/[^/]+\/messages$/.test(r.url()) &&
      r.method() === "POST",
  );
  await page.getByRole("button", { name: /^submit$/i }).click();
  await aMessagesPost;

  // The optimistic user message should be rendered on A.
  await expect(page.getByText(messageA, { exact: true })).toBeVisible();

  // Immediately switch to B via SPA navigation (sidebar click). This is
  // the path that exhibits the leak — `page.goto` would force a full
  // reload and dodge the in-memory state bug.
  //
  // The sidebar entry for B uses the conversation row button; the rows
  // are labelled "Untitled" until auto-title runs, so we match by URL
  // instead via direct Link click. The simplest: navigate via the
  // router using the URL we already captured.
  await page.evaluate((href) => {
    history.pushState({}, "", href);
    // Nudge TanStack Router to re-evaluate without a full reload.
    window.dispatchEvent(new PopStateEvent("popstate"));
  }, urlB);
  await expect(page).toHaveURL(urlB);

  // Give any leaking stream a chance to land in the DOM. With the bug
  // present, tokens from A's response stream into B's `useChat` state
  // and the user message from A also lingers. With the fix (key per
  // sessionId) <Chat> remounts and the leaked state is gone.
  await page.waitForTimeout(2500);

  // B must not show A's user message.
  await expect(page.getByText(messageA, { exact: true })).toHaveCount(0);

  // B must show its own empty-state heading — proving the new Chat
  // instance mounted with B's (empty) initialMessages, not A's.
  await expect(
    page.getByRole("heading", { name: /chat with corpus/i }),
  ).toBeVisible();
});

test("two tabs of the same session see the same submitted message", async ({
  context,
  baseURL,
}) => {
  // Cross-tab fan-out: open two tabs on the same conversation. Submit a
  // user message in tab A; tab B should pick the same message up via
  // its own `/stream` subscription (or the next history poll the hook
  // triggers via the `onTurnEnd` invalidation). The user message is
  // the minimal payload that doesn't depend on an LLM provider being
  // configured — it's persisted by the POST handler before the worker
  // runs.

  const tabA = await context.newPage();
  if (isTier2(baseURL)) await loginViaKeycloak(tabA);

  await tabA.goto("/corpus");
  await expect(tabA).toHaveURL(/\/corpus\/[\w-]+$/, { timeout: 10_000 });
  const newChatBtn = tabA.getByRole("button", { name: /new chat/i });
  await expect(newChatBtn).toBeVisible();

  const urlBefore = tabA.url();
  await newChatBtn.click();
  await tabA.waitForURL(
    (u) => /\/corpus\/[\w-]+$/.test(u.pathname) && u.href !== urlBefore,
    { timeout: 10_000 },
  );
  const sharedUrl = tabA.url();

  // Open the SAME conversation in a second tab. The context is shared
  // so the OIDC session cookie carries over — no second login dance.
  const tabB = await context.newPage();
  await tabB.goto(sharedUrl);
  await expect(tabB).toHaveURL(sharedUrl);
  // Wait for B's loader to settle (history GET completes; the chat
  // surface mounts with whatever's persisted, which is currently empty).
  await expect(
    tabB.getByPlaceholder(/type a message/i),
  ).toBeVisible({ timeout: 10_000 });

  // Submit in tab A. Wait on the 202 so B can race the persisted
  // message via either its open /stream OR the post-onTurnEnd refetch.
  const messageA = `tab-fanout ${Date.now()} ${Math.random().toString(36).slice(2, 8)}`;
  const aPost = tabA.waitForResponse(
    (r) =>
      /\/api\/chat\/conversations\/[^/]+\/messages$/.test(r.url()) &&
      r.request().method() === "POST",
  );
  await tabA.getByPlaceholder(/type a message/i).fill(messageA);
  await tabA.getByRole("button", { name: /^submit$/i }).click();
  const aRes = await aPost;
  expect(aRes.status()).toBe(202);

  // Tab A sees its own optimistic message instantly.
  await expect(tabA.getByText(messageA, { exact: true })).toBeVisible();

  // Tab B should pick it up. The user message is persisted by POST
  // synchronously, and the worker's `d:` frame triggers a sidebar +
  // conversation cache invalidation on every tab — which re-fetches
  // `GET /api/chat/conversations/{id}` and exposes the user row to B.
  // Give that round-trip a generous deadline so a slow CI runner
  // doesn't false-fail.
  await expect(tabB.getByText(messageA, { exact: true })).toBeVisible({
    timeout: 15_000,
  });

  await tabA.close();
  await tabB.close();
});
