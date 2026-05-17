/**
 * Chat persistence: create a new conversation, send a user message,
 * navigate away, return to the same session, and verify the message
 * survived the round trip through the database.
 *
 * Post chat-streaming redesign: the backend persists the user message
 * synchronously inside `POST /api/chat/conversations/{id}/messages`
 * and returns 202 Accepted; the assistant reply streams asynchronously
 * over the separate `GET /stream` subscription. This test waits for
 * the POST's 202 (= user row written) before navigating away, so it
 * does not depend on the LLM provider being configured.
 *
 * Untagged: runs in tier1 and tier2 (tier2 needs the Keycloak login
 * dance).
 */

import { test, expect } from "@playwright/test";

import { loginViaKeycloak } from "../helpers/login";

function isTier2(baseURL?: string): boolean {
  return !!baseURL && new URL(baseURL).port !== "5173";
}

test("user message persists across navigation away and back", async ({
  page,
  baseURL,
}) => {
  if (isTier2(baseURL)) {
    await loginViaKeycloak(page);
  }

  // Land somewhere under /corpus — beforeLoad will pick the most recent
  // conversation or mint one.
  await page.goto("/corpus");
  await expect(page).toHaveURL(/\/corpus\/[\w-]+$/, { timeout: 10_000 });

  // Sidebar must be present before we can click "New chat".
  const newChatButton = page.getByRole("button", { name: /new chat/i });
  await expect(newChatButton).toBeVisible();
  const urlBeforeClick = page.url();

  // Mint a fresh conversation so we don't share state with prior runs.
  // After the click the route navigates to the new session id; waiting
  // on the URL change is more robust than racing the POST response.
  await newChatButton.click();
  await page.waitForURL(
    (u) => /\/corpus\/[\w-]+$/.test(u.pathname) && u.href !== urlBeforeClick,
    { timeout: 10_000 },
  );
  const sessionUrl = page.url();

  // Unique payload so reruns can't false-pass on stale rows. Includes
  // the test run timestamp so failures are debuggable from DB.
  const message = `e2e persistence ${Date.now()} ${Math.random().toString(36).slice(2, 8)}`;

  const textarea = page.getByPlaceholder(/type a message/i);
  await textarea.fill(message);

  // The chat surface POSTs to `/api/chat/conversations/<key>/messages`.
  // Wait for the *response* (not just the request) so the backend has
  // finished persisting the user row before we navigate away.
  const messagesResponse = page.waitForResponse(
    (r) =>
      /\/api\/chat\/conversations\/[^/]+\/messages$/.test(r.url()) &&
      r.request().method() === "POST",
  );
  await page.getByRole("button", { name: /^submit$/i }).click();

  // The user message renders optimistically — quick sanity check that
  // the submit actually fired, before we wait on the network.
  await expect(page.getByText(message, { exact: true })).toBeVisible();

  const res = await messagesResponse;
  // POST is fire-and-forget under the new contract: a 202 means the
  // user row is persisted and the worker is dispatched. The assistant
  // reply arrives separately on the GET /stream subscription, which
  // this test deliberately doesn't gate on (LLM may be unavailable in
  // CI without provider credentials).
  expect(res.status()).toBe(202);

  // Navigate away, then back via URL — this remounts the route and
  // forces the loader to fetch the conversation from the backend.
  await page.goto("/feed");
  await expect(page).not.toHaveURL(/\/corpus\//);
  await page.goto(sessionUrl);
  await expect(page).toHaveURL(sessionUrl);

  // After the loader resolves, the persisted user message must be
  // rendered into the chat surface.
  await expect(page.getByText(message, { exact: true })).toBeVisible({
    timeout: 10_000,
  });
});
