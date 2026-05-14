/**
 * Chat persistence: create a new conversation, send a user message,
 * navigate away, return to the same session, and verify the message
 * survived the round trip through the database.
 *
 * The backend persists the user message *before* invoking the LLM
 * (see `backend/src/api/chat.rs`), so this test does not depend on the
 * LLM provider being configured — only on the conversation + messages
 * tables being correctly read back by the route loader.
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
  // Even if the LLM bails (no key, provider down), the user row is
  // persisted before stream_chat() is invoked, so any 2xx/4xx/5xx
  // response means the write happened. We don't gate the test on the
  // assistant stream completing.
  expect(res.status()).toBeGreaterThanOrEqual(200);

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
