/**
 * Chat persistence: create a new conversation, send a user message,
 * wait for the stream to complete (which signals `commit_turn` has
 * persisted both rows), navigate away, return, and verify the message
 * survived the round trip through the database.
 *
 * Post chat-streaming redesign: POST `/messages` IS the stream. The
 * worker writes the user+assistant pair atomically via `commit_turn`
 * only after the LLM finishes. We wait for the trailing `d:` frame
 * (response body close) before navigating away.
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

  await page.goto("/corpus");
  await expect(page).toHaveURL(/\/corpus\/[\w-]+$/, { timeout: 10_000 });

  const newChatButton = page.getByRole("button", { name: /new chat/i });
  await expect(newChatButton).toBeVisible();
  const urlBeforeClick = page.url();

  await newChatButton.click();
  await page.waitForURL(
    (u) => /\/corpus\/[\w-]+$/.test(u.pathname) && u.href !== urlBeforeClick,
    { timeout: 10_000 },
  );
  const sessionUrl = page.url();

  const message = `e2e persistence ${Date.now()} ${Math.random().toString(36).slice(2, 8)}`;

  const textarea = page.getByPlaceholder(/type a message/i);
  await textarea.fill(message);

  // POST /messages now returns the stream body; we wait for that
  // request to complete (body finished = `d:` emitted = commit_turn
  // ran), so the persisted pair is on disk by the time we navigate.
  const messagesResponse = page.waitForResponse(
    (r) =>
      /\/api\/chat\/conversations\/[^/]+\/messages$/.test(r.url()) &&
      r.request().method() === "POST",
    { timeout: 30_000 },
  );
  await page.getByRole("button", { name: /^submit$/i }).click();

  // Optimistic render of the user message.
  await expect(page.getByText(message, { exact: true })).toBeVisible();

  const res = await messagesResponse;
  expect(res.status()).toBe(200);
  // Drain the response body so the worker's `d:` frame has been seen
  // by Playwright before we navigate (`commit_turn` runs server-side
  // just before the `d:` frame).
  await res.body();

  // Navigate away, then back via URL — this remounts the route and
  // forces the loader to fetch the conversation from the backend.
  await page.goto("/feed");
  await expect(page).not.toHaveURL(/\/corpus\//);
  await page.goto(sessionUrl);
  await expect(page).toHaveURL(sessionUrl);

  await expect(page.getByText(message, { exact: true })).toBeVisible({
    timeout: 10_000,
  });
});
