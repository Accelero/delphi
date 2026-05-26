import { expect, test } from "@playwright/test";
import { ulid } from "ulid";

import { deleteConversationsWithPrefix } from "../helpers/chat-api.js";
import { loginViaKeycloak } from "../helpers/login.js";

const TITLE_PREFIX = "E2E API chat";

test("chat API creates, lists, reads, renames, and deletes conversations", async ({ page }) => {
  await loginViaKeycloak(page);
  const api = page.context().request;
  await deleteConversationsWithPrefix(api, TITLE_PREFIX);

  const title = `${TITLE_PREFIX} ${Date.now()}`;

  const created = await api.post("/api/chat/conversations", {
    data: { title }
  });
  expect(created.status()).toBe(201);
  const createdBody = (await created.json()) as { id: string; title: string; messages: unknown[] };
  expect(createdBody.title).toBe(title);
  expect(createdBody.messages).toEqual([]);

  const listed = await api.get("/api/chat/conversations");
  expect(listed.status()).toBe(200);
  const rows = (await listed.json()) as Array<{ id: string; title: string }>;
  expect(rows.some((row) => row.id === createdBody.id && row.title === title)).toBe(true);

  const fetched = await api.get(`/api/chat/conversations/${createdBody.id}`);
  expect(fetched.status()).toBe(200);
  await expectJson(fetched, expect.objectContaining({ id: createdBody.id, title }));

  const renamedTitle = `${title} renamed`;
  const renamed = await api.patch(`/api/chat/conversations/${createdBody.id}`, {
    data: { title: renamedTitle }
  });
  expect(renamed.status()).toBe(200);
  await expectJson(
    renamed,
    expect.objectContaining({ id: createdBody.id, title: renamedTitle })
  );

  const deleted = await api.delete(`/api/chat/conversations/${createdBody.id}`);
  expect(deleted.status()).toBe(204);

  const missing = await api.get(`/api/chat/conversations/${createdBody.id}`);
  expect(missing.status()).toBe(404);
});

test("compact chat API lists history and accepts a message", async ({ page }) => {
  await loginViaKeycloak(page);
  const api = page.context().request;
  await deleteConversationsWithPrefix(api, TITLE_PREFIX);

  const title = `${TITLE_PREFIX} compact ${Date.now()}`;
  const created = await api.post("/chat", {
    data: { title }
  });
  expect(created.status()).toBe(201);
  const conversation = (await created.json()) as { id: string; title: string; messages: unknown[] };
  expect(conversation.title).toBe(title);
  expect(conversation.messages).toEqual([]);

  const listed = await api.get("/chat");
  expect(listed.status()).toBe(200);
  const rows = (await listed.json()) as Array<{ id: string; title: string }>;
  expect(rows.some((row) => row.id === conversation.id && row.title === title)).toBe(true);

  const fetched = await api.get(`/chat/${conversation.id}`);
  expect(fetched.status()).toBe(200);
  await expectJson(fetched, expect.objectContaining({ id: conversation.id, title }));

  const accepted = await api.post(`/chat/${conversation.id}`, {
    data: {
      text: "API-only compact endpoint test",
      parent_message_id: null
    }
  });
  expect(accepted.status()).toBe(202);
  await expectJson(accepted, { turn_id: expect.any(String) });

  await expect
    .poll(async () => {
      const response = await api.get(`/chat/${conversation.id}`);
      if (!response.ok()) return [];
      const body = (await response.json()) as { messages: Array<{ role: string; content: string }> };
      return body.messages.map((message) => message.role);
    }, { timeout: 45_000 })
    .toEqual(["user", "assistant"]);
});

test("chat API accepts a first turn for an empty conversation", async ({ page }) => {
  await loginViaKeycloak(page);
  const api = page.context().request;
  await deleteConversationsWithPrefix(api, TITLE_PREFIX);

  const created = await api.post("/api/chat/conversations", {
    data: { title: `${TITLE_PREFIX} submit ${Date.now()}` }
  });
  expect(created.status()).toBe(201);
  const conversation = (await created.json()) as { id: string };
  const userMessageId = ulid();
  const turnId = ulid();

  const accepted = await api.post(`/api/chat/conversations/${conversation.id}/turns`, {
    data: {
      user_message_id: userMessageId,
      turn_id: turnId,
      text: "API-only acceptance test",
      parent_message_id: null
    }
  });
  expect(accepted.status()).toBe(202);
  await expectJson(accepted, { turn_id: turnId });

  await expect
    .poll(async () => {
      const response = await api.get(`/api/chat/conversations/${conversation.id}`);
      if (!response.ok()) return 0;
      const body = (await response.json()) as { messages: unknown[] };
      return body.messages.length;
    }, { timeout: 45_000 })
    .toBe(2);

  await api.delete(`/api/chat/conversations/${conversation.id}`);
});

test("chat API rejects stale parent ids before enqueueing a turn", async ({ page }) => {
  await loginViaKeycloak(page);
  const api = page.context().request;

  const created = await api.post("/api/chat/conversations", {
    data: { title: `${TITLE_PREFIX} stale ${Date.now()}` }
  });
  expect(created.status()).toBe(201);
  const conversation = (await created.json()) as { id: string };
  const userMessageId = ulid();
  const turnId = ulid();

  const rejected = await api.post(`/api/chat/conversations/${conversation.id}/turns`, {
    data: {
      user_message_id: userMessageId,
      turn_id: turnId,
      text: "This should be rejected",
      parent_message_id: "01HX0000000000000000000999"
    }
  });
  expect(rejected.status()).toBe(409);
  await expectJson(
    rejected,
    expect.objectContaining({
      error: expect.objectContaining({ code: "stale_parent" })
    })
  );

  await api.delete(`/api/chat/conversations/${conversation.id}`);
});

async function expectJson(response: { json: () => Promise<unknown> }, expected: unknown) {
  expect(await response.json()).toEqual(expected);
}
