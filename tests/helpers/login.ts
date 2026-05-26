import type { Page } from "@playwright/test";

export const TEST_USER = {
  username: process.env.E2E_USERNAME ?? "test",
  password: process.env.E2E_PASSWORD ?? "test",
  email: "test@example.com",
  tenant: "tenant-a"
} as const;

export async function loginViaKeycloak(page: Page): Promise<void> {
  await page.goto("/oauth2/sign_in");

  await page.locator("#username").fill(TEST_USER.username);
  await page.locator("#password").fill(TEST_USER.password);
  await page.locator("#kc-login").click();

  await page.waitForURL(/\/$/);
  await page.getByText("Delphi").waitFor({ state: "visible" });
}
