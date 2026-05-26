import { chromium } from "@playwright/test";
import { loginViaKeycloak } from "../helpers/login";

const doc = {
  canonical_id: `manual-t2-${Date.now()}`,
  source_type: "manual",
  source_uri: `https://example.test/t2/${Date.now()}`,
  title: "Manual T2 ingest — live update test",
  authors: ["Alice"],
  summary: "Pushed via Playwright through oauth2-proxy + Keycloak.",
};

const browser = await chromium.launch();
const ctx = await browser.newContext({ baseURL: "http://localhost" });
const page = await ctx.newPage();
await loginViaKeycloak(page, "alice");
const res = await page.request.post("/api/ingestion/documents", { data: doc });
console.log("status", res.status());
console.log(await res.text());
await browser.close();
