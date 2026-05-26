/**
 * Compose stack helpers. Tests assume the relevant stack is already up
 * (developers run `make up` / `make full-up` first, CI does the same in a
 * step before invoking Playwright). These helpers exist for the cases
 * where a specific test wants to bounce a service or reset DB state.
 */

import { execSync } from "node:child_process";

const COMPOSE_FILE = {
  tier1: "docker-compose.yml",
  tier2: "docker-compose.full.yml",
} as const;

export type Tier = keyof typeof COMPOSE_FILE;

const REPO_ROOT = new URL("../../", import.meta.url).pathname;

function compose(tier: Tier, args: string): string {
  return execSync(`docker compose -f ${COMPOSE_FILE[tier]} ${args}`, {
    cwd: REPO_ROOT,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
}

/** Wipe DB state without nuking the volume — fast reset between tests. */
export function wipeDb(tier: Tier): void {
  compose(tier, "exec -T backend /usr/local/bin/delphi admin wipe");
}

/** Bring a single service down + up (debugging helper). */
export function restart(tier: Tier, service: string): void {
  compose(tier, `restart ${service}`);
}
