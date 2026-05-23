import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

/** shadcn/ui's standard className combiner. */
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

/**
 * Scheme-allowlist a URL before it's used in an `href`/`src`.
 *
 * XSS is a render-time sink: a stored `javascript:`/`data:`/`vbscript:` URL
 * becomes code execution the moment it lands in an anchor. React escapes
 * text but does **not** gate URL schemes, so every render site that turns a
 * server-supplied string into a link must pass it through here first
 * (audit M9). Returns the URL only when it resolves to an `http(s)` URL,
 * else `undefined` so the caller can render plain text instead of a link.
 */
export function safeHref(url: string | null | undefined): string | undefined {
  if (!url) return undefined;
  try {
    const parsed = new URL(url, window.location.origin);
    return parsed.protocol === "http:" || parsed.protocol === "https:"
      ? url
      : undefined;
  } catch {
    return undefined;
  }
}
