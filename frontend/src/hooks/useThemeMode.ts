import { useEffect, useState } from "react";

export type ThemeMode = "light" | "dark" | "system";
export type ResolvedTheme = "light" | "dark";

const STORAGE_KEY = "delphi.theme";
const MODES = new Set<ThemeMode>(["light", "dark", "system"]);

function storedThemeMode(): ThemeMode {
  try {
    const value = window.localStorage.getItem(STORAGE_KEY);
    return MODES.has(value as ThemeMode) ? (value as ThemeMode) : "system";
  } catch {
    return "system";
  }
}

function resolveThemeMode(mode: ThemeMode): ResolvedTheme {
  const systemDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
  return mode === "dark" || (mode === "system" && systemDark) ? "dark" : "light";
}

function applyThemeMode(mode: ThemeMode): ResolvedTheme {
  const resolved = resolveThemeMode(mode);
  const dark = resolved === "dark";
  document.documentElement.dataset.themeMode = mode;
  document.documentElement.dataset.theme = resolved;
  document.documentElement.classList.toggle("dark", dark);
  document.documentElement.style.colorScheme = dark ? "dark" : "light";
  return resolved;
}

export function useThemeMode() {
  const [mode, setMode] = useState<ThemeMode>(() => storedThemeMode());
  const [resolved, setResolved] = useState<ResolvedTheme>(() => resolveThemeMode(storedThemeMode()));

  useEffect(() => {
    try {
      window.localStorage.setItem(STORAGE_KEY, mode);
    } catch {
      // Storage may be unavailable in hardened browser modes; the live setting still applies.
    }
    setResolved(applyThemeMode(mode));
  }, [mode]);

  useEffect(() => {
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    const onChange = () => setResolved(applyThemeMode(mode));
    media.addEventListener("change", onChange);
    return () => media.removeEventListener("change", onChange);
  }, [mode]);

  return { mode, resolved, setMode };
}
