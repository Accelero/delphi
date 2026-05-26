import { useEffect, useRef, useState } from "react";

const MIN_CHARS_PER_SECOND = 40;
const BACKLOG_ACCELERATION = 2;

export function useSmoothedContent(target: string, isStreaming: boolean) {
  const [shown, setShown] = useState(isStreaming ? "" : target);
  const shownRef = useRef(shown);
  const lastFrameRef = useRef<number | null>(null);
  const carryRef = useRef(0);

  useEffect(() => {
    if (!isStreaming) {
      shownRef.current = target;
      setShown(target);
      return;
    }

    let raf = 0;
    const tick = (now: number) => {
      const previous = lastFrameRef.current ?? now;
      lastFrameRef.current = now;
      const elapsed = Math.max(0, now - previous) / 1000;
      const backlog = target.length - shownRef.current.length;

      if (backlog > 0) {
        const rate = MIN_CHARS_PER_SECOND + BACKLOG_ACCELERATION * backlog;
        carryRef.current += rate * elapsed;
        const take = Math.max(1, Math.min(backlog, Math.floor(carryRef.current)));
        carryRef.current = Math.max(0, carryRef.current - take);
        shownRef.current = target.slice(0, shownRef.current.length + take);
        setShown(shownRef.current);
      }

      raf = window.requestAnimationFrame(tick);
    };

    raf = window.requestAnimationFrame(tick);
    return () => {
      window.cancelAnimationFrame(raf);
      lastFrameRef.current = null;
      carryRef.current = 0;
    };
  }, [target, isStreaming]);

  useEffect(() => {
    if (isStreaming && target.length < shownRef.current.length) {
      shownRef.current = "";
      setShown("");
    }
  }, [target, isStreaming]);

  return shown;
}
