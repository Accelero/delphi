/**
 * UploadProvider — mounts a single UploadManager above the router and puts
 * it on context, so the manager (and its Uppy instance + polling) survives
 * route changes. The always-mounted `<UploadTracker/>` and the `/upload`
 * route both read it from here.
 */
import {
  createContext,
  useContext,
  useRef,
  useSyncExternalStore,
  type ReactNode,
} from "react";

import { UploadManager, type UploadTask } from "@/lib/uploadManager";
import { createUppyDriver } from "@/lib/uppyDriver";

const UploadContext = createContext<UploadManager | null>(null);

export function UploadProvider({ children }: { children: ReactNode }) {
  // Instantiate the manager + Uppy driver exactly once.
  const ref = useRef<UploadManager | null>(null);
  if (ref.current === null) {
    const manager = new UploadManager();
    manager.setDriver(createUppyDriver(manager));
    ref.current = manager;
  }
  return (
    <UploadContext.Provider value={ref.current}>
      {children}
    </UploadContext.Provider>
  );
}

export function useUploadManager(): UploadManager {
  const m = useContext(UploadContext);
  if (!m) throw new Error("useUploadManager must be used within UploadProvider");
  return m;
}

/** Reactive view of the manager's task list via useSyncExternalStore. */
export function useUploadTasks(): UploadTask[] {
  const manager = useUploadManager();
  return useSyncExternalStore(manager.subscribe, manager.getSnapshot);
}
