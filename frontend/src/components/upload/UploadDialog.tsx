/**
 * UploadDialog — the thin enqueue surface for `/upload`.
 *
 * Core principle: pressing Upload hands files to the global UploadManager
 * and returns immediately, then clears the local selection/form. No
 * in-flight upload state lives here — the always-mounted `<UploadTracker/>`
 * shows progress. Drop/select one *or many* files; the metadata prefill
 * form is active only for a single file (a shared form can't title N
 * documents), and greyed out for multi-file.
 */
import { useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import type { UploadPrefill } from "@/lib/api";
import { useUploadManager } from "./UploadProvider";

export function UploadDialog() {
  const manager = useUploadManager();
  const inputRef = useRef<HTMLInputElement>(null);

  const [files, setFiles] = useState<File[]>([]);
  const [title, setTitle] = useState("");
  const [summary, setSummary] = useState("");
  const [authors, setAuthors] = useState("");
  const [language, setLanguage] = useState("");
  const [notice, setNotice] = useState<string | null>(null);

  const multi = files.length > 1;
  const formDisabled = multi || files.length === 0;

  function onPick(list: FileList | null) {
    if (!list) return;
    setFiles(Array.from(list));
    setNotice(null);
  }

  function onDrop(e: React.DragEvent) {
    e.preventDefault();
    onPick(e.dataTransfer.files);
  }

  function reset() {
    setFiles([]);
    setTitle("");
    setSummary("");
    setAuthors("");
    setLanguage("");
    if (inputRef.current) inputRef.current.value = "";
  }

  function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (files.length === 0) return;
    // Prefill only applies to single-file uploads.
    const prefill: UploadPrefill = multi
      ? {}
      : {
          ...(title.trim() ? { title: title.trim() } : {}),
          ...(summary.trim() ? { summary: summary.trim() } : {}),
          ...(language.trim() ? { language: language.trim() } : {}),
          ...(authors.trim()
            ? {
                authors: authors
                  .split(",")
                  .map((a) => a.trim())
                  .filter(Boolean),
              }
            : {}),
        };
    const n = files.length;
    manager.enqueue(files, prefill);
    reset();
    setNotice(`Added ${n} file${n === 1 ? "" : "s"} — track progress in the tracker.`);
  }

  return (
    <form onSubmit={onSubmit} className="max-w-xl space-y-4">
      <h1 className="text-lg font-semibold">Upload documents</h1>

      <div
        onDrop={onDrop}
        onDragOver={(e) => e.preventDefault()}
        className="rounded-md border border-dashed border-[var(--border)] p-6 text-center text-sm text-muted-foreground"
      >
        <p>Drag &amp; drop files here, or</p>
        <Button
          type="button"
          variant="outline"
          className="mt-2"
          onClick={() => inputRef.current?.click()}
        >
          Choose files
        </Button>
        <input
          ref={inputRef}
          type="file"
          multiple
          className="hidden"
          aria-label="Choose files"
          onChange={(e) => onPick(e.target.files)}
        />
      </div>

      {files.length > 0 && (
        <ul className="space-y-1 text-sm" aria-label="Selected files">
          {files.map((f) => (
            <li key={f.name} className="truncate">
              {f.name}
            </li>
          ))}
        </ul>
      )}

      <fieldset
        disabled={formDisabled}
        className="space-y-3 disabled:opacity-50"
        aria-label="Metadata"
      >
        {multi && (
          <p className="text-xs text-muted-foreground">
            Metadata is auto-filled for batch uploads.
          </p>
        )}
        <div>
          <label className="mb-1 block text-sm">Title</label>
          <Input
            value={title}
            onChange={(e) => setTitle(e.target.value)}
            placeholder="Optional"
          />
        </div>
        <div>
          <label className="mb-1 block text-sm">Authors (comma-separated)</label>
          <Input
            value={authors}
            onChange={(e) => setAuthors(e.target.value)}
            placeholder="Optional"
          />
        </div>
        <div>
          <label className="mb-1 block text-sm">Language</label>
          <Input
            value={language}
            onChange={(e) => setLanguage(e.target.value)}
            placeholder="Optional"
          />
        </div>
        <div>
          <label className="mb-1 block text-sm">Summary</label>
          <Textarea
            value={summary}
            onChange={(e) => setSummary(e.target.value)}
            placeholder="Optional"
          />
        </div>
      </fieldset>

      <Button type="submit" disabled={files.length === 0}>
        Upload
      </Button>

      {notice && <p className="text-sm text-muted-foreground">{notice}</p>}
    </form>
  );
}
