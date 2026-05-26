# Object Validator — Design

**Status:** implemented (2026-05-24)
**Sister docs:** [`ingestion.md`](./ingestion.md), the metadata validator
in [`metadata-extractor.md`](./metadata-extractor.md), and the NATS ingest
plan in [`scaling-nats.md`](./scaling-nats.md).

The object validator is the **corpus-admission gate** for uploaded bytes.
It runs at `POST /api/ingestion/uploads/:id/complete`, after the browser
has PUT the object directly to S3, and decides whether those bytes are
allowed to become a document. This doc specifies its target shape: a
**dispatch-on-file-ending** structure — one validator per known ending,
plus a byte-sniffing **prober** for unknown endings.

It documents the dispatch-oriented validator under
`ingestion/validation/object/`, wired from `ingestion/completion.rs`.

---

## 1. Threat model (why the validator is shaped this way)

The upload path keeps bytes **off the backend**: browser → S3 directly
via presigned PUT. The backend reads the committed object back **once**,
at `/complete`, under a hard byte cap (`text_extract.rs`). So the
validator is not protecting the upload path — it is deciding *admission*
and making sure that single read-back can't hurt us.

Delphi **never executes** an uploaded file. The only code that *touches*
the bytes is a text extractor (`pdftotext` today; an HTML tag-stripper
next). That narrows "malicious" to concrete, bounded risks:

| Concern | Where it actually bites | Defense |
|---|---|---|
| **Malformed** | extractor crash / hang / OOM on read-back | `pdftotext` runs as a **bounded subprocess** (timeout, `kill_on_drop`, capped stdout); `html2text` is in-process (see §15) + size cap |
| **Malicious** | extractor parser exploit; resource-exhaustion | allowlist (minimise parser surface) + bounded subprocess + caps |
| **Injection** | bytes flowing to a *sink* | defended **at each sink**, never by scanning the file |
| **Wrong type** | type confusion / polyglot | sniff is authoritative; polyglots rejected (head-only — §15) |

**Injection is defended at the sink, not by scanning input here** — same
principle as the metadata validator:

- **Shell** — the only shell sink is the extractor; bytes go in via
  file/stdin, never as argv. No injection surface as long as the filename
  and content are never interpolated into a command line.
- **SurrealQL** — parameterised binds; content never reaches a query as
  text.
- **LLM (prompt injection)** — Layer 3, deferred, mitigated at
  consumption.
- **Browser (XSS)** — render-time, handled by `safeHref` + React
  escaping. We never render uploaded HTML raw; we strip it to text.

The validator therefore does **not** grow a "scan for `<script>` / shell
metacharacters" step. That is the same false-comfort input-scanning we
rejected for metadata.

---

## 2. Scope: what we admit

Two things only ever touch attacker bytes with a *parser*: PDF
(`pdftotext`) and HTML (a tag-stripper). Everything else is plain UTF-8
text that we validate and store verbatim — no parser, minimal risk. So
the corpus admits:

| Class | Endings | Canonical type | Parser | Notes |
|---|---|---|---|---|
| PDF | `.pdf` (or `%PDF-` magic) | `application/pdf` | `pdftotext` (sandboxed) | binary; bytes must confirm |
| Markup | `.html`, `.htm` | `text/html` | HTML→text (new) | text; tags stripped at extraction |
| Markdown | `.md` | `text/markdown` | none (passthrough) | text |
| **Any other UTF-8 text** | `.txt`, `.py`, `.json`, `.csv`, none, … | `text/plain` | none (passthrough) | text; **liberal** — it's all text |

The breadth is deliberate (decided 2026-05-24): **if the bytes are valid
UTF-8 text, we take them.** Source code, configs, logs, notes — all
ingest as `text/plain`. This is safe precisely because plain text runs
*no parser*; the only checks that matter for it are "is it really text?"
(reject disguised binaries) and the size cap (resource).

ZIP-container formats (DOCX/PPTX/EPUB) are explicitly **out** — they add a
decompression-bomb + zip-path-traversal class we are not taking on now.

**One parser per risky format is the security budget.** Adding a third
parser (e.g. DOCX) is a deliberate future decision, not a free "more
formats."

---

## 3. The dispatch architecture

> **The file ending *selects* a validator. The bytes *confirm* it.**
> The ending is an attacker-controlled claim (`evil.bin` → `paper.pdf`),
> so it can route but never decide. **All text shares one validator with
> identical checks** — the ending only picks PDF-vs-text and tags which
> extractor runs downstream.

```
validate_uploaded_object(ending, key, declared_size, store, policy)
  │
  1. HEAD            → actual size vs declared (shared)
  2. GET range 0..N  → sniff window               (shared)
  3. dispatch on the (lowercased) file ending:
  │
  │   ".pdf"          → PdfValidator        (bytes MUST be %PDF-, else reject)
  │   ".html"/".htm"  → TextValidator → text/html      (extractor: strip)
  │   ".md"           → TextValidator → text/markdown  (extractor: passthrough)
  │   ".txt"          → TextValidator → text/plain     (extractor: passthrough)
  │   <anything else, incl. missing> → Prober (sniff-and-recover):
  │                       • %PDF- magic       → application/pdf
  │                       • valid UTF-8 text  → text/plain
  │                       • else              → reject NotInAllowlist
  └─► ValidatedAttrs { size, etag, sniffed_content_type }
```

### Two design rules this encodes

**Reject on mismatch — never silently re-route.** A *known* ending commits
to its validator. A `.pdf` whose bytes aren't `%PDF-` is **rejected**, not
quietly reclassified as text — the user fixes the ending and retries. Only
*unrecognised* endings reach the Prober, which is the one place we sniff
to recover (e.g. an extension-less real PDF still works). This is a chosen
trade: a mislabeled-as-`.pdf` text file is rejected, but a no-extension
PDF is accepted. Strictness applies to *positive* claims.

**All text is one validator.** `.txt`, `.md`, `.html`, `.py`, `.json`,
`anything.weird` — every text path runs the *same* `TextValidator` checks
(valid UTF-8, not a disguised binary, size cap). There is no per-text-
format security check, because the security boundary for text isn't here:
it's the size cap (resource), the disguised-binary sniff, and the *render
sink* (XSS — see §6). The ending only attaches a subtype label
(`text/plain`/`markdown`/`html`) that selects the extractor; for the
liberal default (any UTF-8 text) the label is `text/plain`.

### Why dispatch on the ending at all (vs. pure sniffing)

`.txt`, `.md`, `.html`, source code are **byte-identical UTF-8** — magic
bytes cannot tell them apart, and `infer` ships no text/markdown/HTML
detector. Today the code already leans on a declared hint to split
`text/plain` vs `text/markdown` (`object.rs:249`). The ending is a better,
structured version of that hint, and it is *required* to know we should
run the HTML tag-stripper rather than store raw markup. The security
decision (is this allowed? is a `.pdf` really a PDF?) still rests on the
bytes — a **refinement, not a reversal**, of byte-authority.

### The `FormatValidator` contract

```rust
/// Confirm a sniff window against the format the file ending claimed.
///
/// The ending dispatched me, but the ending is attacker-controlled — I
/// MUST confirm against the bytes before returning Ok. Implementors that
/// trust the ending are a security bug.
trait FormatValidator {
    fn validate(
        &self,
        sniff: &[u8],          // bytes 0..sniff_window (already fetched)
        head: &ObjectMeta,     // size + etag from HEAD
        policy: &ObjectPolicy,
    ) -> Result<ValidatedAttrs, ObjectReject>;
}
```

Shared I/O (HEAD, the ranged sniff GET, the size-tolerance check) stays in
the dispatcher so every validator sees the same already-fetched window and
no validator re-does I/O. Only **two** impls exist: `PdfValidator` and
`TextValidator` (constructed with the subtype its ending implies). The
`Prober` is the dispatcher's `else` arm, not a third trait impl.

### Per-validator confirmation rules

- **PdfValidator** — reject unless `sniff` starts with `%PDF-`; enforce the
  PDF size cap; reject ZIP/PDF polyglots (head-signature probe). Emits
  `application/pdf`. The **active-content scan** is *not* in this validator
  (it only sees the 4 KB sniff window); it runs as a separate pipeline
  stage on the full bounded bytes — see **§12**. *Page-count is also out of
  scope here* — it needs a full parse and stays a reserved knob
  (`pdf_max_pages`, see §10).
- **TextValidator(subtype)** — reject unless the window is valid UTF-8
  text (`looks_like_utf8_text`, tolerant of a multi-byte char split at the
  window boundary) **and** the bytes aren't a disguised *binary*. Subtlety:
  `infer` recognises some **text** types too (`text/html`, `text/xml`), so
  the disguised-binary check (`sniff::infer_binary`) ignores any `text/*`
  match and rejects only a recognised non-text signature (PNG/ZIP/PDF under
  a text ending). Emits its configured subtype. One impl, every text
  ending.
- **Prober** (the `else` arm; unknown/missing ending) — sniff-and-recover:
  `%PDF-` magic → run the PDF rules; else valid UTF-8 text → `text/plain`;
  else reject `NotInAllowlist`. Cannot positively detect HTML (no magic),
  so an unknown-ending HTML file is admitted as `text/plain` — ingested,
  just not tag-stripped. Acceptable.

---

## 4. Module structure (well-structured per the repo rules)

Promote the single file to a module folder; `mod.rs` is the public
interface, the rest are hidden internals (siblings, free to import each
other):

```
ingestion/validation/object/
├── mod.rs        # PUBLIC: validate_uploaded_object (dispatcher: HEAD,
│                 #   sniff GET, ending→validator routing, prober else-arm),
│                 #   ObjectPolicy, ObjectReject, ValidatedAttrs
├── format.rs     # FormatValidator trait + Ending parsing + dispatch table
├── pdf.rs        # PdfValidator
├── text.rs       # TextValidator(subtype)
└── sniff.rs      # shared infer wrapper + polyglot probe +
                  #   looks_like_utf8_text + the sniff-and-recover routine
```

Two trait impls only (`PdfValidator`, `TextValidator`); the **prober** is
the dispatcher's `else` arm calling a `sniff.rs` helper, not a third impl.
Only `validate_uploaded_object`, `ObjectPolicy`, `ObjectReject`,
`ValidatedAttrs` are re-exported from `mod.rs`. The trait and the concrete
validators are internal — callers compose validation only through
`validate_uploaded_object`. The existing `validation/mod.rs` re-exports
are unchanged.

---

## 5. Plumbing the file ending

The ending must reach `/complete`. Nothing carries it today.

1. **`CreateUploadRequest`** — add `#[serde(default)] pub filename:
   Option<String>`. Untrusted; used for **two** things only:
   (a) extension dispatch, (b) sanitised title fallback (roadmap §2). It
   is **never** an S3 key (keys are server-minted UUIDs — no zip-slip) and
   **never** a shell arg.
2. **`CreateUploadSessionParams` + `UploadSession`** (`storage/models.rs`)
   — add `filename: Option<String>`.
3. **`schema.surql`** — `DEFINE FIELD IF NOT EXISTS filename ON
   upload_session TYPE option<string>;`
4. **`CompletionCtx`** — already holds `&session`, so the ending is
   reachable; the dispatcher derives the extension from
   `session.filename`.
5. **SPA** — `upload` flow already has `file.name`; send it as `filename`
   on create.

**Extension derivation** (untrusted-input hygiene): take the final
dot-segment of the basename, lowercase it, bound its length; missing /
empty / unknown → `ProbeValidator`. Filenames like `..`, `a.tar.gz`,
`.PDF`, embedded path separators all resolve safely (we only read the last
segment for routing; we never open a path).

This plumbing is **dual-purpose** — roadmap §2 (filename → title fallback)
needs exactly the same field, so the two land together.

---

## 6. HTML → text extraction

**Stripping is extraction fidelity, not security.** We strip tags so the
text we embed / index / feed to RAG is `Hello world`, not
`<html><body>Hello world…` — tags are noise in the index. It is *not* an
XSS defense. XSS is killed at the **render sink**, uniformly for every
stored string: the SPA renders content as text (React escapes), never as
raw HTML, so a `<script>` in a `.txt`, a `.md`, or text extracted from a
`.pdf` is inert. (The one place to stay alert is rendering stored content
*as markdown with raw-HTML enabled* — a renderer-config sink that would
apply to `.md` too; the fix lives there, not in this validator.)

Consequence: only `text/html` (the `.html`/`.htm` ending) runs the
stripper. Every other text subtype is stored **verbatim** — a `<p>` in a
`.txt` is literal content the user typed, not markup to strip.

`extract_text` (`text_extract.rs`) branches on the sniffed type. Add a
`text/html` arm:

```rust
"text/html" => extract_html(bytes),   // bounded read already applied
```

- `extract_html` strips tags to flat reading-order text via the
  **`html2text`** crate (the maintained, purpose-built choice; `scraper` is
  the lighter fallback) — **not** hand-rolled regex. Note this is *not*
  `ammonia`: `ammonia` (the Rust DOMPurify-equivalent, html5ever-based)
  produces *safe HTML*, which is the tool we'd reach for at a **render
  sink** if we ever displayed stored markup raw — see §6's caveat and §13.
  Our path extracts to plain text, so `html2text`.
- Bounds are inherited: the input is the same capped ranged-GET
  (`pdf_max_input_bytes`) the other arms use. HTML has no XXE / entity-
  expansion bomb class (that's XML); pathological nesting is bounded by
  the capped input. No network fetch, no resource loading.
- Output is a `Content { text, format: "text", extractor: "html2text" }`,
  same shape as the text/markdown arm. It does **not** go through the
  `Vec<Word>`/bbox `TextExtractor` trait — that's PDF-position-specific.

---

## 7. Config & policy

`ObjectPolicy.allowed_content_types` gains `text/html`. Default set
becomes `{application/pdf, text/plain, text/markdown, text/html}`. The
env override (`INGEST_OBJECT_ALLOWED_TYPES`, parsed in `uploads.rs`) is
unchanged in shape. No new env required for the dispatch itself.

---

## 8. Enforce-on (roadmap §0)

Independent of everything above and shippable first: in
`completion.rs:77`, replace the always-accept fallback with
`.map_err(CompletionError::ObjectRejected)?`. The handler arm
(`uploads.rs:555`, `handle_reject`) already wipes S3, records the
rejection, and returns 422 — no handler change. Un-ignore
`complete_with_validator_reject_records_rejection`
(`tests/ingestion_uploads.rs:308`).

---

## 9. Tests

- **Unit (per validator)** — colocated `#[cfg(test)]` in each file:
  - `pdf.rs`: real `%PDF-` accepted; truncated/empty rejected; oversize
    rejected without download; non-PDF bytes under a `.pdf` ending
    rejected.
  - `text.rs`: UTF-8 text accepted per subtype; disguised binary (PNG
    bytes under `.txt`) rejected; multi-byte char split at window boundary
    tolerated; an arbitrary text ending (`.py`) → `text/plain`.
  - `sniff.rs` (prober): PDF-by-magic routed; UTF-8 → text/plain; unknown
    binary → `NotInAllowlist`; unknown-ending HTML admitted as text/plain.
  - `format.rs`: extension derivation matrix (uppercase, multi-dot, no
    ext, path separators, absurd length); **reject-on-mismatch** (`.pdf`
    with text bytes → reject, not re-routed).
- **Integration** (`tests/ingestion_uploads.rs`): un-ignore the reject
  test; add an HTML happy-path; add "declared `.pdf`, bytes are HTML →
  rejected"; add "`.html` round-trips and extracts text."
- **Security matrix** — mirror the metadata-validator effort: feed every
  `ObjectReject` variant a triggering input and assert the reason code +
  that S3 is wiped + a rejection row is written.

---

## 10. Decisions

**Resolved (2026-05-24):**

- **Text breadth: liberal.** Any valid UTF-8 text is admitted as
  `text/plain` (source code, JSON, CSV, logs included). Only `.html`/`.htm`
  and `.md` get a distinct subtype; everything else text → `text/plain`.
- **Unknown ending: sniff-and-recover.** `%PDF-` magic → PDF; valid UTF-8
  → `text/plain`; else reject. (A no-extension PDF still works.)
- **Reject on mismatch for known endings.** A `.pdf` that isn't a PDF is
  rejected, not re-routed.
- **Stripping = fidelity, not security** (§6). All text validates
  identically; XSS is a render-sink concern.
- **PDF active-content scan is in the single-user stack** (§12) — a
  PDFiD-style lexical reject for embedded JS / launch / embedded files.
- **HTML→text crate: `html2text`** (§6); `ammonia` is reserved for the
  render-sink case (§13), not extraction.

**Still open:**

1. **PDF page-count enforcement.** The validator can't count pages from a
   4 KB window. Options: (a) leave `pdf_max_pages` reserved, rely on the
   size cap (simplest, current posture); (b) enforce in the PDF extractor
   (it already does a full bounded download) and turn an over-count into a
   *reject* — but that couples a non-fatal extraction step to a fatal
   gate. **Lean (a)** for this milestone.

---

## 11. Phasing

- **Phase 0 — enforce-on (tiny, independent).** Flip the always-accept
  fallback; un-ignore the reject test. Delivers roadmap §0 with the
  existing byte-authoritative logic. No structural change.
- **Phase 1 — dispatch structure + ending plumbing.** Restructure
  `object.rs` → `object/` module with the `FormatValidator` trait, Pdf /
  Text validators + the prober, and ending-based dispatch. Plumb `filename`
  through (schema + models + handler + SPA). Behaviour-preserving for the
  existing byte-types. Also unlocks roadmap §2 (title fallback).
- **Phase 1.5 — PDF active-content scan (single-user stack).** Add the
  PDFiD-style scan to `PdfValidator` + the `PdfActiveContent` reject
  variant + the bounded-download share (§12). One new behavioural change:
  active-content PDFs now reject. Also assert the SPA viewer disables PDF
  JS (§12).
- **Phase 2 — HTML.** Add the `text/html` dispatch entry, `extract_html`
  (`html2text`), and `text/html` to the allowlist. New parser, new tests.
- **Later (opt-in, SaaS) — ClamAV / CDR.** Not in the single-user stack;
  documented as upgrade paths in §14.

---

## 12. PDF active-content scan (single-user stack)

The one SOTA technique worth adopting even in the minimal single-user
deployment. Malicious PDFs are the classic upload payload: they embed
JavaScript, auto-run actions, launch commands, or carry embedded files.
The lightweight, canonical detector is **PDFiD** (Didier Stevens) — a
*lexical* scan for the structural keywords that signal active content. We
replicate it inline.

**Why it matters for us specifically.** The backend never executes the
PDF (our extractor is `pdftotext`, which doesn't run PDF JS), so the
backend isn't the victim. The exposure is the **download path**: we serve
the *original* object back to the browser via signed URLs for the in-app
viewer (`object-access.md`). A booby-trapped PDF therefore threatens
whoever opens it later. Tenancy bounds the blast radius (same-tenant
only), but in a multi-user tenant one user could poison another. Rejecting
active content at admission closes that.

**What it does.** A substring search over the bounded PDF bytes for:

```
/JavaScript   /JS          → embedded scripting
/OpenAction   /AA          → auto-run on open / additional actions
/Launch                    → launch external programs
/EmbeddedFile              → carried payloads
```

A hit → `ObjectReject::PdfActiveContent` (reason code `pdf_active_content`).

**Why it's cheap and safe.** It is a byte-substring scan, **not a PDF
parser** — it adds *no* new parser/CVE surface (unlike actually parsing
the PDF object graph). It is bounded by the same `pdf_max_input_bytes` cap
as everything else.

**Placement / download share (as built).** Unlike the `%PDF-` / size /
polyglot checks (sniff window only), this needs the *full* bounded bytes —
keywords live throughout the file, often in the trailer. So it is **not**
inside `PdfValidator`; it's a pipeline stage in `run_completion`:
stage **4b** reads the committed bytes back once (one ranged GET capped at
`pdf_max_input_bytes`), stage **4c** runs `scan_pdf_active_content` over
them when the resolved type is `application/pdf`, and stage **5**
(`extract_text`) reuses those same bytes — no second GET. Any PDF that
reaches 4b is already ≤ the size cap (the validator rejects larger), so the
read captures the whole file. Reject happens before commit; read-back
failure is fatal (we never commit a PDF we couldn't scan).

**Frontend belt-and-suspenders.** Even with the scan, assert the SPA's PDF
viewer renders with embedded JavaScript **disabled** (pdf.js disables it
by default — `isEvalSupported: false` / no `enableScripting`). Defense in
depth: the validator stops it at the door, the viewer wouldn't run it
anyway.

**Tunable.** A policy flag (`reject_pdf_active_content`, default **on**)
lets a single-user deployment that trusts its own PDFs turn it off. The
token list lives next to `PdfValidator`.

---

## 13. Standards alignment (OWASP / SOTA)

Our layer-1 design tracks the
[OWASP File Upload Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html)
controls almost verbatim:

| OWASP control | Delphi |
|---|---|
| Extension **allowlist**, never blocklist | ✅ allowlist (PDF + text family + HTML) |
| Don't trust the `Content-Type` header | ✅ declared MIME dropped; byte-authoritative |
| Verify **magic bytes** (but not alone) | ✅ `infer` sniff + format confirmation |
| **Extension ⇄ content must agree** | ✅ "ending selects, bytes confirm, reject on mismatch" (§3) |
| Random UUID storage filename | ✅ server-minted UUID keys; user filename only for dispatch + title |
| Size limits | ✅ size cap; archive formats excluded |
| Store outside the webroot | ✅ S3 object store, never app-served; originals via short-TTL signed URLs |
| Run CDR / AV "if applicable / available" | ⏭️ documented as upgrade paths (§14) |

**Library choices (Rust):**

- **`infer`** — magic-number file-type detection (in use). `tree_magic_mini`
  / `file-format` are richer (libmagic-style) alternatives we'd only need
  if the format set widened.
- **`html2text`** — HTML → plain-text extraction (Phase 2). Purpose-built;
  `scraper` is the lighter fallback.
- **`ammonia`** — Rust DOMPurify-equivalent HTML *sanitizer* (html5ever).
  **Not** used for extraction; it's the tool of record for the render-sink
  caveat (§6) if we ever display stored markup as HTML.

The deltas from full enterprise practice (e.g. OPSWAT MetaDefender:
30+ AV engines + CDR) are **antivirus** and **CDR** — deliberately out of
the single-user stack, documented next.

---

## 14. Upgrade paths: heavier validators (ClamAV, CDR)

These are **not** in the single-user stack. They are documented so the
escalation path is known when a deployment's threat model warrants it
(notably multi-tenant SaaS, where uploads from one user are served to
others). Both slot in **behind the existing seams** — the validator
pipeline and the `IngestSink` decorator chain (`ARCH.md`) — so adopting
them is additive, not a rewrite.

### 14.1 Antivirus — ClamAV

**What:** signature-based malware scanning (the free, self-hostable
standard; commercial equivalents are VirusTotal / OPSWAT multi-engine).

**Where it slots in:** a new validator stage *after* structural validation
and *before* commit, or — better for latency — an async `IngestSink`
decorator that quarantines the document until the scan returns. ClamAV runs
as its own daemon (`clamd`); the backend talks to it over its socket
(`clamav-client` crate, or a thin INSTREAM call). It is **I/O to a
sidecar**, never in-process — keep it behind a trait
(`trait MalwareScanner { async fn scan(&self, bytes) -> Verdict }`) with a
no-op default, mirroring `ClaimsExtractor` / `MetadataExtractor`.

**Cost / caveats:** a `clamav` service in the Tier-2 compose stack
(~1 GB signature DB, periodic `freshclam` updates); per-file scan latency;
"free" runs into real operational time. Off in single-user, opt-in via env
(`INGEST_AV_ENABLED`) for SaaS.

**Why deferred:** the backend never executes uploads, and same-tenant blast
radius is small in single-user. AV earns its keep when uploads are
redistributed across a trust boundary (multi-user tenants, public-ish
corpora).

### 14.2 Content Disarm & Reconstruction (CDR)

**What:** the heaviest tier (OPSWAT Deep CDR, Everfox). Assumes every file
is hostile and **rebuilds** it stripped of active content — flattens PDFs,
removes macros/scripts/embedded objects — rather than detecting malware.
Signature-less, so it covers zero-days and polymorphic payloads.

**Where it slots in:** a *transforming* stage, not a gate — it replaces the
stored object with a sanitized rebuild. It would live as an
`IngestSink`/transform between validation and commit, swapping
`storage_uri` to point at the reconstructed artefact (and keeping the
original quarantined or dropped). Our PDFiD-style scan (§12) is a *poor
man's CDR for the one case we care about* (PDF active content) — it
*rejects* rather than *rebuilds*.

**Cost / caveats:** no credible self-hosted OSS CDR engine — this is a
commercial dependency (per-file or per-seat licensing) or a managed API
(OPSWAT MetaDefender Cloud). It also alters the bytes the user gets back,
which can break fidelity (a flattened PDF loses forms, signatures). Known
drawbacks: latency, fidelity loss, and that it's not a silver bullet for
content-level (non-active) malware.

**Why deferred (likely indefinitely):** CDR targets organisations that
*redistribute* untrusted documents at scale. Delphi extracts text for RAG
and serves same-tenant originals — the §12 reject + AV (§14.1) cover the
realistic threat without a commercial dependency or fidelity loss. Revisit
only if a SaaS tenant's compliance regime mandates it.

### 14.3 Richer file-type detection

If the format set ever widens past "PDF + any UTF-8 text + HTML," swap
`infer` for `tree_magic_mini` or `file-format` (libmagic-style, hundreds of
types) behind the same `sniff.rs` seam. Adding a ZIP-container format
(DOCX/EPUB) additionally requires the decompression-bomb + zip-path-
traversal defenses called out in §2 — a separate, deliberate project.

---

## 15. Known limitations & gaps (as built)

Honest record of what this implementation does **not** do, so the gap
between "shipped" and "hardened" is explicit. None of these are blockers
for the single-user posture; several matter more under multi-tenant SaaS
(same logic as §14 — uploads served across a trust boundary).

### 15.1 Feature / technique gaps

1. **The PDFiD active-content scan is a naive substring match.** It finds
   literal `/JavaScript`, `/OpenAction`, … only. A determined attacker
   evades it via (a) **hex-encoded name tokens** (`/J#61vaScript` — legal
   PDF name syntax), (b) whitespace/comment splitting, or (c) payloads in
   **compressed object streams** (`/ObjStm`) our scan never decompresses.
   Real PDFiD/CDR normalize names and crack object streams. It catches
   lazy/unobfuscated active content, not a skilled adversary. **Cheapest
   upgrade:** hex-normalize `#xx` in name tokens before scanning (closes
   (a), the easiest evasion). Object-stream cracking is CDR territory.
2. **Polyglot detection is head-only.** We probe for PDF+ZIP signatures at
   offset 0. A polyglot whose second type lives in the **trailer/EOF** or
   at an embedded offset is not caught.
3. **`html2text` runs in-process, *not* sandboxed.** Memory-safe Rust, so
   no RCE class — but the residual **availability** risks (CPU/memory
   amplification, and a **stack overflow on deep nesting that aborts the
   whole process**, uncatchable) are bounded only by the input-size cap,
   not by a process boundary. Contrast `pdftotext` (bounded subprocess).
   Proportionate mitigation: `spawn_blocking` + output cap + recursion-depth
   guard. Full fix: subprocess + `setrlimit`/`seccomp` — the same upgrade
   as 15.4. Gated by deployment mode (single-user = self-DoS).
4. **`pdftotext` isolation is a *bounded subprocess*, not OS confinement.**
   It has timeout + `kill_on_drop` + capped stdout + stdin-only input, but
   **no `setrlimit`** (no hard memory/CPU ceiling), **no `seccomp`/Landlock**
   (poppler can make any syscall), and **no namespace/cgroup** isolation
   (runs as the backend user, shares its fs/network). Strong on
   crash/hang/output-bomb; absent on OS-level confinement. Upgrade:
   `setrlimit` (RLIMIT_AS/CPU/STACK) + seccomp on the spawn.
5. **PDF page-count is not enforced.** `pdf_max_pages` is a reserved knob;
   we rely on the byte-size cap (§10). Counting pages needs a full parse.
6. **No antivirus / CDR.** Deliberate; deployment-mode-gated upgrade paths
   in §14.

### 15.2 Test gaps (vs SOTA for untrusted-byte validators)

Example/table coverage of every decision branch and reject variant is in
place (§9). Missing, in rough priority order:

1. **Fuzzing** (`cargo-fuzz`/libFuzzer) of the byte boundary —
   `validate_uploaded_object`, `scan_pdf_active_content`, and the
   `html2text` path — asserting *never panics/hangs* and *never returns
   `Ok` with a non-allowlisted type*. The standard practice for parsers on
   hostile input; the highest-value addition.
2. **Property tests** (`proptest`) for the core invariant
   `Ok(v) ⟹ v.sniffed_content_type ∈ allowlist`, over arbitrary bytes +
   endings. (The repo already lists property tests as a planned guardrail
   in `testing.md`.)
3. **Real malicious-sample corpus** fixtures — a few benign + known-bad
   PDFs (PDFiD samples), a polyglot (corkami/mitra), an EICAR-style file.
4. **Differential testing** of the sniffer vs `libmagic`/Tika on a shared
   corpus.
5. **Browser e2e upload** (Playwright `@tier2`) — already an open
   `testing.md` TODO (`tests/e2e/upload-flow.spec.ts`).
