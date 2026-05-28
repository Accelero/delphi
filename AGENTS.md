# Agent Instructions

## Engineering Approach

- Prefer solutions that are correct for a scaling production system, not just
  the smallest patch that works locally today.
- Plan ahead for concurrency, failure modes, observability, testability,
  maintainability, and future feature growth.
- When a fast tactical fix and a durable architecture differ, call out the
  tradeoff and default to doing it right the first time unless the user
  explicitly asks for a short-term workaround.
- Keep changes scoped and pragmatic, but avoid knowingly introducing patterns
  that will need to be replaced as soon as the system scales.

## Communication

- Keep answers concise and short unless the user asks for more detail.
- End lengthy or detailed responses with:
  - `TL;DR` — one or two sentences summarizing the answer.
  - `Issues / Problems` — optional, for concrete blockers, risks, gaps, or
    concerns. If included and there are none, write `None known.`
- Keep the ending concise; do not duplicate the whole response.
- Do not add this footer to short confirmations, status notes, or simple
  one-step answers.
