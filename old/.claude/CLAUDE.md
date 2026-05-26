# Project Context

@../docs/SPEC.md
@../docs/ARCH.md
@../docs/architecture/testing.md

# Coding Rules

## Module Structure

A module is an opaque unit: a **public interface** plus **hidden internals**. The interface file (`mod.rs`, `__init__.py`, `index.ts`) defines the module — not the directory. Folders only group modules; everything below an interface file belongs to that module.

### Imports

1. **Within a module, import siblings freely.** Files inside the same module are allowed to be tightly coupled — that is the payoff for keeping things-that-change-together together.
2. **Across modules, import only from the public interface.** If a symbol is not re-exported from the interface file, it is not public and you do not use it. No reaching past the interface into another module's internals.
3. **Mark internals explicitly.** Leading underscore in Python (`_helpers.py`), `internal/` in Go, non-exported symbols in Rust/TS. Privacy must be visible at a glance.
4. **Do not import from your ancestors.** A module must not import from its parent or any module that contains it. That signals inverted abstraction — the "child" is actually the higher-level concept — and tends to cycle with the parent's re-exports. **Siblings of ancestors are explicitly allowed**: traversing up the folder tree to reach an aunt/uncle module (or great-aunt, etc.) via its public interface is fine and often unavoidable. The rule forbids only your own direct line of ancestors.
5. **No cycles, ever.** If A imports B and B imports A, one of them is wrong. Fix it by extracting a shared third module, or by inverting one direction via an interface defined by the consumer (dependency injection, events, callbacks).

### Design

6. **Dependency Inversion — depend on abstractions, not implementations.** High-level code defines the interface (trait, protocol, abstract class) it needs alongside itself; low-level modules implement it and are injected. High-level concepts never depend on low-level ones directly.
7. **Cross-feature imports are a code smell.** Before adding `orders/ → billing/`, ask whether `orders` should emit an event that `billing` reacts to instead. Direct imports are the tightest form of coupling — use them only when the relationship is genuinely synchronous and essential.

# Testing

Strategy lives in [`docs/architecture/testing.md`](../docs/architecture/testing.md), auto-imported above. Follow the placement rules in that doc when adding tests; do not duplicate them here.
