# Source Reference

Source reference documentation is generated with the native toolchains and is
not committed to the repository.

## Rust

Build Rust source documentation:

```bash
make docs-rust
```

Open:

```text
target/doc/delphi_auth/index.html
target/doc/delphi_config/index.html
target/doc/delphi_contracts/index.html
target/doc/delphi_llm/index.html
target/doc/delphi_nats/index.html
target/doc/delphi_storage/index.html
target/doc/api_service/index.html
target/doc/realtime_service/index.html
target/doc/chat_worker/index.html
```

## Frontend

Build TypeScript/React source documentation:

```bash
make docs-frontend
```

Open:

```text
frontend/docs/index.html
```

## Everything

Build architecture docs plus source docs:

```bash
make docs-all
```

Generated outputs are ignored:

```text
doc/site/
target/doc/
frontend/docs/
```
