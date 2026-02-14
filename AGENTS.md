# Repository Guidelines

## Project Structure & Module Organization
This repository is a Rust workspace for a database management microservice system.

- `gateway/`: API gateway (`src/main.rs`, routing/proxy/handlers)
- `connection-service/`: connection lifecycle and pool management
- `query-service/`: SQL execution service
- `ai-service/`: AI-powered intelligent query service (Text2SQL, RAG, semantic understanding)
- `common/`: shared models, middleware, config, and utility code
- Root files: `Cargo.toml` (workspace), `Dockerfile`, `docker-compose.yml`, `start-all.bat`, `stop-all.bat`

Keep cross-service contracts (DTOs, response wrappers, shared errors) in `common/src/` to avoid duplication.

## Build, Test, and Development Commands
Use workspace-aware Cargo commands from the repository root:

- `cargo check --workspace`: fast compile check for all crates
- `cargo build --workspace`: build all services
- `cargo run -p gateway`: run a single service locally (replace with `connection-service` or `query-service`)
- `cargo test --workspace`: run unit tests across the workspace
- `docker compose up --build`: build and run all services with container networking

On Windows, `start-all.bat` launches service processes and `stop-all.bat` stops them.

## Coding Style & Naming Conventions
- Rust edition: 2021 (`Cargo.toml` workspace setting)
- Formatting: run `cargo fmt --all` before pushing
- Linting: run `cargo clippy --workspace --all-targets -- -D warnings`
- Naming: `snake_case` for files/modules/functions, `PascalCase` for structs/enums, `SCREAMING_SNAKE_CASE` for constants
- Keep handlers thin; place business logic in `service.rs`/`pool_manager.rs` and shared helpers in `common`

## Testing Guidelines
Current tests are primarily unit tests in `common` (for example `common/src/utils/sql_validator.rs`).

- Add `#[cfg(test)]` unit tests near core logic
- Name tests by behavior, e.g. `rejects_drop_statement()`
- Run `cargo test --workspace` before opening a PR

## Commit & Pull Request Guidelines
Recent history uses short, descriptive Chinese commit titles (for example `代码整理`, `结构分析`). Keep that style consistent and specific.

- Prefer one logical change per commit
- PRs should include: purpose, affected crates, test results, and config/API changes
- Link related issues and include sample request/response payloads when endpoints change
