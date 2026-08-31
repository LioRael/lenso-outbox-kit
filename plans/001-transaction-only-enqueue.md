# Plan 001: Make Outbox enqueue require an active PostgreSQL transaction

> Drift check: `git diff --stat e507516..HEAD -- src tests README.md Cargo.toml`.

## Status

- **Priority**: P2
- **Effort**: M
- **Risk**: MED
- **Depends on**: none
- **Category**: bug
- **Planned at**: commit `e507516`, 2026-08-30

## Why this matters

The public API promises transactional outbox atomicity but accepts `&mut PgConnection`,
so a plain autocommit pool connection compiles and can persist an event after the
business mutation rolls back.

## Current state

- `src/store.rs:30-38` documents an active transaction but types the argument as a connection.
- `README.md:39-59` relies on callers remembering `transaction.as_mut()`.

## Scope

In scope: public enqueue API, repo tests/examples/docs, and compile-time API coverage.
Out of scope: dispatcher semantics, event schema, or exactly-once guarantees.

## Steps

1. Change the API to accept `&mut Transaction<'_, Postgres>` or an unforgeable wrapper
   constructible only from that type; execute SQL through the transaction internally.
2. Update repository callsites and examples. Search the framework workspace and report
   external compile callsites, but do not edit other repositories in this worktree.
3. Add rollback acceptance proving both business row and event disappear, plus a
   compile-fail/doc-test boundary proving a raw pool connection is rejected.

## Verification

- `cargo test --workspace --all-targets` from this repo -> all pass.
- `cargo check --workspace --all-targets` -> exit 0.
- `git diff --check` -> no output.

## STOP conditions

Stop if SQLx's transaction type cannot express the API without unsafe code; prefer a
small safe wrapper rather than returning to documentation-only enforcement.
