# AGENTS.md

Read `CONTEXT.md` before changing interfaces or persistence semantics.

Use the shared Lenso Cargo wrapper for local checks. Preserve the transactional
enqueue seam, lease fencing, stable event identity, and at-least-once contract.
Do not add Kernel dependencies, a shared schema, transport provisioning, or
automatic dead-letter replay.

Keep ordinary Rust modules below 600 lines when a cohesive responsibility seam
exists. Use Conventional Commits and stage only intended files.
