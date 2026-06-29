# Neon mode (opt-in): real storage/compute split + branching

By default ztap-oss runs a plain Postgres container. The **Neon overlay** swaps
that for **Neon OSS** (pageserver + safekeepers + a stateless compute), so:

- **Postgres pages live on object storage (MinIO)** — the same substrate as the
  Delta lakehouse. Real storage/compute separation, not a single local disk.
- **Branching is free** — a branch is a copy-on-write fork of a Neon *timeline*
  plus a throwaway compute. Instant, isolated, and **CDC-free**.

This closes the two gaps the base README flags (no storage/compute split, no
branching) with one move, and the forward CDC path is unchanged — Debezium tails
the Neon compute's logical replication exactly as it tailed plain Postgres.

## Run it

```bash
make neon-up          # = docker compose -f docker-compose.yml -f docker-compose.neon.yml up -d --build
make ps-neon          # status (both files)
```

Brings up pageserver, 3 safekeepers, storage_broker, and a compute (`compute`
alias, host port 55434). The control plane, sink, sync, proxy and reverse-watcher
all point at the Neon compute automatically. First run pulls ~9 GB of Neon images.

## Branching model (CDC on main only)

The lakehouse has no writable branches (Delta isn't Iceberg), and you don't want
them: **CDC runs on main, which stays the system of record. Branches are
ephemeral, compute-only dev/test/migration forks with no CDC tail.** When you're
done, drop the branch — main is untouched.

```bash
make branch NAME=dev            # create a CoW branch + throwaway compute (host port 55435)
# ... connect to localhost:55435 (cloud_admin/postgres), run migrations/tests ...
scripts/neon_branch.sh --drop dev   # throw it away
make branch-test                # automated proof: fork sees main's data, writes stay isolated
```

A branch forks main's data at the current LSN (copy-on-write — instant, no copy
cost), runs as its own compute, and its writes never touch main. Verified by
`scripts/neon_branch_test.sh`.

## What this is / isn't

- **Is:** genuine storage/compute separation (pages on S3) and genuine Neon
  branching (timeline forks + isolated computes).
- **Isn't (yet):** read-replica computes for horizontal read scaling (the
  pageserver supports it; this overlay runs a single primary compute), and
  control-plane-integrated "project = branch" provisioning. Both are natural
  next steps on this foundation.
- **Heavy:** ~9 GB of images and several Rust services — that's why it's opt-in,
  not the default single-`docker compose up`.
