# scrap-metal worker

A worker is a thin agent that polls the central server for chunks of
(ip, port) work, runs the scan with the server-supplied options, and
ships results back. Workers are stateless and self-configuring — they
need three env vars and outbound HTTPS to the server. Everything else
(CIDRs, ports, scan tunables) comes from the chunk.

This folder is a self-contained Go module with **zero external
dependencies**. Copy or `git clone` it onto any worker host and build
with the standard Go toolchain. No Docker required.

## Provisioning a new worker

On the server host:

```sh
docker compose exec server /app issue-key --worker eu-1 --label "EU residential"
```

This prints the bearer token **once**. Copy the `Token:` value into the
worker host's `.env` as `WORKER_API_KEY`. Lost tokens cannot be
recovered; mint a new one and revoke the old one with
`docker compose exec server /app worker revoke <id>`.

## Running a worker

On any Linux/macOS box with Go 1.26+:

```sh
git clone <this-repo> && cd <repo>/worker
go build -o scrap-metal-worker .

export SERVER_URL=https://hello.azlo.pro
export WORKER_API_KEY=worker-eu-1-AbCdEfGh...
export WORKER_ID=eu-1            # optional; defaults to hostname

./scrap-metal-worker
```

The worker loops:

1. Replays any spooled batches from a previous run (`./spool/` by default).
2. POSTs `/api/v1/claim` to ask for the next chunk. Empty response → sleep `WORKER_IDLE_POLL` (default 10s).
3. Scans the chunk with the server-supplied `dial_timeout`, `read_timeout`, `body_limit`, `scan_workers`, and `user_agent`.
4. POSTs results to `/api/v1/results` in batches and acks the chunk via `/api/v1/chunk-complete`.
5. Heartbeats to `/api/v1/heartbeat` every `WORKER_HEARTBEAT` (default 60s) so the dashboard's "last seen" stays fresh.

## Configuration

Every setting is available as both a CLI flag and an env var. Precedence
is **flag > env > default**, so a flag always wins. Run `./scrap-metal-worker -h`
for the full list.

| Flag | Env var | Default | Notes |
| --- | --- | --- | --- |
| `--server` | `SERVER_URL` | — | **Required.** e.g. `https://hello.azlo.pro` |
| `--api-key` | `WORKER_API_KEY` | — | **Required.** Bearer token from `issue-key` |
| `--id` | `WORKER_ID` | `hostname()` | |
| `--spool-dir` | `WORKER_SPOOL_DIR` | `./spool` | Offline result spool |
| `--heartbeat` | `WORKER_HEARTBEAT` | `60s` | 0 disables |
| `--idle-poll` | `WORKER_IDLE_POLL` | `10s` | Sleep between empty claims |
| `--spool-replay` | `WORKER_SPOOL_REPLAY` | `5m` | Periodic re-try of the spool dir; 0 = startup-only |
| `--compress` | `WORKER_COMPRESS` | `true` | gzip `/results` POST bodies |

CIDRs, ports, and scan tunables are **not** worker config — they belong
to the job, set on the server with `cmd/server job create`.

## Failure modes

- **401 Unauthorized**: the server rejected the token. The worker exits.
  Reissue and update `WORKER_API_KEY`.
- **5xx / network errors on results**: retried with exponential backoff;
  after ~5 attempts the batch is written to `WORKER_SPOOL_DIR` and
  replayed on next start.
- **Worker dies mid-chunk**: the chunk's lease expires server-side
  (default 15 min) and another worker picks it up.
- **Hard kill before flush**: in-memory results not yet POSTed are lost.

## Keeping in sync with the server

The wire types in [api.go](api.go) (`ingestRequest`, `claimResponse`,
`chunkCompleteRequest`, path constants) must match
`internal/api/types.go` in the server-side project. If a worker stops
working after a server upgrade, that's the first place to look.
