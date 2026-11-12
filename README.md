# PayScale HTTPE 3.0

The repository contains a FastAPI payment API, a PostgreSQL 16 schema and atomic transfer query, Redis idempotency/velocity Lua guard, shared-memory fraud checks, durable outbox/audit workers, an XState v5 React dashboard, and a local correctness verifier.

## Local correctness run

The API defaults to PostgreSQL. The verifier explicitly selects an in-memory SQLite adapter because Docker/PostgreSQL are not prerequisites for its repeatable local tests; SQLite is single-process and is not the production transaction store.

```powershell
.\myenv\Scripts\python.exe -m pip install -r backend\requirements.txt
.\myenv\Scripts\python.exe -m tests.verify_gates
```

To run the API without the Docker services for a local smoke test, explicitly opt in to SQLite with `$env:DATABASE_URL = "sqlite:///:memory:"` before starting Uvicorn.

The verifier starts the API itself, exercises concurrent idempotent and merchant payments, checks tombstone cancellation and ledger/balance behavior, and prints the eight-gate result.

## Chaos and concurrency run

The chaos suite targets an already-running API (`127.0.0.1:8000` by default; override with `HTTPE_BASE_URL`). For local validation, start it with SQLite and an ephemeral chaos-control token, then run the suite in another terminal:

```powershell
$env:DATABASE_URL = "sqlite:///:memory:"
$env:HTTPE_CHAOS_TOKEN = "<temporary-local-test-token>"
.\myenv\Scripts\python.exe -m uvicorn backend.api.main:app --host 0.0.0.0 --port 8000
```

```powershell
$env:HTTPE_CHAOS_TOKEN = "<same-temporary-local-test-token>"
.\myenv\Scripts\python.exe -m tests.chaos_and_load_suite
```

Chaos-control routes are registered only when `HTTPE_CHAOS_TOKEN` is configured and require that token in `X-Chaos-Token`; never enable them on a public deployment. The local suite exercises SQLite's unique-key/transaction path and concurrent API behavior. PostgreSQL deadlock behavior and Redis socket-failure recovery require those services to be configured and cannot be established by the SQLite verifier.

## PostgreSQL and Redis development services

With Docker Compose available:

```powershell
docker compose up -d
$env:DATABASE_URL = "postgresql://payscale:local-dev-only@localhost:5432/payscale"
$env:REDIS_URL = "redis://localhost:6379/0"
$env:AUDIT_HMAC_KEY = "<at-least-32-random-bytes>"
.\myenv\Scripts\python.exe -m uvicorn backend.api.main:app --host 0.0.0.0 --port 8000
```

The schema is applied automatically on a fresh PostgreSQL volume. Compose credentials are for local development only. PostgreSQL startup requires an externally managed audit HMAC key; keep it in a secret store and do not commit it.

The Compose stack is a local single-node setup. It does not by itself provide synchronous multi-node PostgreSQL replication, failover, or prove RPO=0, nor does the local verifier establish the 12,000 TPS/latency targets. Those require a replicated production deployment and load/failover testing.

## Dashboard

```powershell
Set-Location frontend
npm.cmd install
npm.cmd run dev
```

Vite proxies `/v2` HTTP and WebSocket traffic to `127.0.0.1:8000`.
