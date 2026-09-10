# Football ELT Pipeline

Scheduled Python pulls football data from [API-Football](https://www.api-football.com/)
(API-Sports), lands it as **raw JSON in BigQuery**, and dbt transforms it into an
analytics-ready star schema.

```
API-Football  ──►  elt/ (Python)  ──►  football_raw.raw_*  ──►  dbt  ──►  football_marts.*
                   • paces itself under the rate limit          (Phase 3, in progress)
                   • validates every response envelope
                   • lands raw JSON, parses nothing
                   • appends only, never mutates
                   • records every run in _ingestion_runs
```

The design goal is stated in one line: **an upstream API change should never be
able to break ingestion.** Python does no parsing at all. It writes each element
of the API's response into a `STRING` column verbatim and lets dbt interpret the
shape at query time. That is what makes this ELT rather than ETL, and it means a
renamed field costs a SQL edit instead of a re-fetch.

📄 **Design write-up:**
[*Designing for Day 40*](blog-post-1-extract-load.md) — part 1 walks through the
extract-and-load decisions and why each one went the way it did. *(Medium link to
follow.)*
📊 **Diagrams:** [`DATAFLOW.md`](DATAFLOW.md) — context diagram, level-1 DFD, and
the control-flow / retry flowcharts.

---

## Status

| Phase | Scope | State |
|---|---|---|
| 0 | GCP project, service account, BigQuery datasets | ✅ Done |
| 1 | Repo scaffold, config, Makefile, dbt project skeleton | ✅ Done |
| 2 | Ingestion: API client, BigQuery loader, extractors, run tracking | ✅ Done |
| 3 | dbt staging + marts (`dim_league`, `dim_team`, `fct_fixture`) | 🚧 Project scaffolded, **models not written yet** |
| 4 | Dockerfile, docker-compose, cron entry | ⬜ Not started |
| 5 | Unit tests against captured API fixtures | ⬜ Not started |

Everything currently runs on demand from the command line. `make daily` will run
`dbt build` after ingestion, but with no models in `dbt/football/models/` yet
that step is a no-op.

---

## Why the API client looks the way it does

Two behaviours of API-Football drove most of the design, both verified rather
than assumed:

**1. HTTP 200 does not mean success.** An expired or revoked key, a key with a
typo that's still the right length, a missing required parameter, or an unknown
parameter all come back as **`200 OK`** with an empty `response` and a populated
`errors` object:

```json
{"get": "fixtures", "parameters": [], "results": 0, "response": [],
 "errors": {"token": "Error/Missing application key. ..."}}
```

A client that trusts `resp.raise_for_status()` writes zero rows and reports
success. So `ApiFootballClient._validate()` treats a non-empty `errors` as the
real failure signal, before the caller ever touches `response`. `errors` is an
empty **list** on success and a populated **dict** on failure, so the check has
to be container-agnostic — plain truthiness.

*(The exception: no key header at all, or a key that isn't even the right shape,
gets a real `403` from an edge firewall before reaching the application. `get()`
catches that separately via its `status >= 400` branch.)*

**2. Rate limits are enforced by a firewall, not just a `429`.** Sustained
bursting can get a key blocked without warning, so the client paces itself
*before* each request rather than reacting to failures: a slot-wait gate at the
top of `get()`, exponential backoff **with jitter** capped at 5 attempts, and
never a retry on a 4xx auth error. Plan limits (Pro: 300/min, 7,500/day) are
confirmed via `GET /status`; the limiter is set to 250/min for headroom, and
every response's rate-limit headers are logged.

---

## The raw schema

Every `response[]` element becomes one row. All four raw tables share an
identical six-column schema; only the bytes inside `payload` differ.

| Column | Type | Purpose |
|---|---|---|
| `run_id` | STRING | uuid4 per pipeline invocation |
| `ingested_at` | TIMESTAMP | stamped once per invocation |
| `source_endpoint` | STRING | `leagues`, `teams`, `fixtures`, `standings` |
| `request_params` | STRING | the exact params that produced this row (JSON) |
| `payload` | STRING | one `response[]` element, JSON-encoded verbatim |
| `record_hash` | STRING | sha256 of `payload`, for cheap change detection |

Partitioned on `DATE(ingested_at)`. **Append-only** — the loader never issues an
`UPDATE` or `DELETE`, so re-running is always safe and deduplication is pushed
into dbt (`QUALIFY ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY ingested_at DESC) = 1`).

Tables: `raw_leagues`, `raw_teams`, `raw_fixtures`, `raw_standings`, plus
`_ingestion_runs` for run telemetry (written best-effort — a broken tracker logs
a warning and never takes down the pipeline).

---

## Setup

**Requirements:** Python 3.11+ (developed on 3.12.3), a GCP project with
BigQuery enabled, and an API-Sports key from
[dashboard.api-football.com](https://dashboard.api-football.com/).

```bash
git clone https://github.com/sayeedtahseen/football-elt-pipeline.git
cd football-elt-pipeline

python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

**Credentials.** Create a service account with `roles/bigquery.dataEditor` and
`roles/bigquery.jobUser`, download its JSON key to `secrets/sa.json` (gitignored):

```bash
mkdir -p secrets
gcloud iam service-accounts keys create ./secrets/sa.json \
  --iam-account=football-elt@<your-project>.iam.gserviceaccount.com
```

**Config.** Copy the template and fill it in:

```bash
cp .env.example .env
```

| Variable | Purpose |
|---|---|
| `API_FOOTBALL_KEY` | API-Sports key |
| `API_FOOTBALL_HOST` | `v3.football.api-sports.io` (already carries `/v3`) |
| `API_RATE_LIMIT_RPM` | client-side limiter, default `250` |
| `GCP_PROJECT` | your GCP project id |
| `BQ_LOCATION` | must be one location for every dataset — BigQuery cannot join across locations |
| `BQ_RAW_DATASET` | default `football_raw` |
| `GOOGLE_APPLICATION_CREDENTIALS` | path to the SA key; resolved to an absolute path at load |
| `LEAGUES` / `SEASONS` | comma-separated ids, e.g. `39,140` / `2024,2025` |
| `DAILY_LOOKBACK_DAYS` / `DAILY_LOOKAHEAD_DAYS` | fixtures window for `--mode daily` (3 / 7) |
| `DBT_PROFILES_DIR` | `./dbt/football` — dbt never auto-discovers `profiles.yml` in the project dir |

Config is validated at startup and reports **every** problem at once rather than
one failed run at a time. Verify without echoing secrets:

```bash
.venv/bin/python -c "from elt.config import load_settings; print(load_settings())"
```

The API key is declared `field(repr=False)`, so it won't appear in that output or
in a rendered traceback.

---

## Usage

Datasets and tables are created on first run, so no manual BigQuery setup is
needed.

```bash
# smallest possible real call — one endpoint, one season
.venv/bin/python -m elt.run --mode backfill --endpoints leagues --season 2025

# full backfill: every endpoint x every configured league x season
make backfill

# refresh: leagues/teams/standings in full + a rolling fixtures window
make ingest

# ingest, then dbt build (dbt is skipped if ingest exits non-zero)
make daily
```

CLI:

```
python -m elt.run --mode {daily,backfill}
                  [--endpoints {leagues,teams,fixtures,standings} ...]
                  [--season YEAR ...] [--league ID ...]
```

`--season` and `--league` are repeatable and override `SEASONS` / `LEAGUES` from
the environment. Exit code is non-zero if any endpoint fails, so cron surfaces it.

**Make targets:** `make help` lists them. `ingest`, `backfill`, `daily`, `dbt`,
`dbt-deps`, `dbt-debug`, `test`, `clean`.

---

## Verifying it works

```sql
-- rows landed, and the payloads are real
SELECT COUNT(*) AS rows, MIN(ingested_at) AS first_seen
FROM `<project>.football_raw.raw_leagues`;

SELECT JSON_VALUE(payload, '$.league.name') AS league
FROM `<project>.football_raw.raw_leagues` LIMIT 5;

-- one row per endpoint per run, with status and row counts
SELECT * FROM `<project>.football_raw._ingestion_runs`
ORDER BY started_at DESC LIMIT 20;
```

**The failure test that matters.** Run with a *well-formed but wrong* key (take
your real one and change the last character) and confirm the pipeline raises
`ApiFootballError` and exits non-zero, rather than silently writing nothing. A
junk string like `not-a-real-key` won't do — that gets a real `403` from the edge
firewall and never exercises the 200-with-`errors` path.

---

## Layout

```
elt/
  config.py         env-driven frozen Settings, validated at load
  errors.py         ELTError / ApiFootballError / QuotaExhaustedError / RetryableHTTPError
  api_football.py   HTTP client: rate limiter, retry, envelope validation, pagination
  bigquery.py       dataset/table ensure + append-only loads with an explicit schema
  extractors.py     one function per endpoint → rows for the raw schema
  state.py          _ingestion_runs tracking (best-effort)
  run.py            CLI entrypoint, task planning, per-task orchestration
dbt/football/       dbt project (profile + packages configured; models pending)
tests/              unit tests against captured fixtures (Phase 5)
```

Custom exceptions live in their own leaf module so every other module can raise
them with no circular-import risk, and `run.py` gets a single `except ELTError`
to separate expected failures from genuine bugs.

---

## Notes

- **Config is env-vars only.** No hardcoded project ids, dataset names, or
  credentials anywhere in `elt/`. That's what keeps an eventual move to Cloud Run
  a deployment change rather than a rewrite.
- **`profiles.yml` is committed** at `dbt/football/`, templated entirely with
  `env_var()` so it holds no secrets and travels with the repo. It requires
  `DBT_PROFILES_DIR` to be set — dbt does not look in the project directory.
- **All markdown is gitignored** except this file and `DATAFLOW.md`; planning docs
  stay local by design.
