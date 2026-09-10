# Data Flow — Extraction → BigQuery Load (Phase 2)

How a `python -m elt.run` invocation moves data from API-Football into the raw
BigQuery tables. Covers `elt/` only; the dbt transform layer is out of scope
here (see `PLAN.md` §Phase 3).

This document contains two kinds of diagram, deliberately kept separate:

- **Data-flow diagrams (DFDs)** — sections 1–2. *What data moves where.*
  Processes, data stores, external entities, labelled data flows. Flow labels
  carry a **step number** as a reading aid, but the diagrams still express no
  decisions or branching — for those, see the flowcharts.
- **Flowcharts** — sections 4–5. *In what order, with what branches.* Decision
  points, loops, sequencing — things a DFD cannot express but that matter here
  (failure propagation, the retry loop).

Section 3 is a data dictionary, not a diagram.

Notation (DFDs): rounded boxes = **processes** (functions/classes), square
boxes = **external entities**, cylinders = **data stores**, labelled arrows =
**data flows**.

---

## 1. DFD — Level 0 (context diagram)

```mermaid
flowchart LR
    ENV[".env / environment<br/>(external entity)"]
    API["API-Football / API-Sports<br/>v3.football.api-sports.io<br/>(external entity)"]
    CRON["host cron / operator<br/>(external entity)"]

    PIPE(("ELT pipeline<br/>python -m elt.run"))

    BQRAW[("BigQuery<br/>football_raw.raw_*")]
    BQRUNS[("BigQuery<br/>football_raw._ingestion_runs")]

    CRON -- "1  --mode {daily,backfill}" --> PIPE
    ENV -- "2  key, host, project, leagues, seasons" --> PIPE
    PIPE -- "3  run start / terminal rows<br/>(per task)" --> BQRUNS
    PIPE -- "4  GET /leagues,/teams,/fixtures,/standings<br/>(x-apisports-key, per task)" --> API
    API -- "5  JSON envelope {response:[...], errors, paging}" --> PIPE
    PIPE -- "6  append rows (WRITE_APPEND, per task)" --> BQRAW
    PIPE -- "7  exit 0 / 1" --> CRON
```

Steps 3–6 repeat once per planned task (endpoint × league × season).

---

## 2. DFD — Level 1 (pipeline internals)

Processes and the data flowing between them. Arrow labels are prefixed with a
**step number** giving the execution sequence; the data description follows.
Steps 6–17 run once per planned task (the `_execute` loop); steps 1–5 run once
per invocation.

```mermaid
flowchart TB
    subgraph EXT["External"]
        ENV[".env / environment"]
        API["API-Football (API-Sports)"]
    end

    subgraph RUN["elt.run"]
        MAIN(("main()"))
        PLAN(("_plan_tasks()"))
        EXEC(("_execute() loop"))
    end

    subgraph CFG["elt.config"]
        LOAD(("load_settings()"))
        VALID(("_validate()"))
    end

    subgraph CLIENT["elt.api_football.ApiFootballClient"]
        PAG(("paginate()"))
        GET(("get()  @retry"))
        WAIT(("_wait_for_slot()"))
        VALENV(("_validate(envelope)"))
    end

    subgraph XTR["elt.extractors"]
        EXFN(("extract_leagues / teams / fixtures / standings"))
        ROWS(("_rows_from()"))
    end

    subgraph LOAD_BQ["elt.bigquery.BigQueryRawLoader"]
        ENSD(("ensure_dataset()"))
        ENST(("ensure_table(name)"))
        APP(("append_rows(table, rows)"))
    end

    subgraph STATE["elt.state.IngestionRunTracker"]
        START(("start_run()"))
        FINISH(("finish_run()"))
        REC(("_record()  best-effort"))
    end

    RAWT[("football_raw.raw_leagues<br/>raw_teams / raw_fixtures / raw_standings")]
    RUNST[("football_raw._ingestion_runs")]

    %% config  (once per invocation)
    ENV -- "1  os.environ / dotenv" --> LOAD
    LOAD -- "1a  coerced values" --> VALID
    LOAD -- "2  Settings (frozen)" --> MAIN

    %% planning  (once per invocation)
    MAIN -- "3  args + settings" --> PLAN
    PLAN -- "4  list[(endpoint, kwargs)]" --> EXEC

    %% per-task orchestration  (loop: steps 5-17 per task)
    EXEC -- "5  run_id, endpoint, params" --> START
    EXEC -- "7  client, run_id, ingested_at, **params" --> EXFN

    %% tracking - start
    START -- "6  running row" --> REC

    %% extraction
    EXFN -- "8  endpoint, params" --> PAG
    PAG -- "9  endpoint, params(+page>=2)" --> GET
    GET -- "10  space to RPM/60" --> WAIT
    GET -- "11  x-apisports-key request" --> API
    API -- "12  HTTP status + JSON envelope" --> GET
    GET -- "12a  raise ApiFootballError on status >= 400<br/>(RetryableHTTPError on 429/499/5xx)" --> GET
    GET -- "13  envelope (2xx only)" --> VALENV
    VALENV -- "13a  raise ApiFootballError / QuotaExhaustedError<br/>on non-empty errors" --> GET
    GET -- "14  envelope['response']" --> PAG
    PAG -- "15  yield each element" --> EXFN
    EXFN -- "16  elements + metadata" --> ROWS
    ROWS -- "17  list[dict] (6-col raw schema)" --> EXEC

    %% load
    EXEC -- "18  table name, rows" --> APP
    APP -- "18a  (first call only)" --> ENSD
    APP -- "18b  (first call only)" --> ENST
    ENSD -- "CREATE SCHEMA IF NOT EXISTS" --> RAWT
    ENST -- "CREATE TABLE (partition DATE(ingested_at), cluster source_endpoint)" --> RAWT
    APP -- "19  load_table_from_json WRITE_APPEND + explicit schema" --> RAWT
    APP -- "20  rows_loaded (int)" --> EXEC

    %% tracking - finish
    EXEC -- "21  status=success, rows_loaded<br/>OR status=error, error_message" --> FINISH
    FINISH -- "22  terminal row" --> REC
    REC -- "insert_rows_json (ensure + swallow errors)" --> RUNST

    %% exit  (once per invocation)
    EXEC -- "23  failed endpoints[]" --> MAIN
```

| step | when | operation |
|---|---|---|
| 1–2 | once | load `.env` → validate → frozen `Settings` |
| 3–4 | once | `main()` → `_plan_tasks()` expands mode into the task list |
| 5–6 | per task | write the `running` row to `_ingestion_runs` |
| 7 | per task | `_execute` invokes the endpoint's extractor |
| 8–15 | per task | `paginate` → `get` (rate-limit wait, request, envelope `_validate`) → elements |
| 16–17 | per task | `_rows_from` wraps each element into the 6-column raw row |
| 18–20 | per task | `append_rows` (first call also ensures dataset + table), `WRITE_APPEND` load |
| 21–22 | per task | write the terminal (`success`/`error`) row |
| 23 | once | `_execute` returns failed endpoints → exit 0 / 1 |

---

## 3. Data dictionary — the row that gets written

Every `response[]` element becomes one row, identical schema across all four
`raw_*` tables:

| column | source | example |
|---|---|---|
| `run_id` | `uuid4().hex`, once per invocation | `a2a1813c94de47f7…` |
| `ingested_at` | `datetime.now(timezone.utc).isoformat()`, once per invocation | `2026-09-09T13:54:20…Z` |
| `source_endpoint` | the endpoint name | `fixtures` |
| `request_params` | `json.dumps(params, sort_keys=True)` | `{"league": 39, "season": 2025}` |
| `payload` | `json.dumps(element, sort_keys=True, separators=(",",":"))` | `{"fixture":{…},"teams":{…},"goals":{…}}` |
| `record_hash` | `sha256(payload)` | `e3b0c44298fc1c14…` |

Partitioned on `DATE(ingested_at)`, clustered on `source_endpoint`.

---

## 4. Flowchart — control flow / failure handling

Not a DFD: this has decisions and loops. It shows the order the `_execute` loop
runs things and how each failure class is handled.

```mermaid
flowchart TD
    S(["_execute: for each task"]) --> ST["start_run → running row"]
    ST --> TRY{"extract + append_rows"}
    TRY -- ok --> FS["finish_run → success row (rows_loaded)"] --> NEXT([next task])
    TRY -- "QuotaExhaustedError" --> FQ["finish_run → error row"] --> RAISE["re-raise → main returns 1<br/>(abort: never keep hitting a blocked key)"]
    TRY -- "ApiFootballError / other ELTError" --> FE["finish_run → error row<br/>log.error (no traceback)"] --> APPEND["failed.append(endpoint)"] --> NEXT
    TRY -- "unexpected Exception" --> FX["finish_run → error row<br/>log.exception (traceback)"] --> APPEND
    NEXT --> DONE{"more tasks?"}
    DONE -- yes --> ST
    DONE -- no --> RET["return failed[]"]
    RET --> EXIT{"failed empty?"}
    EXIT -- yes --> E0["exit 0"]
    EXIT -- no --> E1["exit 1 (cron surfaces it)"]
```

Key invariants:

- **`run_id` + `ingested_at` are stamped once** in `main()` and shared by every
  row of every task in that invocation — this is what links `raw_*` rows to
  their `_ingestion_runs` entry.
- **Append-only.** `append_rows` never updates or deletes. Re-running the same
  day appends duplicate payloads; `record_hash` makes them cheap to detect and
  dbt's `ROW_NUMBER() … ORDER BY ingested_at DESC` collapses them downstream.
- **Tracking is best-effort.** A failure inside `IngestionRunTracker` is logged
  and swallowed — telemetry never crashes the pipeline it observes.
- **The envelope hides the HTTP status.** API-Football returns `200` on most
  failures (wrong-but-well-formed key, quota exhaustion, missing/unknown
  params), so `_validate(envelope)` in the client is the real success/failure
  gate, not `resp.status_code`. The exception is a missing or malformed key
  header, rejected at the edge with a real `403` — which `get()` still catches
  via its `status >= 400` branch, so both cases surface as `ApiFootballError`.

---

## 5. Flowchart — rate limiting & retries (inside `get()`)

Not a DFD: the retry loop and status branching are control flow.

```mermaid
flowchart LR
    C(["get(endpoint, params)"]) --> W["_wait_for_slot()<br/>sleep until ≥ 60/RPM since last request"]
    W --> R["session.get(...)  timeout=30"]
    R --> SC{"status"}
    SC -- "429 / 499 / 5xx" --> RT["raise RetryableHTTPError"] --> TN["tenacity: backoff+jitter,<br/>≤5 attempts, then reraise"]
    TN --> W
    SC -- "other 4xx" --> AE["raise ApiFootballError (no retry)"]
    SC -- "2xx" --> J["resp.json()"] --> V["_validate(envelope)"]
    V -- "errors non-empty + quota wording" --> Q["raise QuotaExhaustedError"]
    V -- "errors non-empty" --> A2["raise ApiFootballError"]
    V -- "errors empty" --> OK["return envelope"]
```
