# Designing for Day 40: Building a Football ELT Pipeline That Survives a Changing API

*Part 1: extract and load — API-Football to BigQuery in Python. How the API breaks, why the "obvious" design choices didn't sit right with me, and what a raw layer actually buys you.*

<details>
<summary><sub>A note on how this was built (expand)</sub></summary>

<sub>This is a learning project, and I used Claude throughout: brainstorming the
architecture, pressure-testing design decisions, writing and reviewing code. A
lot of what follows started as a back-and-forth. I'd propose something, it'd
point out the failure mode I'd missed, I'd push back, and we'd land somewhere
better than where I started. Used well, an LLM has been one of the best ways
I've found to learn a domain quickly: not by having it hand you answers, but by
having something to argue with that knows where the bodies are buried. The
decisions here are ones I understand and can defend. I just didn't arrive at all
of them alone.</sub>

</details>

---

Most "build a data pipeline" tutorials follow the same arc. Call an API. Load the
response into a dataframe. `to_gbq()`. Add a cron line. Ship a screenshot of a
bar chart. Done.

They're all fine right up until the API misbehaves, and the API I picked
misbehaves in a way that makes the naive version *silently wrong* instead of
loudly broken. Your table just quietly stops growing, and you find out a week
later when someone asks why last Tuesday is missing.

This is the first of two posts about a small ELT pipeline I'm building: Python
pulls football data from [API-Football](https://www.api-football.com/), lands it
as raw JSON in BigQuery, and dbt turns it into an analytics-ready star schema —
meant to run unattended on a schedule (a couple of times a week is plenty for
football), though wiring up the schedule itself is a later phase. This post
covers **extract and load**: getting bytes out of the API and safely into the
warehouse. The dbt half comes next.

The full code is on GitHub:
[sayeedtahseen/football-elt-pipeline](https://github.com/sayeedtahseen/football-elt-pipeline).
If you'd rather see the whole extract-load path drawn out before reading the
prose — context diagram, level-1 DFD, the retry-loop flowchart — that's
[`DATAFLOW.md`](https://github.com/sayeedtahseen/football-elt-pipeline/blob/main/DATAFLOW.md)
(GitHub renders the diagrams).

There's rarely one right way to build something like this, so rather than
present conclusions, I'll show you the forks: the handful of points where the
obvious move didn't hold up, what was wrong with it, and where I went instead.
That's where most of the actual thinking went. Calling an API and writing to a
table is the easy part. The harder part is making it still be correct on day 40,
when the API returns something it didn't return on day 1.

**Who this is for:** you're comfortable with Python and SQL, you've maybe wired
up a cron job before, and you've read enough tutorials to be dangerous. I won't
assume you know what a slowly changing dimension is or why anyone separates
staging from marts — that's part 2.

---

## The actual problem

The goal: football data that can answer real questions (league tables, form over
time, head-to-head records), refreshed a few times a week, with no manual steps.

The first instinct is to skip the warehouse entirely: just have the dashboard
call the API on each page load. It's worth sitting with that idea for a second,
because seeing where it falls apart is the whole case for everything that
follows:

- **Rate limits.** Every viewer burns your quota.
- **Latency.** You're now as slow as the slowest API call, on every render.
- **No history.** The API tells you the standings *today*. It won't tell you
  what they were three Saturdays ago, because you didn't write it down.
- **No cross-cutting questions.** The API has the endpoints it has. "Which teams
  drop the most points from winning positions" is not one of them. If you own
  the data, it's a `GROUP BY`.

The last two points are the real case for a warehouse, and they're worth
saying out loud because tutorials tend to assume it. You're not caching the API.
You're building an asset the API doesn't have: **an accumulating, queryable
history.**

### Scope discipline

The first version does **fixtures, results, standings, leagues, and teams**.
Not player stats. Not expected-goals models. Not live in-match scores.

The temptation is to ingest everything, because the API offers everything. But
every extra endpoint is another parser, another set of tests, another shape
that can change upstream and break the next scheduled run. Start with the smallest set
that answers one real question (*what does the league table look like, and how
did we get here*), then extend once the skeleton has held up for a while.

---

## Decision 1: Which API, and the abstraction I didn't build

I weighed four options:

- **API-Football (API-Sports)** — *chosen.* The broadest coverage of the four:
  fixtures, standings, lineups, per-match events, player stats. The depth is the
  point; it means the pipeline has somewhere to grow.
- **football-data.org** — genuinely free and a clean REST design, but thin
  coverage and a 10 requests/minute ceiling.
- **StatsBomb open data** — free, event-level detail, no key needed. But it's
  static files, not an API: no scheduling story, and really a different project.
- **"Decide later"** — a pluggable client that defers the source choice and
  stays source-agnostic. *Rejected*, and that one's worth unpacking.

The last option is the interesting one. The instinct a lot of us have, me included
on a bad day, is to build a `FootballDataSource` interface with swappable
backends so we're "not locked in."

I talked myself out of it. I had exactly one source and only a vague idea of
what a second one would even look like, so any abstraction I designed now would
really just be a guess. And a guess that turns out wrong felt worse than no
abstraction at all: it adds indirection, hides the concrete behaviour that
matters, and *still* needs a rewrite when the real second source shows up and
doesn't fit the shape I imagined.

So I wrote it concretely against the API I actually have, and figured I'd
generalise on the second use, when there are two real things to find the common
shape of. The config-driven design (more on that below) already keeps the
*deployment* portable, and that felt like the lock-in actually worth worrying
about.

---

## Decision 2: HTTP 200 does not mean success

This is the one. If you take nothing else from this post, take this.

Let's look at the envelope. Every API-Football response comes back in the same
shape:

```json
{
  "get": "fixtures",
  "parameters": { "league": "39", "season": "2025" },
  "errors": [],
  "results": 380,
  "paging": { "current": 1, "total": 1 },
  "response": [ /* the actual data */ ]
}
```

On most failures (an expired or revoked key, a key with a typo in it that's still
the right length, a missing required parameter, an unknown parameter) the API
does **not** return a 4xx. It returns **200 OK**, with an empty `response`
and a populated `errors`:

```json
{
  "get": "fixtures",
  "parameters": [],
  "errors": { "token": "Error/Missing application key. Go to https://www.api-football.com/documentation-v3 to learn how to get your API application key." },
  "results": 0,
  "response": []
}
```

There's a narrow exception. If you send *no* key header at all, or a key that
isn't even the right shape (`not-a-real-key`), you get a real `403`: that's an
edge firewall rejecting you before the request reaches the application. But every
failure that gets *past* the firewall comes back as a `200`, and in production
that's all of them, because your key is well-formed and your code sends the
header. I found this out the annoying way while building the demo for this
section. A deliberately garbage key gave me a clean `403`, and it took a
well-formed-but-wrong key to reproduce what the pipeline actually has to defend
against.

There are two traps here, stacked on top of each other.

**Trap one: the status code is a lie.** The canonical Python pattern —

```python
resp = requests.get(url, headers=headers, params=params)
resp.raise_for_status()
data = resp.json()["response"]
```

— sails straight through. `raise_for_status()` sees a 200 and is happy. We get
an empty list, we write zero rows, and the pipeline logs a cheerful `success`.
Nothing is on fire. Nothing tells us anything is wrong. The table just stops
growing.

**Trap two: `errors` changes type.** On success it's an empty **list**. On
failure it's a **dict** keyed by what went wrong (`{"token": "..."}`,
`{"league": "..."}`). Any handling that assumes one shape breaks on the other:
`errors[0]` gets you a `KeyError` on the dict. What you want is a check that
doesn't care about the container at all, which means plain truthiness (or
`len(errors) > 0` — both work on `[]` and `{}` alike).

Here's the check the whole pipeline hinges on:

```python
def _validate(self, body) -> None:
    """API-Football returns HTTP 200 on most failures (wrong-but-well-formed
    key, quota, bad params). `errors` is an empty list on success and a
    populated dict on failure (a list shape is handled too, defensively).
    Reject a non-empty `errors` of either shape before the caller touches
    `response`. A missing/malformed key is the exception: a real 403 that
    `get` has already raised on before reaching here.
    """
    errors = body.get("errors")
    if not errors:           # [] and {} are both falsy — the one check that
        return               # works for both shapes

    if isinstance(errors, dict):
        message = "; ".join(f"{k}: {v}" for k, v in errors.items())
    elif isinstance(errors, list):
        message = "; ".join(str(item) for item in errors)
    else:
        message = str(errors)

    if self._looks_like_quota(message):
        raise QuotaExhaustedError(message)
    raise ApiFootballError(message)
```

`not errors` is doing the heavy lifting. Empty list, empty dict, `None`: all
falsy, all "fine." Anything non-empty is a failure regardless of type, and we
raise *before* the caller ever looks at `response`. There is no code path where
a failed call silently returns an empty result set.

Quota exhaustion gets its own exception type (`QuotaExhaustedError`) because it
needs different handling: one failed endpoint shouldn't stop the run, but
running *out of quota* should abort immediately rather than hammer the API with
calls that can't succeed. More on why that matters in the next section.

To see the difference, run the pipeline with a well-formed but wrong
`API_FOOTBALL_KEY` (take your real one and change the last character) under each
style of handling. Naive `raise_for_status()`: exit code 0, zero rows written, a
log line that says `success`. With `_validate` in place: an `ApiFootballError`
carrying the API's own `token` message, a non-zero exit, and (once this is on a
schedule) a cron failure you'll actually notice.

![The same broken-key run under each style of handling: the naive version exits
0 and reports success having written nothing; the pipeline raises and exits
non-zero.](images/key_validation.png)

### The same trap, a second time: pagination

The `paging` block in the envelope drives a small loop: request `page=1`, read
`paging.total`, keep going until you've pulled them all. Two guards earn their
keep: don't send the `page` param at all on page 1 (so the call is byte-identical
to a plain request), and refuse to iterate if `paging.total` comes back absurd:

```python
call_params = params if page == 1 else {**params, "page": page}
...
if total > _MAX_PAGES:
    raise ApiFootballError(
        f"{endpoint} reports {total} pages (> {_MAX_PAGES}): refusing to iterate"
    )
```

That first line is there because the trap sprang again. My first paginator added
`page=1` to *every* request. The endpoints that don't paginate (`/leagues`,
`/teams`, `/standings`) reject an unknown field by returning **200** with
`errors: {"page": "The Page field do not exist."}`. So a one-line bug in the
paginator didn't surface as an HTTP 400. It surfaced as my own `ApiFootballError`,
raised by the exact `_validate` above. The envelope hides the status in every
direction, and the fix was to only send `page` from page 2 onward.

---

## Decision 3: Rate limits are an availability problem, not an etiquette one

Rate limiting usually gets taught as "be polite, add a `sleep`." For this API
it's more serious than that.

I'm on the Pro plan: 300 requests/minute and 7,500/day, both confirmed by
calling `GET /status` rather than reading the pricing page. The limiter in the
client is set lower, at 250/minute, for headroom. Two reasons the headroom
matters:

1. A 429 costs you a round-trip and a retry. Pure waste.
2. **Sustained bursting past the limit can get your key firewall-blocked
   without warning.** Not throttled. Blocked. From the API's side, a client
   that keeps slamming a rate-limited endpoint in a tight loop is
   indistinguishable from an attack, and it gets treated like one.

A `while True: retry()` loop isn't just impolite here; it's how you lose access
to the API entirely. So the design leans defensive on both ends:

**Pace *before* the request, not after a failure.** A token-bucket-style
limiter spaces calls out to the target rate so we mostly never hit the limit
in the first place:

```python
def __init__(self, settings):
    self._min_interval = 60.0 / settings.api_rate_limit_rpm  # e.g. 0.24s
    self._last_request = 0.0

def _wait_for_slot(self) -> None:
    wait = self._min_interval - (time.monotonic() - self._last_request)
    if wait > 0:
        time.sleep(wait)
    self._last_request = time.monotonic()
```

`_wait_for_slot()` is called at the top of every request. It's not `sleep()`
calls scattered through the extract loop; it's one gate every call passes
through, so the pacing is impossible to forget.

**Back off with jitter, and cap the attempts.** Retries use exponential backoff
*with jitter* (so a burst of failures doesn't retry in lockstep), capped at 5
attempts. [tenacity](https://tenacity.readthedocs.io/) handles this:

```python
@retry(
    retry=retry_if_exception_type((RetryableHTTPError, requests.ConnectionError, requests.Timeout)),
    wait=wait_exponential_jitter(initial=1, max=30),
    stop=stop_after_attempt(5),
    reraise=True,
)
def get(self, endpoint: str, params: dict) -> dict:
    ...
```

**Never retry a 4xx auth error.** This is the important one. A 401/403 means
your key is bad, and retrying it changes nothing except making you look *more*
like an attacker. The client sorts failures into "retryable" (429, 499, 5xx,
connection errors) and "permanent" (everything else 4xx) at the point the
response comes back, and only the retryable ones are even eligible:

```python
status = resp.status_code
if status == 429 or status == 499 or status >= 500:
    raise RetryableHTTPError(status, f"{endpoint} -> HTTP {status}")
if status >= 400:
    raise ApiFootballError(f"{endpoint} -> HTTP {status} (not retried): {resp.text[:200]}")
```

**Watch the quota burn in the logs.** Every response carries four rate-limit
headers: `x-ratelimit-remaining` / `-limit` for the per-minute budget, and
`x-ratelimit-requests-remaining` / `-limit` for the daily one. Reading them costs
nothing, so the client logs all four on every call and escalates to a WARNING
once the daily allowance drops below 10%:

```python
log.debug("quota: daily %s/%s, minute %s/%s",
          daily_remaining, daily_limit, minute_remaining, minute_limit)

try:
    if int(daily_remaining) <= int(daily_limit) * _QUOTA_WARN_FRACTION:
        log.warning("daily quota low: %s of %s requests remaining",
                    daily_remaining, daily_limit)
except (TypeError, ValueError):
    pass  # header absent or not a number -- nothing to warn about
```

On a live call that prints `quota: daily 7498/7500, minute 298/300`, which is how
I confirmed the plan's real ceilings rather than trusting the pricing page. Note
the `try`/`except`: a missing or non-numeric header must never take down a
request that otherwise succeeded. Observability that can break the thing it
observes is worse than none, which is a theme I hit again further down.

---

## Decision 4: ELT, not ETL — Python lands raw JSON and does zero parsing

Here's the load design in one sentence: **Python never looks inside the
response. It writes the JSON to a string column, untouched. dbt does every bit
of parsing later.**

That "later" is the whole point. Extract, Load, *then* Transform: the `T`
happens in the warehouse, after the data is already safely stored. That's what
makes this ELT rather than ETL, and it's not a cosmetic distinction.

### Schema-on-write vs schema-on-read

The two approaches, concretely:

**Schema-on-write (classic ETL).** The loader parses the JSON and maps each
field to its own typed column: `league_id INT64`, `league_name STRING`,
`founded_year INT64`, and so on. Lovely to query. But the loader now *depends
on the shape of the response*. The day the API renames `founded` to
`founded_year`, or nests `venue` one level deeper, or adds a field your schema
doesn't know about, the load breaks or, worse, silently drops the new data.
And fixing it means re-fetching, because you never stored the original.

**Schema-on-read (what we're doing here).** One `payload` column, type `STRING`,
holds the entire response element as text. BigQuery never parses it. The
structure is interpreted at *query* time, in dbt, with `PARSE_JSON(payload)`. An
upstream schema change **cannot break ingestion, because ingestion never reads
the shape.** It just moves bytes.

Every raw table shares one identical six-column schema:

```
run_id           STRING     uuid4 per pipeline invocation; ties rows to the run that wrote them
ingested_at      TIMESTAMP  when we fetched it
source_endpoint  STRING     leagues / teams / fixtures / standings
request_params   STRING     the exact params that produced this row (JSON)
payload          STRING     one element of the API's response array, as a JSON string  <-- the whole point
record_hash      STRING     sha256 of payload, for cheap change detection
```

```python
RAW_SCHEMA = [
    bigquery.SchemaField("run_id",          "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("ingested_at",     "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("source_endpoint", "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("request_params",  "STRING"),
    bigquery.SchemaField("payload",         "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("record_hash",     "STRING",    mode="REQUIRED"),
]
```

One constant defines `raw_leagues`, `raw_teams`, `raw_fixtures`, and
`raw_standings`. Only the bytes inside `payload` differ. Tables are partitioned
on `DATE(ingested_at)`, so a query bounded by time only scans the days it needs,
which is what keeps the raw layer cheap to read as it grows.

They're also clustered on `source_endpoint`, and I'll be honest: that one does
nothing today. Because each endpoint gets its own table, every row in
`raw_leagues` has `source_endpoint = 'leagues'` — a clustering key with exactly
one distinct value prunes nothing. It's harmless, but it isn't doing the work the
partition is. It would start earning its keep only if I collapsed all four tables
into a single `raw_events` table, which is a real option I looked at and didn't
take: separate tables mean a schema change to one endpoint can never affect
another, and four small tables are easier to reason about than one wide one. If
that ever flips, the clustering is already there.

### What this actually buys us

- **API adds or renames a field → ingestion keeps working.** Only a SQL model
  needs a change, and the old raw data is still there to backfill the new
  column from.
- **Found a parsing bug? Re-run dbt, not the extract.** The fix runs over data
  we already have. No quota spent, no risk that the API no longer returns what
  it returned last month.
- **The raw table is an audit log.** Exactly what the API said, exactly when it
  said it. That turns out to matter more than I expected — see the surprise
  below.

### The counter-argument, stated fairly

We pay to store data we might never parse, and raw tables are genuinely
unpleasant to query directly — every column you want is buried in a JSON string.

Both true. But the numbers make the trade lopsided. In my warehouse `raw_leagues`
(every league in the world for the season) is 1.4 MB and a season of one league's
`raw_fixtures` is 0.4 MB; teams and standings are smaller still. The whole raw
layer is a couple of megabytes today. Football is a low-volume domain — a league
plays maybe ten matches a week — so even re-pulling everything a couple of times
a week, the table grows by single-digit megabytes per week. BigQuery active
storage runs $0.02 per GB per month; a year of accumulated history still rounds
to a rounding error. Re-fetching history you didn't store, on the other hand, is
often *impossible*: the API won't hand you back last week's live-updating
standings snapshot. Storage is cheap; lost history is permanent. That asymmetry
is what tipped me toward the boring-looking option.

### Three sub-decisions inside the raw layer

**1. One row per element of `response`, not one row per HTTP response.**

The API returns `"response": [ {...}, {...}, ... ]`. We could store the whole
array as one row. Instead we flatten one level at load time — each element
becomes its own row:

```python
for element in response:
    payload = json.dumps(element, sort_keys=True, separators=(",", ":"))
    rows.append({
        "run_id": run_id,
        "ingested_at": ingested_at,
        "source_endpoint": endpoint,
        "request_params": params_json,
        "payload": payload,
        "record_hash": hashlib.sha256(payload.encode()).hexdigest(),
    })
```

Note this doesn't *parse* anything — it just unwraps the outer list and
re-serializes each element deterministically (`sort_keys=True`, no whitespace)
so the same element always produces the same bytes and therefore the same
`record_hash`. The payoff: every downstream dbt model is a flat `SELECT`
instead of an `UNNEST` of an unbounded array.

**2. `payload` is `STRING`, not BigQuery's native `JSON` type.**

BigQuery has a real `JSON` column type, and it *looks* like the sophisticated
choice. But when I tried loading into it via `load_table_from_json` I kept
hitting sharp edges — it wants values that are already JSON-encoded strings and
fails in confusing ways when they're not. `STRING` + `PARSE_JSON()` at query
time reads the same for what we need and leaves far fewer ways to trip up, so
that's where I landed — the boring option, chosen on purpose rather than by
default.

**3. Append-only. Deduplication is dbt's job.**

The loader never runs an `UPDATE` or a `DELETE`. It only appends:

```python
job_config = bigquery.LoadJobConfig(
    schema=RAW_SCHEMA,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)
```

Run the same extract twice and we get two copies of every record, each with its
own `ingested_at`. That's fine. Staging models in dbt keep the latest version of
each record with a window function
(`QUALIFY ROW_NUMBER() OVER (PARTITION BY fixture_id ORDER BY ingested_at DESC) = 1`).

This isn't just insurance against an accidental double-run. The refresh mode
(`--mode daily` in the CLI, though I'll realistically run it twice a week)
*deliberately* re-fetches a rolling fixture window every time: three days back
and seven forward. A match played on Saturday gets its score, and then maybe a
post-match correction, over the days that follow. So the same fixture is
re-ingested three or four times on purpose, each version a little more final
than the last. Append-plus-dedupe isn't a workaround here; it's the mechanism
the refresh strategy is built on.

**Idempotency is something you design in, not a feature you bolt on.** The
loader is idempotent because it does the least possible work — appends, nothing
else — and pushes the "which version wins" decision to the layer that's good at
it. There's a concrete test for this, covered in part 2: run the pipeline twice,
watch raw rows grow and mart rows stay flat.

---

## Decision 5: Config comes from the environment, always

Every configurable value (project ID, dataset names, API host, rate limit, which
leagues and seasons to pull) comes from environment variables, loaded once into
a frozen dataclass:

```python
@dataclass(frozen=True)
class Settings:
    api_football_key: str = field(repr=False)   # never lands in logs or repr
    api_football_host: str = ""
    api_rate_limit_rpm: int = 250
    gcp_project: str = ""
    bq_location: str = "US"
    bq_raw_dataset: str = "football_raw"
    google_application_credentials: str = ""
    leagues: list[int] = field(default_factory=list)
    seasons: list[int] = field(default_factory=list)
    ...
```

Two things worth pointing out:

- **`field(repr=False)` on the key.** `print(settings)` and any traceback that
  renders the object will not contain the API key. You have to opt *in* to
  leaking a secret, not opt out.
- **`frozen=True`.** Config is read once at startup and can't be mutated
  mid-run. If something wants different behaviour it passes an argument, it
  doesn't reach in and change global state.

Config also gets *validated* at load time, raising one error that lists every
problem at once rather than surfacing them one failed run at a time:

```python
missing = [name for name, value in required.items() if not value]
if missing:
    errors.append(f"missing required config: {', '.join(missing)}")
...
if errors:
    raise ELTError("invalid configuration:\n  - " + "\n  - ".join(errors))
```

Why this matters beyond tidiness: **there is not a single hardcoded project ID,
dataset name, or credential anywhere in the code.** That's what keeps the
eventual move to Cloud Run a *deployment* change — new env vars, same image —
rather than a rewrite. The lock-in that actually costs you is infrastructure
lock-in, and env-var config is the cheap insurance against it.

*(One trap I hit: a relative `GOOGLE_APPLICATION_CREDENTIALS` path resolves
against each tool's working directory, and dbt's isn't the repo root. The config
loader now resolves it to an absolute path on load and writes it back to the
environment so every library sees the same thing.)*

---

## Decision 6: Observability that can't take down the thing it observes

There's a `_ingestion_runs` table that records every extractor call: start time,
finish time, status, rows loaded, error message. It exists so that once
this *is* running unattended, a failed run shows up as a row you can query
instead of something you reconstruct from container logs after someone notices
the data is stale.

The first version of it crashed the pipeline it was supposed to be watching.
`start_run()` fires *before* the extractor's try/except, and on a brand-new
project the telemetry table's dataset didn't exist yet, so the very first
`start_run()` threw a 404 and took down the whole run before a single row was
fetched.

The fix makes every telemetry write strictly best-effort:

```python
def _record(self, row: dict) -> None:
    """Best-effort: telemetry must never crash the pipeline, so every failure
    here is logged and swallowed — including a broken ensure_table / dataset."""
    try:
        self.ensure_table()
        errors = self.loader.client.insert_rows_json(self.table_ref, [row])
        if errors:
            log.warning("could not write %s row: %s", self.TABLE, errors)
    except Exception as exc:  # deliberately swallow all telemetry errors
        log.warning("could not write %s row: %s", self.TABLE, exc)
```

"Observability must never be load-bearing" is easy to agree with in the
abstract. It turned out to need actually enforcing: a bare `except Exception`
that's there on purpose, with a comment explaining why the usual "don't catch
everything" rule is being broken.

---

## The surprise that justified the whole design

I ran the `/leagues` backfill twice, a few minutes apart, to test that
append-only did what I thought. One call returns 998 leagues for the season, so
I expected `raw_leagues` to go to 1,996 rows and hold exactly 998 distinct
`record_hash` values. Nothing had changed upstream in five minutes.

The row count hit 1,996 as expected. But the distinct `record_hash` count came
back as **999**, not 998. One league's payload had changed between the two
calls. Some field inside it, a `seasons[].current` flag flipping or an upstream
coverage counter ticking over, was different.

No code did anything wrong. This is *exactly* what `record_hash` plus
append-only exist for. With a schema-on-write loader, that one mutated field is
either silently overwritten (history lost) or a merge conflict (3am page). Here
it's just two timestamped versions of the record sitting in the table, and the
staging layer's `ORDER BY ingested_at DESC` picks the current one.

The strongest argument for the raw layer turned out to be one I stumbled into
rather than one I planned for. The API isn't deterministic. Storing what it said,
verbatim and timestamped, means that's a non-event instead of a bug.

---

## Where part 1 lands

At this point the pipeline does exactly one thing, and does it safely:

```
API-Football  ──►  elt/ (Python)  ──►  football_raw.raw_*  (BigQuery)
                   • paces itself under the rate limit
                   • validates every envelope, 200-OK-on-failure included
                   • lands raw JSON, parses nothing
                   • appends only, never mutates
                   • records every run in _ingestion_runs
```

Four raw tables, one shared six-column schema, JSON payloads sitting in a
`STRING` column waiting to be interpreted. It's deliberately dumb. The dumbness
is the feature: there's almost nothing in the extract-load path that an upstream
change can break, because that path barely looks at the data.

None of this is big. One league-season is four API calls; a full backfill of a
handful of leagues across two seasons is a few dozen calls and a few thousand
rows, done in seconds under the limiter. The scheduled refresh is smaller still.
The engineering here isn't in the volume; it's in staying correct when the thing
on the other end of the wire misbehaves.

Everything so far runs on demand from the command line. Containerising it and
putting it behind cron is a later phase. The whole point of the env-var config
is to keep that a deployment change rather than a rewrite.

**Part 2** picks up from `raw_*` and covers the transform half: dbt sources with
freshness checks (how a silently-dead cron gets caught), staging models that do
nothing but parse-dedupe-rename, a `dim_league` / `dim_team` / `fct_fixture`
star schema, and the test I trust most: computing the league table from
`fct_fixture` and diffing it against a real one on the web. If the points column
matches, every layer is correct end to end.


