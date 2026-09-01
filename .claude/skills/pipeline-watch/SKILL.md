---
name: pipeline-watch
description: Watch a budgetkey data pipeline through a deploy and verify the data it produced. Use after merging a pipeline change ("monitor the deployment", "tell me when it finished running"), when asked for a pipeline's processing status or logs, or to check what landed in a datapackage or DB table on next.obudget.org.
---

# Watching a pipeline through a deploy

Merging a pipeline change does not run it. The image is rebuilt and deployed,
the pipeline is re-registered with a new hash, and only then does the scheduler
pick it up. The whole cycle is typically 10-40 minutes.

## The trap: a "Succeeded" that is not your run

`/api/raw/<id>` reports the **last** run, not a run of your code. Immediately
after a merge it says `"message": "Succeeded", "dirty": false` — that is the
previous nightly run of the **old** code, and it will keep saying so until the
deploy lands. Never report it as "the deploy finished".

The discriminator is the flow hash, at
`pipeline.pipeline[<the step with "run": "flow">].parameters.__flow_hash`. It is
derived from the flow module's source, so it changes when — and only when — the
new code is live. `cache_hash` moves with it.

So a run is yours when **all three** hold:

1. `__flow_hash` differs from the value you recorded before the deploy,
2. `state` is terminal (not `RUNNING` / `QUEUED` / `INIT`),
3. `ended` is later than the `ended` you recorded.

**Capture that baseline before you start waiting.** If you arm the watch after
the deploy already landed, you are waiting for a change that has been and gone.
Before arming, check whether your change is already live — the cheapest way is
to query for something only the new code produces (see *Verifying the data*).

## Step 1 — find the pipeline id

It is the path under `datapackage_pipelines_budgetkey/pipelines/`, plus the
pipeline name in that directory's `pipeline-spec.yaml`:

```
datapackage_pipelines_budgetkey/pipelines/activities/social_services/measurements/
  pipeline-spec.yaml   ->   collect:
                              pipeline: ...

id: activities/social_services/measurements/collect
```

The API takes it unencoded, with no leading `./` (the `id` field in the JSON
response has one; the URL does not).

## Step 2 — record the baseline

```bash
curl -s "https://pipelines.obudget.org/api/raw/<id>" > baseline.json
python3 -c "
import json; d=json.load(open('baseline.json'))
f=[s for s in d['pipeline']['pipeline'] if s.get('run')=='flow'][0]
print('flow_hash', f['parameters']['__flow_hash'])
print('ended    ', d['ended'], d['message'], 'rows=', (d.get('stats') or {}).get('count_of_rows'))
"
```

## Step 3 — arm the watch

`watch.py` (next to this file) polls once a minute, emits a line on every state
change, and exits when a run of the new code reaches a terminal state — success
or failure, so silence never means "it worked".

```bash
python3 .claude/skills/pipeline-watch/watch.py <id> \
    --baseline-flow-hash <hash> --baseline-ended <epoch>
```

Run it through the **Monitor** tool with `persistent: true`, not in the
foreground — a deploy is far longer than a tool call, and Monitor delivers each
state change as it happens while you keep working:

```
Monitor({command: 'python3 -u .claude/skills/pipeline-watch/watch.py ...',
         description: '<pipeline> deploy + run status',
         persistent: true, timeout_ms: 3600000})
```

Omit the baseline flags and it captures the baseline on its first poll, which is
right only if you arm it before the deploy lands.

Do not also poll the API yourself while the monitor is armed.

## Step 4 — read the outcome

Status fields worth reporting: `state`, `message`, `success`, `dirty`,
`error_log` (a list; empty on success), `stats.count_of_rows`, and
`queued` / `started` / `ended` as unix epoch seconds.

A run of the new code can succeed and still not be the result you expected —
check `stats.count_of_rows` against the previous run before calling it good.

On failure, `reason` holds the tail of the run's output. The full log is at
`/api/log/<id>`, which returns **`{"text": [ ...lines... ]}`** — the key is
`text`, not `log`:

```bash
curl -s "https://pipelines.obudget.org/api/log/<id>" | python3 -c "
import json,sys
for l in json.load(sys.stdin)['text']:
    if any(k in l for k in ('Loaded','PROCESSED','Processed','WARNING','ERROR')): print(l)"
```

Airtable-backed pipelines log `Loaded N records for <base>/<table>` per table.
**Read those before blaming your code for a row-count change.** Airtable views
are edited by people outside this repo, so the source can shrink between two
runs — a count that moved because the `RESULTS` view changed looks exactly like
a count that moved because your filter broke, and only the log tells them apart.
Per-row `WARNING` lines from the flow appear here too.

## Verifying the data

Two independent places, and they can disagree if only one half of the flow ran.

**The datapackage** — from the `dump_to_path` argument in the flow, with
`/var/datapackages/` stripped:

```bash
# nginx index: confirms the file mtime is after your run (times here are UTC)
curl -s "https://next.obudget.org/datapackages/<path>/"
curl -s "https://next.obudget.org/datapackages/<path>/datapackage.json"   # count_of_rows, hash, schema
curl -s "https://next.obudget.org/datapackages/<path>/<resource>.csv" | head -3
```

**The database** — from the `dump_to_sql` argument. Query it live:

```bash
Q=$(python3 -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1]))" \
    "select count(1) as n from soproc_measurement")
curl -s "https://next.obudget.org/api/query?query=$Q"
```

Returns `{"rows": [...], "success": true}`, or `{"error": "..."}` — a
`psycopg2.errors.UndefinedColumn` is the clean signal that a new column has not
landed yet, which doubles as the "is my change live?" check from Step 0.

Worth checking after a scoring or schema change, beyond the row count:

- the new columns exist and are populated
- **value ranges**, which are the sharpest evidence a change is live: a column
  that should now span 0–4 still reporting `min=1, max=5` means the old code is
  running, whatever the status endpoint says
- **invariants that should hold across every row** — two columns that are meant
  to be NULL together, a ratio that must stay within 0–1. One query returning
  all zeros is worth more than eyeballing a CSV
- the effect the change was supposed to have, quantified. It may legitimately be
  **zero** on current data (a rule that fires only on shapes no row has yet);
  report that as a measured result, not as a failure

Take the "before" snapshot *before* the deploy lands — the same query, run
early, is what makes the after meaningful. If the source row count also moved,
say so: a before/after average across a changed row population is not a
like-for-like comparison, and presenting it as one is misleading.

## Reporting back

Say which run you are describing (the new flow hash, and when it ended), not
just that a run succeeded. If the data still looks like the old code, the deploy
has not landed — say that rather than reporting the stale figures.
