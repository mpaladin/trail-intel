# TrailIntel

TrailIntel is a CLI app to build a top-athlete report for a trail race by enriching participants
with:

- UTMB index (`api.utmb.world`)
- ITRA score (best-effort via `itra.run` search API; may be blocked by CloudFront)
- Betrail score (catalog match from `betrail.run`, normalized from a 100-scale)

It also includes a route-forecast pipeline that turns a GPX + start time into:

- a forecast PNG
- a browsable static HTML forecast report
- a GitHub issue driven publish workflow for GitHub Pages

## Features

- Input participants from:
  - `--race-url` (auto-detect JSON/CSV/HTML with optional CSS selector)
  - `--participants-file` (CSV, JSON, or TXT)
  - one or more `--participant "First Last"`
- Enrichment from UTMB, ITRA, and Betrail
- Two strategies:
  - `participant-first`: lookup each participant, then keep athletes above threshold
  - `catalog-first`: build high-score athlete catalogs, then match participants
- Name matching with exact + fuzzy scoring
- Accent-aware search (also tries de-accented query variants)
- Score threshold filtering (default strict `>680`)
  - Betrail uses the same threshold divided by 10 (`680` => `68.0`)
- Top-N report in terminal
- Optional export to CSV, JSON, or a static HTML report bundle
- Optional manual ITRA overrides file when live ITRA is blocked
- Optional authenticated ITRA requests via cookie (`--itra-cookie` or `ITRA_COOKIE`)
- Persistent DuckDB cache for participant-first athlete lookups
- Optional Git-backed athlete score repo cache (`--score-repo` or `TRAILINTEL_SCORE_REPO`)
- Separate `trailintel-score` CLI to seed and maintain the score repo
- Separate `trailintel-forecast` CLI for GPX weather reports
- Static forecast bundles with `index.html`, `forecast.png`, `snapshot.json`, and `route.gpx`

## Install

```bash
cd /Users/mpaladin/trailintel
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Primary CLI command:

```bash
trailintel --help
```

Score repo maintenance CLI:

```bash
trailintel-score --help
```

Forecast CLI command:

```bash
trailintel-forecast --help
```

## Usage

### Streamlit UI

```bash
cd /Users/mpaladin/trailintel
source .venv/bin/activate
streamlit run src/trailintel/streamlit_app.py
```

The UI supports:
- race URL / uploaded file / pasted participants
- `participant-first` and `catalog-first`
- same-name handling:
  - `highest`: keep only the top candidate per same-name group
  - `keep_all`: keep all same-name candidates (participant-first)
- strict threshold filtering (`> score`)
- persistent cache controls (enable/disable, force refresh)

### Forecast CLI

```bash
trailintel-forecast forecast \
  ./tests/fixtures/sample_route.gpx \
  --start 2026-07-15T06:30:00+02:00 \
  --duration 03:30 \
  --output ./dist/forecast.png
```

If the start timestamp is naive, pass a timezone explicitly:

```bash
trailintel-forecast forecast \
  ./route.gpx \
  --start 2026-07-15T06:30 \
  --timezone Europe/Rome \
  --duration 03:30 \
  --output ./dist/forecast.png
```

To export the browsable static bundle as well:

```bash
trailintel-forecast forecast \
  ./route.gpx \
  --start 2026-07-15T06:30 \
  --timezone Europe/Rome \
  --duration 03:30 \
  --output ./dist/forecast.png \
  --site-dir ./dist/forecast-site
```

Forecast bundles contain:

- `index.html`
- `forecast.png`
- `snapshot.json`
- `report-meta.json`
- `route.gpx`

The HTML forecast page includes:

- route summary cards
- the generated PNG chart
- per-sample weather rows across the route
- download links for PNG, GPX, and JSON

### 1) From race URL

```bash
trailintel \
  --race-name "My Trail Race 2026" \
  --race-url "https://example.com/participants" \
  --competition-name "Le 40 km" \
  --strategy participant-first \
  --score-threshold 700 \
  --top 20
```

If the URL is HTML and auto-detection is noisy, pass a selector:

```bash
trailintel --race-url "https://example.com/participants" --name-selector ".runner-name"
```

For multi-distance event pages (for example Yaka Inscription), pass
`--competition-name` to select a single distance.

### 2) From file

```bash
trailintel --participants-file ./participants.csv --strategy participant-first --score-threshold 700 --top 30
```

Supported file formats:

- CSV: needs a name-like column (`name`, `fullname`, `athlete`, `runner`)
- JSON:
  - list of strings
  - list of objects with a name-like key
  - object with `participants`, `runners`, or `athletes`
- TXT: one participant name per line

### 3) From direct names

```bash
trailintel --participant "Kilian Jornet" --participant "Jim Walmsley"
```

### 4) Catalog-first workflow (your strategy #2)

```bash
trailintel \
  --participants-file ./participants.csv \
  --strategy catalog-first \
  --score-threshold 700 \
  --catalog-min-match-score 0.85 \
  --top 50
```

In this mode:

- UTMB catalog is fetched from the public paginated endpoint and filtered to `> threshold`
- ITRA catalog is fetched from public ranking payload on `itra.run/Runners/Ranking`
- Betrail catalog is fetched from `betrail.run/api/score/full/level/<offset>/scratch/ALL/ALL`
- Participants are matched against those high-score catalogs
- Fuzzy catalog matching is stricter by default; tune with `--catalog-min-match-score`

### ITRA fallback overrides

When ITRA live lookup is blocked, pass your own mapping file:

```bash
trailintel --participants-file ./participants.csv --itra-overrides ./itra_scores.csv
```

`itra_scores.csv` example:

```csv
name,itra_score
Kilian Jornet,905
Jim Walmsley,930
```

## Output options

```bash
trailintel --participants-file ./participants.csv --output report.csv
trailintel --participants-file ./participants.csv --output report.json --sort-by combined
trailintel --participants-file ./participants.csv --sort-by betrail
trailintel --participants-file ./participants.csv --site-dir ./dist/report-site
trailintel --participants-file ./participants.csv --score-repo /path/to/trail-intel-score
```

`--site-dir` writes a static bundle containing:

- `index.html`
- `report.csv`
- `report.json`
- `snapshot.json`
- `report-meta.json`

The HTML page mirrors the main report view with:

- summary metrics
- top-athlete table with clickable UTMB/ITRA/Betrail links
- score distribution charts for UTMB, ITRA, and Betrail
- “no result on any provider” section
- CSV/JSON download links

## Cache options

By default, `participant-first` lookup results are cached in DuckDB:

- success TTL: 60 days
- miss TTL: 7 days
- default DB path resolution order:
  1. `TRAILINTEL_CACHE_DB`
  2. config file `TRAILINTEL_CONFIG_FILE` (or `~/.config/trailintel/config.toml`) key `[cache].db_path`
  3. fallback `~/.cache/trailintel/trailintel_cache.duckdb`

`~/.config/trailintel/config.toml` example:

```toml
[cache]
db_path = "/Users/mpaladin/.cache/trailintel/trailintel_cache.duckdb"

[score_repo]
path = "/Users/mpaladin/trail-intel-score"
```

```bash
trailintel --participants-file ./participants.csv --no-cache
trailintel --participants-file ./participants.csv --refresh-cache
trailintel --participants-file ./participants.csv --cache-db /tmp/trailintel_cache.duckdb
trailintel --participants-file ./participants.csv --score-repo /path/to/trail-intel-score
trailintel --participants-file ./participants.csv --score-repo /path/to/trail-intel-score --score-repo-read-only
```

## Score Repo Cache

When `--score-repo` (or `TRAILINTEL_SCORE_REPO`) points at a local checkout of
`trail-intel-score`, TrailIntel:

- reads athlete snapshots from the repo before making live provider requests
- refreshes missing or stale provider data lazily
- writes back encountered athletes, including below-threshold runners
- stores one JSON file per athlete under `athletes/<shard>/<athlete_id>.json`
- writes run summaries under `runs/<year>/`

Seed the repo from Betrail athletes above `68.0` and optionally fill UTMB/ITRA:

```bash
trailintel-score seed-betrail \
  --repo /path/to/trail-intel-score \
  --threshold 68 \
  --fill-utmb \
  --fill-itra
```

Import matched athlete entries from an existing TrailIntel DuckDB cache:

```bash
trailintel-score import-duckdb \
  --repo /path/to/trail-intel-score \
  --cache-db /path/to/trailintel_cache.duckdb
```

## Optional ITRA cookie

If you have a valid ITRA session and want the app to try authenticated requests:

```bash
export ITRA_COOKIE='cookie1=value1; cookie2=value2'
trailintel --participants-file ./participants.csv --strategy participant-first
```

or:

```bash
trailintel --participants-file ./participants.csv --itra-cookie 'cookie1=value1; cookie2=value2'
```

If cookie-authenticated ITRA lookup fails (for example stale/invalid cookie or
CloudFront blocking), the app now retries the lookup anonymously for the same
run and keeps your persisted cookie unchanged.

## Notes

- UTMB endpoint is public and currently accessible.
- ITRA endpoint may return `403` depending on environment/WAF; the app keeps
  running and marks missing ITRA values as unavailable.
- Betrail catalog lookups use the public rankings API and page through results
  in 25-runner batches.
- When ITRA live lookup fails but stale cache exists, stale cached candidates
  are used and noted in the report.
- If repeated ITRA failures continue, refresh/replace your cookie or clear it
  and rely on anonymous ITRA mode plus overrides/cache.

## GitHub Actions + Pages

The repo now includes a serverless publishing path:

- `.github/ISSUE_TEMPLATE/generate-race-report.yml`
- `.github/workflows/generate-race-report.yml`
- `.github/ISSUE_TEMPLATE/generate-forecast-report.yml`
- `.github/workflows/generate-forecast-report.yml`

Recommended setup:

1. Keep this repository private as the control repo.
2. Create a separate public repository for GitHub Pages output.
3. Set these repository variables in the control repo:
   - `ALLOWED_REQUESTERS`: comma-separated GitHub logins allowed to request runs
   - `PAGES_REPO`: `owner/public-pages-repo`
   - `PAGES_BRANCH`: optional, defaults to `main`
   - `PAGES_BASE_URL`: optional, defaults to `https://owner.github.io/repo`
   - `SCORE_REPO`: optional, defaults to `mpaladin/trail-intel-score`
   - `SCORE_REPO_BRANCH`: optional, defaults to `main`
4. Set this repository secret in the control repo:
   - `PAGES_REPO_TOKEN`: token with push access to the public Pages repo
   - `SCORE_REPO_TOKEN`: optional token with push access to the score repo
     (falls back to `PAGES_REPO_TOKEN` if that token can access both repos)
Request flow:

1. Open the `Generate race report` issue form from your phone or browser.
2. Fill in race name, race URL, optional competition, threshold, top rows, and strategy.
3. The workflow validates the requester, runs the CLI, uploads the artifact bundle, publishes it to the public Pages repo, comments with the published links, and closes the issue.

During the same run, the workflow also clones the score repo checkout into
`$GITHUB_WORKSPACE/score-repo`, passes it to `trailintel --score-repo`, and
commits/pushes refreshed athlete snapshots when the report adds or updates cache entries.

Forecast request flow:

1. Open the `Generate forecast report` issue form.
2. Fill in route name, start date, start time, timezone, duration, and either:
   - a direct GPX/ZIP URL in `GPX URL`, or
   - one ZIP attachment containing exactly one GPX in `Notes`
3. The workflow downloads the GPX, runs `trailintel-forecast`, uploads the artifact bundle, publishes it to the public Pages repo, comments with the published links, and closes the issue.

The hosted workflow uses anonymous/public Betrail access and optionally
authenticated ITRA access when `ITRA_COOKIE` is configured.

To prepopulate `trail-intel-score` from Betrail rankings, use the manual
`Seed Score Repo` workflow on GitHub. It runs:

```bash
trailintel-score seed-betrail --threshold 68 --fill-utmb --fill-itra
```

against the configured score repo checkout and commits the refreshed athlete
snapshots back to `trail-intel-score`.

Required secrets for score-repo seeding in Actions:

- `SCORE_REPO_TOKEN` or `PAGES_REPO_TOKEN`: required for cloning/pushing the score repo
- `ITRA_COOKIE`: optional, only needed if you want authenticated ITRA enrichment during the seed run

## Backfill Published Pages

To refresh an existing `trail-intel-pages` worktree so historical reports gain
Betrail score/link columns:

```bash
trailintel-backfill-pages --pages-root /path/to/trail-intel-pages
```

Published Pages layout:

- race reports: `/reports/<slug>/<timestamp>/` and `/reports/<slug>/latest/`
- forecasts: `/forecasts/<slug>/<timestamp>/` and `/forecasts/<slug>/latest/`
- root landing page: links to separate race and forecast indexes

For hosted runs, the workflow also reuses the same DuckDB lookup cache across Actions runs:

- path inside the workflow: `.workflow-cache/trailintel_cache.duckdb`
- persisted between runs via GitHub Actions cache restore/save
- success entries are refreshed after 60 days
- miss entries are refreshed after 7 days
