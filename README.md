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
- Optional Git-backed athlete score repo cache (`--score-repo` or `TRAILINTEL_SCORE_REPO`)
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

Forecast CLI command:

```bash
trailintel-forecast --help
```

## Usage

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

## Score Repo Cache

`~/.config/trailintel/config.toml` example:

```toml
[score_repo]
path = "/Users/mpaladin/trail-intel-score"
```

```bash
trailintel --participants-file ./participants.csv --score-repo /path/to/trail-intel-score
trailintel --participants-file ./participants.csv --score-repo /path/to/trail-intel-score --score-repo-read-only
```

When `--score-repo` (or `TRAILINTEL_SCORE_REPO`) points at a local checkout of
`trail-intel-score`, TrailIntel:

- reads athlete snapshots from the repo before making live provider requests
- refreshes missing or stale provider data lazily
- writes back encountered athletes, including below-threshold runners
- stores one JSON file per athlete under `athletes/<shard>/<athlete_id>.json`
- writes run summaries under `runs/<year>/`

## Notes

- UTMB endpoint is public and currently accessible.
- ITRA endpoint may return `403` depending on environment/WAF; the app keeps
  running and marks missing ITRA values as unavailable.
- Betrail catalog lookups use the public rankings API and page through results
  in 25-runner batches.
- When live provider lookup fails but a stale score-repo snapshot exists, that
  snapshot is reused and noted in the report.
- If repeated ITRA failures continue, rely on anonymous ITRA mode plus overrides.

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

Published Pages layout:

- race reports: `/reports/<slug>/<timestamp>/` and `/reports/<slug>/latest/`
- forecasts: `/forecasts/<slug>/<timestamp>/` and `/forecasts/<slug>/latest/`
- root landing page: links to separate race and forecast indexes
