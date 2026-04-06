# TrailIntel

TrailIntel is a CLI app to build a top-athlete report for a trail race by enriching participants
with:

- UTMB index (`api.utmb.world`)
- ITRA score (best-effort via `itra.run` search API; may be blocked by CloudFront)

## Features

- Input participants from:
  - `--race-url` (auto-detect JSON/CSV/HTML with optional CSS selector)
  - `--participants-file` (CSV, JSON, or TXT)
  - one or more `--participant "First Last"`
- Enrichment from UTMB and ITRA
- Two strategies:
  - `participant-first`: lookup each participant, then keep athletes above threshold
  - `catalog-first`: build high-score athlete catalogs, then match participants
- Name matching with exact + fuzzy scoring
- Accent-aware search (also tries de-accented query variants)
- Score threshold filtering (default strict `>680`)
- Top-N report in terminal
- Optional export to CSV, JSON, or a static HTML report bundle
- Optional manual ITRA overrides file when live ITRA is blocked
- Optional authenticated ITRA requests via cookie (`--itra-cookie` or `ITRA_COOKIE`)
- Persistent DuckDB cache for participant-first athlete lookups

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
trailintel --participants-file ./participants.csv --site-dir ./dist/report-site
```

`--site-dir` writes a static bundle containing:

- `index.html`
- `report.csv`
- `report.json`
- `snapshot.json`
- `report-meta.json`

The HTML page mirrors the main report view with:

- summary metrics
- top-athlete table with clickable UTMB/ITRA links
- score distribution charts
- “no result on both providers” section
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
```

```bash
trailintel --participants-file ./participants.csv --no-cache
trailintel --participants-file ./participants.csv --refresh-cache
trailintel --participants-file ./participants.csv --cache-db /tmp/trailintel_cache.duckdb
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
- When ITRA live lookup fails but stale cache exists, stale cached candidates
  are used and noted in the report.
- If repeated ITRA failures continue, refresh/replace your cookie or clear it
  and rely on anonymous ITRA mode plus overrides/cache.

## GitHub Actions + Pages

The repo now includes a serverless publishing path:

- `.github/ISSUE_TEMPLATE/generate-race-report.yml`
- `.github/workflows/generate-race-report.yml`

Recommended setup:

1. Keep this repository private as the control repo.
2. Create a separate public repository for GitHub Pages output.
3. Set these repository variables in the control repo:
   - `ALLOWED_REQUESTERS`: comma-separated GitHub logins allowed to request runs
   - `PAGES_REPO`: `owner/public-pages-repo`
   - `PAGES_BRANCH`: optional, defaults to `main`
   - `PAGES_BASE_URL`: optional, defaults to `https://owner.github.io/repo`
4. Set this repository secret in the control repo:
   - `PAGES_REPO_TOKEN`: token with push access to the public Pages repo

Request flow:

1. Open the `Generate race report` issue form from your phone or browser.
2. Fill in race name, race URL, optional competition, threshold, top rows, and strategy.
3. The workflow validates the requester, runs the CLI, uploads the artifact bundle, publishes it to the public Pages repo, comments with the published links, and closes the issue.

The hosted workflow uses anonymous/public ITRA access only. It does not require an ITRA cookie.

For hosted runs, the workflow also reuses the same DuckDB lookup cache across Actions runs:

- path inside the workflow: `.workflow-cache/trailintel_cache.duckdb`
- persisted between runs via GitHub Actions cache restore/save
- success entries are refreshed after 60 days
- miss entries are refreshed after 7 days
