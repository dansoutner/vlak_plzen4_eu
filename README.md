# Jizdni Rady Tools

Tools for generating Plzeň-Doubravka <-> Plzeň hl.n. timetable HTML from official Czech rail GTFS and attaching live delay status from [Babitron](https://babitron.kam.mff.cuni.cz/).

## What is in this repo

- `get_direct_connection_cli.py`: builds timetable data from GTFS and renders HTML.
- `doubravka_hlavak.sh`: regenerates both timetable pages from the official rail GTFS.
- `train_delays.py`: Flask endpoint `/train_delays/` that scrapes live delay tables from Babitron (deployed on PythonAnywhere).
- `scripts/download_and_convert_official_gtfs.py`: downloads official rail XML and converts it to GTFS.
- `czptt2gtfs/`: CZPTT XML -> GTFS converter used by the download script.
- `doubravka_hlavak.html`, `hlavak_doubravka.html`: generated timetable pages.
- `index.html`: landing page.
- `docs/train_delays_response.md`: delay API response contract.
- `tests/`: parser and matching tests with HTML fixtures.

## Requirements

- Python `>=3.10`
- Dependencies from `pyproject.toml`

## Setup

```bash
python3 -m venv venv
./venv/bin/pip install -e .
```

Or install dependencies directly:

```bash
./venv/bin/pip install beautifulsoup4 fake-headers flask flask-caching html5lib jinja2 pandas python-dotenv requests tqdm
```

## Environment configuration

Create local configuration from the template:

```bash
cp .env.example .env
```

Set `TIMETABLE_DELAYS_ENDPOINT` in `.env` to your deployed delay API. Current production value:

```bash
TIMETABLE_DELAYS_ENDPOINT=https://danielsoutner.pythonanywhere.com/train_delays
```

Supported variables:

- `TIMETABLE_DELAYS_ENDPOINT`
- `TRAIN_DELAYS_SOURCE_R_URL` (default `https://babitron.kam.mff.cuni.cz/zponline.html`, long-distance trains)
- `TRAIN_DELAYS_SOURCE_OS_URL` (default `https://babitron.kam.mff.cuni.cz/zponlineos.html`, regional trains)
- `TRAIN_DELAYS_CACHE_TIMEOUT_SECONDS`
- `TRAIN_DELAYS_CORS_ALLOW_ORIGIN`
- `TRAIN_DELAYS_CORS_ALLOW_METHODS`
- `TRAIN_DELAYS_CORS_ALLOW_HEADERS`
- `TRAIN_DELAYS_CORS_MAX_AGE`

## Generate timetable pages

Pages are generated from the official rail GTFS (see [Download and convert official rail GTFS](#download-and-convert-official-rail-gtfs)):

```bash
./doubravka_hlavak.sh
```

which runs, for each direction:

```bash
./venv/bin/python get_direct_connection_cli.py \
  --from-stop 73265 \
  --to-stop 73275 \
  --from-label "Doubravka" \
  --to-label "Hlavní nádraží" \
  --html-out doubravka_hlavak.html \
  --gtfs-path data/official_rail_work/official_gtfs/2026
```

Stop IDs in the official GTFS: `73265` = Plzeň-Doubravka, `73275` = Plzeň hl.n.

When `--gtfs-path` is omitted, the CLI uses the newest year directory in `data/official_rail_work/official_gtfs/`. Default stops are `73265` → `73275` (Doubravka → hl.n.).

The 2026 official GTFS is valid from 2025-12-14 to 2026-12-12. Before the timetable change, download the next year's data and regenerate the pages.

## Run delay API

```bash
./venv/bin/python train_delays.py
```

Endpoint:

- `GET /train_delays/` (cache timeout from `TRAIN_DELAYS_CACHE_TIMEOUT_SECONDS`, default `60`); `/train_delays` without the trailing slash redirects (308) to it.

The scraper reads Babitron tables (`<table align=CENTER bgcolor=0000ff>`, six columns per row). Babitron pages are UTF-8 without a charset header and leave `<TD>` tags unclosed, so the scraper forces UTF-8 and parses with `html5lib`.

For the PythonAnywhere WSGI deployment, the module exposes `application`. After changing source URLs, update `.env` on the server too, because `.env` values override the defaults in code.

Detailed contract:

- `docs/train_delays_response.md`

## Delay integration behavior in timetable HTML

The generated HTML pages now:

1. Show an **Aktualni odjezdy** block (current departures for active day).
2. Poll delay API every 60 seconds.
3. Match delay records to departures:
   - strict match: unique `train_number` (and optional train-category check) -> **high** confidence
   - strict tie-breaker: if duplicate `train_number` exists, use scheduled time tolerance (`<= 3 min`)
   - fallback match: exact route-code overlap + scheduled time tolerance (`<= 3 min`) -> **medium** confidence
4. For static hour/minute tables:
   - annotate only the active day bucket (today)
   - annotate minute chips only for **high-confidence** matches
   - keep unknown or ambiguous minutes unannotated
5. Missing record in `/train_delays` is treated as **unknown**, not on-time.
6. Debug panel includes:
   - confidence counts (`high`, `medium`, `unknown`)
   - match reason counts (`train_number`, `route_code`, `none`)
   - train-number availability in current departures (`with`, `without`)

Delay endpoint resolution priority in generated HTML:

1. `?delays_endpoint=...` URL query parameter
2. `localStorage` override key `train_delays_endpoint_override`
3. Build-time `TIMETABLE_DELAYS_ENDPOINT` from `.env`
4. Fallback candidates (`/train_delays`, same-origin `/train_delays`, localhost when opened as `file:`)

## Run tests

```bash
./venv/bin/python -m unittest discover -s tests -v
```

Test coverage includes:

- scraping the current Babitron page format (unclosed `<TD>`, missing charset)
- delay parser behavior (`on_time`, `delayed`, `canceled`, `diverted`, `disruption`, `unknown`)
- backward-compatible response keys
- additive normalized fields
- strict train-number and route-code fallback matching, ambiguous cases, and missing-record behavior

## Download and convert official rail GTFS

Use official CZ rail XML data (base + updates), convert with local `czptt2gtfs`, and write GTFS output.

```bash
./venv/bin/python scripts/download_and_convert_official_gtfs.py \
  --year 2026 \
  --work-dir data/official_rail_work \
  --output-dir data/official_rail_work/official_gtfs/2026 \
  --updates-mode all
```

`data/` is git-ignored, so the feed has to be generated locally.

Key flags:

- `--skip-download`: reuse downloaded archives / extracted XML.
- `--skip-convert`: skip conversion and validate existing GTFS output.
- `--updates-mode all|none`: include all monthly updates or only base archive.

Output:

- GTFS feed in `--output-dir`
- download manifest in `<work-dir>/downloads/<year>/manifest.json`
- conversion summary in `<output-dir>/processing_summary.json`

## Notes

- If `TIMETABLE_DELAYS_ENDPOINT` is empty, timetable pages still fall back to same-origin `/train_delays`.
- For cross-origin delay APIs, keep CORS enabled on the API side (configurable via `.env`).
