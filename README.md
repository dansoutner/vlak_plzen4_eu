# Jizdni Rady Tools

Tools for generating Doubravka <-> Hlavni nadrazi timetable HTML and attaching live delay status from Babitron.

## What is in this repo

- `get_direct_connection_cli.py`: builds timetable data from GTFS and renders HTML.
- `get_delays.py`: Flask endpoint `/train_delays` that scrapes live delay tables.
- `doubravka_hlavak.html`, `hlavak_doubravka.html`: generated timetable pages.
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
./venv/bin/pip install beautifulsoup4 fake-headers flask flask-caching jinja2 pandas requests
```

## Generate timetable pages

```bash
./venv/bin/python get_direct_connection_cli.py \
  --from-stop ST_44120 \
  --to-stop ST_44121 \
  --from-label "Doubravka" \
  --to-label "Hlavni nadrazi" \
  --html-out doubravka_hlavak.html \
  --reverse \
  --reverse-html-out hlavak_doubravka.html
```

Default GTFS source is:

- `jizdni-rady-czech-republic/data/merged`

## Run delay API

```bash
./venv/bin/python get_delays.py
```

Endpoint:

- `GET /train_delays` (cached for 60 seconds)

Detailed contract:

- `docs/train_delays_response.md`

## Delay integration behavior in timetable HTML

The generated HTML pages now:

1. Show an **Aktualni odjezdy** block (current departures for active day).
2. Poll `/train_delays` every 60 seconds.
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

## Run tests

```bash
./venv/bin/python -m unittest discover -s tests -v
```

Test coverage includes:

- delay parser behavior (`on_time`, `delayed`, `canceled`, `diverted`, `disruption`, `unknown`)
- backward-compatible response keys
- additive normalized fields
- strict train-number and route-code fallback matching, ambiguous cases, and missing-record behavior

## Download and convert official rail GTFS

Use official CZ rail XML data (base + updates), convert with local `czptt2gtfs`, and write GTFS output.

```bash
./venv/bin/python scripts/download_and_convert_official_gtfs.py \
  --year 2026 \
  --work-dir /Users/dan/Data/STAN/jizdni_rady/data/official_rail_work \
  --output-dir /Users/dan/Data/STAN/jizdni_rady/data/official_rail_work/official_gtfs/2026 \
  --updates-mode all
```

Key flags:

- `--skip-download`: reuse downloaded archives / extracted XML.
- `--skip-convert`: skip conversion and validate existing GTFS output.
- `--updates-mode all|none`: include all monthly updates or only base archive.

Output:

- GTFS feed in `--output-dir`
- download manifest in `<work-dir>/downloads/<year>/manifest.json`
- conversion summary in `<output-dir>/processing_summary.json`

## Notes

- Timetable pages expect `/train_delays` on the same origin.
- If this is deployed cross-origin, configure CORS on the API side.
