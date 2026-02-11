# `/train_delays` Response Contract

This document describes the current return structure of `/train_delays` from `/Users/dan/Data/STAN/jizdni_rady/get_delays.py`.

## Endpoint

- Method: `GET`
- Path: `/train_delays`
- Cache: 60 seconds (`flask-caching`, simple cache)
- Content type: JSON object

## Top-level shape

The response is a JSON object keyed by `train_info`.

| Element | Type | Current behavior |
|---|---|---|
| `GET /train_delays` | endpoint | Returns JSON object keyed by `train_info` |
| cache | server behavior | 60s (`flask-caching`, simple cache) |
| top-level key | string | `train_info` extracted from table column 1 (`get_text`) |
| `train` | string | same as key |
| `name` | string | raw column 2 (may contain HTML fragments) |
| `route` | string | raw column 3 (may contain HTML fragments) |
| `station` | string | raw column 4 (may contain HTML fragments) |
| `scheduled_actual_time` | string | raw column 5 (typically planned/actual string) |
| `delay_text` | string | raw column 6 |
| `delay` | `int \| null` | `0` for “bez zpoždění”/“včas”, `null` for “zrušen”/“odklon”/“výluka”/empty, else first parsed delay number |

## Additive normalized fields

The endpoint now also includes additive normalized fields (backward compatible):

- `status`: `on_time | delayed | canceled | diverted | disruption | unknown`
- `delay_minutes`: `int | null`
- `train_category`: parsed prefix like `Os`, `Sp`, `R`, `Ex`, `EC`, etc. or `null`
- `train_number`: parsed integer or `null`
- `route_text`: cleaned text
- `station_text`: cleaned text
- `scheduled_time_hhmm`: parsed planned `HH:MM` or `null`
- `actual_time_hhmm`: parsed actual `HH:MM` or `null`
- `source_page`: `zponline | zponlineos`

## Known caveats

1. `name/route/station/scheduled_actual_time/delay_text` are not consistently cleaned to plain text.
2. Key collisions overwrite records (`results[train_info]`).
3. `delay = null` conflates multiple states (canceled/diverted/outage/unknown).
4. Legacy parsing by table structure is brittle to upstream HTML layout changes.
