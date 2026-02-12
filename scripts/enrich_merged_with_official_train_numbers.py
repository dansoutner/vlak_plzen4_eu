#!/usr/bin/env python3
"""Download official Czech rail data and enrich merged GTFS with train numbers."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import hashlib
import json
import logging
import re
import shutil
import subprocess
import tarfile
import unicodedata
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("enrich_official_train_numbers")

YEAR_URL_TEMPLATE = "https://portal.cisjr.cz/pub/draha/celostatni/szdc/{year}/"
MONTH_DIR_RE = re.compile(r"/\d{4}-\d{2}/?$")
TRAIN_LABEL_RE = re.compile(r"\b([A-Za-z]{1,8})\s*([0-9]{1,6})\b")

DEFAULT_REPO_ROOT = Path("/Users/dan/Data/STAN/jizdni_rady")
DEFAULT_MERGED_DIR = DEFAULT_REPO_ROOT / "jizdni-rady-czech-republic" / "data" / "merged"
DEFAULT_WORK_DIR = DEFAULT_REPO_ROOT / "data" / "official_rail_work"
DEFAULT_OUTPUT_DIR = DEFAULT_REPO_ROOT / "data" / "merged_enriched_train_numbers"


@dataclass(frozen=True)
class ArchiveDiscovery:
    year_url: str
    base_url: str
    month_urls: list[str]
    update_urls: list[str]


def now_iso() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_stop_name(value: Any) -> str:
    text = str(value or "")
    normalized = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower()
    text = text.replace("-", " ").replace(",", " ")
    text = re.sub(r"\bhlavni\s+nadrazi\b", "hl.n.", text)
    text = re.sub(r"\bhlavni\s+n\.\b", "hl.n.", text)
    text = re.sub(r"hl\.\s*n\.", "hl.n.", text)
    text = re.sub(r"\bhl\.n\b", "hl.n.", text)
    text = re.sub(r"hl\.n\.+", "hl.n.", text)
    text = " ".join(text.split())
    return text.strip()


def parse_train_label(route_short_name: Any) -> str | None:
    text = str(route_short_name or "").strip()
    if not text:
        return None
    match = TRAIN_LABEL_RE.search(text)
    if not match:
        return None
    category = match.group(1).strip()
    number = int(match.group(2))
    return f"{category} {number}"


def parse_hhmm(value: Any) -> str | None:
    text = str(value or "").strip()
    match = re.search(r"\b(\d{1,2}:[0-5]\d)\b", text)
    if not match:
        return None
    hhmm = match.group(1)
    hh, mm = hhmm.split(":")
    return f"{int(hh):02d}:{mm}"


def choose_train_label(votes: Counter[str]) -> dict[str, Any]:
    if not votes:
        return {
            "status": "unmatched",
            "reason": "no_candidates",
            "label": None,
            "top_votes": 0,
            "second_votes": 0,
            "total_votes": 0,
            "ratio": 0.0,
            "candidates": {},
        }

    ordered = votes.most_common()
    top_label, top_votes = ordered[0]
    second_votes = ordered[1][1] if len(ordered) > 1 else 0
    total_votes = sum(votes.values())
    ratio = (top_votes / total_votes) if total_votes else 0.0

    if top_votes < 2:
        status = "ambiguous"
        reason = "low_support"
    elif second_votes >= top_votes:
        status = "ambiguous"
        reason = "tie"
    elif ratio < 0.60:
        status = "ambiguous"
        reason = "low_ratio"
    else:
        status = "matched"
        reason = "matched"

    return {
        "status": status,
        "reason": reason,
        "label": top_label if status == "matched" else None,
        "top_label": top_label,
        "top_votes": int(top_votes),
        "second_votes": int(second_votes),
        "total_votes": int(total_votes),
        "ratio": float(round(ratio, 4)),
        "candidates": dict(ordered),
    }


def extract_links_from_html(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = str(anchor["href"]).strip()
        if not href:
            continue
        absolute = urljoin(base_url, href)
        links.append(absolute)
    # stable unique order
    seen: set[str] = set()
    unique: list[str] = []
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        unique.append(link)
    return unique


def discover_remote_archives(
    year: int,
    updates_mode: str,
    fetch_html: Callable[[str], str],
) -> ArchiveDiscovery:
    year_url = YEAR_URL_TEMPLATE.format(year=year)
    year_html = fetch_html(year_url)
    year_links = extract_links_from_html(year_html, year_url)

    base_filename = f"JR{year}.zip".lower()
    base_candidates = [
        link for link in year_links if Path(urlparse(link).path).name.lower() == base_filename
    ]
    if not base_candidates:
        raise RuntimeError(f"Could not find {base_filename} at {year_url}")
    base_url = sorted(base_candidates)[0]

    month_urls = sorted(
        {
            link.rstrip("/") + "/"
            for link in year_links
            if MONTH_DIR_RE.search(urlparse(link).path)
        }
    )

    update_urls: list[str] = []
    if updates_mode == "all":
        for month_url in month_urls:
            month_html = fetch_html(month_url)
            month_links = extract_links_from_html(month_html, month_url)
            month_zip_urls = sorted(
                {
                    link
                    for link in month_links
                    if Path(urlparse(link).path).suffix.lower() == ".zip"
                }
            )
            update_urls.extend(month_zip_urls)

    return ArchiveDiscovery(
        year_url=year_url,
        base_url=base_url,
        month_urls=month_urls,
        update_urls=update_urls,
    )


def archive_destination(url: str, downloads_dir: Path) -> Path:
    parsed = urlparse(url)
    path_parts = [part for part in parsed.path.split("/") if part]
    filename = path_parts[-1]
    parent = path_parts[-2] if len(path_parts) >= 2 else ""
    if re.fullmatch(r"\d{4}-\d{2}", parent):
        return downloads_dir / parent / filename
    return downloads_dir / filename


def download_archive(url: str, destination: Path, session: requests.Session) -> dict[str, Any]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    reused = False
    if destination.exists() and destination.stat().st_size > 0:
        reused = True
    else:
        temp_file = destination.with_suffix(destination.suffix + ".part")
        if temp_file.exists():
            temp_file.unlink()
        response = session.get(url, stream=True, timeout=120)
        response.raise_for_status()
        with temp_file.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    fh.write(chunk)
        temp_file.replace(destination)

    return {
        "url": url,
        "local_path": str(destination),
        "file_size": destination.stat().st_size,
        "sha256": sha256_file(destination),
        "downloaded_at": now_iso(),
        "reused": reused,
    }


def download_archives(discovery: ArchiveDiscovery, downloads_dir: Path) -> dict[str, Any]:
    downloads_dir.mkdir(parents=True, exist_ok=True)
    with requests.Session() as session:
        base_meta = download_archive(
            discovery.base_url,
            archive_destination(discovery.base_url, downloads_dir),
            session,
        )
        update_meta: list[dict[str, Any]] = []
        for url in discovery.update_urls:
            update_meta.append(download_archive(url, archive_destination(url, downloads_dir), session))
    return {
        "base_archive": base_meta,
        "update_archives": update_meta,
    }


def load_local_archives(year: int, downloads_dir: Path, updates_mode: str) -> dict[str, Any]:
    base_path = downloads_dir / f"JR{year}.zip"
    if not base_path.exists():
        raise FileNotFoundError(f"Missing base archive: {base_path}")

    update_paths: list[Path] = []
    if updates_mode == "all":
        update_paths = sorted(
            [path for path in downloads_dir.glob("*/*.zip") if path.is_file()],
            key=lambda path: (path.parent.name, path.name),
        )

    return {
        "base_archive": {
            "url": None,
            "local_path": str(base_path),
            "file_size": base_path.stat().st_size,
            "sha256": sha256_file(base_path),
            "downloaded_at": now_iso(),
            "reused": True,
        },
        "update_archives": [
            {
                "url": None,
                "local_path": str(path),
                "file_size": path.stat().st_size,
                "sha256": sha256_file(path),
                "downloaded_at": now_iso(),
                "reused": True,
            }
            for path in update_paths
        ],
    }


def _detect_archive_type(archive_path: Path) -> str:
    """Detect archive type by reading file header (magic bytes)."""
    with archive_path.open('rb') as f:
        header = f.read(4)

    # ZIP file magic bytes: 0x50 0x4B 0x03 0x04 (PK..)
    if header[:2] == b'PK':
        return 'zip'
    # GZIP magic bytes: 0x1F 0x8B
    elif header[:2] == b'\x1f\x8b':
        return 'gzip'
    else:
        # Fallback to extension-based detection
        suffix = archive_path.suffix.lower()
        if suffix == '.gz':
            return 'gzip'
        elif suffix == '.zip':
            return 'zip'
        else:
            return 'unknown'


def _extract_xml_archive(
    archive_path: Path,
    xml_output_dir: Path,
    source_by_file: dict[str, str],
    overrides: list[dict[str, str]],
) -> int:
    extracted = 0
    # Detect archive type by file header
    archive_type = _detect_archive_type(archive_path)

    if archive_type == 'gzip':
        # Handle gzip/tar.gz archives
        try:
            with gzip.open(archive_path, 'rb') as gz_file:
                tar_file = tarfile.open(fileobj=gz_file, mode='r|gz')
                for member in tar_file:
                    if member.isdir():
                        continue
                    if not member.name.lower().endswith(".xml"):
                        continue
                    output_name = Path(member.name).name
                    output_path = xml_output_dir / output_name
                    previous_source = source_by_file.get(output_name)
                    if previous_source is not None:
                        overrides.append(
                            {
                                "file": output_name,
                                "replaced_from": previous_source,
                                "replaced_by": archive_path.name,
                            }
                        )
                    with tar_file.extractfile(member) as source, output_path.open("wb") as target:
                        shutil.copyfileobj(source, target)
                    source_by_file[output_name] = archive_path.name
                    extracted += 1
        except (tarfile.TarError, AttributeError) as e:
            logger.warning("Could not read %s as tar.gz archive: %s", archive_path, e)
    elif archive_type == 'zip':
        # Handle zip archives
        with zipfile.ZipFile(archive_path, 'r') as archive:
            for member in archive.infolist():
                if member.is_dir():
                    continue
                if not member.filename.lower().endswith(".xml"):
                    continue
                output_name = Path(member.filename).name
                output_path = xml_output_dir / output_name
                previous_source = source_by_file.get(output_name)
                if previous_source is not None:
                    overrides.append(
                        {
                            "file": output_name,
                            "replaced_from": previous_source,
                            "replaced_by": archive_path.name,
                        }
                    )
                with archive.open(member) as source, output_path.open("wb") as target:
                    shutil.copyfileobj(source, target)
                source_by_file[output_name] = archive_path.name
                extracted += 1
    else:
        logger.warning("Unknown archive type for %s (detected as: %s)", archive_path, archive_type)
    return extracted


def extract_and_merge_xml_archives(
    base_archive: Path,
    update_archives: list[Path],
    xml_output_dir: Path,
) -> dict[str, Any]:
    if xml_output_dir.exists():
        shutil.rmtree(xml_output_dir)
    xml_output_dir.mkdir(parents=True, exist_ok=True)

    source_by_file: dict[str, str] = {}
    overrides: list[dict[str, str]] = []

    base_extracted = _extract_xml_archive(base_archive, xml_output_dir, source_by_file, overrides)
    updates_extracted = 0
    for update_archive in update_archives:
        logger.info("Extracting update archive %s", update_archive)
        updates_extracted += _extract_xml_archive(update_archive, xml_output_dir, source_by_file, overrides)

    return {
        "xml_output_dir": str(xml_output_dir),
        "base_archive": str(base_archive),
        "update_archives": [str(path) for path in update_archives],
        "base_xml_files_written": base_extracted,
        "update_xml_files_written": updates_extracted,
        "final_xml_file_count": len(source_by_file),
        "overrides_count": len(overrides),
        "overrides": overrides,
    }


def official_gtfs_ready(official_gtfs_dir: Path) -> bool:
    required = ["routes.txt", "trips.txt", "stop_times.txt", "stops.txt"]
    return all((official_gtfs_dir / filename).exists() for filename in required)


def convert_official_xml_to_gtfs(repo_root: Path, xml_dir: Path, official_gtfs_dir: Path) -> None:
    converter = repo_root / "czptt2gtfs" / "czptt2gtfs.py"
    sr70 = repo_root / "czptt2gtfs" / "sr70.csv"
    python_bin = repo_root / "venv" / "bin" / "python"

    if not converter.exists():
        raise FileNotFoundError(f"Missing converter: {converter}")
    if not sr70.exists():
        raise FileNotFoundError(f"Missing SR70 file: {sr70}")
    if not python_bin.exists():
        raise FileNotFoundError(f"Missing Python binary: {python_bin}")

    if official_gtfs_dir.exists():
        shutil.rmtree(official_gtfs_dir)
    official_gtfs_dir.mkdir(parents=True, exist_ok=True)

    command = [
        str(python_bin),
        str(converter),
        str(xml_dir),
        str(official_gtfs_dir),
        "--sr70",
        str(sr70),
    ]
    logger.info("Running converter: %s", " ".join(command))
    subprocess.run(command, check=True)


def resolve_gtfs_table_path(feed_dir: Path, table_name: str, required: bool = True) -> Path | None:
    txt = feed_dir / f"{table_name}.txt"
    gz = feed_dir / f"{table_name}.txt.gz"
    if txt.exists():
        return txt
    if gz.exists():
        return gz
    if required:
        raise FileNotFoundError(f"Missing GTFS table {table_name}.txt(.gz) in {feed_dir}")
    return None


def read_gtfs_table(
    feed_dir: Path,
    table_name: str,
    usecols: list[str] | None = None,
    required: bool = True,
    chunksize: int | None = None,
) -> pd.DataFrame | Any:
    path = resolve_gtfs_table_path(feed_dir, table_name, required=required)
    if path is None:
        return None
    kwargs: dict[str, Any] = {"dtype": str, "low_memory": False}
    if usecols is not None:
        kwargs["usecols"] = usecols
    if chunksize is not None:
        kwargs["chunksize"] = chunksize
    if path.suffix == ".gz":
        kwargs["compression"] = "gzip"
    return pd.read_csv(path, **kwargs)


def load_stop_times_filtered(feed_dir: Path, trip_ids: set[str]) -> pd.DataFrame:
    chunks = read_gtfs_table(
        feed_dir,
        "stop_times",
        usecols=["trip_id", "stop_id", "stop_sequence", "departure_time"],
        chunksize=500_000,
    )
    filtered_parts: list[pd.DataFrame] = []
    for chunk in chunks:
        filtered = chunk[chunk["trip_id"].isin(trip_ids)]
        if not filtered.empty:
            filtered_parts.append(filtered)
    if not filtered_parts:
        return pd.DataFrame(columns=["trip_id", "stop_id", "stop_sequence", "departure_time"])
    merged = pd.concat(filtered_parts, ignore_index=True)
    merged["stop_sequence"] = pd.to_numeric(merged["stop_sequence"], errors="coerce")
    merged.dropna(subset=["stop_sequence"], inplace=True)
    return merged


def build_service_days(feed_dir: Path, service_ids: set[str]) -> dict[str, frozenset[int]]:
    mapping: dict[str, set[int]] = {service_id: set() for service_id in service_ids}

    calendar = read_gtfs_table(feed_dir, "calendar", required=False)
    if calendar is not None:
        weekday_columns = [
            ("monday", 0),
            ("tuesday", 1),
            ("wednesday", 2),
            ("thursday", 3),
            ("friday", 4),
            ("saturday", 5),
            ("sunday", 6),
        ]
        for _, row in calendar.iterrows():
            service_id = str(row.get("service_id", ""))
            if service_id not in mapping:
                continue
            for column_name, day_index in weekday_columns:
                if str(row.get(column_name, "0")) == "1":
                    mapping[service_id].add(day_index)

    calendar_dates = read_gtfs_table(feed_dir, "calendar_dates", required=False)
    if calendar_dates is not None:
        active_dates = calendar_dates[calendar_dates.get("exception_type", "1").astype(str) == "1"]
        for _, row in active_dates.iterrows():
            service_id = str(row.get("service_id", ""))
            date_value = str(row.get("date", ""))
            if service_id not in mapping or len(date_value) != 8:
                continue
            try:
                weekday = dt.datetime.strptime(date_value, "%Y%m%d").weekday()
            except ValueError:
                continue
            mapping[service_id].add(weekday)

    all_days = frozenset(range(7))
    return {
        service_id: frozenset(days) if days else all_days
        for service_id, days in mapping.items()
    }


def build_edges(stop_times: pd.DataFrame) -> pd.DataFrame:
    if stop_times.empty:
        return pd.DataFrame(columns=["trip_id", "from_stop_id", "to_stop_id", "departure_time"])
    ordered = stop_times.sort_values(["trip_id", "stop_sequence"], kind="stable").copy()
    ordered["to_stop_id"] = ordered.groupby("trip_id")["stop_id"].shift(-1)
    edges = ordered[ordered["to_stop_id"].notna()].copy()
    edges.rename(columns={"stop_id": "from_stop_id"}, inplace=True)
    return edges[["trip_id", "from_stop_id", "to_stop_id", "departure_time"]]


def build_official_edge_index(official_gtfs_dir: Path) -> tuple[dict[tuple[str, str, str, int], Counter[str]], dict[str, Any]]:
    routes = read_gtfs_table(official_gtfs_dir, "routes", usecols=["route_id", "route_short_name"])
    trips = read_gtfs_table(official_gtfs_dir, "trips", usecols=["trip_id", "route_id", "service_id"])
    stops = read_gtfs_table(official_gtfs_dir, "stops", usecols=["stop_id", "stop_name"])

    route_to_train_label: dict[str, str] = {}
    for _, row in routes.iterrows():
        label = parse_train_label(row.get("route_short_name"))
        if label:
            route_to_train_label[str(row["route_id"])] = label

    relevant_trips = trips[trips["route_id"].isin(route_to_train_label.keys())].copy()
    trip_ids = set(relevant_trips["trip_id"])
    service_days = build_service_days(official_gtfs_dir, set(relevant_trips["service_id"]))

    stop_times = load_stop_times_filtered(official_gtfs_dir, trip_ids)
    edges = build_edges(stop_times)
    if edges.empty:
        return {}, {"official_trips": len(relevant_trips), "official_edges": 0}

    stop_name_map = {
        str(row["stop_id"]): normalize_stop_name(row.get("stop_name"))
        for _, row in stops.iterrows()
    }

    trip_meta = relevant_trips.set_index("trip_id")[["route_id", "service_id"]].to_dict(orient="index")

    edge_index: dict[tuple[str, str, str, int], Counter[str]] = defaultdict(Counter)
    indexed_edges = 0
    for _, edge in edges.iterrows():
        trip_id = str(edge["trip_id"])
        metadata = trip_meta.get(trip_id)
        if metadata is None:
            continue
        route_id = str(metadata["route_id"])
        service_id = str(metadata["service_id"])
        label = route_to_train_label.get(route_id)
        if label is None:
            continue
        from_name = stop_name_map.get(str(edge["from_stop_id"]), "")
        to_name = stop_name_map.get(str(edge["to_stop_id"]), "")
        hhmm = parse_hhmm(edge.get("departure_time"))
        if not from_name or not to_name or hhmm is None:
            continue
        for day_index in service_days.get(service_id, frozenset(range(7))):
            edge_index[(from_name, to_name, hhmm, day_index)][label] += 1
            indexed_edges += 1

    stats = {
        "official_routes_with_train_label": len(route_to_train_label),
        "official_trips_considered": len(relevant_trips),
        "official_edges_considered": len(edges),
        "official_edges_indexed": indexed_edges,
        "official_index_keys": len(edge_index),
    }
    return dict(edge_index), stats


def match_merged_trips(
    merged_dir: Path,
    official_edge_index: dict[tuple[str, str, str, int], Counter[str]],
) -> tuple[dict[str, str], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    routes = read_gtfs_table(merged_dir, "routes", usecols=["route_id", "route_type"])
    trips = read_gtfs_table(merged_dir, "trips", usecols=["trip_id", "route_id", "service_id"])
    stops = read_gtfs_table(merged_dir, "stops", usecols=["stop_id", "stop_name"])

    rail_route_ids = set(routes[routes["route_type"].astype(str) == "2"]["route_id"])
    rail_trips = trips[trips["route_id"].isin(rail_route_ids)].copy()
    rail_trip_ids = set(rail_trips["trip_id"])

    service_days = build_service_days(merged_dir, set(rail_trips["service_id"]))
    stop_times = load_stop_times_filtered(merged_dir, rail_trip_ids)
    edges = build_edges(stop_times)

    stop_name_map = {
        str(row["stop_id"]): normalize_stop_name(row.get("stop_name"))
        for _, row in stops.iterrows()
    }
    trip_meta = rail_trips.set_index("trip_id")[["route_id", "service_id"]].to_dict(orient="index")

    edges["dep_hhmm"] = edges["departure_time"].map(parse_hhmm)
    edges["from_name_norm"] = edges["from_stop_id"].map(stop_name_map)
    edges["to_name_norm"] = edges["to_stop_id"].map(stop_name_map)
    grouped_edges = edges.groupby("trip_id")

    matched_labels: dict[str, str] = {}
    ambiguous_rows: list[dict[str, Any]] = []
    unmatched_rows: list[dict[str, Any]] = []

    for trip_id, trip_rows in grouped_edges:
        metadata = trip_meta.get(str(trip_id))
        if metadata is None:
            continue
        service_id = str(metadata["service_id"])
        days = service_days.get(service_id, frozenset(range(7)))
        votes: Counter[str] = Counter()

        for _, edge in trip_rows.iterrows():
            from_name = str(edge.get("from_name_norm") or "")
            to_name = str(edge.get("to_name_norm") or "")
            hhmm = edge.get("dep_hhmm")
            if not from_name or not to_name or not isinstance(hhmm, str):
                continue
            for day_index in days:
                key = (from_name, to_name, hhmm, int(day_index))
                candidates = official_edge_index.get(key)
                if candidates:
                    votes.update(candidates)

        decision = choose_train_label(votes)
        base_row = {
            "trip_id": str(trip_id),
            "route_id": str(metadata["route_id"]),
            "service_id": service_id,
            "reason": decision["reason"],
            "top_label": decision.get("top_label"),
            "top_votes": decision.get("top_votes", 0),
            "second_votes": decision.get("second_votes", 0),
            "total_votes": decision.get("total_votes", 0),
            "ratio": decision.get("ratio", 0.0),
            "candidates": json.dumps(decision.get("candidates", {}), ensure_ascii=False),
        }

        if decision["status"] == "matched" and decision["label"]:
            matched_labels[str(trip_id)] = str(decision["label"])
        elif decision["reason"] == "no_candidates":
            unmatched_rows.append(base_row)
        else:
            ambiguous_rows.append(base_row)

    stats = {
        "merged_rail_routes": len(rail_route_ids),
        "merged_rail_trips": len(rail_trips),
        "merged_rail_edges": len(edges),
        "matched_trips": len(matched_labels),
        "ambiguous_trips": len(ambiguous_rows),
        "unmatched_trips": len(unmatched_rows),
    }
    return matched_labels, ambiguous_rows, unmatched_rows, stats


def copy_feed_files(source_dir: Path, destination_dir: Path) -> None:
    destination_dir.mkdir(parents=True, exist_ok=True)
    for path in source_dir.iterdir():
        if path.is_file():
            shutil.copy2(path, destination_dir / path.name)


def write_csv_rows(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def rewrite_trips_with_train_numbers(
    merged_dir: Path,
    output_dir: Path,
    matched_labels: dict[str, str],
) -> dict[str, Any]:
    trips_path = resolve_gtfs_table_path(merged_dir, "trips", required=True)
    routes = read_gtfs_table(merged_dir, "routes", usecols=["route_id", "route_type"])
    rail_route_ids = set(routes[routes["route_type"].astype(str) == "2"]["route_id"])
    trips = pd.read_csv(trips_path, dtype=str, low_memory=False)

    if "trip_short_name" not in trips.columns:
        trips["trip_short_name"] = ""

    assigned = 0
    cleared = 0
    for index, row in trips.iterrows():
        route_id = str(row.get("route_id", ""))
        trip_id = str(row.get("trip_id", ""))
        if route_id not in rail_route_ids:
            continue
        label = matched_labels.get(trip_id)
        if label:
            trips.at[index, "trip_short_name"] = label
            assigned += 1
        else:
            trips.at[index, "trip_short_name"] = ""
            cleared += 1

    output_trips_path = output_dir / "trips.txt"
    trips.to_csv(output_trips_path, index=False)
    return {
        "output_trips_path": str(output_trips_path),
        "assigned_trip_short_name": assigned,
        "cleared_trip_short_name": cleared,
    }


def write_reports(
    output_dir: Path,
    summary: dict[str, Any],
    ambiguous_rows: list[dict[str, Any]],
    unmatched_rows: list[dict[str, Any]],
    matched_labels: dict[str, str],
) -> None:
    reports_dir = output_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    summary_path = reports_dir / "enrichment_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    write_csv_rows(
        reports_dir / "ambiguous_trips.csv",
        [
            "trip_id",
            "route_id",
            "service_id",
            "reason",
            "top_label",
            "top_votes",
            "second_votes",
            "total_votes",
            "ratio",
            "candidates",
        ],
        ambiguous_rows,
    )
    write_csv_rows(
        reports_dir / "unmatched_trips.csv",
        [
            "trip_id",
            "route_id",
            "service_id",
            "reason",
            "top_label",
            "top_votes",
            "second_votes",
            "total_votes",
            "ratio",
            "candidates",
        ],
        unmatched_rows,
    )
    matched_sample_rows = [
        {"trip_id": trip_id, "train_label": label}
        for trip_id, label in sorted(matched_labels.items())[:1000]
    ]
    write_csv_rows(
        reports_dir / "matched_trips_sample.csv",
        ["trip_id", "train_label"],
        matched_sample_rows,
    )


def enrich_merged_feed(
    merged_dir: Path,
    official_gtfs_dir: Path,
    output_dir: Path,
) -> dict[str, Any]:
    logger.info("Building official edge index from %s", official_gtfs_dir)
    official_edge_index, official_stats = build_official_edge_index(official_gtfs_dir)

    logger.info("Matching merged rail trips from %s", merged_dir)
    matched_labels, ambiguous_rows, unmatched_rows, merged_stats = match_merged_trips(
        merged_dir,
        official_edge_index,
    )

    logger.info("Copying merged feed into %s", output_dir)
    copy_feed_files(merged_dir, output_dir)
    trips_stats = rewrite_trips_with_train_numbers(merged_dir, output_dir, matched_labels)

    summary = {
        "generated_at": now_iso(),
        "merged_dir": str(merged_dir),
        "official_gtfs_dir": str(official_gtfs_dir),
        "output_dir": str(output_dir),
        "official_stats": official_stats,
        "merged_stats": merged_stats,
        "trips_stats": trips_stats,
        "matched_count": len(matched_labels),
        "ambiguous_count": len(ambiguous_rows),
        "unmatched_count": len(unmatched_rows),
    }

    write_reports(output_dir, summary, ambiguous_rows, unmatched_rows, matched_labels)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download official rail data and enrich merged GTFS with trip_short_name train numbers."
    )
    parser.add_argument("--year", type=int, default=2026, help="Official timetable year.")
    parser.add_argument("--merged-dir", type=Path, default=DEFAULT_MERGED_DIR, help="Path to merged GTFS feed.")
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR, help="Working directory for downloads and conversion.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output enriched GTFS directory.")
    parser.add_argument(
        "--updates-mode",
        choices=["all", "none"],
        default="all",
        help="Whether to include all monthly update archives.",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Reuse previously downloaded archives and extracted XML files.",
    )
    parser.add_argument(
        "--skip-convert",
        action="store_true",
        help="Reuse previously converted official GTFS files.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    merged_dir = args.merged_dir.resolve()
    work_dir = args.work_dir.resolve()
    output_dir = args.output_dir.resolve()
    repo_root = DEFAULT_REPO_ROOT

    downloads_dir = work_dir / "downloads" / str(args.year)
    xml_output_dir = work_dir / "xml_merged" / str(args.year)
    official_gtfs_dir = work_dir / "official_gtfs" / str(args.year)
    manifest_path = downloads_dir / "manifest.json"
    base_archive_path = downloads_dir / f"JR{args.year}.zip"

    if not merged_dir.exists():
        raise FileNotFoundError(f"Merged GTFS directory does not exist: {merged_dir}")

    has_local_xml = xml_output_dir.exists() and any(xml_output_dir.glob("*.xml"))
    can_reuse_converted = args.skip_convert and official_gtfs_ready(official_gtfs_dir)

    if args.skip_download:
        logger.info("Skipping download phase, using local archives from %s", downloads_dir)
        discovery = None
        if base_archive_path.exists():
            download_result = load_local_archives(args.year, downloads_dir, args.updates_mode)
        elif has_local_xml or can_reuse_converted:
            logger.info(
                "No local archives found. Reusing existing XML/GTFS artifacts (xml=%s converted_gtfs=%s).",
                has_local_xml,
                can_reuse_converted,
            )
            download_result = {
                "base_archive": {
                    "url": None,
                    "local_path": str(base_archive_path),
                    "file_size": 0,
                    "sha256": None,
                    "downloaded_at": now_iso(),
                    "reused": True,
                },
                "update_archives": [],
            }
        else:
            raise FileNotFoundError(
                f"Missing local archives in {downloads_dir}. Provide downloads or run without --skip-download."
            )
    else:
        logger.info("Discovering official archives for year %s", args.year)
        with requests.Session() as session:
            discovery = discover_remote_archives(
                args.year,
                args.updates_mode,
                fetch_html=lambda url: session.get(url, timeout=60).text,
            )
        logger.info(
            "Downloading archives: 1 base + %s update files",
            len(discovery.update_urls),
        )
        download_result = download_archives(discovery, downloads_dir)

    base_archive = Path(download_result["base_archive"]["local_path"])
    update_archives = [Path(item["local_path"]) for item in download_result["update_archives"]]

    if args.skip_download and (has_local_xml or can_reuse_converted):
        extraction_log = {
            "xml_output_dir": str(xml_output_dir),
            "reused_existing_xml_dir": has_local_xml,
            "reused_existing_converted_gtfs": can_reuse_converted,
            "final_xml_file_count": len(list(xml_output_dir.glob("*.xml"))) if has_local_xml else 0,
            "overrides_count": 0,
            "overrides": [],
        }
    else:
        logger.info("Extracting and merging XML files into %s", xml_output_dir)
        extraction_log = extract_and_merge_xml_archives(base_archive, update_archives, xml_output_dir)

    manifest_payload = {
        "generated_at": now_iso(),
        "year": args.year,
        "updates_mode": args.updates_mode,
        "discovery": None
        if discovery is None
        else {
            "year_url": discovery.year_url,
            "base_url": discovery.base_url,
            "month_urls": discovery.month_urls,
            "update_urls": discovery.update_urls,
        },
        "base_archive": download_result["base_archive"],
        "update_archives": download_result["update_archives"],
        "extraction": extraction_log,
    }
    downloads_dir.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.skip_convert:
        logger.info("Skipping conversion phase, expecting official GTFS in %s", official_gtfs_dir)
        if not official_gtfs_ready(official_gtfs_dir):
            raise FileNotFoundError(
                f"Official GTFS not ready in {official_gtfs_dir}. Run without --skip-convert."
            )
    else:
        logger.info("Converting official XML to GTFS into %s", official_gtfs_dir)
        convert_official_xml_to_gtfs(repo_root, xml_output_dir, official_gtfs_dir)

    logger.info("Running enrichment into %s", output_dir)
    summary = enrich_merged_feed(merged_dir, official_gtfs_dir, output_dir)
    summary["manifest_path"] = str(manifest_path)
    summary_path = output_dir / "reports" / "enrichment_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(
        "Done. matched=%s ambiguous=%s unmatched=%s",
        summary.get("matched_count"),
        summary.get("ambiguous_count"),
        summary.get("unmatched_count"),
    )


if __name__ == "__main__":
    main()
