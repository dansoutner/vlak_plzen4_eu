#!/usr/bin/env python3
"""Download official Czech rail XML archives and convert them to GTFS."""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import logging
import re
import shutil
import subprocess
import sys
import tarfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

LOGGER = logging.getLogger("official_gtfs_pipeline")

YEAR_URL_TEMPLATE = "https://portal.cisjr.cz/pub/draha/celostatni/szdc/{year}/"
MONTH_DIR_RE = re.compile(r"/\d{4}-\d{2}/?$")
DOWNLOAD_TIMEOUT_SECONDS = 180
HTML_TIMEOUT_SECONDS = 60
DOWNLOAD_CHUNK_SIZE = 1024 * 1024

DEFAULT_REPO_ROOT = Path("/Users/dan/Data/STAN/jizdni_rady")
DEFAULT_WORK_DIR = DEFAULT_REPO_ROOT / "data" / "official_rail_work"


@dataclass(frozen=True)
class ArchiveDiscovery:
    year_url: str
    base_url: str
    month_urls: list[str]
    update_urls: list[str]


@dataclass(frozen=True)
class PipelinePaths:
    repo_root: Path
    work_dir: Path
    downloads_dir: Path
    xml_output_dir: Path
    output_dir: Path
    converter: Path
    sr70: Path
    python_bin: Path
    manifest_path: Path
    summary_path: Path


def now_iso() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(DOWNLOAD_CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def has_xml_files(xml_dir: Path) -> bool:
    return xml_dir.exists() and any(xml_dir.glob("*.xml"))


def extract_links_from_html(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = str(anchor["href"]).strip()
        if not href:
            continue
        links.append(urljoin(base_url, href))

    seen: set[str] = set()
    deduplicated: list[str] = []
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        deduplicated.append(link)
    return deduplicated


def fetch_html(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=HTML_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.text


def discover_remote_archives(
    year: int,
    updates_mode: str,
    session: requests.Session,
) -> ArchiveDiscovery:
    year_url = YEAR_URL_TEMPLATE.format(year=year)
    year_links = extract_links_from_html(fetch_html(session, year_url), year_url)

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
            month_links = extract_links_from_html(fetch_html(session, month_url), month_url)
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


def archive_metadata(
    *,
    url: str | None,
    path: Path,
    reused: bool,
) -> dict[str, Any]:
    return {
        "url": url,
        "local_path": str(path),
        "file_size": path.stat().st_size if path.exists() else 0,
        "sha256": sha256_file(path) if path.exists() else None,
        "downloaded_at": now_iso(),
        "reused": reused,
    }


def download_archive(url: str, destination: Path, session: requests.Session) -> dict[str, Any]:
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists() and destination.stat().st_size > 0:
        return archive_metadata(url=url, path=destination, reused=True)

    temp_file = destination.with_suffix(destination.suffix + ".part")
    if temp_file.exists():
        temp_file.unlink()

    response = session.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT_SECONDS)
    response.raise_for_status()
    with temp_file.open("wb") as file_handle:
        for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
            if chunk:
                file_handle.write(chunk)
    temp_file.replace(destination)

    return archive_metadata(url=url, path=destination, reused=False)


def download_archives(
    discovery: ArchiveDiscovery,
    downloads_dir: Path,
    session: requests.Session,
) -> dict[str, Any]:
    downloads_dir.mkdir(parents=True, exist_ok=True)

    base_meta = download_archive(
        discovery.base_url,
        archive_destination(discovery.base_url, downloads_dir),
        session,
    )
    update_meta: list[dict[str, Any]] = []
    for url in discovery.update_urls:
        update_meta.append(download_archive(url, archive_destination(url, downloads_dir), session))

    return {"base_archive": base_meta, "update_archives": update_meta}


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
        "base_archive": archive_metadata(url=None, path=base_path, reused=True),
        "update_archives": [
            archive_metadata(url=None, path=path, reused=True)
            for path in update_paths
        ],
    }


def build_reuse_xml_download_result(year: int, downloads_dir: Path) -> dict[str, Any]:
    base_archive_path = downloads_dir / f"JR{year}.zip"
    return {
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


def detect_archive_type(archive_path: Path) -> str:
    try:
        with archive_path.open("rb") as file_handle:
            header = file_handle.read(4)
    except OSError:
        return "unknown"

    if header[:2] == b"PK":
        return "zip"
    if header[:2] == b"\x1f\x8b":
        return "gzip"

    suffix = archive_path.suffix.lower()
    if suffix == ".zip":
        return "zip"
    if suffix in {".gz", ".tar"}:
        return "gzip"
    return "unknown"


def register_override(
    output_name: str,
    archive_name: str,
    source_by_file: dict[str, str],
    overrides: list[dict[str, str]],
) -> None:
    previous_source = source_by_file.get(output_name)
    if previous_source is not None:
        overrides.append(
            {
                "file": output_name,
                "replaced_from": previous_source,
                "replaced_by": archive_name,
            }
        )
    source_by_file[output_name] = archive_name


def try_extract_gzip(
    archive_path: Path,
    xml_output_dir: Path,
    source_by_file: dict[str, str],
    overrides: list[dict[str, str]],
) -> int:
    extracted = 0
    try:
        with gzip.open(archive_path, "rb") as gz_file:
            with tarfile.open(fileobj=gz_file, mode="r|gz") as tar_file:
                for member in tar_file:
                    if member.isdir() or not member.name.lower().endswith(".xml"):
                        continue
                    source = tar_file.extractfile(member)
                    if source is None:
                        continue

                    output_name = Path(member.name).name
                    output_path = xml_output_dir / output_name
                    register_override(output_name, archive_path.name, source_by_file, overrides)

                    with source, output_path.open("wb") as target_file:
                        shutil.copyfileobj(source, target_file)
                    extracted += 1
    except (tarfile.TarError, OSError):
        return 0
    return extracted


def try_extract_zip(
    archive_path: Path,
    xml_output_dir: Path,
    source_by_file: dict[str, str],
    overrides: list[dict[str, str]],
) -> int:
    extracted = 0
    try:
        with zipfile.ZipFile(archive_path, "r") as archive:
            for member in archive.infolist():
                if member.is_dir() or not member.filename.lower().endswith(".xml"):
                    continue

                output_name = Path(member.filename).name
                output_path = xml_output_dir / output_name
                register_override(output_name, archive_path.name, source_by_file, overrides)

                with archive.open(member) as source, output_path.open("wb") as target_file:
                    shutil.copyfileobj(source, target_file)
                extracted += 1
    except (zipfile.BadZipFile, OSError):
        return 0
    return extracted


def extract_xml_archive(
    archive_path: Path,
    xml_output_dir: Path,
    source_by_file: dict[str, str],
    overrides: list[dict[str, str]],
) -> int:
    archive_type = detect_archive_type(archive_path)
    if archive_type == "gzip":
        extracted = try_extract_gzip(archive_path, xml_output_dir, source_by_file, overrides)
        if extracted == 0:
            extracted = try_extract_zip(archive_path, xml_output_dir, source_by_file, overrides)
        return extracted
    if archive_type == "zip":
        extracted = try_extract_zip(archive_path, xml_output_dir, source_by_file, overrides)
        if extracted == 0:
            extracted = try_extract_gzip(archive_path, xml_output_dir, source_by_file, overrides)
        return extracted
    return 0


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

    base_extracted = extract_xml_archive(base_archive, xml_output_dir, source_by_file, overrides)
    updates_extracted = 0
    for update_archive in update_archives:
        LOGGER.info("Extracting update archive %s", update_archive)
        updates_extracted += extract_xml_archive(update_archive, xml_output_dir, source_by_file, overrides)

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


def official_gtfs_ready(output_dir: Path) -> bool:
    required = ["routes.txt", "trips.txt", "stops.txt"]
    if not all((output_dir / filename).exists() for filename in required):
        return False
    return (output_dir / "stop_times.txt").exists() or (output_dir / "stop_times.txt.gz").exists()


def convert_official_xml_to_gtfs(paths: PipelinePaths) -> None:
    if not paths.converter.exists():
        raise FileNotFoundError(f"Missing converter: {paths.converter}")
    if not paths.sr70.exists():
        raise FileNotFoundError(f"Missing SR70 file: {paths.sr70}")
    if not paths.python_bin.exists():
        raise FileNotFoundError(f"Missing Python binary: {paths.python_bin}")
    if not has_xml_files(paths.xml_output_dir):
        raise FileNotFoundError(f"No XML files found in: {paths.xml_output_dir}")

    if paths.output_dir.exists():
        shutil.rmtree(paths.output_dir)
    paths.output_dir.mkdir(parents=True, exist_ok=True)

    command = [
        str(paths.python_bin),
        str(paths.converter),
        str(paths.xml_output_dir),
        str(paths.output_dir),
        "--sr70",
        str(paths.sr70),
    ]
    LOGGER.info("Running converter: %s", " ".join(command))
    subprocess.run(command, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download official Czech rail archives and convert them to GTFS."
    )
    parser.add_argument("--year", type=int, default=2026, help="Official timetable year.")
    parser.add_argument("--repo-root", type=Path, default=DEFAULT_REPO_ROOT, help="Repository root.")
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=DEFAULT_WORK_DIR,
        help="Working directory for downloads, extracted XML and output GTFS.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output GTFS directory. Default: <work-dir>/official_gtfs/<year>",
    )
    parser.add_argument(
        "--python-bin",
        type=Path,
        default=Path(sys.executable),
        help="Python binary used to execute the converter.",
    )
    parser.add_argument(
        "--converter",
        type=Path,
        default=None,
        help="Path to czptt2gtfs converter script. Default: <repo-root>/czptt2gtfs/czptt2gtfs.py",
    )
    parser.add_argument(
        "--sr70",
        type=Path,
        default=None,
        help="Path to SR70 CSV file. Default: <repo-root>/czptt2gtfs/data/sr70.csv",
    )
    parser.add_argument(
        "--updates-mode",
        choices=["all", "none"],
        default="all",
        help="Whether to include all monthly update archives.",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Reuse previously downloaded archives / extracted XML.",
    )
    parser.add_argument(
        "--skip-convert",
        action="store_true",
        help="Skip conversion and only validate existing GTFS output.",
    )
    return parser.parse_args()


def resolve_paths(args: argparse.Namespace) -> PipelinePaths:
    repo_root = args.repo_root.resolve()
    work_dir = args.work_dir.resolve()
    downloads_dir = work_dir / "downloads" / str(args.year)
    xml_output_dir = work_dir / "xml_merged" / str(args.year)
    output_dir = (args.output_dir or (work_dir / "official_gtfs" / str(args.year))).resolve()
    converter = (args.converter or (repo_root / "czptt2gtfs" / "czptt2gtfs.py")).resolve()
    sr70 = (args.sr70 or (repo_root / "czptt2gtfs" / "data" / "sr70.csv")).resolve()
    python_bin = args.python_bin.expanduser().absolute()
    manifest_path = downloads_dir / "manifest.json"
    summary_path = output_dir / "processing_summary.json"
    return PipelinePaths(
        repo_root=repo_root,
        work_dir=work_dir,
        downloads_dir=downloads_dir,
        xml_output_dir=xml_output_dir,
        output_dir=output_dir,
        converter=converter,
        sr70=sr70,
        python_bin=python_bin,
        manifest_path=manifest_path,
        summary_path=summary_path,
    )


def resolve_download_result(
    *,
    year: int,
    updates_mode: str,
    skip_download: bool,
    downloads_dir: Path,
    xml_output_dir: Path,
) -> tuple[ArchiveDiscovery | None, dict[str, Any]]:
    has_local_xml = has_xml_files(xml_output_dir)

    if skip_download:
        LOGGER.info("Skipping download phase, using local artifacts from %s", downloads_dir)
        base_archive_path = downloads_dir / f"JR{year}.zip"
        if base_archive_path.exists():
            return None, load_local_archives(year, downloads_dir, updates_mode)
        if has_local_xml:
            LOGGER.info("No local archives found, reusing existing XML files in %s", xml_output_dir)
            return None, build_reuse_xml_download_result(year, downloads_dir)
        raise FileNotFoundError(
            f"Missing local archives in {downloads_dir}. Provide downloads or run without --skip-download."
        )

    LOGGER.info("Discovering official archives for year %s", year)
    with requests.Session() as session:
        discovery = discover_remote_archives(year, updates_mode, session)
        LOGGER.info("Downloading archives: 1 base + %s updates", len(discovery.update_urls))
        download_result = download_archives(discovery, downloads_dir, session)
    return discovery, download_result


def resolve_extraction_log(
    *,
    skip_download: bool,
    xml_output_dir: Path,
    base_archive: Path,
    update_archives: list[Path],
) -> dict[str, Any]:
    if skip_download and has_xml_files(xml_output_dir):
        return {
            "xml_output_dir": str(xml_output_dir),
            "reused_existing_xml_dir": True,
            "final_xml_file_count": len(list(xml_output_dir.glob("*.xml"))),
            "overrides_count": 0,
            "overrides": [],
        }

    LOGGER.info("Extracting and merging XML files into %s", xml_output_dir)
    return extract_and_merge_xml_archives(base_archive, update_archives, xml_output_dir)


def build_manifest_payload(
    *,
    year: int,
    updates_mode: str,
    discovery: ArchiveDiscovery | None,
    download_result: dict[str, Any],
    extraction_log: dict[str, Any],
) -> dict[str, Any]:
    return {
        "generated_at": now_iso(),
        "year": year,
        "updates_mode": updates_mode,
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


def build_summary_payload(args: argparse.Namespace, paths: PipelinePaths) -> dict[str, Any]:
    return {
        "generated_at": now_iso(),
        "year": args.year,
        "work_dir": str(paths.work_dir),
        "xml_output_dir": str(paths.xml_output_dir),
        "official_gtfs_dir": str(paths.output_dir),
        "manifest_path": str(paths.manifest_path),
        "updates_mode": args.updates_mode,
        "skip_download": bool(args.skip_download),
        "skip_convert": bool(args.skip_convert),
        "official_gtfs_ready": official_gtfs_ready(paths.output_dir),
    }


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    paths = resolve_paths(args)

    discovery, download_result = resolve_download_result(
        year=args.year,
        updates_mode=args.updates_mode,
        skip_download=args.skip_download,
        downloads_dir=paths.downloads_dir,
        xml_output_dir=paths.xml_output_dir,
    )

    base_archive = Path(download_result["base_archive"]["local_path"])
    update_archives = [Path(item["local_path"]) for item in download_result["update_archives"]]

    extraction_log = resolve_extraction_log(
        skip_download=args.skip_download,
        xml_output_dir=paths.xml_output_dir,
        base_archive=base_archive,
        update_archives=update_archives,
    )

    manifest_payload = build_manifest_payload(
        year=args.year,
        updates_mode=args.updates_mode,
        discovery=discovery,
        download_result=download_result,
        extraction_log=extraction_log,
    )
    write_json(paths.manifest_path, manifest_payload)

    if args.skip_convert:
        LOGGER.info("Skipping conversion phase, validating GTFS output in %s", paths.output_dir)
        if not official_gtfs_ready(paths.output_dir):
            raise FileNotFoundError(
                f"Official GTFS not ready in {paths.output_dir}. Run without --skip-convert."
            )
    else:
        LOGGER.info("Converting official XML to GTFS into %s", paths.output_dir)
        convert_official_xml_to_gtfs(paths)

    summary_payload = build_summary_payload(args, paths)
    write_json(paths.summary_path, summary_payload)
    LOGGER.info("Done. GTFS output=%s summary=%s", paths.output_dir, paths.summary_path)


if __name__ == "__main__":
    main()
