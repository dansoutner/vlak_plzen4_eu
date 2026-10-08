#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime
import logging
import re
from pathlib import Path
import xml.etree.ElementTree as ET

import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

EXC_ADD = 1
EXC_REMOVE = 2
ACT_STOP = "0001"
ACT_ON_ONLY = "0028"
ACT_OFF_ONLY = "0029"
TIME_RE = re.compile(r"\d{2}:\d{2}:\d{2}\.\d+[+-]\d{2}:\d{2}")

PACKAGE_DIR = Path(__file__).resolve().parent
DEFAULT_KOMERCNI_DRUHY_PATH = PACKAGE_DIR / "data" / "komercni_druhy.xml"
DEFAULT_SR70_PATH = PACKAGE_DIR / "data" / "sr70.csv"

KOMERCNI_DRUHY: dict[str, str] = {}
SR70: dict[int, dict[str, str]] = {}
calendars: dict[frozenset[datetime.date], "Calendar"] = {}


def load_komercni_druhy(path: Path) -> dict[str, str]:
    root = ET.parse(path).getroot()
    return {
        elem.attrib["KodTAF"]: elem.attrib["Kod"]
        for elem in root.findall(".//{http://provoz.szdc.cz/kadr}KomercniDruhVlaku")
    }


def load_sr70(path: Path) -> dict[int, dict[str, str]]:
    mapping: dict[int, dict[str, str]] = {}
    with path.open(newline="", encoding="utf-8") as sr70_file:
        for row in csv.DictReader(sr70_file):
            kod = int(row["SR70"][:-1])  # odebereme koncovou kontrolní číslici
            mapping[kod] = row
    return mapping


class Calendar:
    def __init__(self, cal_elem):
        # Načte kalendář a vrátí ho jako množinu objektů typu date
        bitmap = cal_elem.find("BitmapDays").text.strip()
        start = cal_elem.find("ValidityPeriod/StartDateTime").text.strip()

        if not start.endswith("T00:00:00"):
            raise Exception
        start = datetime.date.fromisoformat(start.split("T")[0])

        if bitmap != "1":  # u vlaků, které jedou jen jeden den, chybí EndDateTime
            end = cal_elem.find("ValidityPeriod/EndDateTime").text.strip()
            if not end.endswith("T00:00:00"):
                raise Exception
            end = datetime.date.fromisoformat(end.split("T")[0])
            if start + datetime.timedelta(days=len(bitmap) - 1) != end:
                raise Exception("Nesedí EndDateTime")

        r = set()
        for offset, active in enumerate(bitmap):
            if active == "1":
                r.add(start + datetime.timedelta(days=offset))
        self.start = start
        self.end = start + datetime.timedelta(days=offset)
        self.dates = frozenset(r)
        self.bitmap = bitmap

    def without(self, removed: set[datetime.date] | frozenset[datetime.date]) -> "Calendar":
        """Kopie kalendáře bez zadaných dnů (období platnosti zůstává stejné)."""
        cal = Calendar.__new__(Calendar)
        cal.start = self.start
        cal.end = self.end
        cal.dates = self.dates - removed
        cal.bitmap = None
        return cal

    @property
    def service_interval(self):
        cur = self.start
        while cur <= self.end:
            yield cur
            cur += datetime.timedelta(1)

    def guess_weekdays(self):
        wd_active = {}
        wd_inactive = {}
        for date in self.service_interval:
            active = date in self.dates
            wd = date.weekday()
            dct = wd_active if active else wd_inactive
            dct.setdefault(wd, 0)
            dct[wd] += 1
        return {wd for wd in range(7) if wd_active.get(wd, 0) > wd_inactive.get(wd, 0)}

    def exceptions(self, regular_wd=None):
        if regular_wd is None:
            regular_wd = self.guess_weekdays()
        r = []
        for date in self.service_interval:
            active = date in self.dates
            wd = date.weekday()
            regular_active = wd in regular_wd
            if active != regular_active:
                r.append((date, EXC_ADD if active else EXC_REMOVE))
        return r


def load_calendar(
    cal_elem, calendars_map: dict[frozenset[datetime.date], "Calendar"] | None = None
) -> "Calendar":
    if calendars_map is None:
        calendars_map = {}
    cal = Calendar(cal_elem)
    if cal.dates in calendars_map:  # stejný kalendář (se stejnou množinou dnů) už jsme viděli, zrecyklujeme ho
        return calendars_map[cal.dates]
    calendars_map[cal.dates] = cal
    return cal


def parse_timing(elem):
    if elem is None:
        return None
    val = elem.find("Time").text
    # Čas je místní; posun bývá +01:00 (základní JŘ) nebo +02:00 (opravy v letním čase) při stejném místním čase.
    if not TIME_RE.fullmatch(val):
        raise ValueError(f"Neočekávaný formát času: {val}")
    return datetime.time.fromisoformat(val.split(".")[0])


def parse_message_time(text: str | None) -> datetime.datetime | None:
    if not text:
        return None
    try:
        return datetime.datetime.fromisoformat(text.strip())
    except ValueError:
        return None


def pa_key_from(ident_elem) -> tuple[str, str]:
    return ident_elem.find("Core").text, ident_elem.find("Variant").text


class Cancellation:
    """Zpráva CZCanceledPTTMessage: zrušení jízdy daného PA ve vybrané dny."""

    def __init__(self, root):
        self.pa_key = pa_key_from(root.find("PlannedTransportIdentifiers[ObjectType='PA']"))
        self.created = parse_message_time(root.findtext("CZPTTCancelation"))
        self.dates = Calendar(root.find("PlannedCalendar")).dates


class Train:
    def __init__(self, file: Path, root=None):
        if root is None:
            root = ET.parse(file).getroot()
        pa_elem = root.find("Identifiers/PlannedTransportIdentifiers[ObjectType='PA']")
        tr_elem = root.find("Identifiers/PlannedTransportIdentifiers[ObjectType='TR']")
        pa_core, pa_variant = pa_key_from(pa_elem)
        tr_core, tr_variant = pa_key_from(tr_elem)
        # Jízda je jednoznačně určena PA (Path Assignment). Náhradní jízdy z měsíčních oprav mají vlastní PA,
        # ale mohou sdílet TR s jinými PA, proto TR jako klíč nestačí.
        self.pa_key = (pa_core, pa_variant)
        self.created = parse_message_time(root.findtext("CZPTTCreation"))
        cal_elem = root.find("CZPTTInformation/PlannedCalendar")
        self.calendar = Calendar(cal_elem)
        self.id = pa_core.strip("-").lstrip("0").rstrip("A") + (
            "-" + pa_variant.lstrip("0") if int(pa_variant) else ""
        )
        self.id_core = tr_core
        self.id_variant = tr_variant
        stops = []

        number = None
        com_type = None

        for loc in root.findall("CZPTTInformation/CZPTTLocation"):
            code = int(loc.find("Location/LocationPrimaryCode").text)
            country = loc.find("Location/CountryCodeISO").text.upper()
            if country != "CZ":
                # Pro jednoduchost přeskočíme všechny body mimo území ČR
                continue
            if code not in SR70:
                # Přeskočíme zastávky, které nejsou v SR70
                logger.warning("Location code %d not found in SR70", code)
                continue
            sr70 = SR70[code]
            name = sr70["Tarifní název"]
            activities = [x.text for x in loc.findall("TrainActivity/TrainActivityType")]
            if ACT_STOP not in activities:
                continue
            arr = parse_timing(loc.find("TimingAtLocation/Timing[@TimingQualifierCode='ALA']"))
            dep = parse_timing(loc.find("TimingAtLocation/Timing[@TimingQualifierCode='ALD']"))
            if arr is None and dep is not None:
                arr = dep
            if dep is None and arr is not None:
                dep = arr
            number_elem = loc.find("OperationalTrainNumber")
            if number is None and number_elem is not None:  # bereme první číslo vlaku, změny po cestě neřešíme
                number = int(number_elem.text)
                traffic_type_elem = loc.find("CommercialTrafficType")
                if traffic_type_elem is not None and traffic_type_elem.text in KOMERCNI_DRUHY:
                    com_type = KOMERCNI_DRUHY[traffic_type_elem.text]
                else:
                    com_type = "unknown"
            stops.append((code, name, arr, dep))

        name_elem = root.find("NetworkSpecificParameter[Name='CZTrainName']")
        if name_elem is not None:
            name = name_elem.find("Value").text
        else:
            name = ""

        self.number = number
        self.com_type = com_type
        self.name = name
        self.short_name = f"{com_type} {number}"
        if stops and name:
            self.long_name = f"{name} ({stops[0][1]} - {stops[-1][1]})"
        elif stops:
            self.long_name = f"{stops[0][1]} - {stops[-1][1]}"
        else:
            self.long_name = name

        self.stops = stops


def normalize_name(name: str) -> str:
    """
    Convert to hl.n.
    """
    return name.replace("hlavní nádraží", "hl.n.")


def convert_gps(s):
    # převede gps z formátu stupně-minuty-vteřiny na desetinné stupně, které očekává GTFS
    s = s.strip()
    if not s or s[0] not in "NE":
        # Pokud chybí GPS souřadnice, vrátíme None
        return None
    deg, rest = s[1:].split("°")
    minutes, seconds = rest.rstrip('"').split("'")
    try:
        deg = int(deg)
    except ValueError:
        logger.warning("Invalid GPS coordinate: %s", s)
        return None
    minutes = int(minutes or "0")
    seconds = float(seconds.replace(",", ".") or "0")
    return deg + minutes / 60 + seconds / 3600


def gtfs_date(date):
    return date.strftime("%Y%m%d")


def gtfs_time(time_value: datetime.time, *, state: dict[str, object], train_short_name: str) -> str:
    last_time = state["last_time"]
    jumped_midnight = state["jumped_midnight"]
    if last_time is not None and time_value < last_time and not jumped_midnight:
        logger.info("midnight jump for %s: %s -> %s", train_short_name, last_time, time_value)
        jumped_midnight = True
    state["last_time"] = time_value
    state["jumped_midnight"] = jumped_midnight

    hour = time_value.hour
    minute = time_value.minute
    second = time_value.second
    if jumped_midnight:
        hour += 24  # časy po půlnoci je třeba zapsat jako e.g. 24:30:00
    return f"{hour}:{minute:02d}:{second:02d}"


def apply_cancellations(trains_by_pa: dict[tuple[str, str], Train], cancellations: list[Cancellation]) -> None:
    applied = unmatched = superseded = removed_days = 0
    for cancellation in cancellations:
        train = trains_by_pa.get(cancellation.pa_key)
        if train is None:
            unmatched += 1  # typicky PA s jedinou zastávkou v ČR, které jsme přeskočili
            continue
        if cancellation.created and train.created and cancellation.created < train.created:
            superseded += 1  # PA bylo po zrušení vydáno znovu, platí novější verze
            continue
        removed = train.calendar.dates & cancellation.dates
        if removed:
            train.calendar = train.calendar.without(removed)
            removed_days += len(removed)
        applied += 1
    logger.info(
        "Zrušení: %d aplikováno (%d dní jízd odebráno), %d bez odpovídajícího PA, %d nahrazeno novější verzí PA",
        applied,
        removed_days,
        unmatched,
        superseded,
    )


def log_overlapping_trains(trains) -> None:
    """Upozorní na vlaky se stejným číslem, které jedou ve stejný den ve více variantách (PA)."""
    by_number: dict[int | None, list[Train]] = {}
    for train in trains:
        by_number.setdefault(train.number, []).append(train)
    overlapping = 0
    for number, variants in by_number.items():
        for i, first in enumerate(variants):
            for second in variants[i + 1 :]:
                shared = first.calendar.dates & second.calendar.dates
                if shared and {s[0] for s in first.stops} & {s[0] for s in second.stops}:
                    overlapping += 1
                    logger.debug("Vlak %s jede %d dní ve variantách %s a %s", number, len(shared), first.id, second.id)
    if overlapping:
        logger.warning("%d dvojic variant téhož vlaku se překrývá v kalendáři i trase", overlapping)


def run_conversion(
    input_dir: Path,
    output_dir: Path,
    *,
    sr70_path: Path,
    komercni_druhy_path: Path,
) -> None:
    global SR70, KOMERCNI_DRUHY, calendars
    SR70 = load_sr70(sr70_path)
    KOMERCNI_DRUHY = load_komercni_druhy(komercni_druhy_path)
    calendars = {}

    output_dir.mkdir(parents=True, exist_ok=True)

    trains_by_pa: dict[tuple[str, str], Train] = {}
    invalid_files = 0
    cancellations: list[Cancellation] = []

    xml_files = [
        file
        for file in sorted(input_dir.iterdir())
        if file.is_file() and file.suffix.lower() == ".xml"
    ]
    for file in tqdm.tqdm(xml_files):
        logger.debug("Processing %s", file)
        root = ET.parse(file).getroot()
        if root.tag == "CZCanceledPTTMessage":
            cancellations.append(Cancellation(root))
            continue
        try:
            train = Train(file, root)
        except Exception:
            invalid_files += 1
            logger.warning("Nelze zpracovat %s", file, exc_info=True)
            continue
        if len(train.stops) <= 1:
            # Vlak s jednou zastávkou nemá smysl. Typicky mezinárodní vlak, který stojí na jediném místě v ČR.
            continue
        previous = trains_by_pa.get(train.pa_key)
        if previous is not None and previous.created and train.created and previous.created > train.created:
            continue
        trains_by_pa[train.pa_key] = train

    if invalid_files:
        logger.warning("Přeskočeno %d nezpracovatelných XML souborů", invalid_files)
    apply_cancellations(trains_by_pa, cancellations)

    trains: dict[str, Train] = {}
    for train in trains_by_pa.values():
        if not train.calendar.dates:
            continue  # všechny dny jízdy byly zrušeny
        if train.id in trains:
            raise ValueError(f"Duplicitní trip_id {train.id} pro PA {train.pa_key}")
        trains[train.id] = train
    log_overlapping_trains(trains.values())

    # Stejné kalendáře (stejná množina dnů) sdílí jedno service_id
    for train in trains.values():
        train.calendar = calendars.setdefault(train.calendar.dates, train.calendar)

    all_stops = {stop[0] for train in trains.values() for stop in train.stops}

    # manuální souřadnice pro stanice, kterým v SR70 chybí
    gps_override = {
        # chybí v SR70
        54225: (50.3214320, 14.9444698),  # Luštěnice, zdroj: OSM
        54688: (50.2574, 14.5076),
        # Neratovice sídliště, zdroj: https://cs.wikipedia.org/wiki/Neratovice_s%C3%ADdli%C5%A1t%C4%9B
        # chybné souřadnice v SR70
        54564: (50.1529201, 15.0288512),  # Hořátev, zdroj: OSM
        74855: (49.7860106, 13.1299738),  # Pňovany zastávka, zdroj: OSM
    }

    with (output_dir / "stops.txt").open("w", newline="", encoding="utf-8") as fh:
        wr = csv.DictWriter(fh, ["stop_id", "stop_name", "stop_lat", "stop_lon", "location_type"])
        wr.writeheader()
        for code in sorted(all_stops):
            sr70 = SR70[code]
            if code in gps_override:
                pos = gps_override[code]
            else:
                gps_y = convert_gps(sr70[f"GPS Y"])
                gps_x = convert_gps(sr70[f"GPS X"])
                if gps_y is None or gps_x is None:
                    logger.warning("Missing GPS coordinates for stop %d (%s)", code, sr70["Tarifní název"])
                    continue
                pos = (gps_y, gps_x)
            wr.writerow(
                {
                    "stop_id": str(code),
                    "stop_name": normalize_name(sr70["Tarifní název"]),
                    "stop_lat": str(pos[0]),
                    "stop_lon": str(pos[1]),
                    "location_type": "0",
                }
            )

    train_list = sorted(trains.values(), key=lambda train: (train.number or 0, train.id_variant, train.id))
    # Protože vlaky nemají linky v konvenčním smyslu, uděláme pro každý vlak vlastní route
    with (output_dir / "routes.txt").open("w", newline="", encoding="utf-8") as fh:
        wr = csv.DictWriter(fh, ["route_id", "route_short_name", "route_long_name", "route_type"])
        wr.writeheader()
        for train in train_list:
            wr.writerow(
                {
                    "route_id": train.id,
                    "route_short_name": train.short_name,
                    "route_long_name": train.long_name,
                    "route_type": "2",
                }
            )

    # Přiřadíme všem kalendářům identifikační čísla
    for idx, cal in enumerate(calendars.values()):
        cal.id = idx

    with (output_dir / "trips.txt").open("w", newline="", encoding="utf-8") as fh:
        wr = csv.DictWriter(fh, ["route_id", "service_id", "trip_id"])
        wr.writeheader()
        for train in train_list:
            wr.writerow({"route_id": train.id, "service_id": str(train.calendar.id), "trip_id": train.id})

    with (output_dir / "stop_times.txt").open("w", newline="", encoding="utf-8") as fh:
        wr = csv.DictWriter(fh, ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"])
        wr.writeheader()
        for train in train_list:
            time_state: dict[str, object] = {"last_time": None, "jumped_midnight": False}
            for idx, (code, _name, arr, dep) in enumerate(train.stops):
                arr_time = gtfs_time(arr, state=time_state, train_short_name=train.short_name)
                dep_time = gtfs_time(dep, state=time_state, train_short_name=train.short_name)
                wr.writerow(
                    {
                        "trip_id": train.id,
                        "arrival_time": arr_time,
                        "departure_time": dep_time,
                        "stop_id": str(code),
                        "stop_sequence": idx + 1,
                    }
                )

    with (output_dir / "calendar.txt").open("w", newline="", encoding="utf-8") as fh:
        wdays = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        wr = csv.DictWriter(fh, ["service_id"] + wdays + ["start_date", "end_date"])
        wr.writeheader()
        for cal in calendars.values():
            row = {"service_id": str(cal.id), "start_date": gtfs_date(cal.start), "end_date": gtfs_date(cal.end)}
            regular_wds = cal.guess_weekdays()
            for wd, colname in enumerate(wdays):
                row[colname] = int(bool(wd in regular_wds))
            wr.writerow(row)

    with (output_dir / "calendar_dates.txt").open("w", newline="", encoding="utf-8") as fh:
        wr = csv.DictWriter(fh, ["service_id", "date", "exception_type"])
        wr.writeheader()
        for cal in calendars.values():
            for date, exc_type in cal.exceptions():
                wr.writerow({"service_id": cal.id, "date": gtfs_date(date), "exception_type": str(exc_type)})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Konverze jízdních řádů z CZPTT do GTFS")
    parser.add_argument("input_dir", help="Dir with unzipped XML files")
    parser.add_argument("output_dir", help="Dir for GTFS output")
    parser.add_argument(
        "--sr70",
        help="CSV file with SR70 codes",
        default=str(DEFAULT_SR70_PATH),
    )
    parser.add_argument(
        "--komercni-druhy",
        help="XML file with CommercialTrafficType mapping",
        default=str(DEFAULT_KOMERCNI_DRUHY_PATH),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    run_conversion(
        Path(args.input_dir),
        Path(args.output_dir),
        sr70_path=Path(args.sr70),
        komercni_druhy_path=Path(args.komercni_druhy),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
