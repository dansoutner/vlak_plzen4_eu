#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime
import logging
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

PACKAGE_DIR = Path(__file__).resolve().parent
DEFAULT_KOMERCNI_DRUHY_PATH = PACKAGE_DIR / "data" / "komercni_druhy.xml"
DEFAULT_SR70_PATH = PACKAGE_DIR / "sr70.csv"

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
    if not val.endswith(".0000000+01:00"):
        raise Exception
    return datetime.time.fromisoformat(val.split(".")[0])


class Train:
    def __init__(self, file: Path):
        tree = ET.parse(file)
        root = tree.getroot()
        pa_elem = root.find("Identifiers/PlannedTransportIdentifiers[ObjectType='PA']")
        tr_elem = root.find("Identifiers/PlannedTransportIdentifiers[ObjectType='TR']")
        pa_core = pa_elem.find("Core").text
        tr_core = tr_elem.find("Core").text
        pa_variant = pa_elem.find("Variant").text
        tr_variant = tr_elem.find("Variant").text
        if pa_core != tr_core or pa_variant != tr_variant:
            logger.warning(
                "PA_ID != TR_ID není podporováno (typicky se vyskytuje u výlukových jízdních řádů) - %s",
                file,
            )
        cal_elem = root.find("CZPTTInformation/PlannedCalendar")
        self.calendar = load_calendar(cal_elem, calendars_map=calendars)
        self.id = tr_core.strip("-").lstrip("0").rstrip("A") + (
            "-" + tr_variant.lstrip("0") if int(tr_variant) else ""
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
            if number is None:  # bereme první číslo vlaku, změny po cestě neřešíme
                number = int(loc.find("OperationalTrainNumber").text)
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

    cals_for_core: dict[str, set[datetime.date]] = {}
    trains: dict[str, Train] = {}

    xml_files = [
        file
        for file in sorted(input_dir.iterdir())
        if file.is_file() and file.suffix.lower() == ".xml"
    ]
    for file in tqdm.tqdm(xml_files):
        logger.debug("Processing %s", file)
        train = Train(file)
        if len(train.stops) <= 1:
            # Vlak s jednou zastávkou nemá smysl. Typicky mezinárodní vlak, který stojí na jediném místě v ČR.
            continue
        cfc = cals_for_core.setdefault(train.id_core, set())
        if train.calendar.dates & cfc:
            logger.warning(
                "VAROVÁNÍ: Překrývající se kalendáře pro varianty core id %s (při přidávání varianty %s), průnik %r",
                train.id_core,
                train.id_variant,
                (train.calendar.dates & cfc),
            )
            continue
        cfc |= train.calendar.dates
        trains[train.id] = train

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

    train_list = sorted(trains.values(), key=lambda train: (train.number, train.id_variant))
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
