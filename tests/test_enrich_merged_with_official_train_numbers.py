import csv
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from scripts import enrich_merged_with_official_train_numbers as enrich


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class DiscoveryTests(unittest.TestCase):
    def test_discover_remote_archives_base_and_all_updates(self):
        year = 2026
        year_url = enrich.YEAR_URL_TEMPLATE.format(year=year)
        month_1 = year_url + "2026-01/"
        month_2 = year_url + "2026-02/"
        html_by_url = {
            year_url: """
                <html><body>
                    <a href="JR2026.zip">JR2026.zip</a>
                    <a href="2026-02/">2026-02/</a>
                    <a href="2026-01/">2026-01/</a>
                </body></html>
            """,
            month_1: """
                <html><body>
                    <a href="u2.zip">u2.zip</a>
                    <a href="u1.zip">u1.zip</a>
                </body></html>
            """,
            month_2: """
                <html><body>
                    <a href="x.zip">x.zip</a>
                </body></html>
            """,
        }
        discovery = enrich.discover_remote_archives(
            year=year,
            updates_mode="all",
            fetch_html=lambda url: html_by_url[url],
        )
        self.assertEqual(discovery.base_url, year_url + "JR2026.zip")
        self.assertEqual(discovery.month_urls, [month_1, month_2])
        self.assertEqual(
            discovery.update_urls,
            [month_1 + "u1.zip", month_1 + "u2.zip", month_2 + "x.zip"],
        )


class PrimitiveTests(unittest.TestCase):
    def test_normalize_stop_name_variants(self):
        self.assertEqual(
            enrich.normalize_stop_name("Plzeň hlavní nádraží"),
            "plzen hl.n.",
        )
        self.assertEqual(
            enrich.normalize_stop_name("Plzeň hl. n."),
            "plzen hl.n.",
        )

    def test_parse_train_label(self):
        self.assertEqual(enrich.parse_train_label("Os 7806"), "Os 7806")
        self.assertEqual(enrich.parse_train_label("rj 070"), "rj 70")
        self.assertIsNone(enrich.parse_train_label("Berounka"))

    def test_choose_train_label_thresholds(self):
        matched = enrich.choose_train_label(enrich.Counter({"Os 7806": 5, "Sp 1706": 2}))
        self.assertEqual(matched["status"], "matched")
        self.assertEqual(matched["label"], "Os 7806")

        low_support = enrich.choose_train_label(enrich.Counter({"Os 7806": 1}))
        self.assertEqual(low_support["status"], "ambiguous")
        self.assertEqual(low_support["reason"], "low_support")

        tie = enrich.choose_train_label(enrich.Counter({"Os 7806": 2, "Sp 1706": 2}))
        self.assertEqual(tie["status"], "ambiguous")
        self.assertEqual(tie["reason"], "tie")


class IntegrationTests(unittest.TestCase):
    def test_enrichment_assigns_unique_and_reports_others(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            merged_dir = tmp_path / "merged"
            official_dir = tmp_path / "official"
            output_dir = tmp_path / "output"

            write_csv(
                merged_dir / "routes.txt",
                ["route_id", "route_type"],
                [
                    {"route_id": "R1", "route_type": "2"},
                    {"route_id": "R2", "route_type": "2"},
                    {"route_id": "R3", "route_type": "2"},
                    {"route_id": "RBUS", "route_type": "3"},
                ],
            )
            write_csv(
                merged_dir / "trips.txt",
                ["trip_id", "route_id", "service_id", "trip_short_name"],
                [
                    {"trip_id": "T1", "route_id": "R1", "service_id": "SV1", "trip_short_name": ""},
                    {"trip_id": "T2", "route_id": "R2", "service_id": "SV1", "trip_short_name": ""},
                    {"trip_id": "T3", "route_id": "R3", "service_id": "SV1", "trip_short_name": ""},
                    {"trip_id": "T4", "route_id": "RBUS", "service_id": "SV1", "trip_short_name": "BUS 1"},
                ],
            )
            write_csv(
                merged_dir / "stops.txt",
                ["stop_id", "stop_name"],
                [
                    {"stop_id": "S1", "stop_name": "Alpha"},
                    {"stop_id": "S2", "stop_name": "Beta"},
                    {"stop_id": "S3", "stop_name": "Gamma"},
                    {"stop_id": "S4", "stop_name": "Delta"},
                    {"stop_id": "S5", "stop_name": "Epsilon"},
                    {"stop_id": "S6", "stop_name": "Zeta"},
                    {"stop_id": "S7", "stop_name": "Eta"},
                    {"stop_id": "S8", "stop_name": "Theta"},
                    {"stop_id": "S9", "stop_name": "Bus A"},
                    {"stop_id": "S10", "stop_name": "Bus B"},
                ],
            )
            write_csv(
                merged_dir / "calendar.txt",
                [
                    "service_id",
                    "monday",
                    "tuesday",
                    "wednesday",
                    "thursday",
                    "friday",
                    "saturday",
                    "sunday",
                ],
                [
                    {
                        "service_id": "SV1",
                        "monday": "1",
                        "tuesday": "1",
                        "wednesday": "0",
                        "thursday": "0",
                        "friday": "0",
                        "saturday": "0",
                        "sunday": "0",
                    }
                ],
            )
            write_csv(
                merged_dir / "stop_times.txt",
                ["trip_id", "stop_id", "stop_sequence", "departure_time"],
                [
                    {"trip_id": "T1", "stop_id": "S1", "stop_sequence": "1", "departure_time": "10:00:00"},
                    {"trip_id": "T1", "stop_id": "S2", "stop_sequence": "2", "departure_time": "10:05:00"},
                    {"trip_id": "T1", "stop_id": "S3", "stop_sequence": "3", "departure_time": "10:10:00"},
                    {"trip_id": "T2", "stop_id": "S4", "stop_sequence": "1", "departure_time": "11:00:00"},
                    {"trip_id": "T2", "stop_id": "S5", "stop_sequence": "2", "departure_time": "11:05:00"},
                    {"trip_id": "T2", "stop_id": "S6", "stop_sequence": "3", "departure_time": "11:10:00"},
                    {"trip_id": "T3", "stop_id": "S7", "stop_sequence": "1", "departure_time": "12:00:00"},
                    {"trip_id": "T3", "stop_id": "S8", "stop_sequence": "2", "departure_time": "12:05:00"},
                    {"trip_id": "T4", "stop_id": "S9", "stop_sequence": "1", "departure_time": "13:00:00"},
                    {"trip_id": "T4", "stop_id": "S10", "stop_sequence": "2", "departure_time": "13:10:00"},
                ],
            )

            write_csv(
                official_dir / "routes.txt",
                ["route_id", "route_short_name"],
                [
                    {"route_id": "OR1", "route_short_name": "Os 100"},
                    {"route_id": "OR2", "route_short_name": "Sp 200"},
                    {"route_id": "OR3", "route_short_name": "R 300"},
                ],
            )
            write_csv(
                official_dir / "trips.txt",
                ["trip_id", "route_id", "service_id"],
                [
                    {"trip_id": "OT1", "route_id": "OR1", "service_id": "OSV1"},
                    {"trip_id": "OT2", "route_id": "OR2", "service_id": "OSV1"},
                    {"trip_id": "OT3", "route_id": "OR3", "service_id": "OSV1"},
                ],
            )
            write_csv(
                official_dir / "stops.txt",
                ["stop_id", "stop_name"],
                [
                    {"stop_id": "O1", "stop_name": "Alpha"},
                    {"stop_id": "O2", "stop_name": "Beta"},
                    {"stop_id": "O3", "stop_name": "Gamma"},
                    {"stop_id": "O4", "stop_name": "Delta"},
                    {"stop_id": "O5", "stop_name": "Epsilon"},
                    {"stop_id": "O6", "stop_name": "Zeta"},
                ],
            )
            write_csv(
                official_dir / "calendar.txt",
                [
                    "service_id",
                    "monday",
                    "tuesday",
                    "wednesday",
                    "thursday",
                    "friday",
                    "saturday",
                    "sunday",
                ],
                [
                    {
                        "service_id": "OSV1",
                        "monday": "1",
                        "tuesday": "1",
                        "wednesday": "0",
                        "thursday": "0",
                        "friday": "0",
                        "saturday": "0",
                        "sunday": "0",
                    }
                ],
            )
            write_csv(
                official_dir / "stop_times.txt",
                ["trip_id", "stop_id", "stop_sequence", "departure_time"],
                [
                    {"trip_id": "OT1", "stop_id": "O1", "stop_sequence": "1", "departure_time": "10:00:00"},
                    {"trip_id": "OT1", "stop_id": "O2", "stop_sequence": "2", "departure_time": "10:05:00"},
                    {"trip_id": "OT1", "stop_id": "O3", "stop_sequence": "3", "departure_time": "10:10:00"},
                    {"trip_id": "OT2", "stop_id": "O4", "stop_sequence": "1", "departure_time": "11:00:00"},
                    {"trip_id": "OT2", "stop_id": "O5", "stop_sequence": "2", "departure_time": "11:05:00"},
                    {"trip_id": "OT3", "stop_id": "O4", "stop_sequence": "1", "departure_time": "11:00:00"},
                    {"trip_id": "OT3", "stop_id": "O5", "stop_sequence": "2", "departure_time": "11:05:00"},
                ],
            )

            summary = enrich.enrich_merged_feed(merged_dir, official_dir, output_dir)
            self.assertEqual(summary["matched_count"], 1)
            self.assertEqual(summary["ambiguous_count"], 1)
            self.assertEqual(summary["unmatched_count"], 1)

            out_trips = pd.read_csv(output_dir / "trips.txt", dtype=str, keep_default_na=False)
            out_map = {row["trip_id"]: (row.get("trip_short_name") or "") for _, row in out_trips.iterrows()}

            self.assertEqual(out_map["T1"], "Os 100")
            self.assertEqual(out_map["T2"], "")
            self.assertEqual(out_map["T3"], "")
            self.assertEqual(out_map["T4"], "BUS 1")

            ambiguous = pd.read_csv(output_dir / "reports" / "ambiguous_trips.csv", dtype=str)
            unmatched = pd.read_csv(output_dir / "reports" / "unmatched_trips.csv", dtype=str)
            self.assertIn("T2", set(ambiguous["trip_id"]))
            self.assertIn("T3", set(unmatched["trip_id"]))


if __name__ == "__main__":
    unittest.main()
