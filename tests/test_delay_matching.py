import unittest

import pandas as pd

import get_direct_connection_cli as cli


class DelayMatchingTests(unittest.TestCase):
    def test_strict_exact_train_match(self):
        departure = {
            "departure_time": "10:14:00",
            "route_short_name": "P2/S70",
            "train_number": 7806,
            "train_category": "Os",
        }
        delay_records = [
            {
                "train_number": 7806,
                "scheduled_time_hhmm": "10:15",
                "status": "delayed",
                "delay_minutes": 4,
                "train_category": "Os",
                "route_text": "P2/S70 Plzen - Rokycany",
            }
        ]

        match = cli.match_departure_to_delay_records(departure, delay_records)
        self.assertEqual(match["status"], "delayed")
        self.assertEqual(match["confidence"], "high")
        self.assertEqual(match["match_reason"], "train_number")

    def test_strict_ambiguous_train_match(self):
        departure = {
            "departure_time": "10:14:00",
            "route_short_name": "P2/S70",
            "train_number": 7806,
            "train_category": "Os",
        }
        delay_records = [
            {
                "train_number": 7806,
                "scheduled_time_hhmm": "10:13",
                "status": "on_time",
                "train_category": "Os",
                "route_text": "P2/S70 Plzen - Rokycany",
            },
            {
                "train_number": 7806,
                "scheduled_time_hhmm": "10:15",
                "status": "delayed",
                "delay_minutes": 2,
                "train_category": "Os",
                "route_text": "P2/S70 Plzen - Rokycany",
            },
        ]

        match = cli.match_departure_to_delay_records(departure, delay_records)
        self.assertEqual(match["status"], "unknown")
        self.assertEqual(match["confidence"], "none")
        self.assertEqual(match["match_reason"], "none")

    def test_strict_match_train_number_works_without_time_proximity(self):
        departure = {
            "departure_time": "12:50:00",
            "route_short_name": "Os 27324",
            "train_number": 27324,
            "train_category": "Os",
        }
        delay_records = [
            {
                "train_number": 27324,
                "scheduled_time_hhmm": "12:41",
                "status": "delayed",
                "delay_minutes": 1,
                "train_category": "Os",
                "route_text": "P13 Plzen - Radnice",
            }
        ]

        match = cli.match_departure_to_delay_records(departure, delay_records)
        self.assertEqual(match["status"], "delayed")
        self.assertEqual(match["confidence"], "high")
        self.assertEqual(match["match_reason"], "train_number")

    def test_strict_match_uses_train_category_to_disambiguate_duplicate_number(self):
        departure = {
            "departure_time": "10:10:00",
            "route_short_name": "Os 12",
            "train_number": 12,
            "train_category": "Os",
        }
        delay_records = [
            {
                "train_number": 12,
                "scheduled_time_hhmm": "09:00",
                "status": "on_time",
                "train_category": "R",
                "route_text": "R12 Praha - Brno",
            },
            {
                "train_number": 12,
                "scheduled_time_hhmm": "11:00",
                "status": "delayed",
                "delay_minutes": 5,
                "train_category": "Os",
                "route_text": "Os12 Mesto - Mesto",
            },
        ]

        match = cli.match_departure_to_delay_records(departure, delay_records)
        self.assertEqual(match["status"], "delayed")
        self.assertEqual(match["confidence"], "high")
        self.assertEqual(match["match_reason"], "train_number")

    def test_route_code_single_candidate_fallback(self):
        departure = {
            "departure_time": "10:44:00",
            "route_short_name": "P13",
            "train_number": None,
        }
        delay_records = [
            {
                "train_number": None,
                "scheduled_time_hhmm": "10:47",
                "status": "on_time",
                "route_text": "P13 Plzen - Radnice",
            }
        ]

        match = cli.match_departure_to_delay_records(departure, delay_records)
        self.assertEqual(match["status"], "on_time")
        self.assertEqual(match["confidence"], "medium")
        self.assertEqual(match["match_reason"], "route_code")

    def test_route_code_ambiguous_candidates(self):
        departure = {
            "departure_time": "10:44:00",
            "route_short_name": "P13",
            "train_number": None,
        }
        delay_records = [
            {
                "train_number": None,
                "scheduled_time_hhmm": "10:44",
                "status": "on_time",
                "route_text": "P13 Plzen - Radnice",
            },
            {
                "train_number": None,
                "scheduled_time_hhmm": "10:46",
                "status": "delayed",
                "route_text": "P13 Plzen - Radnice",
            },
        ]

        match = cli.match_departure_to_delay_records(departure, delay_records)
        self.assertEqual(match["status"], "unknown")
        self.assertEqual(match["confidence"], "none")
        self.assertEqual(match["match_reason"], "none")

    def test_missing_record_returns_unknown(self):
        departure = {
            "departure_time": "10:44:00",
            "route_short_name": "P13",
            "train_number": None,
        }
        delay_records = []

        match = cli.match_departure_to_delay_records(departure, delay_records)
        self.assertEqual(match["status"], "unknown")
        self.assertEqual(match["confidence"], "none")
        self.assertEqual(match["match_reason"], "none")

    def test_route_code_requires_exact_token_match_not_substring(self):
        departure = {
            "departure_time": "10:44:00",
            "route_short_name": "P2",
            "train_number": None,
        }
        delay_records = [
            {
                "train_number": None,
                "scheduled_time_hhmm": "10:44",
                "status": "on_time",
                "route_text": "XP20 Plzen - Klatovy",
            }
        ]

        match = cli.match_departure_to_delay_records(departure, delay_records)
        self.assertEqual(match["status"], "unknown")
        self.assertEqual(match["confidence"], "none")
        self.assertEqual(match["match_reason"], "none")

    def test_route_code_uses_three_minute_window(self):
        departure = {
            "departure_time": "10:44:00",
            "route_short_name": "P13",
            "train_number": None,
        }
        delay_records = [
            {
                "train_number": None,
                "scheduled_time_hhmm": "10:48",
                "status": "on_time",
                "route_text": "P13 Plzen - Radnice",
            }
        ]

        match = cli.match_departure_to_delay_records(departure, delay_records)
        self.assertEqual(match["status"], "unknown")
        self.assertEqual(match["confidence"], "none")
        self.assertEqual(match["match_reason"], "none")

    def test_strict_match_uses_train_number_fallback_from_route_short_name(self):
        departure = {
            "departure_time": "10:14:00",
            "route_short_name": "Os 7806",
            "train_number": None,
        }
        delay_records = [
            {
                "train_number": 7806,
                "scheduled_time_hhmm": "10:15",
                "status": "on_time",
                "route_text": "Plzen - Rokycany",
            }
        ]

        match = cli.match_departure_to_delay_records(departure, delay_records)
        self.assertEqual(match["status"], "on_time")
        self.assertEqual(match["confidence"], "high")
        self.assertEqual(match["match_reason"], "train_number")


class DepartureRecordTests(unittest.TestCase):
    def test_build_departure_records_falls_back_to_route_short_name_for_train_identity(self):
        rows = pd.DataFrame(
            [
                {
                    "trip_id": "T1",
                    "route_id": "R1",
                    "route_short_name": "Os 27326",
                    "route_long_name": "Radnice - Plzen hlavni nadrazi",
                    "departure_time": "14:52:00",
                    "trip_short_name": "",
                }
            ]
        )

        records = cli.build_departure_records(rows, "73265", "73275")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["train_category"], "Os")
        self.assertEqual(records[0]["train_number"], 27326)


class RouteCodeExtractionTests(unittest.TestCase):
    def test_extract_route_codes_normalizes_and_splits(self):
        codes = cli.extract_route_codes("P2/S70, R16; x3a 7806 Os")
        self.assertEqual(codes, {"p2", "s70", "r16", "x3a"})

    def test_extract_route_codes_ignores_non_code_tokens(self):
        codes = cli.extract_route_codes("ČD Os Radnice 7806")
        self.assertEqual(codes, set())


if __name__ == "__main__":
    unittest.main()
