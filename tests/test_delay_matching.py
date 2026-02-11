import unittest

import get_direct_connection_cli as cli


class DelayMatchingTests(unittest.TestCase):
    def test_strict_exact_train_match(self):
        departure = {
            "departure_time": "10:14:00",
            "route_short_name": "P2/S70",
            "train_number": 7806,
        }
        delay_records = [
            {
                "train_number": 7806,
                "scheduled_time_hhmm": "10:15",
                "status": "delayed",
                "delay_minutes": 4,
                "route_text": "P2/S70 Plzen - Rokycany",
            }
        ]

        match = cli.match_departure_to_delay_records(departure, delay_records)
        self.assertEqual(match["status"], "delayed")
        self.assertEqual(match["confidence"], "high")

    def test_heuristic_single_candidate(self):
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

    def test_heuristic_ambiguous_candidates(self):
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


if __name__ == "__main__":
    unittest.main()
