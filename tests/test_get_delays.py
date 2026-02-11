import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import get_delays


FIXTURES_DIR = Path(__file__).parent / "fixtures"


class GetDelaysUnitTests(unittest.TestCase):
    def test_get_delay_legacy_values(self):
        self.assertEqual(get_delays.get_delay("bez zpoždění"), 0)
        self.assertEqual(get_delays.get_delay("včas"), 0)
        self.assertEqual(get_delays.get_delay("5 min"), 5)
        self.assertIsNone(get_delays.get_delay("zrušen"))
        self.assertIsNone(get_delays.get_delay("odklon"))
        self.assertIsNone(get_delays.get_delay("výluka"))
        self.assertIsNone(get_delays.get_delay("neznámý text"))

    def test_parse_delay_status_and_minutes(self):
        self.assertEqual(get_delays.parse_delay_status_and_minutes("bez zpoždění"), ("on_time", 0))
        self.assertEqual(get_delays.parse_delay_status_and_minutes("včas"), ("on_time", 0))
        self.assertEqual(get_delays.parse_delay_status_and_minutes("3 min"), ("delayed", 3))
        self.assertEqual(get_delays.parse_delay_status_and_minutes("zrušen"), ("canceled", None))
        self.assertEqual(get_delays.parse_delay_status_and_minutes("odklon"), ("diverted", None))
        self.assertEqual(get_delays.parse_delay_status_and_minutes("výluka"), ("disruption", None))
        self.assertEqual(get_delays.parse_delay_status_and_minutes(""), ("unknown", None))

    def test_parse_train_identity(self):
        self.assertEqual(get_delays.parse_train_identity("Os 7806"), ("Os", 7806))
        self.assertEqual(get_delays.parse_train_identity("R1234"), ("R", 1234))
        self.assertEqual(get_delays.parse_train_identity("bez cisla"), (None, None))

    def test_parse_scheduled_actual_times(self):
        self.assertEqual(
            get_delays.parse_scheduled_actual_times("10:14 / 10:19"),
            ("10:14", "10:19"),
        )
        self.assertEqual(
            get_delays.parse_scheduled_actual_times("12:35"),
            ("12:35", None),
        )
        self.assertEqual(
            get_delays.parse_scheduled_actual_times("bez casu"),
            (None, None),
        )

    @patch("get_delays.Headers.generate", return_value={"User-Agent": "unit-test"})
    @patch("get_delays.requests.get")
    def test_scrape_contract_from_zponline_fixture(self, mock_get: Mock, _mock_headers: Mock):
        fixture = (FIXTURES_DIR / "zponline.html").read_text(encoding="utf-8")
        mock_get.return_value = Mock(status_code=200, text=fixture)

        result = get_delays.scrape_babitron_delays("https://example.local/zponline")
        self.assertIn("Os 7806", result)
        item = result["Os 7806"]

        # Legacy keys (backward compatibility)
        for key in [
            "train",
            "name",
            "route",
            "station",
            "scheduled_actual_time",
            "delay_text",
            "delay",
        ]:
            self.assertIn(key, item)

        # Normalized additive keys
        for key in [
            "status",
            "delay_minutes",
            "train_category",
            "train_number",
            "route_text",
            "station_text",
            "scheduled_time_hhmm",
            "actual_time_hhmm",
            "source_page",
        ]:
            self.assertIn(key, item)

        self.assertEqual(item["status"], "on_time")
        self.assertEqual(item["delay"], 0)
        self.assertEqual(item["delay_minutes"], 0)
        self.assertEqual(item["train_category"], "Os")
        self.assertEqual(item["train_number"], 7806)
        self.assertEqual(item["scheduled_time_hhmm"], "10:14")
        self.assertEqual(item["actual_time_hhmm"], "10:14")
        self.assertEqual(item["source_page"], "zponline")

        delayed_item = result["R 1234"]
        self.assertEqual(delayed_item["status"], "delayed")
        self.assertEqual(delayed_item["delay"], 5)
        self.assertEqual(delayed_item["delay_minutes"], 5)

    @patch("get_delays.Headers.generate", return_value={"User-Agent": "unit-test"})
    @patch("get_delays.requests.get")
    def test_scrape_contract_from_zponlineos_fixture(self, mock_get: Mock, _mock_headers: Mock):
        fixture = (FIXTURES_DIR / "zponlineos.html").read_text(encoding="utf-8")
        mock_get.return_value = Mock(status_code=200, text=fixture)

        result = get_delays.scrape_babitron_delays("https://example.local/zponlineos")
        self.assertIn("Sp 1111", result)
        item = result["Sp 1111"]
        self.assertEqual(item["status"], "canceled")
        self.assertIsNone(item["delay"])
        self.assertIsNone(item["delay_minutes"])
        self.assertEqual(item["source_page"], "zponlineos")


if __name__ == "__main__":
    unittest.main()
