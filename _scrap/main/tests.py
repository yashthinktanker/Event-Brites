from django.test import SimpleTestCase

from .scraper import extract_event_schedule
from .scraper import normalize_eventbrite_url
from .views import extract_event_urls_from_import
from .views import is_eventbrite_host


class EventScheduleParsingTests(SimpleTestCase):
    def test_parses_single_day_time_range_with_dash(self):
        start_date, start_time, end_date, end_time = extract_event_schedule(
            "Saturday, April 25 5 PM - 9 PM"
        )

        self.assertEqual(start_date, "04/25/2026")
        self.assertEqual(start_time, "05:00 PM")
        self.assertEqual(end_date, "04/25/2026")
        self.assertEqual(end_time, "09:00 PM")

    def test_parses_multi_day_range_with_to_separator(self):
        start_date, start_time, end_date, end_time = extract_event_schedule(
            "Thu, Apr 16, 9 AM to Sun, Apr 19, 5 PM"
        )

        self.assertEqual(start_date, "04/16/2026")
        self.assertEqual(start_time, "09:00 AM")
        self.assertEqual(end_date, "04/19/2026")
        self.assertEqual(end_time, "05:00 PM")


class EventbriteHostValidationTests(SimpleTestCase):
    def test_accepts_supported_eventbrite_hosts(self):
        self.assertTrue(is_eventbrite_host("eventbrite.com"))
        self.assertTrue(is_eventbrite_host("www.eventbrite.com"))
        self.assertTrue(is_eventbrite_host("eventbrite.sg"))
        self.assertTrue(is_eventbrite_host("www.eventbrite.sg"))
        self.assertTrue(is_eventbrite_host("eventbrite.ca"))
        self.assertTrue(is_eventbrite_host("www.eventbrite.ca"))

    def test_rejects_unsupported_hosts(self):
        self.assertFalse(is_eventbrite_host("example.com"))


class EventbriteUrlNormalizationTests(SimpleTestCase):
    def test_rewrites_sg_and_ca_domains_to_com(self):
        self.assertEqual(
            normalize_eventbrite_url("https://www.eventbrite.sg/e/sample-event-tickets-123"),
            "https://eventbrite.com/e/sample-event-tickets-123",
        )
        self.assertEqual(
            normalize_eventbrite_url("https://eventbrite.ca/e/sample-event-tickets-123"),
            "https://eventbrite.com/e/sample-event-tickets-123",
        )


class ImportedEventUrlParsingTests(SimpleTestCase):
    def test_extracts_eventbrite_urls_from_csv_import(self):
        file_bytes = (
            "Event Name,Event URL\n"
            "Sample One,https://www.eventbrite.sg/e/sample-one-tickets-123\n"
            "Sample Two,https://eventbrite.ca/e/sample-two-tickets-456\n"
        ).encode("utf-8")

        event_urls = extract_event_urls_from_import("events.csv", file_bytes)

        self.assertEqual(
            event_urls,
            [
                "https://eventbrite.com/e/sample-one-tickets-123",
                "https://eventbrite.com/e/sample-two-tickets-456",
            ],
        )

    def test_requires_event_url_column(self):
        file_bytes = "Name,Link\nSample,https://www.eventbrite.com/e/sample-tickets-123\n".encode("utf-8")

        with self.assertRaisesMessage(ValueError, "The file must contain an 'Event URL' column."):
            extract_event_urls_from_import("events.csv", file_bytes)
