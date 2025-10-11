
import unittest
from unittest.mock import patch
import datetime
from dedupe_copy.core import info_parser

class TestInfoParser(unittest.TestCase):
    @patch("dedupe_copy.core.datetime")
    def test_info_parser_handles_timestamp_errors(self, mock_datetime):
        mock_datetime.datetime.fromtimestamp.side_effect = OverflowError("mocked overflow error")
        data = {"some_md5": [["a/file/path", 100, 1234567890]]}
        results = list(info_parser(data))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][2], "Unknown")
