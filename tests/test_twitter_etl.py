import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from dags import twitter_etl


class Response:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        return False

    def read(self):
        return json.dumps(self.payload).encode()


class TwitterEtlTest(unittest.TestCase):
    def test_local_csv_columns_feed_the_transform(self):
        with tempfile.TemporaryDirectory() as directory:
            source = pd.DataFrame(
                [
                    {
                        "author": "example",
                        "content": "A useful post",
                        "date_time": "2026-07-20 00:00",
                        "number_of_likes": 3,
                        "number_of_shares": 2,
                    }
                ]
            )
            source.to_csv(Path(directory) / "tweets.csv", index=False)

            with (
                patch.object(twitter_etl, "DATA_PATH", directory),
                patch.dict(os.environ, {}, clear=True),
            ):
                twitter_etl.extract_data()
                twitter_etl.transform_data()

            transformed = pd.read_csv(Path(directory) / "transformed.csv")
            self.assertEqual(transformed.loc[0, "user"], "example")
            self.assertEqual(transformed.loc[0, "tweet"], "A useful post")
            self.assertEqual(transformed.loc[0, "tweet_length"], 13)

    def test_xquik_uses_current_contract_and_preserves_zero_metrics(self):
        payload = {
            "tweets": [
                {
                    "author": {"username": "example"},
                    "createdAt": "2026-07-20T00:00:00Z",
                    "likeCount": 0,
                    "retweetCount": 0,
                    "text": "Current result",
                }
            ]
        }
        captured = {}

        def open_request(request, timeout):
            captured["headers"] = dict(request.header_items())
            captured["timeout"] = timeout
            captured["url"] = request.full_url
            return Response(payload)

        with (
            patch.dict(os.environ, {"XQUIK_API_KEY": "test-key"}),
            patch.object(twitter_etl, "urlopen", open_request),
        ):
            rows = twitter_etl._xquik_rows()

        self.assertEqual(captured["timeout"], 30)
        self.assertIn("limit=200", captured["url"])
        self.assertEqual(captured["headers"]["X-api-key"], "test-key")
        self.assertEqual(
            captured["headers"]["Xquik-api-contract"],
            twitter_etl.XQUIK_API_CONTRACT,
        )
        self.assertEqual(rows[0]["favorite_count"], 0)
        self.assertEqual(rows[0]["retweet_count"], 0)

    def test_xquik_limit_rejects_invalid_values(self):
        with patch.object(twitter_etl, "XQUIK_LIMIT", "201"):
            with self.assertRaisesRegex(ValueError, "1 to 200"):
                twitter_etl._xquik_limit()


if __name__ == "__main__":
    unittest.main()
