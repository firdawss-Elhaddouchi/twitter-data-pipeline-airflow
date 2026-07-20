import json
import os
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

DATA_PATH = os.getenv("AIRFLOW_DATA_PATH", "/opt/airflow/data")
LOCAL_TWEETS_FILE = "tweets.csv"
XQUIK_TWEET_SEARCH_URL = os.getenv(
    "XQUIK_TWEET_SEARCH_URL",
    "https://xquik.com/api/v1/x/tweets/search",
)
XQUIK_QUERY = os.getenv("XQUIK_QUERY", "from:elonmusk")
XQUIK_LIMIT = os.getenv("XQUIK_LIMIT", "200")
XQUIK_API_CONTRACT = "2026-04-29"
XQUIK_TWEET_COLUMNS = [
    "user",
    "tweet",
    "favorite_count",
    "retweet_count",
    "created_at",
]
LOCAL_COLUMN_NAMES = {
    "author": "user",
    "content": "tweet",
    "date_time": "created_at",
    "number_of_likes": "favorite_count",
    "number_of_shares": "retweet_count",
}


def _data_file(name):
    return os.path.join(DATA_PATH, name)


def _tweet_text(tweet):
    return (
        tweet.get("text")
        or tweet.get("full_text")
        or tweet.get("content")
        or tweet.get("body")
        or ""
    )


def _tweet_author(tweet):
    author = tweet.get("author")
    if isinstance(author, dict):
        return author.get("username") or author.get("screen_name") or ""
    return tweet.get("user") or tweet.get("username") or ""


def _first_present(mapping, *keys):
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _xquik_limit():
    try:
        limit = int(XQUIK_LIMIT)
    except ValueError as exc:
        raise ValueError("XQUIK_LIMIT must be an integer from 1 to 200") from exc
    if not 1 <= limit <= 200:
        raise ValueError("XQUIK_LIMIT must be an integer from 1 to 200")
    return limit


def _xquik_rows():
    api_key = os.getenv("XQUIK_API_KEY")
    if not api_key:
        return None

    query = urlencode({"q": XQUIK_QUERY, "limit": _xquik_limit()})
    request = Request(
        f"{XQUIK_TWEET_SEARCH_URL}?{query}",
        headers={
            "Accept": "application/json",
            "x-api-key": api_key,
            "xquik-api-contract": XQUIK_API_CONTRACT,
        },
    )
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.load(response)
    except (HTTPError, URLError, TimeoutError) as exc:
        raise RuntimeError("Xquik tweet search request failed") from exc

    tweets = payload.get("tweets", [])
    return [
        {
            "user": _tweet_author(tweet),
            "tweet": _tweet_text(tweet),
            "favorite_count": _first_present(tweet, "likeCount", "like_count"),
            "retweet_count": _first_present(tweet, "retweetCount", "retweet_count"),
            "created_at": _first_present(tweet, "createdAt", "created_at"),
        }
        for tweet in tweets
    ]


def _local_dataframe():
    dataframe = pd.read_csv(_data_file(LOCAL_TWEETS_FILE))
    return dataframe.rename(columns=LOCAL_COLUMN_NAMES)


def _source_dataframe():
    rows = _xquik_rows()
    if rows is not None:
        return pd.DataFrame(rows, columns=XQUIK_TWEET_COLUMNS)
    return _local_dataframe()


def run_twitter_etl():
    df = _source_dataframe()
    df.to_csv(_data_file("elonmusk_twitter_data.csv"), index=False)


def extract_data():
    df = _source_dataframe()
    df.to_csv(_data_file("extracted.csv"), index=False)
    return "Extraction OK"


def transform_data():
    df = pd.read_csv(_data_file("extracted.csv"))
    df = df.dropna(subset=["tweet"])
    df["tweet_length"] = df["tweet"].apply(len)
    df.to_csv(_data_file("transformed.csv"), index=False)
    return "Transformation OK"


def load_data():
    df = pd.read_csv(_data_file("transformed.csv"))
    df.to_csv(_data_file("cleaned_tweets.csv"), index=False)
    return "Chargement OK"
