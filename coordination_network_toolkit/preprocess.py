import csv
import json
from typing import Iterable, List
import zlib

from twarc import ensure_flattened

from coordination_network_toolkit.database import initialise_db



def preprocess_csv_files(db_path: str, input_filenames: List[str]):
    for message_file in input_filenames:
        # Skip the header
        print(f"Begin preprocessing {message_file} into {db_path}")

        with open(message_file, "r") as messages:
            reader = csv.reader(messages)
            # Skip header
            next(reader)
            preprocess_data(db_path, reader)

        print(f"Done preprocessing {message_file} into {db_path}")


def preprocess_data(db_path: str, messages: Iterable):
    """

    Add messages to the dataset from the specified CSV files..

    Messages should be an iterator of messages with the content for each message
    in the following order:

    - message_id: the unique identifier of the message on the platform
    - user_id: the unique identifier of the user on the platform
    - username: the text of the username (only used for display)
    - repost_id: if the message is a verbatim report of another message (such as a retweet 
        or reblog), this is the identifier of that other message. Empty strings will be 
        converted to null
    - reply_id: if the message is in reply to another message, the identifier for that other
        message. Empty strings will be converted to null.
    - message: the text of the message.
    - timestamp: A timestamp in seconds for the message. The absolute offset does not matter,
        but it needs to be consistent across all rows
    - urls: A space delimited string containing all of the URLs in the message

    """

    db = initialise_db(db_path)

    try:
        db.execute("begin")
        processed = (
            (
                message_id,
                user_id,
                username,
                repost_id or None,
                reply_id or None,
                message,
                len(message),
                zlib.adler32(message.encode("utf8")),
                # This will be populated only when similarity calculations are necessary
                None,
                float(timestamp),
                urls.split(" ") if urls else [],
            )
            for message_id, user_id, username, repost_id, reply_id, message, timestamp, urls in messages
        )

        for row in processed:
            db.execute(
                "insert or ignore into edge values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                row[:-1],
            )

            message_id, user_id = row[:2]
            timestamp = row[-2]

            # Ignore url shared in reposts
            if not row[3]:
                for url in row[-1]:
                    db.execute(
                        "insert or ignore into message_url values(?, ?, ?, ?)",
                        (message_id, url, timestamp, user_id),
                    )

        db.execute("commit")
    finally:
        db.close()


def preprocess_twitter_json_files(db_path: str, input_filenames: List[str]):
    for message_file in input_filenames:
        # Skip the header
        print(f"Begin preprocessing {message_file} into {db_path}")
        with open(message_file, "r") as tweets:
            try:
                # Try v2 format
                preprocess_twitter_v2_json_data(db_path, tweets)
            except:
                # Fallback to v1.1 format
                preprocess_twitter_json_data(db_path, tweets)

        print(f"Done preprocessing {message_file} into {db_path}")


def preprocess_twitter_json_data(db_path: str, tweets: Iterable[str]):
    """

    Add messages to the dataset from the specified tweets in Twitter JSON format.

    Tweets must be in Twitter JSON format as collected from the v1.1 JSON API.

    """

    db = initialise_db(db_path)

    try:
        db.execute("begin")

        for raw_tweet in tweets:

            tweet = json.loads(raw_tweet)

            # Try grabbing the full_text field from the extended format, otherwise
            # check if there's a extended_tweet object.
            # print(sorted(tweet.keys()))
            if "full_text" in tweet:
                tweet_text = tweet["full_text"]
            elif "extended_tweet" in tweet:
                tweet_text = tweet["extended_tweet"]["full_text"]
            else:
                tweet_text = tweet["text"]

            retweet = tweet.get("retweeted_status", {})

            row = (
                tweet["id_str"],
                tweet["user"]["id_str"],
                tweet["user"]["screen_name"],
                retweet.get("id_str"),
                tweet.get("in_reply_to_status_id_str", None),
                tweet_text,
                len(tweet_text),
                zlib.adler32(tweet_text.encode("utf8")),
                # This will be populated only when similarity calculations are necessary
                None,
                # Twitter epoch in seconds
                (int(tweet["id"]) >> 22) / 1000,
            )

            db.execute(
                "insert or ignore into edge values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", row,
            )

            # If it's a retweet, don't consider any of the urls as candidates
            if not row[3]:
                if retweet:
                    url_entities = retweet.get("extended_tweet", retweet)["entities"][
                        "urls"
                    ]
                else:
                    url_entities = tweet.get("extended_tweet", tweet)["entities"]["urls"]

                message_id, user_id = row[:2]
                timestamp = row[-1]

                urls = [u["expanded_url"] for u in url_entities]
                # Ignore urls shared in reposts
                for url in urls:
                    db.execute(
                        "insert or ignore into message_url values(?, ?, ?, ?)",
                        (message_id, url, timestamp, user_id),
                    )

        db.execute("commit")
    except:
        db.execute("rollback")
        raise
    finally:
        db.close()


def preprocess_twitter_v2_json_data(db_path: str, tweets: Iterable[str]):
    """

    Add messages to the dataset from the specified tweets in Twitter JSON format.

    Tweets must be in Twitter JSON format as collected from the v1.1 JSON API.

    """

    db = initialise_db(db_path)

    try:
        db.execute("begin")

        for page in tweets:

            tweets = ensure_flattened(json.loads(page))

            for tweet in tweets:

                # Pick out referenced tweets
                referenced_tweets = tweet.get("referenced_tweets", [])
                retweeted_tweet_id, replied_to_id = None, None
                for referenced_tweet in referenced_tweets:
                    if referenced_tweet["type"] == "retweeted":
                        retweeted_tweet_id = referenced_tweet["id"]
                    if referenced_tweet["type"] == "replied_to":
                        replied_to_id = referenced_tweet["id"]

                row = (
                    tweet["id"],
                    tweet["author_id"],
                    tweet["author"]["username"],
                    retweeted_tweet_id,
                    replied_to_id,
                    tweet["text"],
                    len(tweet["text"]),
                    zlib.adler32(tweet["text"].encode("utf8")),
                    # This will be populated only when similarity calculations are necessary
                    None,
                    # Twitter epoch in seconds
                    (int(tweet["id"]) >> 22) / 1000,
                )

                db.execute(
                    "insert or ignore into edge values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", row,
                )

                # If it's a retweet, don't consider any of the urls as candidates
                if not retweeted_tweet_id:
                    url_entities = tweet.get("entities", {}).get("urls", [])

                    message_id, user_id = row[:2]
                    timestamp = row[-1]

                    urls = [u["expanded_url"] for u in url_entities]
                    # Ignore urls shared in reposts
                    for url in urls:
                        db.execute(
                            "insert or ignore into message_url values(?, ?, ?, ?)",
                            (message_id, url, timestamp, user_id),
                        )

        db.execute("commit")
    except:
        db.execute("rollback")
        raise
    finally:
        db.close()