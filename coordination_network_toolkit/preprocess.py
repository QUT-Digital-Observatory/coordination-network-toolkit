import csv
import json
from typing import Iterable, List
from urllib.parse import urlparse
import zlib

from twarc import ensure_flattened
from twarc.client2 import Twarc2

from coordination_network_toolkit.database import initialise_db


def preprocess_csv_files(db_path: str, input_filenames: List[str]):
    """
    Preprocess the given list of CSV files.

    The expected format is the following columns, with a header row:

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
    - urls: A space delimited list of URLs (for example: "www.google.com youtu.be/123 www.facebook.com")

    """

    for message_file in input_filenames:
        # Skip the header
        print(f"Begin preprocessing {message_file} into {db_path}")

        with open(message_file, "r") as messages:
            reader = csv.reader(messages)
            # Skip header
            next(reader)

            data = ((*row[:-1], row[-1].split(" ")) for row in reader)
            preprocess_data(db_path, data)

        print(f"Done preprocessing {message_file} into {db_path}")


def preprocess_data(db_path: str, messages: Iterable):
    """

    Add messages to the dataset from the iterator of messages.

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
    - urls: A list of URLs present in the message - None will be converted to an empty list.

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
                # These will be populated when co-tweet calculations are necessary
                None,
                None,
                None,
                # This will be populated only when similarity calculations are necessary
                None,
                float(timestamp),
                urls,
            )
            for message_id, user_id, username, repost_id, reply_id, message, timestamp, urls in messages
        )

        for row in processed:
            db.execute(
                "insert or ignore into edge values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                None,
                None,
                None,
                # This will be populated only when similarity calculations are necessary
                None,
                # Twitter epoch in seconds
                (int(tweet["id"]) >> 22) / 1000,
            )

            db.execute(
                "insert or ignore into edge values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                row,
            )

            # If it's a retweet, don't consider any of the urls as candidates
            if not row[3]:
                if retweet:
                    url_entities = retweet.get("extended_tweet", retweet)["entities"][
                        "urls"
                    ]
                else:
                    url_entities = tweet.get("extended_tweet", tweet)["entities"][
                        "urls"
                    ]

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

    Tweets must be in Twitter JSON format as collected from the v2 JSON API.

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
                    None,
                    None,
                    None,
                    # This will be populated only when similarity calculations are necessary
                    None,
                    # Twitter epoch in seconds
                    (int(tweet["id"]) >> 22) / 1000,
                )

                db.execute(
                    "insert or ignore into edge values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    row,
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


def preprocess_twitter_v2_likes_retweets(
    db_path: str, data_pages: Iterable[str], hydrate_client=None
):
    """
    Process pages of Twitter likes/retweets from the V2 endpoints into a
    coordination structure.

    `hydrate_client` is an instance of a Twarc2 client, used to hydrate
    usernames for the case of user_profile lookups. If not provided the username
    column will not be present for the calls to the liked_tweets endpoint.

    In this case, a "message" is a no longer a single tweet, but the act of
    liking or retweeting a tweet by a specific user. The message id in this
    case is then the composite key '{liked_tweet}_{liking_user_id}' or '
    {retweeted_tweet}_{retweeting_user_id}'.

    As the Twitter data in this case does not report when the like was
    created, only the relative chronological order of likes/retweets. In
    this case the "time" window is a logical time, with the first entry in
    the stream being the first tweet in the function.

    Assumptions:

    - This relies on the Twitter API returning results in reverse chronological
      order to construct a logical 'time' for that like relative to when the tweet was
      posted.
    - A call to a particular URL path (eg, the retweeting users of a specific tweet)
      is only present once in a file. If the information is refreshed later, it needs
      to completely replace the original timeline of likes instead.

    Limitations:

    - This function requires the `__twarc` metadata be present to infer the
      structure of which tweet/user is doing the liking/retweeting or being
      liked. This function will fail if this is missing.
    - The data for this can only be from the liked-tweets, liking-users, and
      retweeted-by endpoints of the V2 Twitter API.
    - The only supported network is co_retweet using the logical time.
    - All data from the call to a specific path need to be contiguous within
      the incoming stream of pages. Only one type of endpoint is supported at
      a time, you can only insert data from one of `liking-users`,
      `liked-tweets`, or `retweeted-by` into the same database.
    - Data will only be inserted into an empty database, no incremental updates
      because of the logical clock.

    """

    db = initialise_db(db_path)

    try:
        db.execute("begin")

        edge_count = list(db.execute("select count(*) from edge"))[0][0]

        if edge_count:
            raise ValueError("This preprocess function requires an empty database.")

        current_path = ""

        user_to_username = {}
        seen_calls = set()
        tw = None

        for page in data_pages:

            data_page = json.loads(page)

            called_url = urlparse(data_page["__twarc"]["url"])

            # Todo: keep track of, and detect overlapping results from
            # different calls to the same path to raise an error.
            if called_url.path != current_path:
                # Reset the logical time.
                current_logical_time = 0
                current_path = called_url.path

            url_path_components = called_url.path.split("/")

            # From twarc, this is the format of the URLs for the endpoints
            # that work with this function.
            # f"https://api.twitter.com/2/tweets/{tweet_id}/liking_users"
            # f"https://api.twitter.com/2/users/{user_id}/liked_tweets"
            # f"https://api.twitter.com/2/tweets/{tweet_id}/retweeted_by"

            call_type = url_path_components[-1]

            if call_type not in {
                "liking_users",
                "liked_tweets",
                "retweeted_by",
            }:
                raise ValueError(
                    "Only data from the 'liking-users', 'liked-tweets', "
                    "'retweeted-by' endpoints are supported for this format."
                )

            seen_calls.add(call_type)
            if len(seen_calls) > 1:
                raise TypeError(
                    "Data inserted in this format can only be from one of the following endpoints, "
                    "no mixing is allowed: 'liking-users', 'liked-tweets', 'retweeted-by'"
                )

            if call_type in {"liking_users", "retweeted_by"}:

                # For this path, all of the information we need can be derived
                # from the URL and returned profiles.
                reference_tweet_id = url_path_components[-2]

                def return_row(data_object):

                    user_id = data_object["id"]
                    username = data_object["username"]

                    return (
                        f"{reference_tweet_id}_{user_id}",
                        user_id,
                        username,
                        reference_tweet_id,
                        current_logical_time,
                    )

            else:
                # For this path, we don't necessarily have the information
                # about the user who's likes we're looking at.
                reference_user_id = url_path_components[-2]

                if hydrate_client and reference_user_id not in user_to_username:
                    profile = hydrate_client.user_lookup(users=[reference_user_id])
                    if "data" in profile:
                        user_to_username[reference_user_id] = profile["data"][0][
                            "username"
                        ]
                    else:
                        # TODO: could actually lookup the error if the user isn't
                        # available anymore and mark that here.
                        user_to_username[
                            reference_user_id
                        ] = f"{reference_user_id}_profile_unavailable"

                reference_username = user_to_username.get(
                    reference_user_id, str(reference_user_id)
                )

                def return_row(data_object):

                    reference_tweet_id = data_object["id"]

                    # The logical time for this endpoint is always 0,
                    # as there's no way to establish a relative ordering
                    # with respect to a single users like behaviour, this can only
                    # be established w.r.t. the likes on a specific tweet.
                    return (
                        f"{reference_tweet_id}_{reference_user_id}",
                        reference_user_id,
                        reference_username,
                        reference_tweet_id,
                        0,
                    )

            for an_object in data_page["data"]:

                db.execute(
                    """
                    insert or ignore into edge(message_id, user_id, username, repost_id, timestamp)
                        values (?, ?, ?, ?, ?)
                    """,
                    return_row(an_object),
                )

                current_logical_time += 1

        db.execute("commit")
    except:
        db.execute("rollback")
        raise
    finally:
        db.close()


def preprocess_twitter_v2_like_retweet_files(db_path: str, input_filenames: List[str]):
    def iterate_all_pages(input_filenames):
        for message_file in input_filenames:
            with open(message_file, "r") as pages:
                for page in pages:
                    yield page

            print(f"Done preprocessing {message_file} into {db_path}")

    preprocess_twitter_v2_likes_retweets(db_path, iterate_all_pages(input_filenames))
