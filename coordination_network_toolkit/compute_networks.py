from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import math
import sqlite3 as lite
import threading
from typing import Callable
from zlib import adler32

from coordination_network_toolkit.similarity import (
    similarity,
    tokenize,
    message_preprocessor,
)


local = threading.local()


network_queries = {
    "co_tweet": """
        -- This query will create the table representing the edges of the network.
        create table co_tweet_network as
        select
            e_1.user_id as user_1,
            e_2.user_id as user_2,
            count(*) as weight
        from edge e_1
        inner join edge e_2
            on (e_1.transformed_message_length, e_1.transformed_message_hash, e_1.transformed_message) =
               (e_2.transformed_message_length, e_2.transformed_message_hash, e_2.transformed_message)
            and e_2.timestamp between e_1.timestamp - ?1 and e_1.timestamp + ?1
            and e_1.repost_id is null
            and e_2.repost_id is null
        group by e_1.user_id, e_2.user_id
        having weight >= ?2
        """,
    "co_reply": """
        -- This query will create the table representing the edges of the network.
        create table co_reply_network as
        select
            e_1.user_id as user_1,
            e_2.user_id as user_2,
            count(*) as weight
        from edge e_1
        inner join edge e_2
            on e_1.reply_id = e_2.reply_id
            and e_2.timestamp between e_1.timestamp - ?1 and e_1.timestamp + ?1
            and e_1.repost_id is null
            and e_2.repost_id is null
        group by e_1.user_id, e_2.user_id
        having weight >= ?2
        """,
    "co_similar_tweet": """
        -- This query will create the table representing the edges of the network.
        create table co_similar_tweet_network as
        select
            e_1.user_id as user_1,
            e_2.user_id as user_2,
            count(*) as weight
        from edge e_1 indexed by user_time
        inner join edge e_2
            -- Length filtering of the messages
            on e_2.timestamp between e_1.timestamp - ?1 and e_1.timestamp + ?1
        -- Note that this will only work where the Python similarity function has been
        -- registered on the connection - this is not a SQLite native function.
        where
            e_1.repost_id is null
            and e_2.repost_id is null
            and e_1.token_set is not null
            and e_2.token_set is not null
            and similarity(e_1.token_set, e_2.token_set) >= ?3
        group by e_1.user_id, e_2.user_id
        having weight >= ?2
        """,
    "co_link": """
        -- This query will create the table representing the edges of the network.
        create table co_link_network as
        select
            e_1.user_id as user_1,
            e_2.user_id as user_2,
            count(*) as weight
        from message_url e_1
        inner join message_url e_2
            on e_1.url = e_2.url
            and e_2.timestamp between e_1.timestamp - ?1 and e_1.timestamp + ?1
        group by e_1.user_id, e_2.user_id
        having weight >= ?2
        """,
    "co_link_resolved": """
        -- This query will create the table representing the edges of the network.
        create table co_link_network as
        select
            e_1.user_id as user_1,
            e_2.user_id as user_2,
            count(*) as weight
        from resolved_message_url e_1
        inner join resolved_message_url e_2
            on e_1.resolved_url = e_2.resolved_url
            -- Keep any row where the retweets are by different users and within n
            -- seconds of each other.
            and e_2.timestamp between e_1.timestamp - ?1 and e_1.timestamp + ?1
        group by e_1.user_id, e_2.user_id
        having weight >= ?2
        """,
}


def compute_co_tweet_network(
    db_path,
    time_window,
    min_edge_weight=1,
    preprocessor: Callable = message_preprocessor,
    reprocess_text=False,
):
    """
    Compute a co-tweet network on the given database.

    The text_preprocessor is used to apply transformations to ensure certain invariants,
    and ensure that non-semantic changes in the text don't occlude matches of otherwise
    identical content.

    The default transformation removes @mentions as well as normalising case and some
    whitespace.

    If reprocess_text is True, apply the preprocessor to all text again. This is necessary
    if you have changed preprocessing functions and need to update already processed data.

    """
    db = lite.connect(db_path)
    db.create_function("preprocessor", 1, message_preprocessor)
    db.create_function("message_hash", 1, lambda x: adler32(x.encode("utf8")))

    with db:

        print("Applying text normalisation for cotweet")
        db.execute(
            f"""
            update edge set
                transformed_message = preprocessor(message),
                transformed_message_length = length(preprocessor(message)),
                transformed_message_hash = message_hash(preprocessor(message))
            where repost_id is null {
                "" if reprocess_text else "and transformed_message is null"
            }
            """
        )
        print("Ensuring the necessary index exists")
        db.execute(
            """
            create index if not exists message_content on edge(
                transformed_message_length, transformed_message_hash, timestamp
            ) where repost_id is null
            """
        )
        db.execute("drop table if exists co_tweet_network")
        db.execute(
            network_queries["co_tweet"],
            [time_window, min_edge_weight],
        )


def compute_co_reply_network(db_path, time_window, min_edge_weight=1):
    """ """
    db = lite.connect(db_path)

    with db:
        print("Ensuring the necessary index exists")
        db.execute("create index if not exists user_time on edge(user_id, timestamp)")
        db.execute(
            """
            create index if not exists replies on edge(reply_id, timestamp)
            where repost_id is null
            """
        )
        db.execute("drop table if exists co_reply_network")
        db.execute(
            network_queries["co_reply"],
            [time_window, min_edge_weight],
        )


def compute_co_link_network(db_path, time_window, min_edge_weight=1, resolved=False):
    """ """
    db = lite.connect(db_path)

    print("Ensuring the necessary index exists")

    with db:

        db.execute("drop table if exists co_link_network")

        if resolved:

            db.execute(
                """
                create index if not exists resolved_url_message on resolved_message_url(
                    resolved_url, timestamp
                )
                """
            )
            db.execute(
                network_queries["co_link_resolved"],
                [time_window, min_edge_weight],
            )

        else:

            db.execute(
                """
                create index if not exists url_message on message_url(
                    url, timestamp
                )
                """
            )

            db.execute(
                network_queries["co_link"],
                [time_window, min_edge_weight],
            )


def compute_co_similar_tweet(
    db_path,
    time_window,
    n_threads=4,
    similarity_threshold=0.9,
    min_edge_weight=1,
    similarity_function: Callable = similarity,
    reprocess_text=False,
):
    """

    Create a network where tweets with a certain similarity are counted as coordinated
    edges.

    An arbitrary text similarity function can be applied. It must take the string
    representation of the two messages to compared. The default is the Jaccard
    similarity, with a normalised similarity between 0 and 1.

    Currently the n_threads argument is not used.

    """
    db = lite.connect(db_path, isolation_level=None)
    db.create_function("similarity", 2, similarity_function)
    db.create_function("tokenize", 1, tokenize)

    db.executescript(
        """
        create index if not exists user_time on edge(user_id, timestamp);
        create index if not exists to_tokenize on edge(message_id)
            where repost_id is null and token_set is null;
        create index if not exists timestamp on edge(timestamp);
        drop table if exists co_similar_tweet_network;
        """
    )

    db.execute("begin")

    # Tokenize text
    print("Tokenizing messages")
    db.execute(
        f"""
        update edge set token_set = tokenize(message)
        where repost_id is null {
            "" if reprocess_text else "and token_set is null"
        }
        """
    )

    print("Calculating similarity")
    db.execute(
        network_queries["co_similar_tweet"],
        [time_window, min_edge_weight, similarity_threshold],
    )

    db.execute("commit")


def compute_co_retweet_parallel(db_path, time_window, n_threads=4, min_edge_weight=1):

    lock = threading.Lock()

    def calculate_user_edges(user_ids, time_window):
        try:
            db = local.db
        except AttributeError:
            db = lite.connect(db_path)
            db.executescript(
                """
                pragma synchronous=normal;
                create temporary table local_network (
                    user_id_1,
                    user_id_2,
                    weight
                );
                create temporary table user_id (
                    user_id primary key
                );
                """
            )
            local.db = db

        with db:
            db.execute("delete from user_id")
            db.execute("delete from local_network")
            db.executemany(
                "insert into user_id values(?)", ((user_id,) for user_id in user_ids)
            )

            db.execute(
                f"""
                insert into local_network
                    select
                        e_1.user_id as user_1,
                        e_2.user_id as user_2,
                        count(*) as weight
                        -- TODO: Add additional summary statistics
                    from edge e_1
                    inner join edge e_2
                        on e_1.repost_id = e_2.repost_id
                        and e_2.timestamp between e_1.timestamp - ?1
                            and e_1.timestamp + ?1
                        and user_1 in (select user_id from user_id)
                    group by e_1.user_id, e_2.user_id
                    having count(*) >= ?
                """,
                [time_window, min_edge_weight],
            )

        with lock, db:
            db.execute("insert into co_retweet_network select * from local_network")

    db = lite.connect(db_path)

    print("Ensure the indexes exist to drive the join.")
    db.executescript(
        """
        create index if not exists user_time on edge(user_id, timestamp);
        create index if not exists repost_time on edge(
            repost_id, timestamp
        ) where repost_id is not null;
        drop table if exists co_retweet_network;
        create table co_retweet_network (
            user_1,
            user_2,
            weight,
            primary key (user_1, user_2)
        ) without rowid;
        """
    )

    pool = ThreadPoolExecutor(n_threads)

    print("Calculating the co-retweet network")
    waiting = set()
    count = 0
    completed = 0
    submitted = 0

    user_ids = []

    batch_size = 1000
    n_batches = math.ceil(
        list(db.execute("select count(distinct user_id) from edge"))[0][0] / batch_size
    )

    for (user_id,) in db.execute("select distinct user_id from edge"):

        user_ids.append(user_id)

        if len(user_ids) == batch_size:
            waiting.add(pool.submit(calculate_user_edges, user_ids, time_window))

            submitted += 1

            user_ids = []

            if len(waiting) >= 20:
                done, waiting = wait(waiting, return_when=FIRST_COMPLETED)

                for d in done:
                    d.result()
                    completed += 1

            if not (submitted % 100):
                print(f"Completed {completed} / {n_batches}")

    else:
        print("Waiting for final batch.")
        waiting.add(pool.submit(calculate_user_edges, user_ids, time_window))
        wait(waiting)

    db.close()

    return completed
