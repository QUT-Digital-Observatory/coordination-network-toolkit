from concurrent.futures import (
    ProcessPoolExecutor,
    wait,
    FIRST_COMPLETED,
)
import math
import multiprocessing as mp
import sqlite3 as lite
from typing import Callable
from zlib import adler32

from coordination_network_toolkit.similarity import (
    similarity,
    tokenize,
    message_preprocessor,
)


def compute_co_tweet_network(
    db_path,
    time_window,
    min_edge_weight=1,
    preprocessor: Callable = message_preprocessor,
    reprocess_text=False,
    n_threads=4,
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
            """
            create table co_tweet_network (
                user_1,
                user_2,
                weight,
                primary key (user_1, user_2)
            ) without rowid
            """
        )

    query = """
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
        where user_1 in (select user_id from user_id)
        group by e_1.user_id, e_2.user_id
        having weight >= ?2
    """

    return parallise_query_by_user_id(
        db_path,
        "co_tweet_network",
        query,
        (time_window, min_edge_weight),
        n_processes=n_threads,
        sqlite_functions=None
    )


def compute_co_reply_network(db_path, time_window, min_edge_weight=1, n_threads=4):
    """ """
    db = lite.connect(db_path, isolation_level=None)

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
        """
        create table co_reply_network (
            user_1,
            user_2,
            weight,
            primary key (user_1, user_2)
        ) without rowid
        """
    )

    query = """
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
        where user_1 in (select user_id from user_id)
        group by e_1.user_id, e_2.user_id
        having weight >= ?2
    """

    return parallise_query_by_user_id(
        db_path,
        "co_reply_network",
        query,
        (time_window, min_edge_weight),
        n_processes=n_threads,
        sqlite_functions=None
    )



def compute_co_link_network(db_path, time_window, n_threads=4, min_edge_weight=1, resolved=False):

    db = lite.connect(db_path, isolation_level=None)

    print("Ensure the indexes exist to drive the join.")

    db.execute("begin")

    db.execute("drop table if exists co_link_network")
    db.execute(
        """
        create table co_link_network (
            user_1,
            user_2,
            weight,
            primary key (user_1, user_2)
        ) without rowid;
        """
    )

    if resolved:

        db.execute(
            """
            create index if not exists resolved_url_message on resolved_message_url(
                resolved_url, timestamp
            )
            """
        )

        db.execute(
            """
            create index if not exists resolved_user_url on message_url(
                user_id, url, timestamp
            )
            """
        )

        query ="""
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
            where user_1 in (select user_id from user_id)
            group by e_1.user_id, e_2.user_id
            having weight >= ?2
        """

    else:

        db.execute(
            """
            create index if not exists url_message on message_url(
                url, timestamp
            )
            """
        )
        db.execute(
            """
            create index if not exists user_url on message_url(
                user_id, url, timestamp
            )
            """
        )
        query = """
            select
                e_1.user_id as user_1,
                e_2.user_id as user_2,
                count(*) as weight
            from message_url e_1
            inner join message_url e_2
                on e_1.url = e_2.url
                and e_2.timestamp between e_1.timestamp - ?1 and e_1.timestamp + ?1
            where user_1 in (select user_id from user_id)
            group by e_1.user_id, e_2.user_id
            having weight >= ?2
        """

    db.execute("commit")

    print("Calculating the co-link network")
    return parallise_query_by_user_id(
        db_path,
        "co_link_network",
        query,
        (time_window, min_edge_weight),
        n_processes=n_threads,
        sqlite_functions=None
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
        create table co_similar_tweet_network (
            user_1,
            user_2,
            weight,
            primary key (user_1, user_2)
        ) without rowid;
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

    db.execute("commit")

    print("Calculating similarity")

    query = """
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
            and user_1 in (select user_id from user_id)
        group by e_1.user_id, e_2.user_id
        having weight >= ?2
    """

    return parallise_query_by_user_id(
        db_path,
        "co_similar_tweet_network",
        query,
        [time_window, min_edge_weight, similarity_threshold],
        n_processes=n_threads,
        sqlite_functions={'similarity': (similarity_function, 2)}
    )


def compute_co_retweet_parallel(db_path, time_window, n_threads=4, min_edge_weight=1):

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
    query = """
        select
            e_1.user_id as user_1,
            e_2.user_id as user_2,
            count(*) as weight
        from edge e_1
        inner join edge e_2
            on e_1.repost_id = e_2.repost_id
            and e_2.timestamp between e_1.timestamp - ?1
                and e_1.timestamp + ?1
            and user_1 in (select user_id from user_id)
        group by e_1.user_id, e_2.user_id
        having count(*) >= ?
    """
    print("Calculating the co-retweet network")
    return parallise_query_by_user_id(
        db_path,
        "co_retweet_network",
        query,
        (time_window, min_edge_weight),
        n_processes=n_threads,
        sqlite_functions=None
    )


def _run_query(
    db_path,
    target_table,
    query,
    query_parameters,
    user_ids,
    sqlite_functions,
):
    """Run the target query on the subset of user_ids provided."""

    db = lite.connect(db_path)
    db.execute(
        """
        create temporary table local_network (
            user_id_1,
            user_id_2,
            weight
        );
        """
    )
    db.execute(
        """
        create temporary table user_id (
            user_id primary key
        );
        """
    )

    for func_name, (func, n_args) in sqlite_functions.items():
        db.create_function(func_name, n_args, func)

    # Note that this part is writing to temporary databases, which are independent
    # between processes, so this part can always be done in parallel
    with db:
        db.executemany(
            "insert into user_id values(?)", ((user_id,) for user_id in user_ids)
        )

        db.execute(
            f"""
            insert into local_network
                {query}
            """,
            query_parameters,
        )

    # This part requires the lock because it's writing back to the shared database.
    # Alternatively this could just spin on busy, so we don't need to worry about
    # the lock...
    with lock, db:
        db.execute(f"insert into {target_table} select * from local_network")


# Via this snippet from stackoverflow:
# https://stackoverflow.com/questions/25557686/python-sharing-a-lock-between-processes
# TLDR - have to take special care when trying to pass locks to a background
# process in a pool.
def __init(l):
    global lock
    lock = l


def parallise_query_by_user_id(
    db_path,
    target_table,
    query,
    query_parameters,
    n_processes=4,
    sqlite_functions=None,
):
    """
    Helper utility for executing network calculations that are parallelisable at the user level.

    Parallelisation is done using multiprocessing, allowing user defined functions to run in the SQLite
    layer without being constrained by the GIL.

    The query must be a select query, describing part of the calculation according to the following rules:

    - it must reference the subset of user_ids as the local temporary table called temp_users
    - the query parameters will be passed through directly to the query without alteration.

    The query should be writable to target_table with the following schema:

        create table {target_table} (
            user_1,
            user_2,
            weight,
            primary key (user_1, user_2)
        ) without rowid;

    Extra functions is a map from an SQLite function named in the query to a
    Python function/number of arguments, to be setup in the background
    processes. This looks like the following dict, which maps to the SQLite
    wrapper call `db.create_function("similarity", 2, similarity_function)`:

        {'similarity': (similarity_function, 2)}

    """

    lock = mp.Lock()
    pool = ProcessPoolExecutor(
        max_workers=n_processes, initializer=__init, initargs=(lock,)
    )

    db = lite.connect(db_path)

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

            waiting.add(
                pool.submit(
                    _run_query,
                    db_path,
                    target_table,
                    query,
                    query_parameters,
                    user_ids,
                    sqlite_functions or {},
                )
            )

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
        waiting.add(
            pool.submit(
                _run_query,
                db_path,
                target_table,
                query,
                query_parameters,
                user_ids,
                sqlite_functions or {},
            )
        )
        wait(waiting)

    db.close()

    return completed
