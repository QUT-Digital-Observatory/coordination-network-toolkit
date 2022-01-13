"""
compute_networks.py

This is the core module for the actual computation of coordination networks.
It provides some infrastructure for "fast-enough" computation of coordination
networks. Fast enough in this context means:

- Providing a framework for parallel computation of parts of graphs, including
  a recommended starting point for how to decompose the problem into parts of
  the graph. Importantly, this is designed to allow Python code to be
  parallelised so we can make the most of our local compute while still
  working in a higher level language.
- Enable delegation of work to SQLite rather than Python - SQLite can be
  faster, and naturally enables out-of-core computation rather than explicitly
  loading and working with large datasets in memory.
- Make the most of data locality of reference for effective computation at
  each stage of the process. This is principally enabled by the construction
  of appropriate database indexes for particular problems.

The recommended starting point for adding a new network type is to start with
the `parallelise_query_by_user_id` function. This automatically handles
creating batches of user ids and dispatching them to execute a the provided
query on each of those batches in parallel. If you can write an SQLite query
that computes your desired network for a subset of users specified in a
temporary table called `user_id` this function will handle parallelisation,
storing local temporary results and then aggregating them into a single table
at the end.

"""


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


def parallise_query_by_user_id(
    db_path,
    target_table,
    query,
    query_parameters,
    n_processes=4,
    sqlite_functions=None,
    user_selection_query="select distinct user_id from edge",
    user_selection_query_parameters=None,
):
    """
    Helper utility for executing network calculations that are parallelisable
    at the user level.

    Parallelisation is done using multiprocessing, allowing user defined
    functions to run in the SQLite layer without being constrained by the
    GIL.

    The query must be a select query, describing part of the calculation
    according to the following rules:

    - it must reference the a subset of user_ids in the local temporary table
      called `user_id`
    - the query parameters will be passed through directly to the query
      without alteration.

    The query should be writable to target_table with the following schema:

        create table {target_table} (
            user_1,
            user_2,
            weight,
            primary key (user_1, user_2)
        ) without rowid;

    sqlite_functions: a map from an SQLite function named in the query to a
    Python function/number of arguments, to be setup in the background
    processes. This looks like the following dict, which maps to the SQLite
    wrapper call `db.create_function("similarity", 2, similarity_function)`:

        {'similarity': (similarity_function, 2)}

    user_selection_query: an SQLite query that selects a column of user_id's
    to use in calculation. The default is to select all users, but for some
    calculations it is possible to prune user's early and avoid some
    computation. It can only use base SQLite features and cannot take any
    parameters. Note that this query will run in a single thread - if you're
    not careful running this query will take more time than the actual
    calculation, so use with care.

    """

    manager = mp.Manager()
    lock = manager.Lock()
    pool = ProcessPoolExecutor(max_workers=n_processes)

    db = lite.connect(db_path)

    waiting = set()
    count = 0
    completed = 0
    submitted = 0

    user_ids = [
        row[0]
        for row in db.execute(
            user_selection_query, user_selection_query_parameters or []
        )
    ]

    target_batches = n_processes * 10
    batch_size = max(math.floor(len(user_ids) / target_batches), 1)

    # Generate batches of user_ids
    batches = [
        user_ids[i : i + batch_size] for i in range(0, len(user_ids), batch_size)
    ]

    for batch in batches:
        waiting.add(
            pool.submit(
                _run_query,
                db_path,
                target_table,
                query,
                query_parameters,
                batch,
                sqlite_functions or {},
                lock,
            )
        )

    while waiting:
        done, waiting = wait(waiting, return_when=FIRST_COMPLETED)

        for d in done:
            # Observe errors
            d.result()
            completed += 1

            if not (completed % math.ceil(len(batches) / 10)):
                print(f"Completed {completed} / {len(batches)}")

    print(f"Completed {completed} / {len(batches)}")

    db.close()

    return completed


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
            """
            create index if not exists text_needs_processing on edge(message_id)
                where repost_id is null and transformed_message is null;
            """
        )
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
        print("Ensuring the necessary indexes exists")

        db.execute("drop index if exists user_time")
        db.execute(
            """
            create index if not exists non_repost_user_time on edge(user_id, timestamp)
                where repost_id is null;
            """
        )
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

    # Optimisation - a user can never have an edge if the account doesn't have more
    # then min_edge_weight non-repost messages in the dataset
    user_selection_query = """
        select
            user_id
        from edge
        where repost_id is null
        group by user_id
        having count(*) >= ?
    """

    user_selection_query_parameters = [min_edge_weight]

    query = """
        select
            e_1.user_id as user_1,
            e_2.user_id as user_2,
            count(distinct e_1.message_id) as weight
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

    print("computing co_tweet network")
    return parallise_query_by_user_id(
        db_path,
        "co_tweet_network",
        query,
        (time_window, min_edge_weight),
        n_processes=n_threads,
        sqlite_functions=None,
        user_selection_query=user_selection_query,
        user_selection_query_parameters=user_selection_query_parameters,
    )


def compute_co_reply_network(db_path, time_window, min_edge_weight=1, n_threads=4):
    """
    Compute a co-reply network on the given database.

    Co replies occur when users reply to the same specific message within
    `time_window` of each other.

    """
    db = lite.connect(db_path, isolation_level=None)

    print("Ensuring the necessary indexes exists")
    db.execute(
        """
        create index if not exists reply_non_repost_user_time on edge(
            -- The repost_id is totally superfluous, but it's a small
            -- cost overhead to make SQLite happy to use this as a
            -- covering index.
            user_id, timestamp, reply_id, repost_id
        )
            where repost_id is null and reply_id is not null;
        """
    )
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
            count(distinct e_1.message_id) as weight
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

    # Optimisation - a user can never have an edge if the account doesn't have more
    # then min_edge_weight replies in the dataset
    user_selection_query = """
        select
            user_id
        from edge
        where repost_id is null
            and reply_id is not null
        group by user_id
        having count(*) >= ?
    """

    user_selection_query_parameters = [min_edge_weight]

    return parallise_query_by_user_id(
        db_path,
        "co_reply_network",
        query,
        (time_window, min_edge_weight),
        n_processes=n_threads,
        sqlite_functions=None,
        user_selection_query=user_selection_query,
        user_selection_query_parameters=user_selection_query_parameters,
    )


def compute_co_post_network(db_path, time_window, min_edge_weight=1, n_threads=4):
    """
    Compute a co-post network on the given database.

    Co-post messages are the most basic unit of possible coordination:
    people posting messages within `time_window` of each other, regardless of
    the content or characteristics of the message. They are useful as a kind
    of null model, because they describe the maximum possible edge weight
    possible between two accounts.

    """
    db = lite.connect(db_path, isolation_level=None)

    print("Ensuring the necessary indexes exists")
    db.execute("create index if not exists user_time on edge(user_id, timestamp)")
    db.execute("create index if not exists timestamp_user on edge(timestamp, user_id)")
    db.execute("drop table if exists co_post_network")
    db.execute(
        """
        create table co_post_network (
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
            count(distinct e_1.message_id) as weight
        from edge e_1
        inner join edge e_2
            on e_2.timestamp between e_1.timestamp - ?1 and e_1.timestamp + ?1
        where user_1 in (select user_id from user_id)
        group by e_1.user_id, e_2.user_id
        having weight >= ?2
    """

    # Optimisation - a user can never have an edge if the account doesn't have more
    # then min_edge_weight messages
    user_selection_query = """
        select
            user_id
        from edge
        group by user_id
        having count(*) >= ?
    """

    user_selection_query_parameters = [min_edge_weight]

    return parallise_query_by_user_id(
        db_path,
        "co_post_network",
        query,
        (time_window, min_edge_weight),
        n_processes=n_threads,
        sqlite_functions=None,
        user_selection_query=user_selection_query,
        user_selection_query_parameters=user_selection_query_parameters,
    )


def compute_co_link_network(
    db_path, time_window, n_threads=4, min_edge_weight=1, resolved=False
):

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

        # TODO: need to check that messages have actually been resolved, otherwise the
        # resolved_message_url table won't exist!

        db.execute(
            """
            create index if not exists resolved_url_message on resolved_message_url(
                resolved_url, timestamp
            )
            """
        )

        db.execute(
            """
            create index if not exists resolved_user_url on resolved_message_url(
                user_id, resolved_url, timestamp
            )
            """
        )

        query = """
            select
                e_1.user_id as user_1,
                e_2.user_id as user_2,
                count(distinct e_1.message_id) as weight
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

        # Optimisation - a user can never have an edge if the account hasn't posted
        # more than min_edge_weight links
        user_selection_query = """
            select
                user_id
            from resolved_message_url
            group by user_id
            having count(*) >= ?
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
                count(distinct e_1.message_id) as weight
            from message_url e_1
            inner join message_url e_2
                on e_1.url = e_2.url
                and e_2.timestamp between e_1.timestamp - ?1 and e_1.timestamp + ?1
            where user_1 in (select user_id from user_id)
            group by e_1.user_id, e_2.user_id
            having weight >= ?2
        """

        # Optimisation - a user can never have an edge if the account hasn't posted
        # more than min_edge_weight links
        user_selection_query = """
            select
                user_id
            from message_url
            group by user_id
            having count(*) >= ?
        """

    db.execute("commit")

    print("Calculating the co-link network")
    return parallise_query_by_user_id(
        db_path,
        "co_link_network",
        query,
        (time_window, min_edge_weight),
        n_processes=n_threads,
        sqlite_functions=None,
        user_selection_query=user_selection_query,
        user_selection_query_parameters=[min_edge_weight],
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
        drop index if exists user_time;
        create index if not exists non_repost_user_time on edge(user_id, timestamp)
            where repost_id is null;
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
            count(distinct e_1.message_id) as weight
        from edge e_1 indexed by non_repost_user_time
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

    # Optimisation - a user can never have an edge if the account doesn't have more
    # then min_edge_weight non-repost messages in the dataset. This is the same as
    # co-tweet behaviour
    user_selection_query = """
        select
            user_id
        from edge
        where repost_id is null
        group by user_id
        having count(*) >= ?
    """

    return parallise_query_by_user_id(
        db_path,
        "co_similar_tweet_network",
        query,
        [time_window, min_edge_weight, similarity_threshold],
        n_processes=n_threads,
        sqlite_functions={"similarity": (similarity_function, 2)},
        user_selection_query=user_selection_query,
        user_selection_query_parameters=[min_edge_weight],
    )


def compute_co_retweet_parallel(db_path, time_window, n_threads=4, min_edge_weight=1):

    db = lite.connect(db_path)

    print("Ensure the indexes exist to drive the join.")
    db.executescript(
        """
        create index if not exists repost_user_time on edge(user_id, timestamp, repost_id)
            where repost_id is not null;
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
            count(distinct e_1.message_id) as weight
        from edge e_1
        inner join edge e_2
            on e_1.repost_id = e_2.repost_id
            and e_2.timestamp between e_1.timestamp - ?1
                and e_1.timestamp + ?1
            and user_1 in (select user_id from user_id)
        group by e_1.user_id, e_2.user_id
        having weight >= ?
    """

    print("Calculating the co-retweet network")

    # Optimisation - a user can never have an edge if the account doesn't have
    # more then min_edge_weight reposted messages in the dataset.
    user_selection_query = """
        select
            user_id
        from edge
        where repost_id is not null
        group by user_id
        having count(*) >= ?
    """

    return parallise_query_by_user_id(
        db_path,
        "co_retweet_network",
        query,
        (time_window, min_edge_weight),
        n_processes=n_threads,
        sqlite_functions=None,
        user_selection_query=user_selection_query,
        user_selection_query_parameters=[min_edge_weight],
    )


def _run_query(
    db_path, target_table, query, query_parameters, user_ids, sqlite_functions, lock
):
    """Run the target query on the subset of user_ids provided."""

    db = lite.connect(db_path)
    db.execute(
        f"""
        create temporary table local_network as
            select *
            from {target_table}
            limit 0
        ;
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
