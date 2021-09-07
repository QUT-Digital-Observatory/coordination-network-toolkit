import sqlite3 as lite


# Provides the mapping from the CLI command to the table in the database.
COMMAND_TABLE_MAPPING = {
    "co_retweet": "co_retweet_network",
    "co_tweet": "co_tweet_network",
    "co_reply": "co_reply_network",
    "co_link": "co_link_network",
    "co_similar_tweet": "co_similar_tweet_network",
}


def initialise_db(db_path: str):
    """
    Initialise the database, ensuring the correct schema is in place.

    Raises a ValueError if the on disk format is incompatible with this version.

    """

    db = lite.connect(db_path, isolation_level=None)

    db.executescript(
        """
        pragma journal_mode=WAL;
        pragma synchronous=normal;

        create table if not exists edge (
            message_id primary key,
            user_id not null,
            username text,
            repost_id,
            reply_id,
            message text,
            -- The following now describes the transformation of the message,
            -- as co-tweet analysis needs to be robust to some non-consequential
            -- variations.
            transformed_message text,
            transformed_message_length integer,
            transformed_message_hash blob,
            token_set text,
            timestamp integer
        );

        create table if not exists message_url(
            message_id references edge(message_id),
            url,
            timestamp,
            user_id,
            primary key (message_id, url)
        );

        create table if not exists resolved_url(
            url primary key,
            resolved_url,
            ssl_verified,
            resolved_status
        );

        create trigger if not exists url_to_resolve after insert on message_url
            begin
                insert or ignore into resolved_url(url) values(new.url);
            end;

        create table if not exists metadata (
            property primary key,
            value
        );

        insert or ignore into metadata values('version', 1);
        """
    )

    # Sniff the columns in the ondisk format, to handle databases created
    # before the version check
    edge_columns = {row[1] for row in db.execute("pragma table_info('edge')")}

    # Current version in the database
    version = list(db.execute("select value from metadata where property = 'version'"))[
        0
    ][0]

    if "message_length" in edge_columns or version != 1:
        raise ValueError(
            "This database is not compatible with this version of the "
            "coordination network toolkit - you will need to reprocess your data "
            "into a new database."
        )

    return db
