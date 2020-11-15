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
    """Initialise the database, ensuring the correct schema is in place."""

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
            message_length integer,
            message_hash blob,
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

        """
    )

    return db
