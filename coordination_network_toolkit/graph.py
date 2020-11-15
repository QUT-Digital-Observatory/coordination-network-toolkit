import sqlite3 as lite
import networkx as nx

from coordination_network_toolkit.database import COMMAND_TABLE_MAPPING


def get_edge_rows(db_path, command, symmetric=False, loops=False):
    """
    Return an iterator over the rows of the source table in the given database file.

    """

    if command not in COMMAND_TABLE_MAPPING:
        raise ValueError(f"No known table for command {command}.")

    table = COMMAND_TABLE_MAPPING[command]

    if symmetric and loops:
        query = f"select *, '{command}' from {table}"
    elif symmetric:
        query = f"select *, '{command}' from {table} where user_1 != user_2"
    elif loops:
        query = f"select *, '{command}' from {table} where user_2 >= user_1"
    else:
        query = f"select *, '{command}' from {table} where user_2 > user_1"

    db = lite.connect(db_path)

    rows = db.execute(query)

    return rows


def get_node_rows(db_path, n_messages=10):
    """Get an iterator of rows in the node table."""
    db = lite.connect(db_path)

    db.execute("create index if not exists user_time on edge(user_id, timestamp)")

    user_ids = (row[0] for row in db.execute("select distinct user_id from edge"))

    for user_id in user_ids:

        username = db.execute(
            "select max(username) from edge where user_id = ?", [user_id]
        ).fetchone()[0]

        tweets = [
            row[0]
            for row in db.execute(
                """
                select message 
                from edge
                where user_id = ?
                order by timestamp desc
                limit ? 
                """,
                [user_id, n_messages],
            )
        ]

        tweets.extend([""] * (n_messages - len(tweets)))

        yield [user_id, username] + tweets


def load_networkx_graph(db_path, command, symmetric=False, loops=False, n_messages=10):
    """Return a networkx graph object representing the given source table."""

    g = nx.Graph()

    edges = get_edge_rows(db_path, command, symmetric=symmetric, loops=loops)

    # Add the edges
    for user_1, user_2, weight, edge_type in edges:
        g.add_edge(user_1, user_2, weight=weight, edge_type=edge_type)

    # Add the node annotations
    nodes = get_node_rows(db_path, n_messages=n_messages)
    for row in nodes:
        user_id = row[0]

        # Only add the node annotations if the node is present from an edge
        if user_id in g.nodes:
            attrs = {"username": row[1]}
            for i, message in enumerate(row[2:]):
                attrs[f"message_{i}"] = message
            g.add_node(user_id, **attrs)

    return g
