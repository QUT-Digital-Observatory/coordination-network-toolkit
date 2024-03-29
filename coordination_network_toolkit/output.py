import csv
import networkx as nx

from coordination_network_toolkit.graph import (
    get_node_rows,
    get_edge_rows,
    load_networkx_graph,
)


def output_node_csv(db_path, output_file, n_messages=10):
    """
    Annotate a node with a sample of their n_messages most recent posts.

    This can be imported into tools like Gephi to complement the edge lists generated by
    networks.

    """

    with open(output_file, "w") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(
            ["Id", "username"] + [f"message_{i}" for i in range(n_messages)]
        )

        for row in get_node_rows(db_path, n_messages=n_messages):
            writer.writerow(row)


def output_gephi_csv(db_path, command, output_file, loops=False):

    with open(output_file, "w") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)

        writer.writerow(["source", "target", "weight", "edge_type"])

        rows = get_edge_rows(db_path, command, loops=loops)

        for row in rows:
            writer.writerow(row)


def output_graphml(db_path, command, output_file, loops=False, n_messages=10):
    """
    Output a graphml file, representing the nodes and edges of the given table.

    """

    graph = load_networkx_graph(db_path, command, loops=loops, n_messages=n_messages)

    nx.write_graphml(graph, output_file)


def write_output(
    database_name, network_type, output_filename, output_type="csv", **kwargs
):
    """Write the output, based on the given arguments."""
    if output_type == "graphml":
        output_fn = output_graphml
    elif output_type == "csv":
        output_fn = output_gephi_csv
    else:
        raise ValueError(
            f"{output_type} is not recognised as an available export format"
        )

    output_fn(database_name, network_type, output_filename, **kwargs)
