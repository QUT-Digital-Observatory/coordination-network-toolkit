"""
Tests of the graph construction procedure.

"""
import os

from coordination_network_toolkit import preprocess, compute_networks, graph


def test_graph_construction_workflow(tmpdir):

    source_data = "tests/data/coretweet_test_input.csv"
    db = os.path.join(tmpdir, "test.db")

    preprocess.preprocess_csv_files(db, [source_data])

    settings_nodes = (
        # (Time window, min_edge_weight, loops in the output), expected nodes
        ((1, 1, False), 2),
        ((1, 1, True), 9),  # Self loops will always include a single retweet
        ((1, 2, False), 0),  # No edges over the cutoff weight
        ((1, 2, True), 5),  # 5 accounts with more than one retweet for the self loop
        # Note that in this example, uid10 retweets a message twice - this is
        # counted as a self loop so not included in the output nodes.
        ((60, 1, False), 6),  
        ((60, 1, True), 9),  # All of the nodes should be included with self loops
    )

    for n_threads in (1, 2, 4):

        for (time_window, min_edge_weight, self_loops), nodes in settings_nodes:
            compute_networks.compute_co_retweet_parallel(
                db, time_window, min_edge_weight=min_edge_weight, n_threads=n_threads
            )

            g = graph.load_networkx_graph(
                db, "co_retweet", symmetric=True, loops=self_loops, n_messages=10
            )

            print(time_window, min_edge_weight, self_loops)
            assert len(g.nodes) == nodes
