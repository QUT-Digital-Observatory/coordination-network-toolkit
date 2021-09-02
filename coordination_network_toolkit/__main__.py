"""
A Python and command line tool for calculating coordination
networks of various types. It is primarily aimed at social media data, and the
examples and this is reflected in the names and examples used.

The input data for the preprocess command is a CSV file, with a header row,
containing fields in the following order:

- message_id: a unique id for the message. Messaged will be deduplicated using this
    field.
- user_id: a unique identifier for the user who posted the message.
- repost_id: for platforms that allow unedited broadcasting of other messages on the
    platform (such as retweets), this is the identifier of the original message.
    If this is an empty string this will be converted to null. If this field is
    populated, then co_link and co_message network computations will exclude this
    message, as a plain rebroadcast is always a duplicate.
- message: the text of the message.
- timestamp: the unix epoch timestamp of the message, in integer seconds.
- urls: a space delimited string containing urls. For Twitter URLs, it is better to
    use the expanded_url, not the default short_url Twitter automatically generates.
    If you use the short_url, co-link analysis will not be useful without running
    the resolver.
"""
import argparse
from coordination_network_toolkit.compute_networks import (compute_co_link_network,
    compute_co_reply_network, compute_co_retweet_parallel, compute_co_similar_tweet,
    compute_co_tweet_network)
from coordination_network_toolkit.preprocess import preprocess_csv_files, preprocess_twitter_json_files
from coordination_network_toolkit.output import write_output, output_node_csv
from coordination_network_toolkit.urls import resolve_all_urls
from coordination_network_toolkit.similarity import get_similarity_fn_from_min_size


_network_types = {
    'co_retweet': compute_co_retweet_parallel,
    'co_tweet': compute_co_tweet_network,
    'co_reply': compute_co_reply_network,
    'co_similar_tweet': compute_co_similar_tweet,
    'co_link': compute_co_link_network
}


def main():
    # TODO: how to best reference the docs?
    parser = argparse.ArgumentParser(
        prog="compute_networks.py",
        description=__doc__,
        epilog="For more information, see documentation at [docs url]",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "database",
        help="The path to the SQLite database holding the results. "
             "If it does not exist it will be created.",
    )

    subparsers = parser.add_subparsers(
        title="Commands",
        description="These are the available commands for coordination network processing. "
        "Each of the following sub-commands has further help information "
             "available by calling, for example, coordination_network_toolkit example.db preprocess --help",
        dest='command'
    )

    # preprocess subcommand
    preprocess_parser = subparsers.add_parser(
        'preprocess',
        help='Load data into a pre-processed format for network computation'
    )
    preprocess_parser.add_argument(
        "files",
        nargs="*",
        help="The files to be imported when running the preprocessing. Specify the "
             "format of the files with --format. All files must be the same format.",
    )
    preprocess_parser.add_argument(
        "--format",
        default="csv",
        choices=["csv", "twitter_json"],
        help="The format of the input files, defaulting to CSV. "
            "Twitter JSON format supports both V1.1 and V2 API formats. "
            "See documentation for a list of columns expected in CSV format."
    )

    # resolve_urls subcommand
    resolve_urls_parser = subparsers.add_parser(
        "resolve_urls",
        help="Resolve URLs in data with resolve_urls."
    )
    resolve_urls_parser.add_argument(
        "--max_redirects",
        type=int,
        default=1,
        help="The maximum number of redirects considered when resolving URLs",
    )

    # compute subcommand
    compute_parser = subparsers.add_parser(
        'compute',
        help="Compute network."
    )
    compute_parser.add_argument(
        'network_type',
        choices=_network_types.keys(),
        help="Which type of network to compute."
    )
    network_params_group = compute_parser.add_argument_group(
        'network_parameters',
        "Parameters for computing networks. Not all parameters apply to all types of"
        "network, please refer to the full documentation for details."
    )
    network_params_group.add_argument(
        "--time_window",
        type=int,
        default=1,
        help="The maximum number of seconds to consider when calculating coordination: "
        "if two events are recorded as further than this far apart in time they are "
        "not considered coordinated.",
    )
    network_params_group.add_argument(
        "--min_edge_weight",
        type=int,
        default=2,
        help="The minimum weight of an edge to be included in the output network. Edges "
        "are weighted by the number of tweets that are identified as coordinated between "
        "two users. A small mininum edge weight may include a very large number of "
        "spurious edges that indicate coincidence rather than coordination.",
    )
    network_params_group.add_argument(
        "--similarity_threshold",
        type=float,
        default=0.8,
        help="Only used for co-similarity networks. The similarity threshold for considering two tweets as similar "
        "documents. The default similarity function is Jaccard similarity and is "
        "normalised between 0 and 1.",
    )
    network_params_group.add_argument(
        "--min_document_size_similarity",
        type=float,
        default=1,
        help="Only used for co-similarity networks. The minimum document size (in tokens) to be considered when calculating "
        "document similarity. Documents smaller than this size are considered to have "
        "zero similarity with any other document.",
    )
    network_params_group.add_argument(
        "--resolved",
        action="store_true",
        help="Only used for co-link networks. When caculating a co-link network, use the resolved urls instead of the "
             "original URLs. Default is False. Requires that augment_data resolve_urls has "
             "been run beforehand, otherwise there will be no edges found.",
    )
    network_params_group.add_argument(
        "--n_cpus",
        type=int,
        default=2,
        help="The number of threads to use when calculating the co_retweet networks. "
             "Ignored for all other operations.",
    )
    network_params_group.add_argument(
        "--output_file",
        default=None,
        help="(optional) Immediately save the output to this file."
    )
    network_params_group.add_argument(
        "--output_format",
        choices=["csv", "graphml"],
        default="graphml",
        help="What format to export the network graph in. Default is graphml. "
        "Note that graphml output requires the whole graph to be loaded into "
        "memory before loading, and may not be appropriate for large graphs, or "
        "with low values for --min_edge_weight. Ignored if --output_file is not set.",
    )
    network_params_group.add_argument(
        "--include_symmetric_edges",
        help="Include the duplicate edges in the opposite direction. "
        "Default is False, so if there is an edge between A and B, there will be no "
        "edge recorded between B and A.",
        action="store_true",
    )
    network_params_group.add_argument(
        "--include_self_loops",
        help="Include loops (where users interact with their own posts.). Default is "
        "not to include loops.",
        action="store_true",
    )
    network_params_group.add_argument(
        "--n_messages",
        help="Number of messages to include in annotating a node's description.",
        type=int,
        default=10,
    )

    network_export_parser = subparsers.add_parser(
        'export_network',
        help='Export computed network edges to a file'
    )
    network_export_parser.add_argument(
        "output_file",
        help="Where to save the calculated output file."
    )
    network_export_parser.add_argument(
        'network_type',
        choices=_network_types.keys(),
        help='Which network to export. Network type must have previously been computed.'
    )
    network_export_parser.add_argument(
        "--output_format",
        choices=["csv", "graphml"],
        default="graphml",
        help="What format to export the network graph in. Default is graphml. "
        "Note that graphml output requires the whole graph to be loaded into "
        "memory before loading, and may not be appropriate for large graphs, or "
        "with low values for --min_edge_weight",
    )
    network_export_parser.add_argument(
        "--include_symmetric_edges",
        help="Include the duplicate edges in the opposite direction. "
        "Default is False, so if there is an edge between A and B, there will be no "
        "edge recorded between B and A.",
        action="store_true",
    )
    network_export_parser.add_argument(
        "--include_self_loops",
        help="Include loops (where users interact with their own posts.). Default is "
        "not to include loops.",
        action="store_true",
    )
    network_export_parser.add_argument(
        "--n_messages",
        help="Number of messages to include in annotating a node's description.",
        type=int,
        default=10,
    )

    # export_user_nodes subparser
    export_user_nodes_subparser = subparsers.add_parser(
        'export_user_nodes',
        help="Create an additional file listing user nodes and including their attributes."
    )
    export_user_nodes_subparser.add_argument(
        'output_file',
        help="The location to save the user node list to. The list will be saved as a"
             "CSV file."
    )


    # Process arguments
    args = parser.parse_args()

    if args.command == "preprocess":

        if not args.files:
            parser.error(
                "No files specified to preprocess."
            )

        if args.format == "csv":
            preprocess_csv_files(args.database, args.files)
        elif args.format == "twitter_json":
            preprocess_twitter_json_files(args.database, args.files)

    elif args.command == "resolve_urls":
        print(f"Resolving URLs on {args.database}")

        resolve_all_urls(args.database, args.max_redirects)

    elif args.command == "compute":
        network_calculation_status = (
            "Calculating a {args.network_type} network on {args.database} "
            "with the following settings:\n"
            "    time_window: {args.time_window} seconds\n"
            "    min_edge_weight: {args.min_edge_weight} co-ocurring messages\n"
            "    n_cpus: {args.n_cpus} processors\n"
            "    output_file: {args.output_file}"
        )

        if args.network_type == "co_retweet":
            print(network_calculation_status.format(args=args))
            compute_co_retweet_parallel(
                args.database,
                args.time_window,
                n_threads=args.n_cpus,
                min_edge_weight=args.min_edge_weight,
            )

        elif args.network_type == "co_tweet":
            print(network_calculation_status.format(args=args))
            compute_co_tweet_network(
                args.database, args.time_window, min_edge_weight=args.min_edge_weight
            )

        elif args.network_type == "co_reply":
            print(network_calculation_status.format(args=args))
            compute_co_reply_network(
                args.database, args.time_window, min_edge_weight=args.min_edge_weight
            )

        elif args.network_type == "co_similar_tweet":
            print(network_calculation_status.format(args=args))

            compute_co_similar_tweet(
                args.database,
                args.time_window,
                min_edge_weight=args.min_edge_weight,
                similarity_threshold=args.similarity_threshold,
                similarity_function=get_similarity_fn_from_min_size(args.min_document_size_similarity),
            )

        elif args.network_type == "co_link":
            print(network_calculation_status.format(args=args))
            print(f"    resolved: {args.resolved}")
            compute_co_link_network(
                args.database,
                args.time_window,
                min_edge_weight=args.min_edge_weight,
                resolved=args.resolved,
            )

        if args.output_file:
            print(args)
            write_output(
                args.database, args.network_type, args.output_file,
                output_type=args.output_format,
                symmetric=args.include_symmetric_edges,
                loops=args.include_self_loops,
                n_messages=args.n_messages
            )

    elif args.command == "export_network":
        write_output(
            args.database, args.network_type, args.output_file,
            output_type=args.output_format,
            symmetric=args.include_symmetric_edges,
            loops=args.include_self_loops,
            n_messages=args.n_messages
        )

    elif args.command == "export_user_nodes":
        print("Generating a user node annotation file")
        output_node_csv(args.database, args.output_file)


if __name__ == "__main__":
    main()
