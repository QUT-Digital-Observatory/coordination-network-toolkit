# This is a shell script for end to end testing, exercising the CLI and each
# network computational method It is intended for manual testing on real
# data, rather than automated testing of small datasets. It is particularly
# useful for testing the overall impact of changes for timing and
# repeatability of results.
#
# Usage: Run this shell script on an already processed database containing
# data:
#
# ./tests/all_networks_test.sh example_data.db
#
# It will run each of the networks, than return the number of edges in the
# result networks (including both directions and self-loops)

compute_networks $1 compute co_tweet --time_window $2 --n_cpus $3 --min_edge_weight $4
compute_networks $1 compute co_retweet --time_window $2 --n_cpus $3 --min_edge_weight $4
compute_networks $1 compute co_link --time_window $2 --n_cpus $3 --min_edge_weight $4
compute_networks $1 compute co_reply --time_window $2 --n_cpus $3 --min_edge_weight $4
compute_networks $1 compute co_similar_tweet --time_window $2 --similarity_threshold 0.8 --n_cpus $3 --min_edge_weight $4


sqlite3 $1 "select 'co-link edges', count(*) from co_link_network; \
select 'co-tweet edges', count(*) from co_tweet_network; \
select 'co-reply edges', count(*) from co_reply_network; \
select 'co-retweet edges', count(*) from co_retweet_network; \
select 'co-similar edges', count(*) from co_similar_tweet_network"
