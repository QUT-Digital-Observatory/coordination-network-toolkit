# Coordination Network Toolkit

A small command line tool and set of functions for studying coordination networks
in Twitter and other social media data.

## Rationale and Background


## Reading List


## Installation and Requirements

This tool requires a working Python 3.6 (or later) environment. 

This tool can be installed from pip - this will handle installing the necessary
dependencies.

`pip install coordination_network_toolkit`


## Basic Usage

Using this tool requires:

1. Collecting data from the social media platform of choice
2. Preparing or preprocessing the data into one of the formats the tool accepts:
	- either a specific CSV format that works across all platforms OR
	- using the platform native format (currently only Twitter JSON is supported)
3. Preprocessing the raw data into an SQLite database to setup the data structures for
   efficient computation of the different networks
4. Generating the network of choice, storing the output in a specified file.


## Worked Example

1. Collect Twitter data in the native Twitter JSON format using
[twarc](https://github.com/docnow/twarc/)
    
    - `twarc search '#banana' --output banana.json`

2. Bring the data into a local database file called processed_bananas.db. The original
data file is not modified in any way. The toolkit will handle processing the Twitter
JSON format into the necessary format, including handling things like retweets, replies,
urls, and extracting the text of the tweet. It will also handle deduplication, so if a
tweet is  present more than once only the first instance will be recorded.

    - `python -m coordination_network_toolkit processed_bananas.db preprocess --format twitter_json banana.json`

3. Calculate a retweet network, saving the output to a graphml format that can be
directly opened in a tool like (Gephi)[https://gephi.org]. These settings indicate that
if two users have retweeted the same tweet within 60 seconds of each other, there is a
potential link.

    - `python -m coordination_network_toolkit processed_bananas.db compute co_retweet --time_window 60 --output_file bananas_retweet_60s.graphml --output_format graphml`

3. Calculate a co-link network, again saving the output in graphml format. By default
this will use the plain text of the URL for matching, so the output here will confuse
urls that are shortened.

    - `python -m coordination_network_toolkit processed_bananas.db compute co_link --time_window 60 --output_file bananas_colink_unresolved_60s.graphml --output_format graphml`

4. Resolve collected URLs, to handle link shortening services. Note that this process
is intentionally rate limited to resolve no more than 25 urls/second. Once resolved,
URLs will not be retried, so you can safely run this command again.

    - `python -m coordination_network_toolkit processed_bananas.db resolve_urls`

5. Calculate the co-link network, this time using the resolved urls.

    - `python -m coordination_network_toolkit processed_bananas.db compute co_link --time_window 60 --output_file bananas_colink_resolved_60s.graphml --output_format graphml --resolved`


## Supported Input Formats

### Twitter

- JSON data from V1.1 of the Twitter API can be ingested directly.


### CSV (All other platforms)

. The files must be "
             "CSV files with 5 columns in the following order: message_id, user_id, repost_id, "
             "message_text and timestamp. The timestamp needs to be a unix epoch format number "
             "seconds.

To use the CSV ingest format, construct a CSV with a header and the following columns.
The names of the columns don't matter but the order does.

- message_id: the unique identifier of the message on the platform
- user_id: the unique identifier of the user on the platform
- username: the text of the username (only used for display)
- repost_id: if the message is a verbatim report of another message (such as a retweet 
	or reblog), this is the identifier of that other message. Empty strings will be 
	converted to null
- reply_id: if the message is in reply to another message, the identifier for that other
    message. Empty strings will be converted to null.
- message: the text of the message.
- timestamp: A timestamp in seconds for the message. The absolute offset does not matter,
    but it needs to be consistent across all rows
- urls: A space delimited string containing all of the URLs in the message
