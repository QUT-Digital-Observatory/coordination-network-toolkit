# Coordination Network Toolkit

A small command line tool and set of functions for studying coordination networks
in Twitter and other social media data.

## Rationale and Background

<img src="https://i.ibb.co/qJVwBKR/coretweet-min-Weight2-coloured-By-Account-Status.png" alt="Network visualisation of a co-retweet network" width="400" height="400" align="right">

Social media activity doesn't occur in a vaccuum. Individuals on social media are often taking part in *coordinated activities* such as protest movements or interest-based communities. 

Social media platforms are also used strategically to boost particular messages in line with political campaign goals or for commercial profit and scamming. This involves multiple accounts posting or reposting the same content, repeatedly and within a short time window (e.g. within 1 minute).

This software provides a toolkit to detect coordinated activity on social media and to generate networks that map the actors and their relationships. It provides a general purpose toolkit for multiple types of coordinated activity on any type of social media platform.

Fundamentally this toolkit produces networks where the nodes are accounts, and the weighted edges between these accounts are the number of messages from those accounts that meet some criterion for a type of *coordination*. This toolkit already implements several approaches to detecting different types of coordination, and is intended to be extensible to more cases in the future.

Firstly, it includes functionality for *co-tweeting* and *co-retweeting* (Keller et al., 2019; Schafer et al., 2017), where accounts post exactly the same text (co-tweets) or repost the same post within a short time window (co-retweets). Secondly, it includes functionality for *co-link* analysis, where multiple accounts post the same URLs repeatedly and in a short time window of each other (Giglietto et al., 2020). Thirdly, it adds two new types of network types: *co-reply*, where accounts are replying to the same post repeatedly together; and *co-similarity*, where accounts post similar text (but not exact duplicates), which relaxes the strict assumption of co-tweeting. 

### Five types of coordination networks

1. Co-retweet: reposting the same post
2. Co-tweet: posting identical text
3. Co-similarity: posting similar text (Jaccard similarity or user-defined)
4. Co-link: posting the same link
5. Co-reply: replying to the same post

Default time window is 60 seconds for all network types.

## Reading List

Giglietto, F., Righetti, N., Rossi, L., & Marino, G. (2020). It takes a village to manipulate the media: coordinated link sharing behavior during 2018 and 2019 Italian elections. Information, Communication and Society, 1–25.

Graham, T., Bruns, A., Zhu, G., & Campbell, R. (2020). Like a virus: The coordinated spread of coronavirus disinformation. Report commissioned for the Centre for Responsible Technology.

Keller, F. B., Schoch, D., Stier, S., & Yang, J. (2020). Political Astroturfing on Twitter: How to coordinate a disinformation Campaign. Political Communication, 37(2), 256-280.

Schafer, F., Evert, S., & Heinrich, P. (2017). Japan’s 2014 General Election: Political Bots, Right-Wing Internet Activism, and Prime Minister Shinz Abe’s Hidden Nationalist Agenda. Big Data, 5(4), 294–309.


## Installation and Requirements

This tool requires a working Python 3.6 (or later) environment. 

This tool can be installed from pip - this will handle installing the necessary
dependencies.

`pip install coordination_network_toolkit`

Once you have installed, you can use the toolkit in either of two ways:

1. As a command-line tool (run `compute_networks --help` to find out how)
2. As a Python library (`import coordination_network_toolkit`)


## Basic Usage

Using this tool requires:

1. Collecting data from the social media platform of choice
2. Preparing or preprocessing the data into one of the formats the tool accepts:
	- either a specific CSV format that works across all platforms OR
	- using the platform native format (currently Twitter JSON from both
        V1.1 and V2 APIs, including the Twitter Academic track, are supported)
3. Preprocessing the raw data into an SQLite database to setup the data structures for
   efficient computation of the different networks
4. Generating the network of choice, storing the output in a specified file.


## Examples

This is just a quick example - see also [the tutorial.](docs/tutorial.md)

### Worked example - CLI tool

1. Collect Twitter data in the native Twitter JSON format using
[twarc](https://github.com/docnow/twarc/), from either the V1.1 and V2 APIs
    
    - `twarc search '#banana' --output banana.json`
    - `twarc2 search --limit 1000 --archive '#purple' purple.json  `

2. Bring the data into a local database file called processed_bananas.db. The original
data file is not modified in any way. The toolkit will handle processing the Twitter
JSON formats into the necessary format, including handling things like retweets, replies,
urls, and extracting the text of the tweet. It will also handle deduplication, so if a
tweet is  present more than once only the first instance will be recorded.

    - `compute_networks processed_bananas.db preprocess --format twitter_json banana.json purple.json`

3. Calculate a retweet network, saving the output to a graphml format that can be
directly opened in a tool like (Gephi)[https://gephi.org]. These settings indicate that
if two users have retweeted the same tweet within 60 seconds of each other, there is a
potential link.

    - `compute_networks processed_bananas.db compute co_retweet --time_window 60 --output_file bananas_retweet_60s.graphml --output_format graphml`

3. Calculate a co-link network, again saving the output in graphml format. By default
this will use the plain text of the URL for matching, so the output here will confuse
urls that are shortened.

    - `compute_networks processed_bananas.db compute co_link --time_window 60 --output_file bananas_colink_unresolved_60s.graphml --output_format graphml`

4. Resolve collected URLs, to handle link shortening services. Note that this process
is intentionally rate limited to resolve no more than 25 urls/second. Once resolved,
URLs will not be retried, so you can safely run this command again.

    - `compute_networks processed_bananas.db resolve_urls`

5. Calculate the co-link network, this time using the resolved urls.

    - `compute_networks processed_bananas.db compute co_link --time_window 60 --output_file bananas_colink_resolved_60s.graphml --output_format graphml --resolved`


### Python library usage example

You can find the following example as a Jupyter notebook you can run yourself in
`examples/example.ipynb`.

```
import coordination_network_toolkit as coord_net_tk
import networkx as nx

# Preprocess CSV data into database
coord_net_tk.preprocess.preprocess_csv_files(db_name, [csv_filename])

# Calculate similarity network
coord_net_tk.compute_networks.compute_co_similar_tweet(db_name, 60)

# Load data as a networkx graph
similarity_graph = coord_net_tk.graph.load_networkx_graph(db_name, "co_similar_tweet")

# Play with the graph!
for g in nx.connected_components(similarity_graph):
    print(g)
```


## Supported Input Formats

### Twitter

JSON data from V1.1 and V2 of the Twitter API can be ingested directly.


### CSV (All other platforms)

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


## Cite the Coordination Network Toolkit

Graham, Timothy; QUT Digital Observatory; (2020): Coordination Network Toolkit. 
Queensland University of Technology. (Software) 
https://doi.org/10.25912/RDF_1632782596538


## Looking for Help?

Are you getting stuck somewhere or want to ask questions about using this toolkit? Please open an issue or bring your questions to the Digital Observatory's fortnightly [office hours](https://research.qut.edu.au/digitalobservatory/office-hours/).