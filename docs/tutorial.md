# Using the Coordination Network Toolkit from the Command Line

This tutorial will work through an example of using the toolkit to analyse potential coordination in a social media dataset. We will be working in a command line environment for this tutorial.

## Installation

### Python

This toolkit is built in the Python software language, and requires that Python (any version from version 3.7 or newer) be installed on your system to be run. If you don't already have Python installed, we recommend using the official installers for [Mac](https://www.python.org/downloads/macos/) and [Windows](https://www.python.org/downloads/windows/) systems.

We recommend using the latest available version of Python (that is version 3.10 as of 2021-10-19).


### Installing the Toolkit

Once you have Python installed you will need to install the toolkit from the [Python Package Index (PyPI)](https://pypi.org/) using the `pip` tool. PyPI is a hosting system for open source packages, and `pip` is a allowing you to install and update releases of open source Python packages like this toolkit.

The first step is to open a `terminal` or `shell` for your operating system - a terminal is a text based interface allowing you to issues commands for your operating system to execute. On Windows this can be done by launching the `cmd` application by searching in the start bar. On a Mac you can launch the `Terminal` application from the launchpad.

Once you have a terminal opened you can run the following command to install (or upgrade) the coordination network toolkit:

	python -m pip install --upgrade coordination-network-toolkit

Note that the `--upgrade` flag will ensure that the latest version is installed. As the toolkit is currently under active development it will be worth checking for new versions regularly so that you're getting the latest features. For reproducibility purposes, you might also consider keeping a record of which version of the toolkit you're running.


## Data Collection From Twitter

The Coordination Network Toolkit is built on the idea of transforming your existing social media data into a network to make coordination visible. Therefore, to use the toolkit you need to already have collected data and have it saved as one or more files on your hard drive. The toolkit takes data in two forms: as a CSV file with a specific format, or the JSON format of tweets as returned by the V1.1 and V2 Twitter API's. If you're working with Twitter data the JSON format is simpler and requires no data transformation: the CSV format is only needed if you're working with platforms other than Twitter.

For this tutorial we will focus on collecting tweets in JSON format using [twarc](https://twarc-project.readthedocs.io/en/latest/). To follow this example, you will need to setup a Twitter [developer account](https://developer.twitter.com/en) to enable API access. Once you have the developer account setup, you will need the `bearer_token` available from the dashboard for your project to allow twarc to interact with the Twitter API. Note that the `bearer_token` is functionally very similar to a password, so it shouldn't be shared or made publicly accessible.

## Install

First we will install twarc using pip by using the shell appropriate to our system:

	python -m pip install --upgrade twarc

## Configure

Then we will need to configure twarc to use the `bearer_token` from Twitter by running this command, then entering your `bearer_token` when prompted:

	twarc2 configure

### Keyword Search

Now we can actually collect data. We're going to start by looking at tweets mentioning "cryptocurrency" or "crypto". Before collecting anything it's a good idea to use the `counts` functionality to check volume. This helps you avoid accidentally consuming your Twitter API quota on a spelling mistake.

	twarc2 counts "cryptocurrency OR crypto" --text --granularity day

This shows that there are over 700,000 tweets per day containing either of those terms - we need to be careful not to consume all of our quota, so we will use the `--limit` option to only collect a small number of recent tweets.

	twarc2 search --limit 10000 "cryptocurrency OR crypto" crypto.json


## Using the Toolkit

Having collected some data, we can now start to work with it in the toolkit. Working with the toolkit consists of a few different operations to cover different data processing needs:

1. Preprocessing, where the collected data is transformed into a consistent format for more efficient computation, and also where quality control steps like removing duplicate messages happens.
2. Graph construction, where the actual edges in the specific coordination network is constructed.
3. Graph output, where the calculated network is enriched with profile information and turned into a graph in a standard file format.

The next sections will give a concrete example for the process.

### Preprocessing Data

First we start by preprocessing the data. This takes our as-collected data in the Twitter API JSON format, and converts it into an intermediate SQLite database file. This process selects only the subset of fields that the toolkit needs to compute all of the networks, so is typically smaller than the original data. An intermediate database is used so that you only need to do input data processing once, no matter how many networks you want to compute.

	compute_networks crypto.db preprocess --format twitter_json crypto.json

Breaking this down from left to right:
	- every command in the toolkit starts with `compute_networks`
	- the second argument (`crypto.db`) is always a filename, describing a file on disk - this is the intermediate database, and you will need to make sure it is consistent from call to call.
	- `preprocess` is the action we want to apply to the database
	- `--format twitter_json` tells the toolkit that this data is Twitter JSON data
	- `crypto.json` is the input data we want to preprocess

After running this command you will have a file called `crypto.db` - it should be a lot smaller than `crypto.json`.

If you're stuck, try running:

	compute_networks --help

This will show you the order of options and what they're called if you've forgotten.

If you have many files to process, you can pass multiple input files:

	compute_networks example.db preprocess --format twitter_json example1.json example2.json example3.json

This will preprocess the three `example\*.json` files into `example.db`. The toolkit will ensure that duplicates are removed, so the tweets in each file can overlap.


### Computing Networks

Having preprocessed the data into `crypto.db`, we can now start to construct networks. We'll start with a "retweet" network. When platforms enable verbatim retweeting or reposting of another users message, this provides an affordance for coordinated inauthentic behaviour, to artificially boost the reach of particular messages. This can appear as multiple users retweeting a target users message in a very small window of time.

A command to construct a retweet network is:

	`compute_networks crypto.db compute co_retweet --time_window 60`

Unpacking this:

	- `compute_networks` is our usual entrypoint to the toolkit
	- `crypto.db` is our preprocessed database from earlier
	- `compute` is the action we want to apply to the database
	- `co_retweet` is the network we want to compute
	- `--time_window 60` is the time interval in seconds between retweets that we are using as the cutoff for coordination

Running this command will give status information about computing the network, and show you the options used for that computation. You will note that the output of the computation is stored in the database itself - it isn't directly usable in other programs yet.

The `time_window` parameter is a key variable of interest: at the extreme end multiple accounts retweeting the same message in the same second may be indicative of automation rather than organic behaviour. Longer time periods (3600 seconds/1 hour) may show coordination that indicates political engagement/activism.


### Network Outputs

The last step in using the toolkit is to create a network in a standard format for other tools like Gephi. An example command is:

	compute_networks crypto.db export_network crypto.graphml co_retweet --output_format graphml

This will create a rich graph in graphml format, saved in the `crypto.graphml` file. This file can be natively read by many graphing tools. It is also possible to output a plain CSV of the edges and their weights for tools that don't understand this format.