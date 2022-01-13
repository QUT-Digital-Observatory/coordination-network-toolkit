# About the Networks

The coordination network toolkit builds networks indicating (potentially) coordinated communication by actors. While the focus is on social media, other mediums can be considered too, as long as their is a core affordance of a discrete message posted by identifiable accounts or users.


## What is a Coordination Network?

In this toolkit, a coordination network is a network where:

- users or accounts are nodes in that network
- an edge between user A and user B with weight N in that network indicates that user A has performed a specific action within a specific time of user B performing the same action.

## Choice of Time Period

The choice of time period depends on the questions under investigation and the nature of the data being studied - it is usually appropriate to consider multiple time windows for each type of network as part of the initial analysis. It is also necessary to keep in mind that coordination is a fundamental human activity, and that apparent coordination between accounts may be purely coincidence, or an indicator of nothing more than both accounts being actively engaged with the same issues and/or actors.

As a general rule, events occuring within shorter time windows are likely to provide stronger evidence for coordination. In extreme cases very short windows (< 5 seconds) might even suggest the involvement of automated systems as humans are unlikely to be able to consistently respond or act within that amount of time without prior warning and preparation.

The context of the collected data is also critical: coordination in the context of a 60 minute presidential debate is likely to have different time scales than coordination in the context of a month long election campaign.


## Types of Networks

### Co-retweet

A co-retweet occurs when user A retweets the same tweet as user B within a certain time window. If users A and B repeatedly retweet the same tweets within a small time period, this may indicate coordination between in order to boost specific messages. Clusters of accounts exhibiting this behaviour can indicate a campaign to propagate particular information.

Note that as retweets (and reposts on other platforms that support this) are more references to another post rather than original creations of text, retweets/reposts are excluded from all other network types. This avoids double counting the links, replies and text of retweets in other metrics.

### Co-tweet

A co-tweet occurs when user A creates a tweet with the same text as a tweet by user B (ignoring retweets/reposts) within a certain time window. Often the time window for co-tweeting behaviour can be significantly longer, as posting the the same text is always illustrative.

Note that false positives can occur for co-tweeting and additional analysis of the constructed network may be necessary. Examples of where this can occur are:

- Very short tweets, such as single hashtags or short phrases like "take that!" or "thank god!".
- Sharing links to articles or other web resources - websites embedding share to Twitter buttons often prepopulate the tweet field with a headline or synopsis of the link to be shared.

### Co-similarity

Co-similarity is similar to co-tweeting and shares many of the same considerations. The major difference is that rather than using exact text matches, a tokenised set of words is considered. This means that messages that are different according to exact textual matches can be matched using similarity matching. For example the three messages with text "the cat sat on the mat", "the cat sat on the mat slowly" and "the bat sat on the mat" would all be considered distinct according to co-tweet analysis, but similar under co-similarity.

The default similarity measure and tweet representation for similarity is to represent tweets as sets of words, and the Jaccard similarity between each set of words is taken to be the similarity between those tweets. This is effectively a boolean bag of words approach, and is suitable for short text like most social media platforms. It is possible to customise this using the toolkit as a Python library.

### Co-link

Co-link networks consider the contemporaneous posting of identical links to resources outside of the platform of interest. Note that different URLs may point to the same fundamental resource and may be missed by this analysis - for example the inclusion of a tracking parameter in a URL like www.example.com/?utm=banana means that is considered distinct from www.example.com/.

Note that the toolkit supports the resolving of URLs to final URL paths, to handle link shortening services like bit.ly.

### Co-reply

Co-reply networks are very similar to co-retweet networks, except instead of looking at retweets of a specific tweet, the replies to specific tweets are considered instead.

### Co-post

User A has co-posted with user B if they have created any message with any type of content within the time window of each other. This is not expected to be directly useful, but more as an adjunct to the other more specific network types. In particular, this describes the background levels of total co-temporal behaviour, and in particular the maximum possible weight that any edge between users can take of any other network type.


## Performance Notes

Building of all networks types is currently parallelised, and will by default make use of all available cores on your machine (this can be adjusted if needed with `--n_cpu` option on the command line).

In general, co-retweet, co-tweet, co-link and co-reply are the fastest network types to compute (under 1 minute for 10 million tweets with a time window of 60 seconds) as they exploit the specificity of the types of message, while co-post and co-similarity are the most intensive as they require either counting many more messages due to the lack of specificity, or require cpu intensive text comparisons respectively.


## What's in the GraphML file?

The highest fidelity data type output by the toolkit is in [GraphML](http://graphml.graphdrawing.org/) format. The aim for this format is to provide a complete picture of the edges in the network, and a sketch of the information about the nodes for convenient analysis within graph toolkits. In order to scale to datasets of 100 million messages, the network output is not intended to replace or provide the complete picture of the underlying data.

Nodes in this network contain:

- "user_id": the user ID or unique identifier of that user
- "username": an arbitrarily selected username from all of the usernames that user ID had (to account for possible changes in the username over time)
- "message_N": a snapshot of the Nth most recent message posted by that user in the dataset. By default there will be up to ten such attributes for the ten most recent messages - this can be adjusted on the command line with the --n_messages option.

Edges in this network are directed from user A to user B and contain:

- "edge_type" as the type of coordinated event
- "weight" as the integer number of times user A's messages met the coordination criteria with user B's