# About the Networks

The coordination network toolkit builds networks indicating (potentially) coordinated communication by actors. While the focus is on social media, other mediums can be considered too, as long as their is a core affordance of a discrete message posted by identifiable accounts or users.


## What is a Coordination Network?

In this toolkit, a coordination network is a network where:

- users or accounts are nodes in that network
- an edge between user A and user B with weight N in that network indicates that user A has performed a specific action within a specific time of user B performing the same action.



## What's in the GraphML file?

The highest fidelity data type output by the toolkit is in [GraphML](http://graphml.graphdrawing.org/) format. The aim for this format is to provide a complete picture of the edges in the network, and a sketch of the information about the nodes for convenient analysis within graph toolkits. In order to scale to datasets of 100 million messages, the network output is not intended to replace or provide the complete picture of the underlying data.

Nodes in this network contain:

- "user_id": the user ID or unique identifier of that user
- "username": an arbitrarily selected username from all of the usernames that user ID had (to account for possible changes in the username over time)
- "message_N": a snapshot of the Nth most recent message posted by that user in the dataset. By default there will be up to ten such attributes for the ten most recent messages - this can be adjusted on the command line with the --n_messages option.

Edges in this network are directed from user A to user B and contain:

- "edge_type" as the type of coordinated event
- "weight" as the integer number of times user A's messages met the coordination criteria with user B's