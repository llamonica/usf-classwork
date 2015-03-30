Mini-Twitter is a project from a distributed software development course I 
took at Univesity of San Francisco. The project goal was to build a two-tier 
server system that provided some basic Twitter functionality.

This iteration of the project achieves monotonic consistency, full replication, 
fault tolerance managed by a central server, and lockless consistent global 
state snapshots. The operations supported via the front-end API include posting 
messages, searching for messages via keyword, and requesting snapshots of the 
system's global state.

Consistency: The system guarantees that any search will return results that are 
at least as recent as those of any previous search, regardless of the front-end 
server to which the request is directed. This level of consistency is enforced 
using vector timestamps, as is the consistency guarantee for the global 
snapshots.

Replication: The system regularly replicates data between datastore servers. If 
a server fails, system data will be replicated to it upon its restoration.

Fault tolerance: A central server monitors the liveness of every server with a 
periodic heartbeat request. When a datastore server is suspected of failure, 
any traffic originally headed to that datastore is routed to a server that is 
known to be live.

Snapshots: The snapshot is guaranteed to return a consistent view of the 
system, meaning that the view of each server's state will be consistent with 
the view of every other server. The snapshot is a view of all posted messages at 
a given time (consistent with the vector timestamp defined at the time of the 
snapshot request). This sort of functionality can be extended to monitoring 
much more than just the posted messages.