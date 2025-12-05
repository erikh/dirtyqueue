Still need to write docs:

Short version:

- pretty fast (single threaded: 3sec for push, 2sec for shift, of 100k items for a small payload)
- uses one lock (flock). No mutexes.
- pretty much as durable as any other durable queue when it comes to crashes, etc.
- hashed directory structure, filled breadth first, in tiers (change static `DIRECTORY_HASH_LEVELS`, safely, to change)
    - the filesystem is much happier this way and performance on small files is good in this schema
- stores any serde payload as CBOR
- derive macro takes care of managing some traits
- designed to work with both async and threaded architectures
    - std i/o is used right now; async i/o needs a separate impl

To-do:

- async impl
- docs
    - readme
    - code
    - examples (use tests)
- publish crate

This code is not licensed yet; but it's expected to be an open source license. Until it is, however, you may not copy or re-distribute this code. Thanks!
