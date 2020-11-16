[![Build and Test Gafka](https://github.com/zikwall/gafka/workflows/Build%20and%20test%20Gafka/badge.svg)](https://github.com/zikwall/gafka/actions)

<div align="center">
  <h1>Gafka</h1>
  <h5>The project is similar to Kafka, but it's Gafka!</h5>
</div>

### What and why

This is an experimental project that creates a similar example (or almost) to the Apache Kafka project. Simply. Interesting.

### todo

- [ ] Public API
- [ ] Gafka
    - [ ] Package
    - [ ] Server (separated from this project and using the package)
    - [ ] Client
- [ ] Will review and remove all methods marked as `UNSAFE_`
- [ ] Packet write in storage interface
- [ ] Storages
    - [x] Simple in-memory storage
    - [x] Sharded in-memory storage, segmenting the map with minimal waiting time for locks
        - [ ] Sharded partitions like topics
    - [ ] File storage on disk
        - [ ] Use hybrid asynchronous disk write scheme
    - [ ] Another storage...
- [ ] Metadata and ConsumerMetadata

### Tests

:exclamation: Be careful, there are very long testing examples in the tests at the moment :fire:

- `$ make tests`
