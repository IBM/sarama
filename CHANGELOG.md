# Changelog

#### Version 1.3.0 (2015-04-16)

New Features:
 - The client now tracks consumer group coordinators using
   ConsumerMetadataRequests similar to how it tracks partition leadership using
   regular MetadataRequests ([#411](https://github.com/Shopify/sarama/pull/411)).
   This adds two methods to the client API:
   - `Coordinator(consumerGroup string) (*Broker, error)`
   - `RefreshCoordinator(consumerGroup string) error`

Improvements:
 - ConsumerMetadataResponses now automatically create a Broker object out of the
   ID/address/port combination for the Coordinator; accessing the fields
   individually has been deprecated
   ([#413](https://github.com/Shopify/sarama/pull/413)).
 - Much improved handling of `OffsetOutOfRange` errors in the consumer.
   Consumers will fail to start if the provided offset is out of range
   ([#418](https://github.com/Shopify/sarama/pull/418))
   and they will automatically shut down if the offset falls out of range
   ([#424](https://github.com/Shopify/sarama/pull/424)).
 - Small performance improvement in encoding and decoding protocol messages
   ([#427](https://github.com/Shopify/sarama/pull/427)).

Bug Fixes:
 - Fix a rare race condition in the client's background metadata refresher if
   it happens to be activated while the client is being closed
   ([#422](https://github.com/Shopify/sarama/pull/422)).

#### Version 1.2.0 (2015-04-07)

Improvements:
 - The producer's behaviour when `Flush.Frequency` is set is now more intuitive
   ([#389](https://github.com/Shopify/sarama/pull/389)).
 - The producer is now somewhat more memory-efficient during and after retrying
   messages due to an improved queue implementation
   ([#396](https://github.com/Shopify/sarama/pull/396)).
 - The consumer produces much more useful logging output when leadership
   changes ([#385](https://github.com/Shopify/sarama/pull/385)).
 - The client's `GetOffset` method will now automatically refresh metadata and
   retry once in the event of stale information or similar
   ([#394](https://github.com/Shopify/sarama/pull/394)).
 - Broker connections now have support for using TCP keepalives
   ([#407](https://github.com/Shopify/sarama/issues/407)).

Bug Fixes:
 - The OffsetCommitRequest message now correctly implements all three possible
   API versions ([#390](https://github.com/Shopify/sarama/pull/390),
   [#400](https://github.com/Shopify/sarama/pull/400)).

#### Version 1.1.0 (2015-03-20)

Improvements:
 - Wrap the producer's partitioner call in a circuit-breaker so that repeatedly
   broken topics don't choke throughput
   ([#373](https://github.com/Shopify/sarama/pull/373)).

Bug Fixes:
 - Fix the producer's internal reference counting in certain unusual scenarios
   ([#367](https://github.com/Shopify/sarama/pull/367)).
 - Fix the consumer's internal reference counting in certain unusual scenarios
   ([#369](https://github.com/Shopify/sarama/pull/369)).
 - Fix a condition where the producer's internal control messages could have
   gotten stuck ([#368](https://github.com/Shopify/sarama/pull/368)).
 - Fix an issue where invalid partition lists would be cached when asking for
   metadata for a non-existant topic ([#372](https://github.com/Shopify/sarama/pull/372)).


#### Version 1.0.0 (2015-03-17)

Version 1.0.0 is the first tagged version, and is almost a complete rewrite. The primary differences with previous untagged versions are:

- The producer has been rewritten; there is now a `SyncProducer` with a blocking API, and an `AsyncProducer` that is non-blocking.
- The consumer has been rewritten to only open one connection per broker instead of one connection per partition.
- The main types of Sarama are now interfaces to make depedency injection easy; mock implementations for `Consumer`, `SyncProducer` and `AsyncProducer` are provided in the `github.com/Shopify/sarama/mocks` package.
- For most uses cases, it is no longer necessary to open a `Client`; this will be done for you.
- All the configuration values have been unified in the `Config` struct.
- Much improved test suite.
