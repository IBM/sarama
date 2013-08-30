sarama
======

Sarama is an MIT-licensed Go client library for Apache Kafka 0.8 (and later).

Documentation is available via godoc at http://godoc.org/github.com/Shopify/sarama

It is compatible with Go 1.0 and 1.1 (which means `go vet` on 1.1 will return
some suggestions that we are ignoring for the sake of compatibility with 1.0).

A word of warning: the API is not 100% stable yet. It won't change much (in particular the low-level
Broker and Request/Response objects could *probably* be considered frozen) but there may be the occasional
parameter added or function renamed. As far as semantic versioning is concerned, we haven't quite hit 1.0.0 yet.
It is absolutely stable enough to use, just expect that you might have to tweak things when you update to a newer version.

Other related links:
* https://kafka.apache.org/
* https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
