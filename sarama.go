/*
Package sarama provides client libraries for the Kafka 0.8 protocol. The Client, Producer and Consumer objects are the core of the high-level API. The Broker and Request/Response objects permit more precise control.

The Request/Response objects and properties are mostly undocumented, as they line up exactly with the
protocol fields documented by Kafka at https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
*/
package sarama

import (
  "log"
  "io/ioutil"
)

var (
  Logger = log.New(ioutil.Discard, "[Sarama] ", log.LstdFlags)
)
