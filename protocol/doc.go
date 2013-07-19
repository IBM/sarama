/*
Package protocol provides the low-level primitives necessary for communicating with a Kafka 0.8 cluster.

The core of the package is the Broker. It represents a connection to a single Kafka broker service, and
has methods for querying the broker.

The other types are mostly Request types or Response types. Most of the Broker methods take a Request of a
specific type and return a Response of the appropriate type, for example:

	broker := NewBroker("localhost", 9092)
	err := broker.Connect()
	if err != nil {
		return err
	}

	request := MetadataRequest{Topics:[]string{"myTopic"}}
	response, err := broker.GetMetadata("myClient", request)

	// do things with response

	broker.Close()

The objects and properties in this package are mostly undocumented, as they line up exactly with the
protocol fields documented by Kafka at https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
*/
package protocol
