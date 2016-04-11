# Consumer Producer Demo

This is a simple example showing the usage of consumer and producer API. In the main function, we spawn a producer that produces 10 messages asynchronously using a go routine.

Within the same function, we create a consumer and select the first partition for a topic to consume messages from. For now, Sarama doesn't support coordinated consumption so the client has to manage partition consumption on its own. The consumer is blocking and would wait for ever for new messages. It would gracefully close itself on an interrupt signal.