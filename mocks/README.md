# sarama/mocks

The `mocks` subpackage includes mock implementations that implement the interfaces of the major sarama types.
You can use them to test your sarama applications using dependency injection.

The following mock objects are available:

- [Consumer](https://pkg.go.dev/github.com/IBM/sarama/mocks#Consumer), which will create [PartitionConsumer](https://pkg.go.dev/github.com/IBM/sarama/mocks#PartitionConsumer) mocks.
- [AsyncProducer](https://pkg.go.dev/github.com/IBM/sarama/mocks#AsyncProducer)
- [SyncProducer](https://pkg.go.dev/github.com/IBM/sarama/mocks#SyncProducer)

The mocks allow you to set expectations on them. When you close the mocks, the expectations will be verified,
and the results will be reported to the `*testing.T` object you provided when creating the mock.
