# uncommitted-messages

A setup using Java clients to test how to consomme uncommitted messages

## Dependencies

- docker
- golang 

## How to use it

You need to add this line in your host file:
`127.0.0.1	kafka`

Run `docker-compose up` to load a kafka broker.
Wait for "ready to run" message. There the brokers and messages are setup.

Now you can tweak the lib and run this command as much as you want to see the output:
`go run kafka-console-consumer.go --brokers kafka:9092 --topic topic-test`

You should see something like
```
uncommitted
uncommitted
Committed 1
Committed 2
uncommitted
uncommitted
Committed 3
Committed 4
```

or if you are consuming committed only:
```
Committed 1
Committed 2
Committed 3
Committed 4
```

There is 5 topic `topic-test`,`topic-test-2`,`topic-test-5`,`topic-test-3`,`topic-test-4`.
Each one, in their own way, are trying to mess with you !

## Why add a line in host file?

Because kafka has a `ADV_HOST` variable and he care a lot for that.
Meaning you have to address him using the same host either the query come from your local computer or
the inside of the docker-compose.
