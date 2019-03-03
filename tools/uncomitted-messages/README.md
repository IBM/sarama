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
2019/03/03 18:52:16 test msg: 0
2019/03/03 18:52:16 test msg: 1
2019/03/03 18:52:16 test msg: 2
2019/03/03 18:52:16 test msg: 3
2019/03/03 18:52:16 test msg: 4
2019/03/03 18:52:16 test msg: 5
2019/03/03 18:52:16 test msg: 6
2019/03/03 18:52:16 test msg: 7
2019/03/03 18:52:16 test msg: 8
2019/03/03 18:52:16 test msg: 9
```

if you see all these messages: you are consuming some uncommitted ones.

## Why add a line in host file?

Because kafka has a `ADV_HOST` variable and he care a lot for that.
Meaning you have to address him using the same host either the query come from your local computer or
the inside of the docker-compose.
