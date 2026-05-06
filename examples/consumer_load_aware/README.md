# Load-aware sticky consumer example

This example demonstrates the `sarama.SubscriptionUserDataBalanceStrategy` interface
(see issue [#3505](https://github.com/IBM/sarama/issues/3505)). A
`LoadAwareSticky` strategy wraps the built-in sticky assignor and injects a
fresh `LoadSample` (CPU percent, in-flight count, lag) into the member's
JoinGroup subscription metadata on every rebalance cycle.

`Plan` and `AssignmentData` are delegated unchanged to the sticky strategy;
this example deliberately keeps the assignor logic simple to focus on the
per-cycle metadata hook. A production load-aware assignor would also implement
its own `Plan` and decode each member's `UserData` on the leader to weight the
assignment.

## Run

```bash
$ go run . -brokers="127.0.0.1:9092" -topics="sarama" -group="example"
```

## Files

- `load_aware_sticky.go` — the wrapper strategy that implements
  `SubscriptionUserDataBalanceStrategy`.
- `main.go` — minimal consumer group runner that wires the strategy in via
  `Config.Consumer.Group.Rebalance.GroupStrategies`.
