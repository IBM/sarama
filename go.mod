module github.com/IBM/sarama

go 1.24.0

require (
	github.com/davecgh/go-spew v1.1.1
	github.com/eapache/go-resiliency v1.7.0
	github.com/eapache/queue v1.1.0
	github.com/fortytw2/leaktest v1.3.0
	github.com/jcmturner/gofork v1.7.6
	github.com/jcmturner/gokrb5/v8 v8.4.4
	github.com/klauspost/compress v1.18.3
	github.com/pierrec/lz4/v4 v4.1.25
	github.com/rcrowley/go-metrics v0.0.0-20250401214520-65e299d6c5c9
	github.com/stretchr/testify v1.11.1
	golang.org/x/net v0.50.0
	golang.org/x/sync v0.19.0
)

require (
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/crypto v0.48.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract (
	v1.32.0 // producer hangs on retry https://github.com/IBM/sarama/issues/2150
	[v1.31.0, v1.31.1] // producer deadlock https://github.com/IBM/sarama/issues/2129
	[v1.26.0, v1.26.1] // consumer fetch session allocation https://github.com/IBM/sarama/pull/1644
	[v1.24.1, v1.25.0] // consumer group metadata reqs https://github.com/IBM/sarama/issues/1544
)
