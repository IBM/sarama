module github.com/Shopify/sarama

go 1.17

require (
	github.com/Shopify/toxiproxy/v2 v2.5.0
	github.com/davecgh/go-spew v1.1.1
	github.com/eapache/go-resiliency v1.3.0
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21
	github.com/eapache/queue v1.1.0
	github.com/fortytw2/leaktest v1.3.0
	github.com/hashicorp/go-multierror v1.1.1
	github.com/jcmturner/gofork v1.7.6
	github.com/jcmturner/gokrb5/v8 v8.4.3
	github.com/klauspost/compress v1.15.11
	github.com/pierrec/lz4/v4 v4.1.17
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/stretchr/testify v1.8.0
	github.com/xdg-go/scram v1.1.1
	golang.org/x/net v0.0.0-20220927171203-f486391704dc
	golang.org/x/sync v0.0.0-20220923202941-7f9b1623fab7
)

require (
	github.com/golang/snappy v0.0.4 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/stringprep v1.0.3 // indirect
	golang.org/x/crypto v0.0.0-20220722155217-630584e8d5aa // indirect
	golang.org/x/text v0.3.7 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract (
	v1.32.0 // producer hangs on retry https://github.com/Shopify/sarama/issues/2150
	[v1.31.0, v1.31.1] // producer deadlock https://github.com/Shopify/sarama/issues/2129
	[v1.26.0, v1.26.1] // consumer fetch session allocation https://github.com/Shopify/sarama/pull/1644
	[v1.24.1, v1.25.0] // consumer group metadata reqs https://github.com/Shopify/sarama/issues/1544
)
