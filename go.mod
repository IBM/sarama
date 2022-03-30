module github.com/Shopify/sarama

go 1.16

require (
	github.com/Shopify/toxiproxy/v2 v2.3.0
	github.com/davecgh/go-spew v1.1.1
	github.com/eapache/go-resiliency v1.2.0
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21
	github.com/eapache/queue v1.1.0
	github.com/fortytw2/leaktest v1.3.0
	github.com/frankban/quicktest v1.14.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/hashicorp/go-multierror v1.1.1
	github.com/jcmturner/gofork v1.0.0
	github.com/jcmturner/gokrb5/v8 v8.4.2
	github.com/klauspost/compress v1.15.0
	github.com/pierrec/lz4 v2.6.1+incompatible
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/stretchr/testify v1.7.0
	github.com/xdg-go/scram v1.1.1
	golang.org/x/crypto v0.0.0-20220214200702-86341886e292 // indirect
	golang.org/x/net v0.0.0-20220225172249-27dd8689420f
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

retract (
	v1.32.0 // producer hangs on retry https://github.com/Shopify/sarama/issues/2150
	[v1.31.0, v1.31.1] // producer deadlock https://github.com/Shopify/sarama/issues/2129
	[v1.26.0, v1.26.1] // consumer fetch session allocation https://github.com/Shopify/sarama/pull/1644
	[v1.24.1, v1.25.0] // consumer group metadata reqs https://github.com/Shopify/sarama/issues/1544
)
