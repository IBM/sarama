default: fmt vet errcheck test

test:
	go test -v -timeout 90s -race

vet:
	go vet ./...

errcheck:
	errcheck github.com/Shopify/sarama/...

fmt:
	@if [ -n "$$(go fmt ./...)" ]; then echo 'Please run go fmt on your code.' && exit 1; fi

install_dependencies: install_errcheck glide_install

glide_install:
	go get -u github.com/Masterminds/glide
	cd $(GOPATH)/src/github.com/remerge/sarama && glide install --strip-vendor

install_errcheck:
	go get github.com/kisielk/errcheck

get:
	go get -t

watch:
	go get github.com/cespare/reflex
	reflex -r '\.go$$' -s -- sh -c 'clear && go test -v -run=Test$(T)'
