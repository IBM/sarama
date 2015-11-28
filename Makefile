PACKAGE := github.com/Shopify/sarama

# http://stackoverflow.com/questions/322936/common-gnu-makefile-directory-path#comment11704496_324782
TOP := $(dir $(CURDIR)/$(word $(words $(MAKEFILE_LIST)),$(MAKEFILE_LIST)))

GOOP=goop
GO=$(GOOP) exec go
GOFMT=gofmt -w -s

GOFILES=$(shell git ls-files | grep '\.go$$')

.PHONY: build run watch clean test fmt dep

all: build

build: fmt
	$(GO) build

install: build
	$(GO) install $(PACKAGE)

clean:
	$(GO) clean

lint: install
	go get github.com/alecthomas/gometalinter
	gometalinter --install
	$(GOOP) exec gometalinter -D golint -D gocyclo -D dupl

test: build lint
	$(GO) test -v -timeout 60s -race $(PACKAGE)

bench:
	$(GO) test -bench=. -cpu 4

fmt:
	$(GOFMT) $(GOFILES)

dep:
	go get github.com/nitrous-io/goop
	goop install
	mkdir -p $(dir $(TOP)/.vendor/src/$(PACKAGE))
	ln -nfs $(TOP) $(TOP)/.vendor/src/$(PACKAGE)
