ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
MAKEFLAGS += --always-make
GOARCH ?= $(shell go env GOARCH)

build:
	go build -o dodo main.go

build-darwin: gen-prompt
	GOOS=darwin CGO_ENABLED=0 GOEXPERIMENT=newinliner,greenteagc go build -o dodo-darwin-$(GOARCH) -a -trimpath -ldflags "-w -s"
	tar czf dodo-darwin-$(GOARCH).tar.gz dodo-darwin-$(GOARCH)

build-linux:
	GOOS=linux CGO_ENABLED=0 GOEXPERIMENT=newinliner,greenteagc go build -o dodo-linux-$(GOARCH) -a -trimpath -ldflags "-w -s"
	tar czf dodo-linux-$(GOARCH).tar.gz dodo-linux-$(GOARCH)

run:
	@go run main.go $(ARGS)

test:
	@go test -v ./...

install: build
	cp dodo /usr/local/bin

gen: gen-parser gen-prompt

gen-parser:
	@go generate ./src/parser

gen-prompt:
	@go generate ./src/prompt

fmt:
	@go fmt .
	@goimports -l -w -local "github.com/Thearas/dodo" .

lint:
	@golangci-lint run

addcmd:
	cobra-cli --license apache --author "Thearas thearas850@gmail.com" add $(ARGS)

%:
	@:
