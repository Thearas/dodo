ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
MAKEFLAGS += --always-make
GOARCH ?= $(shell go env GOARCH)
CXX := g++

build:
	go build -o dodo main.go

build-hyper:
	CXX=$(CXX) go build -tags chimera -o dodo main.go

build-darwin:
	CXX=$(CXX) GOOS=darwin CGO_ENABLED=1 GOEXPERIMENT=newinliner CGO_LDFLAGS='-static-libstdc++' go build -tags chimera -o dodo-darwin-$(GOARCH) -a -trimpath -ldflags "-w -s"
	tar czf dodo-darwin-$(GOARCH).tar.gz dodo-darwin-$(GOARCH)

build-linux:
	GOOS=linux CGO_ENABLED=1 GOEXPERIMENT=newinliner go build -tags chimera -o dodo-linux-$(GOARCH) -a -trimpath -installsuffix cgo -ldflags "-w -s -linkmode external -extldflags -static"
	tar czf dodo-linux-$(GOARCH).tar.gz dodo-linux-$(GOARCH)

run:
	@go run main.go $(ARGS)

test:
	@go test -v ./...

install: build
	cp dodo /usr/local/bin

gen:
	@go generate ./...

fmt:
	@go fmt .
	@goimports -l -w -local "github.com/Thearas/dodo" .

addcmd:
	cobra-cli --license apache --author "Thearas thearas850@gmail.com" add $(ARGS)

%:
	@:
