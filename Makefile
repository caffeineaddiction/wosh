# Builds the client and the per-platform binaries `wosh --push` streams to remotes.
PLATFORMS := linux-amd64 linux-arm64 linux-arm darwin-arm64 darwin-amd64 freebsd-amd64
LDFLAGS := -s -w

bin/wosh:
	CGO_ENABLED=0 go build -ldflags="$(LDFLAGS)" -o bin/wosh .

dist: bin/wosh $(addprefix bin/dist/wosh-,$(PLATFORMS))
	bin/wosh _warm bin/wosh $(addprefix bin/dist/wosh-,$(PLATFORMS))

bin/dist/wosh-%:
	CGO_ENABLED=0 GOOS=$(word 1,$(subst -, ,$*)) GOARCH=$(word 2,$(subst -, ,$*)) \
		go build -ldflags="$(LDFLAGS)" -o $@ .

test:
	go test ./...

.PHONY: bin/wosh dist test $(addprefix bin/dist/wosh-,$(PLATFORMS))
