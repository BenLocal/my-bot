.PHONY: fmt vet test test-race build dev-up dev-down tidy clean

fmt:
	gofmt -w .

vet:
	go vet ./...

test:
	go test ./...

test-race:
	go test -race ./internal/...

build:
	go build ./cmd/...

dev-up:
	docker run -d --name mybot-redis -p 6379:6379 redis --appendonly yes

dev-down:
	docker stop mybot-redis && docker rm mybot-redis

tidy:
	go mod tidy

clean:
	go clean -cache -testcache ./...
