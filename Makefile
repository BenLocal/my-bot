.PHONY: fmt vet test test-race test-e2e build dev-up dev-down tidy clean

fmt:
	gofmt -w .

vet:
	go vet ./...

test:
	go test ./...

test-race:
	go test -race ./internal/...

# M0 端到端验收（需要 dapr CLI，redis-server 或 docker；约 10 分钟）
test-e2e:
	go test -tags e2e -count=1 -timeout 30m ./test/e2e/

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
