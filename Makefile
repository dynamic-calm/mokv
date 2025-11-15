.PHONY: compile
compile:
	protoc internal/api/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

.PHONY: test
test:
	go test -cover ./...

.PHONY: start
start:
	go run .

.PHONY: init
init:
	mkdir -p ${CONFIG_PATH}

.PHONY: cicd
cicd: compile test

.PHONY: build
build:
	go build -o bin/mokv ./cmd/mokv.go

.PHONY: perf-set
perf-set:
	ghz --proto ./internal/api/kv.proto \
	--insecure \
	--call api.KV.Set \
	-d '{"key":"test-{{.RequestNumber}}","value":"dGVzdC12YWx1ZQ=="}' \
	-n 10000 \
	-c 10 \
	localhost:8400

.PHONY: perf-get
perf-get:
	ghz --proto ./internal/api/kv.proto \
	--insecure \
	--call api.KV.Get \
	-d '{"key":"test-key"}' \
	-n 100000 \
	-c 10 \
	localhost:8400
