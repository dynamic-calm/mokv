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
	go test -cover -race ./...

.PHONY: start
start:
	go run .

.PHONY: build
build:
	go build -o bin/mokv ./cmd/mokv.go

.PHONY: perf
perf:
	-go test -bench=. -benchtime=5s ./internal/ -run=^# -benchmem