CONFIG_PATH=${HOME}/.mokv

.PHONY: compile
compile:
	protoc internal/api/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

.PHONY: test
test: $(CONFIG_PATH)/model.conf $(CONFIG_PATH)/policy.csv
	go test -cover ./...

.PHONY: start
start:
	go run .

.PHONY: init
init:
	mkdir -p ${CONFIG_PATH}

.PHONY: gencert
gencert:
	cfssl gencert \
		-initca certs/ca-csr.json | cfssljson -bare ca

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=certs/ca-config.json \
		-profile=server \
		certs/server-csr.json | cfssljson -bare server

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=certs/ca-config.json \
		-profile=client \
		-cn="root" \
		certs/client-csr.json | cfssljson -bare root-client

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=certs/ca-config.json \
		-profile=client \
		-cn="nobody" \
		certs/client-csr.json | cfssljson -bare nobody-client

	mv *.pem *.csr ${CONFIG_PATH}

$(CONFIG_PATH)/model.conf:
		cp certs/model.conf $(CONFIG_PATH)/model.conf
$(CONFIG_PATH)/policy.csv:
		cp certs/policy.csv $(CONFIG_PATH)/policy.csv

.PHONY: cicd
cicd: compile gencert test

.PHONY: build
build:
	go build -o bin/mokv ./cmd/mokv.go

.PHONY: perf-set
perf-set:
	ghz --proto ./internal/api/kv.proto \
	--call api.KV.Set \
	--cert ~/.mokv/root-client.pem \
	--key ~/.mokv/root-client-key.pem \
	--cacert ~/.mokv/ca.pem \
	-d '{"key":"test-key","value":"dGVzdC12YWx1ZQ=="}' \
	-n 100000 \
	-c 10 \
	localhost:8400

.PHONY: perf-get
perf-get:
	ghz --proto ./internal/api/kv.proto \
	--call api.KV.Get \
	--cert ~/.mokv/root-client.pem \
	--key ~/.mokv/root-client-key.pem \
	--cacert ~/.mokv/ca.pem \
	--async \
	-d '{"key":"test-key"}' \
	-n 100000 \
	-c 10 \
	localhost:8400
