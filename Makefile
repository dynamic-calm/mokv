CONFIG_PATH=${HOME}/.mokv

.PHONY: compile
compile:
	protoc api/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

.PHONY: test
test:
	go test ./...

.PHONY: start
start:
	go run .

.PHONY: init
init:
	mkdir -p ${CONFIG_PATH}

.PHONY: gencert
gencert:
	cfssl gencert \
		-initca cfssl/ca-csr.json | cfssljson -bare ca

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=cfssl/ca-config.json \
		-profile=server \
		cfssl/server-csr.json | cfssljson -bare server

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=cfssl/ca-config.json \
		-profile=client \
		cfssl/client-csr.json | cfssljson -bare client

	mv *.pem *.csr ${CONFIG_PATH}