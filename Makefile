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
		-initca config/auth/ca-csr.json | cfssljson -bare ca

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=config/auth/ca-config.json \
		-profile=server \
		config/auth/server-csr.json | cfssljson -bare server

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=config/auth/ca-config.json \
		-profile=client \
		-cn="root" \
		config/auth/client-csr.json | cfssljson -bare root-client

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=config/auth/ca-config.json \
		-profile=client \
		-cn="nobody" \
		config/auth/client-csr.json | cfssljson -bare nobody-client

	mv *.pem *.csr ${CONFIG_PATH}

$(CONFIG_PATH)/model.conf:
		cp config/auth/model.conf $(CONFIG_PATH)/model.conf
$(CONFIG_PATH)/policy.csv:
		cp config/auth/policy.csv $(CONFIG_PATH)/policy.csv

cicd: compile gencert test
