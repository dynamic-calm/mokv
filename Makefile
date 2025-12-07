VERSION ?= latest
IMAGE_NAME = mokv
IMAGE_TAG = $(IMAGE_NAME):$(VERSION)

.DEFAULT_GOAL := start

.PHONY: compile
compile:
	protoc ./api/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

.PHONY: test
test:
	go test -cover -race ./...

.PHONY: build
build:
	go build -o bin/mokv .

.PHONY: docker-build
docker-build:
	docker build -t $(IMAGE_TAG) .

.PHONY: kind-load
kind-load:
	kind load docker-image $(IMAGE_TAG)

.PHONY: deploy
deploy:
	helm install mokv deploy/mokv

.PHONY: upgrade
upgrade:
	helm upgrade mokv deploy/mokv

.PHONY: redeploy
redeploy: docker-build kind-load
	kubectl rollout restart statefulset mokv

.PHONY: clean
clean:
	helm uninstall mokv && \
	kubectl delete pvc datadir-mokv-0 datadir-mokv-1 datadir-mokv-2

.PHONY: logs
logs:
	kubectl logs -f mokv-0

.PHONY: status
status:
	kubectl get pods -l app.kubernetes.io/name=mokv

.PHONY: start
start:
	./scripts/start.sh