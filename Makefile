VERSION ?= latest
IMAGE_NAME = mokv
IMAGE_TAG = $(IMAGE_NAME):$(VERSION)

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
	go test -cover -race -v ./...

.PHONY: build
build:
	go build -o bin/mokv .

.PHONY: perf
perf:
	-go test -bench=. -benchtime=5s ./mokv -benchmem -run=^#

.PHONY: perf-long
perf-long:
	-go test -bench=. -benchtime=60s ./mokv -benchmem -run=^#

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
	@echo "Starting up mokv..."
	@kind get clusters | grep -q kind || (echo " Creating kind cluster..." && kind create cluster)
	@echo "Building Docker image..."
	@$(MAKE) docker-build
	@echo "Loading image into kind..."
	@$(MAKE) kind-load
	@helm list | grep -q mokv && (echo "♻ Upgrading existing deployment..." && $(MAKE) upgrade) || (echo "Deploying mokv..." && $(MAKE) deploy)
	@echo "Waiting for pods to be ready..."
	@kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=mokv --timeout=120s
	@echo "mokv is running!"
	@kubectl get pods -l app.kubernetes.io/name=mokv
	@echo ""
	@echo "Run 'kubectl port-forward pod/mokv-0 9400:8400' to access the cluster"
	@echo "Then test with: go run cmd/test_kv.go -addr localhost:9400"