#!/usr/bin/env bash

set -e

VERSION="${VERSION:-latest}"
IMAGE_NAME="mokv"
IMAGE_TAG="${IMAGE_NAME}:${VERSION}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

error() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1" >&2
    exit 1
}

main() {
    log "Starting up mokv..."

    # Step 1: Check/create kind cluster
    log "Checking for kind cluster..."
    if ! kind get clusters 2>/dev/null | grep -q '^kind$'; then
        log "Kind cluster not found. Creating kind cluster..."
        kind create cluster || error "Failed to create kind cluster"
        log "Kind cluster created successfully"
    else
        log "Kind cluster already exists"
    fi

    # Step 2: Build Docker image
    log "Building Docker image: ${IMAGE_TAG}..."
    docker build -t "${IMAGE_TAG}" . || error "Failed to build Docker image"
    log "Docker image built successfully"

    # Step 3: Load image into kind
    log "Loading image into kind cluster..."
    kind load docker-image "${IMAGE_TAG}" || error "Failed to load image into kind"
    log "Image loaded into kind successfully"

    # Step 4: Deploy or upgrade with Helm
    log "Checking for existing mokv deployment..."
    if helm list 2>/dev/null | grep -q 'mokv'; then
        log "Existing deployment found. Upgrading mokv deployment..."
        helm upgrade mokv deploy/mokv || error "Failed to upgrade mokv deployment"
        log "Deployment upgraded successfully"
    else
        log "No existing deployment found. Deploying mokv..."
        helm install mokv deploy/mokv || error "Failed to deploy mokv"
        log "Deployment created successfully"
    fi

    # Step 5: Wait for pods to be ready
    log "Waiting for pods to be ready (timeout: 60s)..."
    if kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=mokv --timeout=60s 2>/dev/null; then
        log "All pods are ready"
    else
        error "Pods failed to become ready within timeout"
    fi

    # Step 6: Show status
    log "mokv is running successfully!"
    echo ""
    kubectl get pods -l app.kubernetes.io/name=mokv

    # Usage instructions
    echo ""
    log "Next steps:"
    echo "  1. Run 'kubectl port-forward pod/mokv-0 8400:8400' to access the cluster"
    echo "  2. Test with: go run cmd/test_kv.go -addr localhost:8400"
    echo "  3. Run 'make clean' to stop."
}

main