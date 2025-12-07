<!-- markdownlint-disable MD033 MD041 -->
<div align='center'>
<img src="mokv.png" alt="mokv logo"/>
<br />
<br />

[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Test](https://github.com/sinclairzx81/typedriver/actions/workflows/build.yml/badge.svg)](https://github.com/dynamic-calm/mokv/actions/workflows/test.yml)

</div>

<!-- markdownlint-enable MD033 MD041 -->

## Example

<!-- markdownlint-disable MD010 -->

```go
package main

import (
	"context"

	"github.com/dynamic-calm/mokv/mokv"
)

func main() {
	client, _ := mokv.NewClient("localhost:8400")
	defer client.Close()

	ctx := context.Background()

	key := `59°19'14.7"N`
	val := []byte(`18°03'39.0"E`)

	client.Set(ctx, key, val)
	client.Get(ctx, key)
	client.Delete(ctx, key)
	client.GetServers(ctx)
}
```

<!-- markdownlint-enable MD010 -->

## Overview

mökv is a distributed key-value store built with [Raft](https://github.com/hashicorp/raft) for consensus, [Serf](https://github.com/hashicorp/serf) for discovery, and [gRPC](https://github.com/grpc/grpc-go) for communication.

Built following the book [Distributed Services with Go](https://pragprog.com/titles/tjgo/distributed-services-with-go/) by Travis Jeffery.

## Features

- **Distributed & Fault-Tolerant**: Data replicated across multiple nodes using Raft consensus
- **In-Memory Storage**: Fast read/write operations with persistent snapshots
- **Service Discovery**: Automatic node discovery and membership via Serf
- **Smart Load Balancing**: Client-side routing directs writes to leader, reads to followers

## Quick Start

### Prerequisites

- [`Docker`](https://www.docker.com/)
- [`kind`](https://kind.sigs.k8s.io/) (for local Kubernetes)
- [`kubectl`](https://kubernetes.io/docs/tasks/tools/)
- [`Helm`](https://helm.sh/)

### Run it locally

```bash
make
```

This will:

1. Create a kind cluster
2. Build the Docker image
3. Load it into kind
4. Deploy with Helm
5. Wait for all pods to be ready

Then test it:

```bash
➜ kubectl port-forward pod/mokv-0 9800:8400
# In another terminal:
➜ go run cmd/test_kv.go -addr localhost:8400
Getting servers:
        - mokv-0.mokv.default.svc.cluster.local:8400 -> is leader: true
        - mokv-1.mokv.default.svc.cluster.local:8400 -> is leader: false
        - mokv-2.mokv.default.svc.cluster.local:8400 -> is leader: false

Setting key 'hello' = 'world'
Set OK: true

Getting key 'hello'
Got: hello = world
```

```bash
kubectl scale statefulset mokv --replicas=5
```

### Configuration

Customize via `deploy/mokv/values.yaml` or override during installation:

```bash
helm install mokv deploy/mokv --set replicas=5 --set storage=2Gi
```

Default values:

```yaml
replicas: 3 # Number of nodes
storage: 1Gi # Persistent volume size per node
rpcPort: 8400 # gRPC port
serfPort: 8401 # Serf discovery port
```

## API

`mökv` exposes a gRPC API defined in `api/kv.proto`:

```proto
service KV {
    rpc Get(GetRequest) returns (GetResponse);
    rpc Set(SetRequest) returns (SetResponse);
    rpc Delete(DeleteRequest) returns (DeleteResponse);
    rpc List(google.protobuf.Empty) returns (stream GetResponse);
    rpc GetServers(google.protobuf.Empty) returns (GetServersResponse);
}
```

## Architecture

**Raft Consensus**: Ensures strong consistency with leader-based replication. Writes go through the leader and are replicated to followers.

**Serf Discovery**: Nodes automatically discover each other via gossip protocol. When a node joins via Serf, it's added as a Raft voter.

**Client-Side Load Balancing**: Custom gRPC resolver and picker route:

- **Writes** (`Set`, `Delete`) → Leader
- **Reads** (`Get`, `List`) → Followers (load balanced)

**Kubernetes Components**:

- **StatefulSet**: Stable network identities (mokv-0, mokv-1, mokv-2)
- **Headless Service**: Direct pod-to-pod communication via FQDNs
- **PersistentVolumeClaims**: Durable storage for Raft logs and snapshots
- **Init Container**: Auto-configures each pod (bootstrap vs join)

## Management

**Update deployment**:

```bash
make docker-build
make kind-load
kubectl rollout restart statefulset mokv
```

**Scale the cluster**:

```bash
helm upgrade mokv deploy/mokv --set replicas=5
```

**Uninstall**:

```bash
make clean
```

## Local Development

For local testing without Kubernetes, see [`example/start_nodes.sh`](example/start_nodes.sh).
