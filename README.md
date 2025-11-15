# mökv

`mökv` is a distributed, in-memory key-value store. It utilizes [`Raft`](https://github.com/hashicorp/raft) for consensus, [`serf`](https://github.com/hashicorp/serf) for discvoery, and [`gRPC`](https://github.com/grpc/grpc-go) for communication.

## Features

- Distributed Architecture: Data is replicated across multiple nodes for fault tolerance.
- In-Memory Storage: Provides fast read and write operations.
- `Raft` Consensus: Ensures data consistency across the cluster.
- `gRPC` Interface: Offers a well-defined `API` for interacting with the store.
- Metrics: Exposes `Prometheus` metrics for monitoring cluster health and performance.
- Service Discovery: Uses `serf` for automatic node discovery and membership management.
- Load Balancing: Implements `gRPC` client-side load balancing, directing write operations to the leader and read operations to followers.

## Getting Started

To run `mökv`:

### Prerequisites

- [`Go`](https://go.dev/dl/)
- [`ghz`](https://ghz.sh/) (for performance testing. Optional)

### Installation

1. Clone the repository:

   ```bash
   git clone git@github.com:dynamic-calm/mokv.git
   cd mokv
   ```

2. Compile the code:

   ```bash
   make build
   ```

   This will create an executable binary `mokv` in the `bin/` directory.

### Configuration

Configuration is done through command-line flags or a configuration file. A sample configuration file (`example/config.yaml`) is provided.

Here's an example `config.yaml`:

```yaml
data-dir: /tmp/mokv-data
node-name: node1
bind-addr: 127.0.0.1:8401
rpc-port: 8400
start-join-addrs: []
bootstrap: true
metrics-port: 4000
```

### Running mökv

1. Start the first node:

   ```bash
   bin/mokv --config-file example/config.yaml
   ```

2. Start additional nodes:

   Modify the `example/config.yaml` file with the appropriate `node-name`, `bind-addr`, and `rpc-port`. Crucially, set `start-join-addrs` to the address of the first node (e.g., `127.0.0.1:8401`). Also set `bootstrap: false` for the additional nodes. Then, run the command again:

   ```bash
   bin/mokv --config-file example/config2.yaml  # Example config for the second node
   ```

   Refer to [`example/start_nodes.sh`](example/start_nodes.sh) for a convenient script to start a cluster.

## Usage

`mökv` exposes a `gRPC` `API` defined in `internal/api/kv.proto`. You can use a `gRPC` client to interact with the store.

```proto
service KV {
    rpc Get(GetRequest) returns (GetResponse) {}
    rpc Set(SetRequest) returns (SetResponse) {}
    rpc Delete(DeleteRequest) returns (DeleteResponse) {}
    rpc List(google.protobuf.Empty) returns (stream GetResponse) {}
    rpc GetServers(google.protobuf.Empty) returns (GetServersResponse){}
}
```

## How it Works: Core Components and Data Flow

`mökv` combines `Serf` for node discovery and `Raft` for consistent data replication. Here's how the key components interact:

- `Serf`: Dynamic Membership: `Serf` uses `UDP` to monitor cluster membership. When a node joins, the `serf.EventMemberJoin` event triggers the `Join` function ([`internal/kv/kv.go`](/internal/kv/kv.go)), adding the node as a `Raft` voter. This ensures the `Raft` cluster reflects the current active nodes.

- `Raft`: Consensus and the FSM: `Raft` guarantees data consistency. One node is `Leader`, handling all write operations. Write operations become `Raft` log entries, replicated to `Followers`. The _Finite State Machine (`FSM`)_ is the core of `Raft's` operation:

  - Applying Log Entries: When a log entry is committed (acknowledged by a quorum), the `Apply` method of the `FSM` (in `internal/kv/kv.go`) is invoked. The `Apply` method handles different request types:

    - Set Request: Updates the in-memory key-value store (`kv.store`) with the new key-value pair.
    - Delete Request: Removes the specified key from the in-memory store.

  - Data Flow for Writes: `gRPC` -> `Raft Leader` -> `Log Entry` -> `Replication to Followers` -> `FSM Apply` -> `kv.store`.

- Persistence (`raft-boltdb`): `mökv` uses `raft-boltdb` to persist Raft's log, stable state, and periodic snapshots to disk. This enables recovery after node failures.

  - Snapshotting: The `FSM's` `Snapshot` method creates a snapshot of the current in-memory state.
  - Restoring State: After a crash, the `FSM's` `Restore` method loads the latest snapshot and replays any subsequent log entries, reconstructing the in-memory `kv.store` to a consistent state. This entire process happens automatically when `setupRaft` is called during startup.

## gRPC

`mökv` uses `gRPC` for efficient communication between clients and the cluster.

- API Definition: The core `gRPC` service, `KV`, is defined in [`internal/api/kv.proto`](internal/api/kv.proto), exposing methods like `Get`, `Set`, `Delete`, `List`, and `GetServers`.

- `gRPC` Server: The server implementation resides in [`internal/server/server.go`](internal/server/server.go), handling `gRPC` requests.

- Interceptors: `gRPC` Interceptors are used to handle:

  - Logging: Each incoming request is logged for monitoring.

- Client-Side Load Balancing (Name Resolution and Picker): `mökv` uses client-side load balancing.

  - Name Resolver ([`internal/discovery/resolver.go`](internal/discovery/resolver.go)): The name resolver periodically calls `GetServers` to discover available `mökv` nodes and their roles (Leader/Follower). It updates the list of available servers with the `is_leader` attribute.
  - Picker ([`internal/discovery/picker.go`](internal/discovery/picker.go)): The Picker directs requests based on the operation type and the leader status of available connections:

    - Writes (`Set`, `Delete`): These are routed to the _Leader_ node to ensure consistency.
    - Reads (`Get`, `List`): These are balanced among available _Follower_ nodes for improved read performance.
