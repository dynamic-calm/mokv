# mökv

`mökv` is a distributed, in-memory key-value store. It utilizes [`Raft`](https://github.com/hashicorp/raft) for consensus, [`serf`](https://github.com/hashicorp/serf) for discvoery, [`gRPC`](https://github.com/grpc/grpc-go) for communication, and `TLS` for security.

> [!NOTE]
> This is a project to learn more about distributed systems and `Go`.

## Features

- Distributed Architecture: Data is replicated across multiple nodes for fault tolerance.
- In-Memory Storage: Provides fast read and write operations.
- `Raft` Consensus: Ensures data consistency across the cluster.
- `gRPC` Interface: Offers a well-defined `API` for interacting with the store.
- `TLS` Encryption: Secures communication between nodes and clients.
- Access Control: Uses `Casbin` for authorization, enabling fine-grained control over data access.
- Metrics: Exposes `Prometheus` metrics for monitoring cluster health and performance.
- Service Discovery: Uses `serf` for automatic node discovery and membership management.
- Load Balancing: Implements `gRPC` client-side load balancing, directing write operations to the leader and read operations to followers.

## Getting Started

To run `mökv`:

### Prerequisites

- [`Go`](https://go.dev/dl/)
- [`cfssl`](https://github.com/cloudflare/cfssl) (for generating `TLS` certificates)
- [`ghz`](https://ghz.sh/) (for performance testing. Optional)

### Installation

1. Clone the repository:

   ```bash
   git clone git@github.com:dynamic-calm/mokv.git
   cd mokv
   ```

2. Generate TLS Certificates:

   ```bash
   make gencert
   ```

   This command uses `cfssl` to generate the necessary `TLS` certificates in the `$HOME/.mokv` directory.

3. Compile the code:

   ```bash
   make build
   ```

   This will create an executable binary `mokv` in the `bin/` directory.

### Configuration

Configuration is done through command-line flags or a configuration file. A sample configuration file (`example/config.yaml`) is provided. Copy `certs/model.conf` and `certs/policy.csv` to `$HOME/.mokv`.

Here's an example `config.yaml`:

```yaml
data-dir: /tmp/mokv-data
node-name: node1
bind-addr: 127.0.0.1:8401
rpc-port: 8400
start-join-addrs: []
bootstrap: true
acl-model-file: $HOME/.mokv/model.conf
acl-policy-file: $HOME/.mokv/policy.csv
server-tls-cert-file: $HOME/.mokv/server.pem
server-tls-key-file: $HOME/.mokv/server-key.pem
server-tls-ca-file: $HOME/.mokv/ca.pem
peer-tls-cert-file: $HOME/.mokv/root-client.pem
peer-tls-key-file: $HOME/.mokv/root-client-key.pem
peer-tls-ca-file: $HOME/.mokv/ca.pem
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

   Refer to `example/start_nodes.sh` for a convenient script to start a cluster.

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

## gRPC API: Communication, Security, and Load Balancing

`mökv` uses gRPC for efficient client-cluster communication, secured with TLS client certificates.

- API Definition: The [`internal/api/kv.proto`](internal/api/kv.proto) file defines the `KV` service (methods: `Get`, `Set`, `Delete`, `List`, `GetServers`).

- Server: Implemented in [`internal/server/server.go`](internal/server/server.go).

- Interceptors:

  - Logging: Logs requests for monitoring.
  - Authentication: Uses TLS client certificates; the certificate's Common Name (CN) is the username for authorization.

- Authorization (Casbin): The [`internal/auth/auth.go`](internal/auth/auth.go) enforces access control using Casbin, allowing actions (produce/consume) based on the authenticated user.

- Client-Side Load Balancing (Name Resolution and Picker):

  - Name Resolver ([`internal/discovery/resolver.go`](internal/discovery/resolver.go)): Discovers `mökv` nodes using `GetServers` and adds attributes indicating the Leader.
  - Picker ([`internal/discovery/picker.go`](internal/discovery/picker.go)): Routes `Set/Delete` (writes) to the Leader and `Get/List` (reads) to followers (randomly selected).
