# mökv

`mökv` is a distributed, in-memory key-value store. It utilizes Raft for consensus, gRPC for communication, and TLS for security.

## Features

- **Distributed Architecture:** Data is replicated across multiple nodes for fault tolerance.
- **In-Memory Storage:** Provides fast read and write operations.
- **Raft Consensus:** Ensures data consistency across the cluster.
- **gRPC Interface:** Offers a well-defined API for interacting with the store.
- **TLS Encryption:** Secures communication between nodes and clients.
- **Access Control:** Uses Casbin for authorization, enabling fine-grained control over data access.
- **Metrics:** Exposes Prometheus metrics for monitoring cluster health and performance.
- **Service Discovery:** Uses Serf for automatic node discovery and membership management.
- **Load Balancing:** Implements gRPC client-side load balancing, directing write operations to the leader and read operations to followers.

## Getting Started

To run `mökv`:

### Prerequisites

- [Go](https://go.dev/dl/)
- [cfssl](https://github.com/cloudflare/cfssl) (for generating TLS certificates)
- [ghz](https://ghz.sh/) (for performance testing. optional)

### Installation

1. **Clone the repository:**

   ```bash
   git clone git@github.com:dynamic-calm/mokv.git
   cd mokv
   ```

2. **Generate TLS Certificates:**

   ```bash
   make gencert
   ```

   This command uses `cfssl` to generate the necessary TLS certificates in the `$HOME/.mokv` directory.

3. **Compile the code:**

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

1. **Start the first node:**

   ```bash
   bin/mokv --config-file example/config.yaml
   ```

2. **Start additional nodes:**

   Modify the `example/config.yaml` file with the appropriate `node-name`, `bind-addr`, and `rpc-port`. Crucially, set `start-join-addrs` to the address of the first node (e.g., `127.0.0.1:8401`). Also set `bootstrap: false` for the additional nodes. Then, run the command again:

   ```bash
   bin/mokv --config-file example/config2.yaml  # Example config for the second node
   ```

   Refer to `example/start_nodes.sh` for a convenient script to start a cluster.

## Usage

`mökv` exposes a gRPC API defined in `internal/api/kv.proto`. You can use a gRPC client to interact with the store.

```proto
service KV {
    rpc Get(GetRequest) returns (GetResponse) {}
    rpc Set(SetRequest) returns (SetResponse) {}
    rpc Delete(DeleteRequest) returns (DeleteResponse) {}
    rpc List(google.protobuf.Empty) returns (stream GetResponse) {}
    rpc GetServers(google.protobuf.Empty) returns (GetServersResponse){}
}
```

## Architecture Overview

`mökv` consists of the following components:

- **gRPC Server:** Handles client requests for setting, getting, deleting, and listing keys.
- **Raft:** Implements a distributed consensus algorithm to ensure data consistency across the cluster. The Raft log is persisted using BoltDB.
- **In-Memory Store:** Provides fast access to key-value data.
- **Serf:** Provides membership and failure detection.
- **Casbin:** Provides Access Control
