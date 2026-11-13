# Local Three-Node Cluster

This loopback-only example starts three PirinDB processes. Nodes A and B own the initial slots; node C starts with zero slots so the rebalance APIs can move data onto it.

From this directory, export one development-only control-plane token in each terminal and start one node per terminal:

```bash
export PIRINDB_CLUSTER_ADMIN_TOKEN=local-development-only-token
go run ../../cmd/pirindb --config node-a.toml
```

Repeat with `node-b.toml` and `node-c.toml`. The generated `cluster-demo-node-*.db` files are ignored by Git. The cluster HTTP API uses plain `http://` addresses and has no certificate configuration.
