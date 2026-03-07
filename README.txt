consul-replctl
==============

Purpose
-------
consul-replctl is a single-executable, config-driven controller that manages
replica-set style clusters for MongoDB and Apache Kafka using HashiCorp Consul
as the coordination backend. It handles automatic leader election, member
health monitoring, failure detection, and controlled replacement of unhealthy
members without requiring external orchestration tools.

Key Features
------------
- MongoDB replica-set controller
    * Active/passive operation with a Consul session lock so only one
      controller is authoritative at a time.
    * Selects the initial 3 members by most-recent oplog timestamp.
    * Debounced initial-settle window prevents premature spec publication.
    * Detects replica-set ID mismatches and marks members for data wipe
      before re-joining the set.
    * Offline-candidate probe: briefly starts a local mongod instance to
      check oplog recency without a live replica set.

- Kafka KRaft controller cluster controller
    * Active/passive operation with a Consul session lock.
    * Publishes a Kafka spec with combined mode and dynamic-voter support.
    * Propagates controller quorum bootstrap servers from selected members'
      controller_addr values.
    * Tracks partition-reassignment progress in the agent TTL note.

- MongoDB agent (per-node)
    * Executes orders (wipe, start, stop, init, reconfigure, add/remove voter).
    * Registers a Consul TTL health check and updates it with the current
      replica-set role (startup / primary / secondary).
    * Reports oplog recency and replica-set ID to the controller via the
      candidates KV prefix.

- Kafka agent (per-node)
    * Executes orders (wipe, start, stop, init, reconfigure,
      reassign_partitions).
    * Registers a Consul TTL health check and updates it with the current
      Kafka role (startup / running / controller-leader / controller-follower).
    * Reports broker_addr and controller_addr to the controller.

- User-service controller & agent
    * Manages arbitrary application services (master/slave pairs) placed
      on different nodes.
    * Waits for MongoDB and/or Kafka to reach a minimum number of passing
      health checks before starting dependent services.

Architecture
------------
The codebase is organized as follows:

    cmd/replctl/       - Main entry point; reads config.yaml and starts
                         enabled tasks.
    cmd/logview/       - Small diagnostic tool that streams structured log
                         lines over UDP.
    internal/
      agents/          - Per-node agents (mongo, kafka, service).
      controllers/     - Active controllers (mongo, kafka, services).
      config/          - YAML config loader with ${env.VAR} interpolation.
      fsm/             - Generic finite-state-machine helpers.
      logging/         - Structured logging setup.
      orders/          - Order / Ack types shared between controllers and agents.
      providers/       - Consul-based order delivery (consulorders).
      runtime/         - OS-signal context helpers.
      servicereg/      - Consul service registration helpers.
      store/consul/    - Low-level Consul KV / session wrappers.
      types/           - Shared domain types (CandidateReport, ReplicaSpec, …).
      workers/         - Background probers for Mongo and Kafka.

Build
-----
    go mod tidy
    go build ./cmd/replctl

Run
---
    ./replctl -config ./config.yaml

Configuration Highlights (config.yaml)
---------------------------------------
- node_name         : Identifies this node; defaults to the OS hostname.
- consul.prefix     : KV path prefix used for all keys (default: replctl/v1).
- consul.order_history_keep : Number of historical orders to retain per target
                      for debugging (0 = disabled).
- tasks.*_controller.instance_number : Set to 1 on the primary candidate
                      controller node, 2 on the standby, etc.
- tasks.mongo_controller.initial_settle_duration : Debounce window before the
                      controller picks the initial 3 Mongo members.
- tasks.mongo_agent.temp_port : Local port used by the offline mongod probe.

Safety Notes
------------
- The "replace member" action is intentionally abstracted behind Provider
  interfaces so that callers control what happens at the infrastructure level:
    * internal/controllers/mongo/provider.go
    * internal/controllers/kafka/provider.go
- The Mongo offline probe starts a local mongod process briefly.
  Verify that mongod_path, dbpath, and file permissions are correct before
  enabling the mongo_agent.
- Consul ACL tokens should be scoped to the consul.prefix path to limit
  blast radius.
