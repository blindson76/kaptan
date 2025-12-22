# consul-replctl (MongoDB + Kafka replica-set style controller via Consul)

Single executable with config-driven task enablement (controllers use `instance_number` for identity):
- Mongo controller (active/passive with Consul lock)
- Kafka controller (active/passive with Consul lock)
- Mongo worker (offline status probe on startup, reports to Consul)
- Kafka worker (offline status probe on startup, reports to Consul)

## Build
```bash
go mod tidy
go build ./cmd/replctl
```

## Run
```bash
./replctl -config ./config.yaml
```

## Notes / Safety
- This project is a **framework**. The "replace member" action is intentionally abstracted behind Providers:
  - `internal/controllers/mongo/provider.go`
  - `internal/controllers/kafka/provider.go`
- Worker probes are implemented and safe by default, but Mongo offline probe starts a local mongod process briefly; verify paths and permissions.


## Controller behavior
- wait enough candidates
- publish initial spec
- wait until health reports mark all spec members healthy
- monitor for failures and publish replacement spec when needed

- Initial spec selection is debounced by `initial_settle_duration`: once >=3 eligible candidates exist, controller waits until the candidate set is stable for that window, then selects the **3 most recent eligible** candidates.

- Mongo initial member selection uses **most recent `local.oplog.rs` timestamp** (`LastOplogTs`) when available; this better matches "most recent" in Mongo terms.

## Kafka combined mode + dynamic voter
- Workers should report `broker_addr` and `controller_addr`.
- Controller publishes a Kafka spec with `KafkaMode=combined` and `KafkaDynamicVoter=true` and sets `KafkaBootstrapServers` from selected members' `controller_addr` (maps to `controller.quorum.bootstrap.servers`).
- Runtime/provider should use Kafka's metadata quorum tooling (add/remove voters) for replacements.

## Mongo replica set ID mismatch wipe
- Set `tasks.mongo_controller.replica_set_id` (replica set name).
- When publishing a spec, controller compares each candidate's `LastSeenReplicaSetID` (from local.system.replset) with `replica_set_id`.
- If they differ, spec marks that member in `MongoWipeMembers[id]=true` so the provider/runtime can **wipe that member's data dir** before joining the new replica set.

## Service roles
Mongo agent TTL check note: startup/primary/secondary. Kafka agent TTL check note: startup/running/controller-leader/controller-follower.


### Progress in TTL notes only
- Mongo TTL note includes `progress` while in STARTUP2 (initial sync).
- Kafka TTL note includes `progress` during `reassign_partitions` (generate/execute/verify/done).


## User services (master/slave)
- Enable `tasks.services_controller` and define `services:` array.
- Each service gets 2 instances (`master` and `slave`) placed on different nodes (best-effort).
- Enable `tasks.services_agent` on every node to execute start orders.
- Services controller waits for `wait_for` dependencies (e.g. mongo+kafka) to have at least `min_passing` passing Consul health checks before starting services.
