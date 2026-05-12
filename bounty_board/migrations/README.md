# Migrations

Schema migrations land here as `NNNN_description.sql` files. `NNNN` is
the `schema_version` the migration brings the DB up to. Migrations run
forward only; downgrade is unsupported.

V0 ships with no migrations. `0001_initial.sql` lands in a follow-up PR
once the operator ratifies the declared-vs-earned capability decision on
the cluster scratchpad (see decision_id
`cluster_brokerless_task_queue_pitch_v0`).
