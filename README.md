# Bounty Board

> Single-binary task queue for multi-agent systems: SQLite-backed atomic
> claims, time-travel DLQ, capability pull routing.

**Status: V0 / pre-alpha — design lane in flight.**

The real README (metaphor framing, hook, "what this is NOT" section,
quickstart, `/inspect` screenshot) lands once the design lane on the
[Context Continuum](https://github.com/Context-Continuum) cluster
scratchpad converges on the elegance decisions:

- Capability model: declared vs earned-from-history
- Idle posture: poll vs filesystem-fsync wake
- `/inspect` dashboard visual metaphor
- On-disk shape: one file per queue vs many queues per file
- State-machine vocabulary: standard (Queued / Claimed / ...) vs metaphor

See `decision_id` **`cluster_brokerless_task_queue_pitch_v0`** for the
design discussion.

## Repo layout (V0)

```
bounty_board/
  __init__.py
  _meta.py             # schema-version check + forward-migration runner
  migrations/          # NNNN_description.sql — empty at V0
tests/
  test_meta.py
pyproject.toml
.github/workflows/test.yml   # pytest + ruff on push (Linux/macOS/Windows × 3.11/3.12)
```

## License

[MIT](./LICENSE).
