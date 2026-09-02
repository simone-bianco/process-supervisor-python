# 02 — Process lifecycle

This chapter explains how a desired process becomes an owned process and how it returns to a clean state.

## From desired capacity to slots

The host describes groups and desired capacity. The resident expands that into slots.

```mermaid
flowchart LR
    Desired[Queue desired = 3] --> Slot0[Slot 0]
    Desired --> Slot1[Slot 1]
    Desired --> Slot2[Slot 2]
    Slot0 --> Job0[Exact child Job]
    Slot1 --> Job1[Exact child Job]
    Slot2 --> Job2[Exact child Job]
```

A slot is not just a PID. It combines lifecycle authority, restart history, deadlines and the exact Job Object that contains the child process tree.

## Spawn flow

The lifecycle is write-ahead. The supervisor records its intent before process creation, then proves ownership after spawn.

```mermaid
sequenceDiagram
    participant Resident
    participant Ledger as Child ledger
    participant Job as Windows Job
    participant Child as Child process

    Resident->>Ledger: arm attempt + ownership id
    Resident->>Job: create exact Job Object
    Resident->>Child: create suspended/restricted child
    Resident->>Job: assign child to Job
    Resident->>Ledger: mark active
    Resident->>Child: allow execution
```

If the sequence becomes ambiguous, the ledger becomes `uncertain` rather than pretending the child is clean.

## Running and stopping

For a long-lived workload such as Reverb, the normal path is:

```mermaid
stateDiagram-v2
    [*] --> Starting
    Starting --> Running
    Running --> Stopping: desired becomes zero
    Stopping --> Stopped: exact Job is empty
    Running --> Backoff: process failure
    Backoff --> Starting: retry allowed
    Backoff --> Fatal: crash budget exhausted
```

The stop deadline gives the child an opportunity to exit cooperatively. If it does not, the supervisor may terminate only the exact Job it owns.

## Queue one-shot recycling

Queue is different because `queue:work --once` is expected to exit after one bounded cycle.

A healthy exit is not a crash.

```mermaid
stateDiagram-v2
    [*] --> Starting
    Starting --> Running
    Running --> Idle: healthy empty/successful cycle
    Idle --> Starting: next poll cycle
    Running --> Backoff: real process failure
    Backoff --> Starting: retry
```

The distinction matters operationally:

- healthy recycle keeps `restart_count = 0`
- crash backoff increments restart accounting
- `idle` is healthy capacity, not a warning state

When the queue is empty, the next one-shot spawn is delayed using the configured sleep interval instead of respawning in a tight loop.

## Watchdogs

Some Laravel worker semantics normally depend on Unix process-control features. The Windows-first engine therefore uses external watchdogs for bounded child lifetime.

```mermaid
flowchart LR
    Start[Child starts] --> Deadline[Watchdog deadline]
    Start --> Exit[Child exits normally]
    Deadline -->|child still alive| Contain[Exact Job containment]
    Exit --> Healthy[Healthy completion]
```

A watchdog failure is a process-health signal. It is different from an application-level job failure handled normally by Laravel.

## Resident heartbeat

The resident periodically publishes status/heartbeat. A host can treat a stale heartbeat with active desired capacity as a control-plane failure even if the last JSON status still says `running`.

This prevents stale “green” state from surviving an externally terminated resident.

## Generation changes

Some configuration changes alter the actual process contract: PHP executable, child environment, port or group parameters. Those changes advance group/runtime generation so the resident replaces old instances instead of silently continuing with stale configuration.

## Clean shutdown invariant

A slot is truly clean only after both conditions hold:

1. the lifecycle ledger no longer claims active/uncertain ownership;
2. the exact Job Object is proven empty/absent.

This is why process cleanup is more strict than merely observing an exit code.