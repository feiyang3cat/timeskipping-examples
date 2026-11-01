[![Tests](https://github.com/feiyang3cat/timeskipping-examples/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/feiyang3cat/timeskipping-examples/actions/workflows/test.yml)

This repo contains examples of time-skipping best practices in testing, along with anti-patterns to avoid.

## Testing features/scenarios

1. Skip user timers in *workflows*
2. Test retry policies (backoff) for *workflows*
3. Test workflow execution and run timeouts
4. Test workflow start delay for *workflows*
5. Test retry policies (backoff) for *activities*
6. Test waiting time for *crons*
7. Test waiting time for *schedulers*
8. Test retry backoffs and start delays for *standalone activities (SAA)*
9. Advance virtual time by a chosen duration: `env.sleep(duration)` in V1, or
   `env.fast_forward(handle, duration)` / `skipper.fast_forward(handle, duration)` in V2. This is useful for:
    - Sleep with schedulers
    - Sleep with retries and cron
    - Sleep for interactions: single workflow, workflow with child workflows

## V1: Java testing server

## V2: two ways to use the new Python SDK

V2 provides per-workflow time skipping through two usage modes:

| Mode | Entry point | Server lifecycle |
| --- | --- | --- |
| **1. New workflow environment** | `WorkflowEnvironment.start_time_skipping_v2(...)` | Starts and stops a local Temporal dev server for the test. Use `env.client` and `env.fast_forward(handle, duration)`. |
| **2. Direct API client** | `Client.connect(...)` + `TimeSkipper(client)` | Connects directly to an existing server, without creating a workflow environment. Use `skipper.client` and `skipper.fast_forward(handle, duration)`. |

`TimeSkipper` is the SDK's client-side helper for the direct API client mode.
Both modes use the same server-side time-skipping APIs and independent workflow
clocks. The test suite runs every V2 scenario through both modes; it starts a
local server as test infrastructure for the direct client mode as well.

The local CLI must be built from
[`temporalio/cli`'s `fx/time-skipping-cli` branch](https://github.com/temporalio/cli/tree/fx/time-skipping-cli).
The CLI used to validate all 40 V2 test cases was commit
[`440c1da397ae4520b3d8a25b44515b26028dc54a`](https://github.com/temporalio/cli/commit/440c1da397ae4520b3d8a25b44515b26028dc54a)
(Server `1.33.0`). See [V2 setup and usage examples](v2/README.md).

## Examples

### By Features

| # | Execution Type | Feature | V1 Example | V2 Testing Env | V2 Direct API |
|---|---------------|---------|------------|----------------|---------------|
| 1 | workflow | User Timers | [workflow](workflow.py#L14) · [test](v1/test_workflow.py#L50) | [tests](v2/test_workflow.py) | [tests](v2/test_workflow.py) |
| 2 | workflow | Workflow Retry | [workflow](workflow.py#L59) · [test](v1/test_workflow.py#L100) | [tests](v2/test_workflow.py) | [tests](v2/test_workflow.py) |
| 3 | workflow | Workflow Execution/Run Timeouts | [workflow](workflow.py#L62) · [test (auto-skip, no sleep)](v1/test_workflow.py#L146) | [tests](v2/test_workflow.py) | [tests](v2/test_workflow.py) |
| 4 | workflow | Workflow Start Delay | [workflow](workflow.py#L79) · [test](v1/test_workflow.py#L168) | [tests](v2/test_workflow.py) | [tests](v2/test_workflow.py) |
| 5 | workflow | Activity Retry | [workflow](workflow.py#L88) · [test](v1/test_workflow.py#L186) | [tests](v2/test_workflow.py) | [tests](v2/test_workflow.py) |
| 6 | workflow | Cron | [workflow](workflow.py#L108) · [test using sleep](v1/test_workflow.py#L232) | [tests](v2/test_workflow.py) | [tests](v2/test_workflow.py) |
| 7 | scheduler | Scheduler | ❌ Schedules API not supported in the SDK testing server | Python SDK not covered yet; server only | Python SDK not covered yet; server only |
| 8 | SAA | Retry, Timeout, Start Delay | ❌ SAA not supported in the SDK testing server | Not covered by the v1 migration | Not covered by the v1 migration |
| 9 | workflow | wait for condition/signals/updates | [workflow](workflow.py#L129) · [test using sleep](v1/test_workflow.py#L274) | [tests](v2/test_workflow.py) | [tests](v2/test_workflow.py) |

Both V2 columns use the same parameterized tests: `environment` selects Testing Env and `time-skipper` selects Direct API ([fixture](v2/conftest.py#L29)).

> v1: SDK testing server · v2: per-workflow time skipping on a compatible Temporal OSS server or Cloud namespace.

See [v2 setup, both APIs, and the full coverage mapping](v2/README.md). All 16 V1 tests and 4 manual lock probes have V2 counterparts, each run through both API entry points (40 cases total).

### More V1 examples with `env.sleep()`

#### Some complicated examples
- parent workflow waiting on signals with timeouts: [[workflow](workflow.py#L161)] · [[test](v1/test_workflow.py#L307)]
- parent + child, both waiting on their own signal windows: [[workflow](workflow.py#L204)] · [[test](v1/test_workflow.py#L347)]

For V1 manual time-skipping exploration and lock behavior, see the [exploration report](manual_time_skipping_report.md).

#### Some anti-patterns
- [busy workflow](workflow.py#L251) — if no failures, retries, or crons are in place, this workflow is always busy with workflow tasks/activities and doesn't need time-skipping or sleep
- [unnecessary `env.sleep()`](workflow.py#L269) — this workflow can just keep time-skipping to the end
