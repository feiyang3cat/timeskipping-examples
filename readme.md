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

## Examples

### By Features

| # | Execution Type | Feature | V1 Local Testing Server | V2 Testing Env | V2 Direct API |
|---|---------------|---------|------------|----------------|---------------|
| 1 | workflow | User Timers | [workflow](workflow.py#L14) · [test](v1/test_workflow.py#L50) | [test](v2/test_workflow.py#L38) | [test](v2/test_workflow_direct_api.py#L38) |
| 2 | workflow | Workflow Retry | [workflow](workflow.py#L59) · [test](v1/test_workflow.py#L100) | [test](v2/test_workflow.py#L100) | [test](v2/test_workflow_direct_api.py#L100) |
| 3 | workflow | Workflow Execution/Run Timeouts | [workflow](workflow.py#L62) · [test (auto-skip, no sleep)](v1/test_workflow.py#L146) | [test](v2/test_workflow.py#L138) · [test](v2/test_workflow.py#L156) | [test](v2/test_workflow_direct_api.py#L138) · [test](v2/test_workflow_direct_api.py#L156) |
| 4 | workflow | Workflow Start Delay | [workflow](workflow.py#L79) · [test](v1/test_workflow.py#L168) | [test](v2/test_workflow.py#L177) | [test](v2/test_workflow_direct_api.py#L177) |
| 5 | workflow | Activity Retry | [workflow](workflow.py#L88) · [test](v1/test_workflow.py#L186) | [test](v2/test_workflow.py#L206) | [test](v2/test_workflow_direct_api.py#L206) |
| 6 | workflow | Cron | [workflow](workflow.py#L108) · [test using sleep](v1/test_workflow.py#L232) | [test](v2/test_workflow.py#L236) | [test](v2/test_workflow_direct_api.py#L236) |
| 7 | scheduler | Scheduler | ❌ Schedules API not supported in the SDK testing server | Python SDK not covered yet; server only | Python SDK not covered yet; server only |
| 8 | SAA | Retry, Timeout, Start Delay | ❌ SAA not supported in the SDK testing server | Not covered by the v1 migration | Not covered by the v1 migration |
| 9 | workflow | wait for condition/signals/updates | [workflow](workflow.py#L129) · [test using sleep](v1/test_workflow.py#L274) | [test](v2/test_workflow.py#L270) | [test](v2/test_workflow_direct_api.py#L270) |

Each V2 column links to a separate test: Testing Env uses `env`, and Direct API uses `skipper` ([fixtures](v2/conftest.py)).

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
