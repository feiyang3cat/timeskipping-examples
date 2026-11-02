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

Test names deliberately retain workflow class names (for example,
`test_ParentChildWorkflowWithUserTimer`) for quick lookup rather than following
Python's usual snake_case convention.

| # | Execution Type | Feature | V1 Local Testing Server | V2 Testing Env | V2 Direct API |
|---|---------------|---------|------------|----------------|---------------|
| 1 | workflow | User Timers | [workflow](workflow.py#L14) · [test](v1/test_workflow.py#L70) | [test](v2/test_workflow.py#L41) | [test](v2/test_workflow_direct_api.py#L41) |
| 2 | workflow | Workflow Retry | [workflow](workflow.py#L59) · [test](v1/test_workflow.py#L118) | [test](v2/test_workflow.py#L103) | [test](v2/test_workflow_direct_api.py#L103) |
| 3 | workflow | Workflow Execution/Run Timeouts | [workflow](workflow.py#L62) · [test (auto-skip, no sleep)](v1/test_workflow.py#L163) | [test](v2/test_workflow.py#L143) · [test](v2/test_workflow.py#L161) | [test](v2/test_workflow_direct_api.py#L143) · [test](v2/test_workflow_direct_api.py#L161) |
| 4 | workflow | Workflow Start Delay | [workflow](workflow.py#L79) · [test](v1/test_workflow.py#L195) | [test](v2/test_workflow.py#L182) | [test](v2/test_workflow_direct_api.py#L182) |
| 5 | workflow | Activity Retry | [workflow](workflow.py#L88) · [test](v1/test_workflow.py#L220) | [test](v2/test_workflow.py#L211) | [test](v2/test_workflow_direct_api.py#L211) |
| 6 | workflow | Cron | [workflow](workflow.py#L108) · [test using sleep](v1/test_workflow.py#L245) | [test](v2/test_workflow.py#L241) | [test](v2/test_workflow_direct_api.py#L241) |
| 7 | scheduler | Scheduler | ❌ Schedules API not supported in the SDK testing server | Python SDK not covered yet; server only | Python SDK not covered yet; server only |
| 8 | SAA | Retry, Timeout, Start Delay | ❌ SAA not supported in the SDK testing server | Not covered by the v1 migration | Not covered by the v1 migration |
| 9 | workflow | wait for condition/signals/updates | [workflow](workflow.py#L129) · [test using sleep](v1/test_workflow.py#L285) | [test](v2/test_workflow.py#L275) | [test](v2/test_workflow_direct_api.py#L275) |

Each V2 column links to a separate test: Testing Env uses `env`, and Direct API uses `skipper` ([fixtures](v2/conftest.py)).

> v1: SDK testing server · v2: per-workflow time skipping on a compatible Temporal OSS server or Cloud namespace.

See [v2 setup, both APIs, and the full coverage mapping](v2/README.md). All 16 V1 tests and 4 manual lock probes have V2 counterparts, each run through both API entry points (40 cases total).

### More V1 examples with `env.sleep()`

#### Some complicated examples
- parent workflow waiting on signals with timeouts: [[workflow](workflow.py#L161)] · [[test](v1/test_workflow.py#L313)]
- parent + child, both waiting on their own signal windows: [[workflow](workflow.py#L204)] · [[test](v1/test_workflow.py#L344)]

For V1 manual time-skipping exploration and lock behavior, see the [exploration report](manual_time_skipping_report.md).

#### Some anti-patterns
- [busy workflow](workflow.py#L251) — if no failures, retries, or crons are in place, this workflow is always busy with workflow tasks/activities and doesn't need time-skipping or sleep
- [unnecessary `env.sleep()`](workflow.py#L269) — this workflow can just keep time-skipping to the end
