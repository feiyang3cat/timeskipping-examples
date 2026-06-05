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
9. Set a duration for `env.sleep()` to pause time-skipping after some time. This is useful for:
    - Sleep with schedulers
    - Sleep with retries and cron
    - Sleep for interactions: single workflow, workflow with child workflows

## Examples

### By Features

| # | Execution Type | Feature | v1 Example | v2 |
|---|---------------|---------|------------|-----|
| 1 | workflow | User Timers | [workflow](workflow.py#L14) · [test](v1/test_workflow.py#L50) | 🔜 |
| 2 | workflow | Workflow Retry | [workflow](workflow.py#L59) · [test](v1/test_workflow.py#L100) | 🔜 |
| 3 | workflow | Workflow Execution/Run Timeouts | [workflow](workflow.py#L62) · [test](v1/test_workflow.py#L136) | 🔜 |
| 4 | workflow | Workflow Start Delay | [workflow](workflow.py#L79) · [test](v1/test_workflow.py#L168) | 🔜 |
| 5 | workflow | Activity Retry | [workflow](workflow.py#L88) · [test](v1/test_workflow.py#L186) | 🔜 |
| 6 | workflow | Cron | [workflow](workflow.py#L108) · [test using sleep](v1/test_workflow.py#L232) | 🔜 |
| 7 | scheduler | Scheduler | ❌ Schedules API not supported in the SDK testing server | 🔜 |
| 8 | SAA | Retry, Timeout, Start Delay | ❌ SAA not supported in the SDK testing server | 🔜 |
| 9 | workflow | wait for condition/signals/updates | [workflow](workflow.py#L129) · [test using sleep](v1/test_workflow.py#L274) | 🔜 |

> 🔜 coming soon · v1: SDK testing server · v2: Temporal OSS server and Cloud

### More examples with sleep

#### Some complicated examples
- parent workflow waiting on signals with timeouts: [[workflow](workflow.py#L161)] · [[test](v1/test_workflow.py#L307)]
- parent + child, both waiting on their own signal windows: [[workflow](workflow.py#L204)] · [[test](v1/test_workflow.py#L347)]

#### Some anti-patterns
- [busy workflow](workflow.py#L251) — if no failures, retries, or crons are in place, this workflow is always busy with workflow tasks/activities and doesn't need time-skipping or sleep
- [unnecessary `env.sleep()`](workflow.py#L269) — this workflow can just keep time-skipping to the end
