This repo contains examples of time-skipping best practices in testing, along with anti-patterns to avoid.

## Testing features/scenarios

1. Skip user timers in *workflows*
2. Test retry policies (backoff) for *workflows*
3. Test workflow execution and run timeouts
4. Test workflow start delay for *workflows*
5. Test retry policies (backoff) for *activities*
6. Test retry backoffs and start delays for *standalone activities (SAA)*
7. Test waiting time for *crons* and *schedulers*
8. Set a duration for `env.sleep()` to pause time-skipping after some time. This is useful for:
    - Sleep with schedulers
    - Sleep with retries and cron
    - Sleep for interactions: single workflow, workflow with child workflows

## Examples

| # | Execution Type | Feature | Example | v1 | v2 |
|---|---------------|---------|---------|----|----|
| 1 | workflow | User Timers | [v1/test_workflow.py#L49](v1/test_workflow.py#L49) | ✅ | — |
| 2 | workflow | Workflow Retry | [v1/test_workflow.py#L99](v1/test_workflow.py#L99) | ✅ | — |
| 3 | workflow | Workflow Execution/Run Timeouts | [v1/test_workflow.py#L117](v1/test_workflow.py#L117) | ✅ | — |
| 4 | workflow | Workflow Start Delay | | — | — |
| 5 | workflow | Activity Retry | [v1/test_workflow.py#L149](v1/test_workflow.py#L149) | ✅ | — |
| 6 | SAA | Retry, Timeout, Start Delay | | ❌ | — |
| 7 | workflow | Cron | [v1/test_workflow.py#L182](v1/test_workflow.py#L182) | ✅ | — |
| 7 | scheduler | Scheduler | | ❌ | — |
| 8 | all types | TimeSkipping Sleep | [v1/test_workflow.py#L212](v1/test_workflow.py#L212) | ✅ | — |

> ✅ example exists · — coming soon · ❌ not supported
>
> v1: SDK testing server · v2: Temporal OSS server and Cloud
