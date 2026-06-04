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

| # | Execution Type | Feature | Example | v1 | v2 |
|---|---------------|---------|---------|----|----|
| 1 | workflow | User Timers | [v1/test_workflow.py#L50](v1/test_workflow.py#L50) | ✅ | — |
| 2 | workflow | Workflow Retry | [v1/test_workflow.py#L100](v1/test_workflow.py#L100) | ✅ | — |
| 3 | workflow | Workflow Execution/Run Timeouts | [v1/test_workflow.py#L136](v1/test_workflow.py#L136) | ✅ | — |
| 4 | workflow | Workflow Start Delay | [v1/test_workflow.py#L168](v1/test_workflow.py#L168) | ✅ | — |
| 5 | workflow | Activity Retry | [v1/test_workflow.py#L186](v1/test_workflow.py#L186) | ✅ | — |
| 6 | workflow | Cron | [v1/test_workflow.py#L219](v1/test_workflow.py#L219) | ✅ | — |
| 7 | scheduler | Scheduler | | ❌ | — |
| 8 | SAA | Retry, Timeout, Start Delay | | ❌ | — |
| 9 | all types | TimeSkipping Sleep | [v1/test_workflow.py#L256](v1/test_workflow.py#L256) | ✅ | — |

> ✅ example exists · — coming soon · ❌ not supported
>
> v1: SDK testing server · v2: Temporal OSS server and Cloud
