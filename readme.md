This repo contains examples of time-skipping best practices in testing, along with anti-patterns to avoid.

## Testing features/scenarios

1. Skip user timers in *workflows*
2. Test retry backoff and retry policies for *workflows*
3. Test workflow execution and run timeouts
4. Test retry backoff and retry policies for *activities*
5. Test retry backoffs and start delays for *standalone activities (SAA)*
6. Test waiting time for *crons* and *schedulers*
7. Set a duration for `env.sleep()` to pause time-skipping after some time. This is useful for:
    - Sleep with schedulers
    - Sleep with retries and cron
    - Sleep for interactions: single workflow, workflow with child workflows

## Examples

| # | Feature | Example | v1 | v2 |
|---|---------|---------|----|----|
| 1 | User Timers | [v1/basic/test_workflow.py](v1/basic/test_workflow.py) | ✅ | — |
| 2 | Workflow Retry | | — | — |
| 3 | Workflow Execution/Run Timeouts | | — | — |
| 4 | Activity Retry| [v1/basic/test_workflow.py](v1/basic/test_workflow.py) | ✅ | — |
| 5 | SAA | | ❌ | - |
| 6 | Cron | | — | — |
| 6 | Scheduler | | ❌ | — |
| 7 | TimeSkipping Sleep| [v1/sleep/test_workflow.py](v1/sleep/test_workflow.py) | — | — |

> ✅ example exists · — coming soon · ❌ not supported

