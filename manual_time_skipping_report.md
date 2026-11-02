# Manual time skipping with `env.sleep()`

## How it works

The time-skipping test server keeps one **Time Locking Counter**. Time skips only while the counter is 0; when it is above 0, the clock runs at real wall-clock speed. The counter records how many locks are held, not who holds them.

| Who | Effect on the counter |
|---|---|
| Server start | starts at **1** ("Test Server is typically started with locked time skipping and Time Locking Counter = 1") |
| `handle.result()` / `execute_workflow` | −1 while waiting, +1 after it returns (`_TimeSkippingWorkflowHandle.result` → `time_skipping_unlocked`) |
| In-flight activity | +1 until it finishes |
| Pending workflow task | +1 until it completes |
| `env.sleep(d)` | −1 now, +1 once server time has advanced by `d` (`UnlockTimeSkippingWithSleep`) |

Sources: `temporal/api/testservice/v1/service.proto` (in the SDK's bundled protos) and `temporalio/testing/_workflow.py`. The two "+1" rows for the in-flight activity and the pending workflow task are not in the proto docs; they are observed in the tests below.

**Conclusion:** `env.sleep()` can break through any lock, wherever it is called from. It only needs:
- **Timing:** call it when the counter is exactly 1, which usually means `result()` is already being awaited. If N locks are held, N concurrent `env.sleep()` calls are needed.
- **Duration:** long enough to reach the next timer. If it is too short, the counter goes back up and time is locked again.

## Verified examples

### `v1/test_workflow.py`

| Example | One-line explanation | Test |
|---|---|---|
| 9++: `env.sleep()` inside an activity fires a user timer | `execute_workflow` is waiting (0) + activity (1) → in-activity `env.sleep(11m)` brings it to 0, so the 10m timer fires first | [`test_WorkflowRacingActivityAndTimer_env_sleep_in_activity_v1`](v1/test_workflow.py#L398) · workflow [`WorkflowRacingActivityAndTimer`](workflow.py#L249) |
| 9+++: `env.sleep()` from the test does not skip | `result()` is not being awaited yet: start (1) + activity (1) = 2 → `env.sleep(3s)` only brings it to 1, so it waits 3 real seconds | [`test_WorkflowRunningLongActivity_env_sleep_from_test_v1`](v1/test_workflow.py#L445) · workflow [`WorkflowRunningLongActivity`](workflow.py#L284) |
| 9++++: two `env.sleep()` calls inside an activity fire two timers | Each `env.sleep(1h)` drops the counter to 0 once, and the two skips add up to +1h, +2h | [`test_WorkflowWithTwoTimersAndActivity_two_sleeps_in_activity_v1`](v1/test_workflow.py#L496) · workflow [`WorkflowWithTwoTimersAndActivity`](workflow.py#L305) |
| 9+++++ (a): `env.sleep()` from the test unlocks an activity | Await `result()` first (0), activity held (1) → two `env.sleep(1h)` calls from the test fire both timers | [`test_WorkflowWithTwoTimersAndActivity_env_sleep_from_test_v1`](v1/test_workflow.py#L551) · workflow [`WorkflowWithTwoTimersAndActivity`](workflow.py#L305) |
| 9+++++ (b): `env.sleep()` from the test unlocks a pending workflow task | No worker, so the workflow task stays pending (1) → `env.sleep(30m)` is too short and locks again; a second `env.sleep(30m)` reaches the 1h execution timeout | [`test_WaitForSignalWorkflow_pending_workflow_task_v1`](v1/test_workflow.py#L596) · workflow [`WaitForSignalWorkflow`](workflow.py#L63) |
| 3 (contrast): a timeout with no `env.sleep()` | The worker completes the workflow task and nothing else holds a lock → `result()` alone brings the counter to 0 and it skips to the timeout | [`test_WaitForSignalWorkflow_execution_timeout_v1`](v1/test_workflow.py#L179) |

### `v1/manual_env_sleep_lock_scenarios.py` (not run in CI; 3 concurrent activities)

| Example | One-line explanation | Test |
|---|---|---|
| 1: three `env.sleep()` calls from the test | `result()` is not being awaited: 1 + 3 = 4 → three sleeps only bring it to 1, so it runs in real time | [`test_ThreeActivities_env_sleep_from_test_v1`](v1/manual_env_sleep_lock_scenarios.py#L132) |
| 2: one `env.sleep()` in each activity | 0 + 3 = 3 → three sleeps bring it to 0, so time skips while all three are in flight | [`test_ThreeActivities_env_sleep_in_activity_v1`](v1/manual_env_sleep_lock_scenarios.py#L165) |
| 3: skip > start_to_close | Same as 2, but the skip goes past the activities' own `start_to_close_timeout`, so all three time out | [`test_ThreeActivities_env_sleep_fires_start_to_close_v1`](v1/manual_env_sleep_lock_scenarios.py#L190) |
| 4: in-activity skip vs a user timer | Same as 2; the 11m skip fires the 10m timer first | [`test_ThreeActivitiesRacedVsTimer_env_sleep_in_activity_v1`](v1/manual_env_sleep_lock_scenarios.py#L212) |

> Note: the docstring in this file says "outside env.sleep cannot release the activities' locks". That is not accurate. The counter does not track who holds each lock; Scenario 1 fails because the start lock is still held (`result()` is not being awaited), not because the call came from outside.

## V2 observations and open questions

The observations below come from the 40 V2 test cases (20 scenarios exercised
through both the new environment and the direct client helper). The tested
builds were Python SDK main commit
[`c1a79fb659ffab144d7949213664a584809089f7`](https://github.com/temporalio/sdk-python/commit/c1a79fb659ffab144d7949213664a584809089f7)
and CLI branch `fx/time-skipping-cli`, commit
[`440c1da397ae4520b3d8a25b44515b26028dc54a`](https://github.com/temporalio/cli/commit/440c1da397ae4520b3d8a25b44515b26028dc54a).
Passing these tests does not resolve the open questions below.

1. **In-flight activities:** [V1 `env.sleep()`](v1/test_workflow.py#L398) can advance time while an activity runs and make the timer win; [V2 `fast_forward()`](v2/test_activity_time_skipping.py#L35) waits for the activity, so the activity wins after release. Whether V2 should support forcing the timer to win remains an open question.

2. **Parent/child clocks — design difference:** V1 shares a global clock, while V2 gives each workflow its own clock; a parent does not inherit its children's skipped duration ([V1 test](v1/test_workflow.py#L93), [V2 test](v2/test_workflow.py#L66)).

3. **Fast-forward return value — unresolved:** The [two-timer test](v2/test_activity_time_skipping.py#L120) observed both `True` and `False`, but exact equality of the target and workflow completion timestamps was not established. Server polling prioritizes completed fast-forward state over workflow closure; the cause of the variation still needs investigation, and the test currently does not assert this return value.

## WIP

### V1 Java testing server: suspected lock leak

These are one-off observations from the legacy Java testing server started by
`WorkflowEnvironment.start_time_skipping()`, not the V2 OSS server. They have
not been established by dedicated lock-leak regression tests.

This section was first added on **2026-10-28** in commit
[`28fc6e2` — `add exploration of manual ts`](https://github.com/feiyang3cat/timeskipping-examples/commit/28fc6e29458bddf3d3a8836f16140e1d5b440d84).
The exact dates of the experiments were not recorded separately.

| Situation | Result |
|---|---|
| Timer wins and the in-flight activity gets cancelled (9++) | That activity's lock is never released; afterwards an idle `env.sleep(3s)` takes 3 real seconds |
| Workflow times out while its workflow task is still pending (9+++++ b) | That workflow task's lock is never released either |

Impact: with the session-scoped `ts_env`, 9++ caused 9+++ to fail and 9++++ to hang. Both tests now use the function-scoped [`isolated_ts_env`](v1/test_workflow.py#L64).
