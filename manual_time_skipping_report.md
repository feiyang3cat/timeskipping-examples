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
| 9++: `env.sleep()` inside an activity fires a user timer | `execute_workflow` is waiting (0) + activity (1) → in-activity `env.sleep(11m)` brings it to 0, so the 10m timer fires first | [`test_env_sleep_in_activity_fires_user_timer`](v1/test_workflow.py#L394) · workflow [`WorkflowRacingActivityAndTimer`](workflow.py#L249) |
| 9+++: `env.sleep()` from the test does not skip | `result()` is not being awaited yet: start (1) + activity (1) = 2 → `env.sleep(3s)` only brings it to 1, so it waits 3 real seconds | [`test_env_sleep_blocks_in_real_time_while_activity_in_flight`](v1/test_workflow.py#L441) · workflow [`WorkflowRunningLongActivity`](workflow.py#L284) |
| 9++++: two `env.sleep()` calls inside an activity fire two timers | Each `env.sleep(1h)` drops the counter to 0 once, and the two skips add up to +1h, +2h | [`test_two_env_sleeps_in_activity_fire_both_timers`](v1/test_workflow.py#L492) · workflow [`WorkflowWithTwoTimersAndActivity`](workflow.py#L305) |
| 9+++++ (a): `env.sleep()` from the test unlocks an activity | Await `result()` first (0), activity held (1) → two `env.sleep(1h)` calls from the test fire both timers | [`test_env_sleep_from_test_unlocks_held_activity`](v1/test_workflow.py#L547) · workflow [`WorkflowWithTwoTimersAndActivity`](workflow.py#L305) |
| 9+++++ (b): `env.sleep()` from the test unlocks a pending workflow task | No worker, so the workflow task stays pending (1) → `env.sleep(30m)` is too short and locks again; a second `env.sleep(30m)` reaches the 1h execution timeout | [`test_env_sleep_from_test_unlocks_pending_workflow_task`](v1/test_workflow.py#L592) · workflow [`WaitForSignalWorkflow`](workflow.py#L63) |
| 3 (contrast): a timeout with no `env.sleep()` | The worker completes the workflow task and nothing else holds a lock → `result()` alone brings the counter to 0 and it skips to the timeout | [`test_workflow_times_out_after_execution_timeout`](v1/test_workflow.py#L175) |

### `v1/manual_env_sleep_lock_scenarios.py` (not run in CI; 3 concurrent activities)

| Example | One-line explanation | Test |
|---|---|---|
| 1: three `env.sleep()` calls from the test | `result()` is not being awaited: 1 + 3 = 4 → three sleeps only bring it to 1, so it runs in real time | [`test_1_outside_env_sleep_blocks_in_real_time`](v1/manual_env_sleep_lock_scenarios.py#L128) |
| 2: one `env.sleep()` in each activity | 0 + 3 = 3 → three sleeps bring it to 0, so time skips while all three are in flight | [`test_2_inside_env_sleep_skips_while_in_flight`](v1/manual_env_sleep_lock_scenarios.py#L161) |
| 3: skip > start_to_close | Same as 2, but the skip goes past the activities' own `start_to_close_timeout`, so all three time out | [`test_3_inside_env_sleep_fires_start_to_close`](v1/manual_env_sleep_lock_scenarios.py#L186) |
| 4: in-activity skip vs a user timer | Same as 2; the 11m skip fires the 10m timer first | [`test_4_inside_env_sleep_loses_race_to_user_timer`](v1/manual_env_sleep_lock_scenarios.py#L208) |

> Note: the docstring in this file says "outside env.sleep cannot release the activities' locks". That is not accurate. The counter does not track who holds each lock; Scenario 1 fails because the start lock is still held (`result()` is not being awaited), not because the call came from outside.

## V2 observations and open questions

The observations below come from the 40 V2 test cases (20 scenarios exercised
through both the new environment and the direct client helper). The tested
builds were Python SDK main commit
[`c1a79fb659ffab144d7949213664a584809089f7`](https://github.com/temporalio/sdk-python/commit/c1a79fb659ffab144d7949213664a584809089f7)
and CLI branch `fx/time-skipping-cli`, commit
[`440c1da397ae4520b3d8a25b44515b26028dc54a`](https://github.com/temporalio/cli/commit/440c1da397ae4520b3d8a25b44515b26028dc54a).
Passing these tests does not resolve the open questions below.

1. **In-flight activities: V1 can manually unlock time skipping; V2 fast-forward remains blocked.**

   In the [V1 activity/timer race](v1/test_workflow.py#L394), the workflow races an
   activity against a 10-minute timer. While `execute_workflow()` is awaiting the
   result, the mocked activity calls `env.sleep(11 minutes)`. This drops the
   remaining global lock counter to zero and advances time while the activity
   is still in flight. The test asserts `winner == "timer"`.

   In the [V2 counterpart](v2/test_activity_time_skipping.py#L31), the mocked
   activity requests `fast_forward(handle, 11 minutes)` in a background task
   and waits on an explicit release event. The test first checks that the
   fast-forward has not completed, then releases the activity. It asserts that
   the workflow result is `"activity"` and the fast-forward returns `False`
   because the workflow completes before reaching that target. Awaiting the
   fast-forward inline inside the held activity would prevent that activity
   from completing and keep the advance blocked.

   The difference is not simply "V1 always skips during activities": the
   [V1 outside-sleep test](v1/test_workflow.py#L441) also blocks in real time
   when `result()` is not being awaited, because the global counter stays
   positive. The [V1 outside-sleep with a background result wait](v1/test_workflow.py#L547)
   can advance through the held activity; the [V2 counterpart](v2/test_activity_time_skipping.py#L186)
   remains blocked even with that background result wait. After release, V2
   observes `activity @0h -> timer1 @1h -> timer2 @2h`, whereas V1 observes
   `timer1 @1h -> timer2 @2h -> activity @2h` (hours are rounded by the workflow).

   **Open question:** Is blocking all manual advancement during an in-flight
   activity the intended V2 testing contract? If so, how should a test simulate
   a workflow timer expiring before a still-running activity without waiting
   for that timer in real time? The V2 test currently verifies the blocking
   behavior; it does not reproduce V1's ability to force the timer to win.

2. **Independent parent/child clocks: a design difference.**

   V1 uses one shared server clock. Its [parent/child timer test](v1/test_workflow.py#L89)
   checks that the parent's completion time is at least two hours after the
   shared baseline when the children sleep for one and two hours.
   The [V2 test](v2/test_workflow.py#L62) instead reads each child's result and
   verifies its own elapsed time: the parent does not acquire the children's
   skipped durations when they finish.

   This changes interaction timelines. In the two-child approval example, V1
   advances the shared clock by 3.5 hours: two hours for the children, one hour
   for the parent's cooldown, and 30 minutes into its approval window
   ([V1 test](v1/test_workflow.py#L310)). V2 advances the parent's clock by
   1.5 hours for that cooldown and approval window; the children's waits happen
   on their own clocks ([V2 test](v2/test_workflow.py#L290)). The
   [separate parent/child signal-window test](v2/test_workflow.py#L314) explicitly
   advances the child through its own handle.

   **Classification:** Treat this as a design difference, not a confirmed
   server bug. A single V1 global timeline cannot be copied directly into V2.

3. **Fast-forward completion versus workflow completion: cause not yet established.**

   In the [two-timer V2 probe](v2/test_activity_time_skipping.py#L116), the
   activity completes first, then two sequential one-hour advances fire the
   one-hour and two-hour timers. The workflow completes after the second timer.
   An initial assertion expected the second fast-forward to return `False`,
   but actual runs also returned `True`, causing that assertion to fail.

   The assertion was changed from
   `assert not await ts_env.fast_forward(handle, timedelta(hours=1))` to
   `await ts_env.fast_forward(handle, timedelta(hours=1))`. The result assertion
   still requires `[["activity", 0], ["timer1", 1], ["timer2", 2]]`.
   Consequently, this test verifies completion order and rounded virtual hours,
   but no longer verifies the second fast-forward's boolean result.

   **Important limitation:** We have not shown that the fast-forward target and
   workflow completion timestamps are exactly equal. Each advance is relative
   to the current workflow clock; real time can pass between calls, and the
   workflow rounds elapsed hours. The earlier explanation that either event
   can win at an exactly equal timestamp was an unverified hypothesis, not a
   demonstrated server guarantee.

   **Expected contract to verify:** If the fast-forward target and workflow
   completion really coincide, fast-forward completion should take precedence
   and return `True`, according to the previously implemented server fix.
   The reason for the observed boolean variation remains unresolved. Compare
   the actual fast-forward target, its reached state, transition/history events,
   and workflow completion timestamp, and confirm that the tested CLI's server
   dependency contains that fix before classifying this as a server bug or
   accepting both outcomes. The relaxed assertion does not establish correctness
   of this boundary behavior.

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

Impact: with the session-scoped `ts_env`, 9++ caused 9+++ to fail and 9++++ to hang. Both tests now use the function-scoped [`isolated_ts_env`](v1/test_workflow.py#L60).
