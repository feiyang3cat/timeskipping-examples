"""
Time-skipping examples (v1) across supported scenarios:
1. User timers (single workflow, parent-child)
2. Workflow retry backoff
3. Workflow execution/run timeout
5. Activity retry backoff
6. Cron waiting time
9. env.sleep() to pause time-skipping mid-execution
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from temporalio import activity
from temporalio.client import WorkflowFailureError
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.exceptions import TimeoutError as TemporalTimeoutError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from activities import dummy_activity
from workflow import (
    ChildA,
    ChildB,
    ChildWithCondition,
    ParentChildBothWaitOnCondition,
    ParentChildWorkflowWithUserTimer,
    ParentWithChildAndWaitCondition,
    ThisWorkflowRunsWithCronOrRetry,
    WaitForSignalWorkflow,
    WorkflowRacingActivityAndTimer,
    WorkflowRunningLongActivity,
    WorkflowWithTwoTimersAndActivity,
    WorkflowWaitingForSignals,
    WorkflowWithActivityRetries,
    WorkflowWithStartDelay,
    WorkflowWithUserTimer,
)

logger = logging.getLogger(__name__)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def ts_env():
    logger.info("Booting TIME-SKIPPING test server...")
    async with await WorkflowEnvironment.start_time_skipping() as env:
        yield env


# For tests that leave the server's Time Locking Counter locked once more than
# it started (a cancelled in-flight activity or a never-completed workflow task
# keeps its lock), which would stall every later test on the shared ts_env.
@pytest_asyncio.fixture
async def isolated_ts_env():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        yield env


# --- Scenario 1: User Timers ---

async def test_time_skipping_in_workflow_with_user_timer(ts_env: WorkflowEnvironment):
    task_queue = f"tq-wf-with-user-timer-{uuid4()}"
    client = ts_env.client
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[WorkflowWithUserTimer],
        activities=[dummy_activity],
    )
    async with worker:
        time_start = await ts_env.get_current_time()
        handle = await client.start_workflow(
            WorkflowWithUserTimer.run,
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
        )
        completed_time = datetime.fromisoformat(await handle.result())
        delta = completed_time - time_start
        assert delta >= timedelta(hours=1)
        logger.info(f"Workflow completed in {delta.total_seconds() / 3600:.2f} hours")


async def test_time_skipping_in_parent_child_workflow_with_user_timer(
    ts_env: WorkflowEnvironment,
):
    task_queue = f"tq-parent-child-wf-with-user-timer-{uuid4()}"
    client = ts_env.client
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[ParentChildWorkflowWithUserTimer, WorkflowWithUserTimer],
        activities=[dummy_activity],
    )
    async with worker:
        time_start = await ts_env.get_current_time()
        handle = await client.start_workflow(
            ParentChildWorkflowWithUserTimer.run,
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
        )
        completed_time = datetime.fromisoformat(await handle.result())
        delta = completed_time - time_start
        assert delta >= timedelta(hours=2)
        logger.info(f"Workflow completed in {delta.total_seconds() / 3600:.2f} hours")


# --- Scenario 2: Workflow Retry ---

async def test_workflow_retry_backoff(ts_env: WorkflowEnvironment):
    task_queue = f"tq-wf-retry-{uuid4()}"
    calls = []

    @activity.defn(name="dummy_activity")
    async def dummy_activity_fail_once(name: str, activity_duration=timedelta()) -> str:
        calls.append(1)
        if len(calls) == 1:
            raise ApplicationError("simulated failure", non_retryable=True)
        return f"done {name}"

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[ThisWorkflowRunsWithCronOrRetry],
        activities=[dummy_activity_fail_once],
    ):
        time_start = await ts_env.get_current_time()
        await ts_env.client.execute_workflow(
            ThisWorkflowRunsWithCronOrRetry.run,
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
            retry_policy=RetryPolicy(
                initial_interval=timedelta(hours=1),
                backoff_coefficient=1.0,
                maximum_attempts=3,
            ),
        )
        time_end = await ts_env.get_current_time()

    assert len(calls) == 2
    assert time_end - time_start >= timedelta(hours=1)


# --- Scenario 3: Workflow Execution/Run Timeout ---
#
# No env.sleep() is needed here. The workflow waits on a signal forever, so the
# only future event is the timeout timer. When the client blocks on
# handle.result() (a long-poll for the next history event) and nothing else can
# make progress, the time-skipping server auto-skips its clock straight to that
# timer and fires the timeout. result() returns in milliseconds of real time.
#
# (Note: handle.describe() would NOT trigger this — it's a one-shot status read,
# not a "wait for progress" request, so it gives the server no reason to skip.)

async def test_workflow_times_out_after_run_timeout(ts_env: WorkflowEnvironment):
    task_queue = f"tq-run-timeout-{uuid4()}"
    async with Worker(ts_env.client, task_queue=task_queue, workflows=[WaitForSignalWorkflow]):
        handle = await ts_env.client.start_workflow(
            WaitForSignalWorkflow.run,
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
            run_timeout=timedelta(hours=1),
        )
        # Time-skipping auto-advances to the run_timeout — no env.sleep() required.
        with pytest.raises(WorkflowFailureError) as exc_info:
            await handle.result()
    assert isinstance(exc_info.value.cause, TemporalTimeoutError)


async def test_workflow_times_out_after_execution_timeout(ts_env: WorkflowEnvironment):
    task_queue = f"tq-execution-timeout-{uuid4()}"
    async with Worker(ts_env.client, task_queue=task_queue, workflows=[WaitForSignalWorkflow]):
        handle = await ts_env.client.start_workflow(
            WaitForSignalWorkflow.run,
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
            execution_timeout=timedelta(hours=1),
        )
        # Time-skipping auto-advances to the execution_timeout — no env.sleep() required.
        with pytest.raises(WorkflowFailureError) as exc_info:
            await handle.result()
    assert isinstance(exc_info.value.cause, TemporalTimeoutError)


# --- Scenario 4: Workflow Start Delay ---

async def test_workflow_start_delay(ts_env: WorkflowEnvironment):
    task_queue = f"tq-start-delay-{uuid4()}"
    async with Worker(ts_env.client, task_queue=task_queue, workflows=[WorkflowWithStartDelay]):
        time_start = await ts_env.get_current_time()
        result = await ts_env.client.execute_workflow(
            WorkflowWithStartDelay.run,
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
            start_delay=timedelta(hours=2),
        )
    completed_time = datetime.fromisoformat(result)
    delta = completed_time - time_start
    assert delta >= timedelta(hours=2)


# --- Scenario 5: Activity Retry ---

@activity.defn(name="retry_activity")
async def retry_activity_mocked() -> int:
    attempt = activity.info().attempt
    if attempt <= 2:
        raise ValueError("mock activity error for retries")
    else:
        return activity.info().attempt

async def test_time_skipping_activity_retry_backoff(ts_env: WorkflowEnvironment):
    task_queue = f"tq-retry-{uuid4()}"
    client = ts_env.client
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[WorkflowWithActivityRetries],
        activities=[retry_activity_mocked],
    )
    async with worker:
        time_start = await ts_env.get_current_time()
        handle = await client.start_workflow(
            WorkflowWithActivityRetries.run,
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
        )
        completed_time = datetime.fromisoformat(await handle.result())
        delta = completed_time - time_start
        # attempt 1 fails → 1h backoff, attempt 2 fails → 2h backoff, attempt 3 succeeds
        assert delta >= timedelta(hours=3)
        logger.info(f"Activity retries completed in {delta.total_seconds() / 3600:.2f} hours")


# --- Scenario 6: Cron ---

async def test_cron_runs_on_schedule(ts_env: WorkflowEnvironment):
    workflow_id = f"cron-wf-{uuid4()}"
    task_queue = f"tq-cron-{uuid4()}"

    # The SDK testing server does not implement ListWorkflowExecutions, so we
    # track cron runs by counting activity invocations via a closure.
    call_count = []

    @activity.defn(name="dummy_activity")
    async def counting_dummy(name: str, activity_duration=timedelta()) -> str:
        call_count.append(1)
        return f"done {name}"

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[ThisWorkflowRunsWithCronOrRetry],
        activities=[counting_dummy],
    ):
        time_start = await ts_env.get_current_time()
        handle = await ts_env.client.start_workflow(
            ThisWorkflowRunsWithCronOrRetry.run,
            id=workflow_id,
            task_queue=task_queue,
            cron_schedule="0 9 * * *",  # every day at 9:00 AM
        )
        await ts_env.sleep(timedelta(days=3).total_seconds())
        await handle.terminate()
        time_end = await ts_env.get_current_time()

    assert len(call_count) >= 3
    assert time_end - time_start >= timedelta(days=3)
    logger.info("Cron fired %d times over %s of virtual time", len(call_count), time_end - time_start)


# --- Scenario 7: Scheduler --- (not supported in v1)

# --- Scenario 8: SAA --- (not supported in v1)

# --- Scenario 9: TimeSkipping Sleep ---
async def test_workflow_waiting_for_signals(ts_env: WorkflowEnvironment):
    # Simulates a real scenario where signals are sent at different time.
    workflow_id = f"signal-wf-{uuid4()}"
    task_queue = f"tq-signal-{uuid4()}"
    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[WorkflowWaitingForSignals],
    ):
        time_start = await ts_env.get_current_time()
        handle = await ts_env.client.start_workflow(
            WorkflowWaitingForSignals.run,
            id=workflow_id,
            task_queue=task_queue,
        )
        await ts_env.sleep(timedelta(minutes=30).total_seconds())
        await handle.signal(WorkflowWaitingForSignals.prepare_done)
        time_after_prepare = await ts_env.get_current_time()
        assert time_after_prepare - time_start >= timedelta(minutes=30)

        await ts_env.sleep(timedelta(minutes=30).total_seconds())
        await handle.signal(WorkflowWaitingForSignals.go)
        result = await handle.result()

    assert result is True


# --- Scenario 10: Complicated Sleep Scenarios ---
async def test_parent_with_child_and_wait_condition(ts_env: WorkflowEnvironment):
    # Simulates a review/approval flow: two parallel jobs must finish before a
    # human-approval window opens. ChildA takes 1h, ChildB takes 2h; the parent
    # then waits 1h more (cooldown) and opens a 1h approval window (+3h to +4h).
    # env.sleep() pauses time-skipping so we can inject the signal at +3.5h,
    # landing inside the window.
    workflow_id = f"parent-two-children-{uuid4()}"
    task_queue = f"tq-parent-two-children-{uuid4()}"
    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[ParentWithChildAndWaitCondition, ChildA, ChildB],
    ):
        time_start = await ts_env.get_current_time()
        handle = await ts_env.client.start_workflow(
            ParentWithChildAndWaitCondition.run,
            id=workflow_id,
            task_queue=task_queue,
        )
        # Advance past children (2h) + cooldown (1h); land at +3.5h inside the approval window
        await ts_env.sleep(timedelta(hours=3, minutes=30).total_seconds())
        time_in_window = await ts_env.get_current_time()
        assert time_in_window - time_start >= timedelta(hours=3)

        await handle.signal(ParentWithChildAndWaitCondition.approve)
        result = await handle.result()

    assert result is True



async def test_parent_child_both_wait_on_condition(ts_env: WorkflowEnvironment):
    # Both parent and child have their own signal windows.
    # The child workflow ID is derived from the parent ID — in production this
    # mirrors looking up the child workflow ID from a DB record and building a
    # handle from it, without needing to query Temporal's workflow list.
    #
    # Timeline (virtual):
    #   t=0h    parent starts, sleeps 1h
    #   t=1h    parent starts child; child sleeps 30min
    #   t=1h30m child opens its 5-min signal window  ← env.sleep(1.5h) → signal child
    #   t=1h30m child completes, parent resumes, sleeps 1h
    #   t=2h30m parent opens its 5-min signal window ← env.sleep(1h)  → signal parent
    workflow_id = f"parent-child-condition-{uuid4()}"
    child_id = f"{workflow_id}-child"
    task_queue = f"tq-parent-child-condition-{uuid4()}"
    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[ParentChildBothWaitOnCondition, ChildWithCondition],
    ):
        handle = await ts_env.client.start_workflow(
            ParentChildBothWaitOnCondition.run,
            id=workflow_id,
            task_queue=task_queue,
        )
        # Build child handle from its known ID (no Temporal list query needed)
        child_handle = ts_env.client.get_workflow_handle(child_id)

        # Advance to t=1.5h: parent has slept, child has slept, child's window is open
        await ts_env.sleep(timedelta(hours=1, minutes=30).total_seconds())
        await child_handle.signal(ChildWithCondition.proceed)

        # Advance to t=2.5h: child done, parent slept 1h more, parent's window is open
        await ts_env.sleep(timedelta(hours=1).total_seconds())
        await handle.signal(ParentChildBothWaitOnCondition.proceed)

        child_signaled, parent_signaled = await handle.result()

    assert child_signaled is True
    assert parent_signaled is True


# --- Scenario 9++: env.sleep() inside an activity fires a racing user timer ---
#
# The workflow races an activity against a 10-min user timer that is well under
# the activity's 30-min start_to_close_timeout. While an activity is in flight
# the server keeps its clock at real wall-clock speed, so normally the timer
# would not fire for 10 *real* minutes. Instead the mocked activity calls
# env.sleep() from inside itself — a manual time skip that advances the virtual
# clock past the 10-min timer even though the activity is still running. The
# activity also does a real asyncio.sleep so it stays in flight until the
# skipped-forward user timer fires and cancels it. As a result the user timer
# wins the race, and the whole test finishes in milliseconds of real time.
async def test_env_sleep_in_activity_fires_user_timer(
    isolated_ts_env: WorkflowEnvironment,
):
    ts_env = isolated_ts_env
    task_queue = f"tq-race-activity-timer-{uuid4()}"

    @activity.defn(name="dummy_activity")
    async def slow_dummy(name: str, activity_duration=timedelta()) -> str:
        await asyncio.gather(
            asyncio.sleep(2),  # real in-flight work
            # manual time skip: advance virtual time past the 10-min user timer
            # while this activity is still running
            ts_env.sleep(timedelta(minutes=11).total_seconds()),
        )
        return f"done {name}"

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[WorkflowRacingActivityAndTimer],
        activities=[slow_dummy],
    ):
        winner = await ts_env.client.execute_workflow(
            WorkflowRacingActivityAndTimer.run,
            args=[
                timedelta(minutes=10).total_seconds(),  # user timer
                timedelta(minutes=30).total_seconds(),  # activity start_to_close_timeout
            ],
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
        )

    # env.sleep() skipped the virtual clock past the 10-min timer while the
    # activity was still sleeping, so the user timer fires first.
    assert winner == "timer"


# --- Scenario 9+++: env.sleep() does NOT skip while an activity is in flight ---
#
# Question: with a 10s activity running, does env.sleep(3s) return instantly or
# after 3s? Answer: after ~3 REAL seconds, NOT instantly.
#
# An in-flight activity holds a time-skipping lock, so the server cannot advance
# the virtual clock while it runs. An env.sleep() overlapping the activity
# therefore elapses in real wall-clock time; only the portion after the activity
# finishes (and releases the lock) is skipped instantly. Here the whole 3s falls
# inside the 10s in-flight window, so nothing gets skipped.
async def test_env_sleep_blocks_in_real_time_while_activity_in_flight(
    ts_env: WorkflowEnvironment,
):
    task_queue = f"tq-sleep-vs-activity-{uuid4()}"

    @activity.defn(name="dummy_activity")
    async def slow_dummy(name: str, activity_duration=timedelta()) -> str:
        await asyncio.sleep(10)  # 10 real seconds in flight
        return f"done {name}"

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[WorkflowRunningLongActivity],
        activities=[slow_dummy],
    ):
        # (A) Nothing in flight: env.sleep(3s) skips instantly (virtual clock jumps).
        wall = time.monotonic()
        await ts_env.sleep(timedelta(seconds=3).total_seconds())
        idle_elapsed = time.monotonic() - wall
        assert idle_elapsed < 1.0

        # (B) Activity in flight: env.sleep(3s) cannot skip, so it blocks ~3 real seconds.
        handle = await ts_env.client.start_workflow(
            WorkflowRunningLongActivity.run,
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
        )
        await asyncio.sleep(0.5)  # let the activity start and take the time-skipping lock
        wall = time.monotonic()
        await ts_env.sleep(timedelta(seconds=3).total_seconds())
        busy_elapsed = time.monotonic() - wall
        assert busy_elapsed >= 2.5

        await handle.result()

    logger.info(
        "env.sleep(3s): idle=%.2fs (skipped) vs activity-in-flight=%.2fs (real time)",
        idle_elapsed,
        busy_elapsed,
    )


# --- Scenario 9++++: two env.sleep() calls in one activity fire two timers ---
#
# The workflow waits on two user timers (1h, 2h) and one activity. While the
# activity is in flight the server keeps its clock at real speed, so neither
# timer would fire for real hours. The mocked activity calls env.sleep() TWICE,
# and the skips ACCUMULATE: the first (+1h) fires the 1h timer, the second
# (+1h → 2h total) fires the 2h timer, then the activity returns. All three
# complete in ~2 real seconds, in order: timer1 @1h, timer2 @2h, activity @2h.
async def test_two_env_sleeps_in_activity_fire_both_timers(
    ts_env: WorkflowEnvironment,
):
    task_queue = f"tq-two-timers-activity-{uuid4()}"

    @activity.defn(name="dummy_activity")
    async def slow_dummy(name: str, activity_duration=timedelta()) -> str:
        async def skip_twice():
            # each env.sleep() manually advances the virtual clock by 1h; the two
            # skips accumulate to 2h while this activity is still in flight
            await ts_env.sleep(timedelta(hours=1).total_seconds())
            await ts_env.sleep(timedelta(hours=1).total_seconds())

        await asyncio.gather(
            asyncio.sleep(2),  # real in-flight work so the activity outlives both timers
            skip_twice(),
        )
        return f"done {name}"

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[WorkflowWithTwoTimersAndActivity],
        activities=[slow_dummy],
    ):
        completion_order = await ts_env.client.execute_workflow(
            WorkflowWithTwoTimersAndActivity.run,
            args=[
                timedelta(hours=1).total_seconds(),  # timer1
                timedelta(hours=2).total_seconds(),  # timer2
                timedelta(hours=3).total_seconds(),  # activity start_to_close_timeout
            ],
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
        )

    # The two env.sleep() calls stacked up: the first fired the 1h timer, the
    # second fired the 2h timer, then the activity finished.
    names = [name for name, _ in completion_order]
    assert names == ["timer1", "timer2", "activity"]
    assert completion_order[0][1] == 1  # timer1 fired at +1h (after the first skip)
    assert completion_order[1][1] == 2  # timer2 fired at +2h (after the second skip)
    logger.info("completion order (name, virtual hours): %s", completion_order)


# --- Scenario 9+++++: env.sleep() from the test also unlocks — if timed right ---
#
# env.sleep() does not have to be called inside the activity. The server only
# keeps a Time Locking Counter and skips whenever it reaches 0, no matter which
# caller unlocks. The counter starts at 1, handle.result() unlocks it, and every
# in-flight activity or pending workflow task locks it. So an env.sleep() from
# the test skips as long as:
#   - timing: result() is already being awaited (otherwise the starting lock is
#     still held and env.sleep() runs in real time, as in Scenario 9+++), and
#   - duration: it reaches the next timer; a shorter skip just locks again.
async def test_env_sleep_from_test_unlocks_held_activity(ts_env: WorkflowEnvironment):
    task_queue = f"tq-outside-sleep-activity-{uuid4()}"
    started = asyncio.Event()
    release = asyncio.Event()

    @activity.defn(name="dummy_activity")
    async def held_dummy(name: str, activity_duration=timedelta()) -> str:
        started.set()
        await release.wait()  # held in flight; no env.sleep() in here
        return f"done {name}"

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[WorkflowWithTwoTimersAndActivity],
        activities=[held_dummy],
    ):
        handle = await ts_env.client.start_workflow(
            WorkflowWithTwoTimersAndActivity.run,
            args=[
                timedelta(hours=1).total_seconds(),  # timer1
                timedelta(hours=2).total_seconds(),  # timer2
                timedelta(hours=3).total_seconds(),  # activity start_to_close_timeout
            ],
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
        )
        result = asyncio.ensure_future(handle.result())  # counter 1 -> 0
        await started.wait()  # activity in flight: counter 0 -> 1

        wall = time.monotonic()
        await ts_env.sleep(timedelta(hours=1).total_seconds())  # 1 -> 0, fires timer1
        await ts_env.sleep(timedelta(hours=1).total_seconds())  # 1 -> 0, fires timer2
        skip_wall = time.monotonic() - wall
        release.set()
        completion_order = await result

    assert skip_wall < 2.0
    assert completion_order == [["timer1", 1], ["timer2", 2], ["activity", 2]]
    logger.info("completion order (name, virtual hours): %s", completion_order)


# A pending workflow task holds a lock too. No worker polls this task queue, so
# the first workflow task never completes and — unlike Scenario 3 — result()
# alone cannot skip to the execution timeout. env.sleep() from the test can.
async def test_env_sleep_from_test_unlocks_pending_workflow_task(
    isolated_ts_env: WorkflowEnvironment,
):
    ts_env = isolated_ts_env
    handle = await ts_env.client.start_workflow(
        WaitForSignalWorkflow.run,
        id=f"wf-{uuid4()}",
        task_queue=f"tq-no-worker-{uuid4()}",
        execution_timeout=timedelta(hours=1),
    )
    result = asyncio.ensure_future(handle.result())
    await asyncio.sleep(1)
    assert not result.done()  # pending workflow task keeps the clock at real speed

    await ts_env.sleep(timedelta(minutes=30).total_seconds())  # too short
    await asyncio.sleep(0.5)
    assert not result.done()

    await ts_env.sleep(timedelta(minutes=30).total_seconds())  # reaches the 1h timeout
    with pytest.raises(WorkflowFailureError) as exc_info:
        await result
    assert isinstance(exc_info.value.cause, TemporalTimeoutError)
