"""
Time-skipping examples (v1) across supported scenarios:
1. User timers (single workflow, parent-child)
2. Workflow retry backoff
3. Workflow execution/run timeout
5. Activity retry backoff
6. Cron waiting time
9. env.sleep() to pause time-skipping mid-execution
"""

import logging
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

async def test_workflow_times_out_after_run_timeout(ts_env: WorkflowEnvironment):
    task_queue = f"tq-run-timeout-{uuid4()}"
    async with Worker(ts_env.client, task_queue=task_queue, workflows=[WaitForSignalWorkflow]):
        handle = await ts_env.client.start_workflow(
            WaitForSignalWorkflow.run,
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
            run_timeout=timedelta(hours=1),
        )
        await ts_env.sleep(2 * 60 * 60)
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
        await ts_env.sleep(2 * 60 * 60)
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


