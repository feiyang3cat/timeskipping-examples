"""V1 scenarios rewritten for per-workflow V2 clocks, through both APIs."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from activities import dummy_activity
from temporalio import activity
from temporalio.client import WorkflowFailureError
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.exceptions import TimeoutError as TemporalTimeoutError
from temporalio.testing import TimeSkipper, WorkflowEnvironment
from temporalio.worker import Worker
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


# --- Scenario 1: User Timers ---


async def test_time_skipping_in_workflow_with_user_timer(
    ts_env: WorkflowEnvironment | TimeSkipper,
):
    task_queue = f"tq-wf-with-user-timer-{uuid4()}"
    client = ts_env.client
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[WorkflowWithUserTimer],
        activities=[dummy_activity],
    )
    async with worker:
        time_start = datetime.now(timezone.utc)
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
    ts_env: WorkflowEnvironment | TimeSkipper,
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
        time_start = datetime.now(timezone.utc)
        handle = await client.start_workflow(
            ParentChildWorkflowWithUserTimer.run,
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
        )
        await handle.result()
        # Each child advances its own clock. The parent does not inherit the
        # maximum child offset when it resumes, so inspect the child results.
        child_durations = []
        async for event in handle.fetch_history_events():
            if event.HasField("child_workflow_execution_started_event_attributes"):
                execution = event.child_workflow_execution_started_event_attributes.workflow_execution
                child = client.get_workflow_handle(
                    execution.workflow_id, run_id=execution.run_id
                )
                completed_time = datetime.fromisoformat(await child.result())
                child_durations.append(completed_time - time_start)
        assert len(child_durations) == 2
        assert min(child_durations) >= timedelta(hours=1)
        assert max(child_durations) >= timedelta(hours=2)


# --- Scenario 2: Workflow Retry ---


async def test_workflow_retry_backoff(ts_env: WorkflowEnvironment | TimeSkipper):
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
        time_start = datetime.now(timezone.utc)
        handle = await ts_env.client.start_workflow(
            ThisWorkflowRunsWithCronOrRetry.run,
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
            retry_policy=RetryPolicy(
                initial_interval=timedelta(hours=1),
                backoff_coefficient=1.0,
                maximum_attempts=3,
            ),
        )
        await handle.result()
        time_end = await ts_env.get_current_time(handle)

    assert len(calls) == 2
    assert time_end - time_start >= timedelta(hours=1)


# --- Scenario 3: Workflow Execution/Run Timeout ---


async def test_workflow_times_out_after_run_timeout(
    ts_env: WorkflowEnvironment | TimeSkipper,
):
    task_queue = f"tq-run-timeout-{uuid4()}"
    async with Worker(
        ts_env.client, task_queue=task_queue, workflows=[WaitForSignalWorkflow]
    ):
        handle = await ts_env.client.start_workflow(
            WaitForSignalWorkflow.run,
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
            run_timeout=timedelta(hours=1),
        )
        with pytest.raises(WorkflowFailureError) as exc_info:
            await handle.result()
    assert isinstance(exc_info.value.cause, TemporalTimeoutError)


async def test_workflow_times_out_after_execution_timeout(
    ts_env: WorkflowEnvironment | TimeSkipper,
):
    task_queue = f"tq-execution-timeout-{uuid4()}"
    async with Worker(
        ts_env.client, task_queue=task_queue, workflows=[WaitForSignalWorkflow]
    ):
        handle = await ts_env.client.start_workflow(
            WaitForSignalWorkflow.run,
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
            execution_timeout=timedelta(hours=1),
        )
        with pytest.raises(WorkflowFailureError) as exc_info:
            await handle.result()
    assert isinstance(exc_info.value.cause, TemporalTimeoutError)


# --- Scenario 4: Workflow Start Delay ---


async def test_workflow_start_delay(ts_env: WorkflowEnvironment | TimeSkipper):
    task_queue = f"tq-start-delay-{uuid4()}"
    async with Worker(
        ts_env.client, task_queue=task_queue, workflows=[WorkflowWithStartDelay]
    ):
        time_start = datetime.now(timezone.utc)
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


async def test_time_skipping_activity_retry_backoff(
    ts_env: WorkflowEnvironment | TimeSkipper,
):
    task_queue = f"tq-retry-{uuid4()}"
    client = ts_env.client
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[WorkflowWithActivityRetries],
        activities=[retry_activity_mocked],
    )
    async with worker:
        time_start = datetime.now(timezone.utc)
        handle = await client.start_workflow(
            WorkflowWithActivityRetries.run,
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
        )
        completed_time = datetime.fromisoformat(await handle.result())
        delta = completed_time - time_start
        # attempt 1 fails → 1h backoff, attempt 2 fails → 2h backoff, attempt 3 succeeds
        assert delta >= timedelta(hours=3)
        logger.info(
            f"Activity retries completed in {delta.total_seconds() / 3600:.2f} hours"
        )


# --- Scenario 6: Cron ---


async def test_cron_runs_on_schedule(ts_env):
    calls = []

    @activity.defn(name="dummy_activity")
    async def counting_dummy(name: str, activity_duration=timedelta()) -> str:
        calls.append(name)
        return f"done {name}"

    task_queue = f"tq-cron-{uuid4()}"
    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[ThisWorkflowRunsWithCronOrRetry],
        activities=[counting_dummy],
    ):
        with ts_env.with_time_skipping_disabled():
            handle = await ts_env.client.start_workflow(
                ThisWorkflowRunsWithCronOrRetry.run,
                id=f"cron-{uuid4()}",
                task_queue=task_queue,
                cron_schedule="0 9 * * *",
            )
        start = await ts_env.get_current_time(handle)
        try:
            assert await ts_env.fast_forward(handle, timedelta(days=3))
            assert await ts_env.get_current_time(handle) - start >= timedelta(days=3)
            assert len(calls) >= 3
        finally:
            await handle.terminate()


# --- Scenario 9: Interactions at controlled virtual times ---


async def test_workflow_waiting_for_signals(ts_env):
    task_queue = f"tq-signals-{uuid4()}"
    async with Worker(
        ts_env.client, task_queue=task_queue, workflows=[WorkflowWaitingForSignals]
    ):
        with ts_env.with_time_skipping_disabled():
            handle = await ts_env.client.start_workflow(
                WorkflowWaitingForSignals.run,
                id=f"wf-{uuid4()}",
                task_queue=task_queue,
            )
        start = await ts_env.get_current_time(handle)
        assert await ts_env.fast_forward(handle, timedelta(minutes=30))
        await handle.signal(WorkflowWaitingForSignals.prepare_done)
        assert await ts_env.get_current_time(handle) - start >= timedelta(minutes=30)
        assert await ts_env.fast_forward(handle, timedelta(minutes=30))
        await handle.signal(WorkflowWaitingForSignals.go)
        assert await handle.result() is True


async def test_parent_with_child_and_wait_condition(ts_env):
    task_queue = f"tq-parent-{uuid4()}"
    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[ParentWithChildAndWaitCondition, ChildA, ChildB],
    ):
        with ts_env.with_time_skipping_disabled():
            handle = await ts_env.client.start_workflow(
                ParentWithChildAndWaitCondition.run,
                id=f"wf-{uuid4()}",
                task_queue=task_queue,
            )
        start = await ts_env.get_current_time(handle)
        # Children each skip their own 1h/2h. Only the parent's 1h cooldown
        # and half of its 1h approval window count on the parent's clock.
        assert await ts_env.fast_forward(handle, timedelta(hours=1, minutes=30))
        assert await ts_env.get_current_time(handle) - start >= timedelta(
            hours=1, minutes=30
        )
        await handle.signal(ParentWithChildAndWaitCondition.approve)
        assert await handle.result() is True


async def test_parent_child_both_wait_on_condition(ts_env):
    task_queue = f"tq-parent-child-{uuid4()}"
    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[ParentChildBothWaitOnCondition, ChildWithCondition],
    ):
        with ts_env.with_time_skipping_disabled():
            handle = await ts_env.client.start_workflow(
                ParentChildBothWaitOnCondition.run,
                id=f"wf-{uuid4()}",
                task_queue=task_queue,
            )
        # Child clocks are independent. Reach its creation, then drive the child
        # separately to its signal window before advancing the parent again.
        assert await ts_env.fast_forward(handle, timedelta(hours=1))
        async for event in handle.fetch_history_events(wait_new_event=True):
            if event.HasField("child_workflow_execution_started_event_attributes"):
                break
        child = ts_env.client.get_workflow_handle(f"{handle.id}-child")
        assert await ts_env.fast_forward(child, timedelta(minutes=30))
        await child.signal(ChildWithCondition.proceed)
        assert await child.result() is True
        assert await ts_env.fast_forward(handle, timedelta(hours=1))
        await handle.signal(ParentChildBothWaitOnCondition.proceed)
        assert await handle.result() == (True, True)


async def test_fast_forward_from_test_waits_for_pending_workflow_task(ts_env):
    """V1's extra unlock could break this lock; V2 waits for a worker."""
    task_queue = f"tq-no-worker-{uuid4()}"
    with ts_env.with_time_skipping_disabled():
        handle = await ts_env.client.start_workflow(
            WaitForSignalWorkflow.run,
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
            execution_timeout=timedelta(hours=1),
        )
    forward = asyncio.create_task(ts_env.fast_forward(handle, timedelta(minutes=30)))
    try:
        await asyncio.sleep(0.3)
        assert not forward.done()
        async with Worker(
            ts_env.client, task_queue=task_queue, workflows=[WaitForSignalWorkflow]
        ):
            assert await forward
            assert not await ts_env.fast_forward(handle, timedelta(minutes=31))
            with pytest.raises(WorkflowFailureError) as exc_info:
                await handle.result()
            assert isinstance(exc_info.value.cause, TemporalTimeoutError)
    finally:
        forward.cancel()
        await asyncio.gather(forward, return_exceptions=True)
