"""
Time-skipping examples (v1) across supported scenarios:
1. User timers (single workflow, parent-child)
2. Workflow retry backoff
3. Workflow run timeout
4. Activity retry backoff
6. Cron waiting time
7. env.sleep() to pause time-skipping mid-execution
"""

import logging
from datetime import datetime, timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from temporalio import activity
from temporalio.client import WorkflowFailureError
from temporalio.common import RetryPolicy
from temporalio.exceptions import TimeoutError as TemporalTimeoutError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from activities import dummy_activity
from workflow import (
    FailFirstAttemptWorkflow,
    ParentChildWorkflowWithUserTimer,
    ThisWorkflowRunsWithCronOrRetry,
    TwoTimerWorkflow,
    WaitForSignalWorkflow,
    WorkflowWithActivityRetries,
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
        assert delta < timedelta(hours=2)
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
        assert delta < timedelta(hours=3)
        logger.info(f"Workflow completed in {delta.total_seconds() / 3600:.2f} hours")


# --- Scenario 2: Workflow Retry ---

async def test_workflow_retry_backoff(ts_env: WorkflowEnvironment):
    task_queue = f"tq-wf-retry-{uuid4()}"
    async with Worker(ts_env.client, task_queue=task_queue, workflows=[FailFirstAttemptWorkflow]):
        result = await ts_env.client.execute_workflow(
            FailFirstAttemptWorkflow.run,
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
            retry_policy=RetryPolicy(
                initial_interval=timedelta(hours=1),
                backoff_coefficient=1.0,
                maximum_attempts=3,
            ),
        )
    assert result == "succeeded on attempt 2"


# --- Scenario 3: Workflow Run Timeout ---

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


# --- Scenario 4: Activity Retry ---

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
        attempted = await handle.result()
        time_end = await ts_env.get_current_time()
        assert attempted > 1
        delta = time_end - time_start
        assert delta > timedelta(hours=1)


# --- Scenario 6: Cron ---

async def test_cron_runs_on_schedule(ts_env: WorkflowEnvironment):
    workflow_id = f"cron-wf-{uuid4()}"
    task_queue = f"tq-cron-{uuid4()}"
    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[ThisWorkflowRunsWithCronOrRetry],
        activities=[dummy_activity],
    ):
        handle = await ts_env.client.start_workflow(
            ThisWorkflowRunsWithCronOrRetry.run,
            id=workflow_id,
            task_queue=task_queue,
            cron_schedule="* * * * *",
        )
        await ts_env.sleep(3 * 60)
        await handle.terminate()

    runs = [
        w async for w in ts_env.client.list_workflows(
            f'WorkflowId = "{workflow_id}" AND ExecutionStatus = "Completed"'
        )
    ]
    assert len(runs) >= 2
    start_times = sorted(r.start_time for r in runs)
    logger.info("Cron completed %d runs: %s", len(runs), [t.isoformat() for t in start_times])


# --- Scenario 7: env.sleep() ---

async def test_partial_clock_advance_fires_only_short_timer(ts_env: WorkflowEnvironment):
    task_queue = f"tq-two-timer-{uuid4()}"
    async with Worker(ts_env.client, task_queue=task_queue, workflows=[TwoTimerWorkflow]):
        handle = await ts_env.client.start_workflow(
            TwoTimerWorkflow.run,
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
        )
        await ts_env.sleep(3 * 60 * 60)

        short_fired = await handle.query(TwoTimerWorkflow.short_fired)
        long_fired = await handle.query(TwoTimerWorkflow.long_fired)

        assert short_fired is True
        assert long_fired is False
        logger.info("After 3h advance: short_fired=%s, long_fired=%s", short_fired, long_fired)

        await handle.cancel()
