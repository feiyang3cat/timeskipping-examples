import time
import pytest_asyncio
import logging
from uuid import uuid4

from temporalio.worker import Worker
from temporalio.testing import WorkflowEnvironment
from datetime import timedelta, datetime

from basic.workflow import (
    WorkflowWithUserTimer,
    ParentChildWorkflowWithUserTimer,
    dummy_activity,
    RetryingActivityWorkflow,
    flaky_activity,
    attempt_scheduled_times,
)

logger = logging.getLogger(__name__)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def ts_env():
    logger.info("Booting TIME-SKIPPING test server...")
    async with await WorkflowEnvironment.start_time_skipping() as env:
        yield env

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
        delta = (completed_time - time_start)
        assert delta >= timedelta(hours=1)
        assert delta < timedelta(hours=2)
        logger.info(f"Workflow completed in {delta.total_seconds()/3600:.2f} hours")

async def test_time_skipping_in_parent_child_workflow_with_user_timer(ts_env: WorkflowEnvironment):
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
        logger.info(f"Workflow completed in {delta.total_seconds()/3600:.2f} hours")

async def test_time_skipping_skips_activity_retry_backoffs(ts_env: WorkflowEnvironment):
    # Activity fails attempts 1-4, succeeds on 5. Retry policy backs off
    # 10s -> 20s -> 40s -> 80s (~150s of virtual time total). Time-skipping should
    # fast-forward those backoffs so the workflow finishes in near-zero wall-clock.
    attempt_scheduled_times.clear()
    task_queue = f"tq-retry-backoff-{uuid4()}"
    client = ts_env.client
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[RetryingActivityWorkflow],
        activities=[flaky_activity],
    )
    async with worker:
        wall_t0 = time.monotonic()
        result = await client.execute_workflow(
            RetryingActivityWorkflow.run,
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
        )
        wall_elapsed = time.monotonic() - wall_t0

    assert result == "succeeded on attempt 5"
    # All 5 attempts ran.
    assert len(attempt_scheduled_times) == 5

    # Each retry backoff (10s -> 20s -> 40s -> 80s) shows up as an exponentially
    # growing gap between consecutive attempts' (virtual) scheduled times. Note:
    # this test server reports `current_attempt_scheduled_time` lagged by one
    # attempt, so the gaps come out ~[0, 10, 20, 40] rather than [10, 20, 40, 80];
    # we assert the exponential *shape* (each meaningful gap ~2x the previous)
    # rather than exact values.
    gaps = [
        (attempt_scheduled_times[i + 1] - attempt_scheduled_times[i]).total_seconds()
        for i in range(4)
    ]
    backoffs = [g for g in gaps if g > 1.0]
    assert len(backoffs) >= 3, f"expected several growing backoff gaps, got {gaps}"
    for prev, nxt in zip(backoffs, backoffs[1:]):
        assert abs(nxt - 2 * prev) <= max(1.0, prev * 0.2), f"gaps not doubling: {backoffs}"

    # Tens of seconds of VIRTUAL backoff time elapsed across the retries...
    virtual_span = (
        attempt_scheduled_times[-1] - attempt_scheduled_times[0]
    ).total_seconds()
    assert virtual_span >= 60, f"virtual span only {virtual_span}s"
    # ...yet almost no WALL-CLOCK time passed -> the test server skipped the backoffs.
    assert wall_elapsed < 10, f"wall-clock {wall_elapsed:.1f}s suggests backoff was NOT skipped"
    logger.info(
        "retry backoff gaps (virtual)=%s s, virtual span=%.0f s, wall-clock=%.2f s",
        gaps, virtual_span, wall_elapsed,
    )
