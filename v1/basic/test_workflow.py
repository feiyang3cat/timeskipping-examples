"""
Examples of the Time Skipping in 5 major scenarios
1. user timers (single wf, with child wf)
2. activity retry backoff
3. worfklow retry
4. cron
5. pause in the middle of time skipping to send signals/updates
"""

import pytest_asyncio
import logging
from uuid import uuid4

from temporalio.worker import Worker
from temporalio.testing import WorkflowEnvironment
from datetime import timedelta, datetime
from temporalio import activity

from activities import dummy_activity
from workflow import (
    WorkflowWithUserTimer,
    ParentChildWorkflowWithUserTimer,
    WorkflowWithActivityRetries,
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
