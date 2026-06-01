import logging
import time
import uuid
from datetime import datetime, timedelta

import pytest
from temporalio.client import WorkflowFailureError
from temporalio.common import RetryPolicy
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from activities import slow_activity
from workflow_single import (
    AlwaysFailWorkflow,
    BusyWorkflow,
    SomeChildWorkflow,
    WaitingWorkflowOnBothConditionAndTimer,
    WaitingWorkflowOnCondition,
    WaitingWorkflowOnUserTimer,
)

logger = logging.getLogger(__name__)


async def test_busy_workflow_runs_in_wall_clock_time(
    ts_env: WorkflowEnvironment,
) -> None:
    task_queue = f"tq-{uuid.uuid4()}"

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[BusyWorkflow],
        activities=[slow_activity],
    ):
        wall_t0 = time.time()
        result = await ts_env.client.execute_workflow(
            BusyWorkflow.run,
            0.3,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        wall_ms = (time.time() - wall_t0) * 1000

    # Result is the final virtual time; presence of an isoformat string is enough.
    datetime.fromisoformat(result)
    # 3 × 300ms activities are real wall-clock work and cannot be skipped.
    assert wall_ms >= 900
    logger.info("busy: wall=%.2f ms", wall_ms)


async def test_long_timer_workflow_completes_quickly_under_time_skipping(
    ts_env: WorkflowEnvironment,
) -> None:
    task_queue = f"tq-{uuid.uuid4()}"

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[WaitingWorkflowOnUserTimer],
    ):
        wall_t0 = time.time()
        result = await ts_env.client.execute_workflow(
            WaitingWorkflowOnUserTimer.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        wall_ms = (time.time() - wall_t0) * 1000

    assert result == "done"
    # 10-day timer should fast-forward to near-zero wall time.
    assert wall_ms < 5000
    logger.info("user-timer: wall=%.2f ms", wall_ms)


async def test_condition_workflow_completes_when_signal_received(
    ts_env: WorkflowEnvironment,
) -> None:
    task_queue = f"tq-{uuid.uuid4()}"
    wf_id = f"wf-{uuid.uuid4()}"

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[WaitingWorkflowOnCondition],
    ):
        handle = await ts_env.client.start_workflow(
            WaitingWorkflowOnCondition.run,
            id=wf_id,
            task_queue=task_queue,
        )
        await handle.signal(WaitingWorkflowOnCondition.go)
        result = await handle.result()

    datetime.fromisoformat(result)


async def test_both_workflow_returns_condition_won_when_signaled_in_race(
    ts_env: WorkflowEnvironment,
) -> None:
    task_queue = f"tq-{uuid.uuid4()}"
    wf_id = f"wf-{uuid.uuid4()}"

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[WaitingWorkflowOnBothConditionAndTimer],
    ):
        handle = await ts_env.client.start_workflow(
            WaitingWorkflowOnBothConditionAndTimer.run,
            id=wf_id,
            task_queue=task_queue,
        )
        # Signals are queued; sending early is fine — the workflow will observe
        # _signal_count >= 1 once it enters wait_condition after the 1h sleep.
        await handle.signal(WaitingWorkflowOnBothConditionAndTimer.go)
        result = await handle.result()

    assert result == "condition won"


async def test_both_workflow_returns_timer_won_when_no_signal_arrives(
    ts_env: WorkflowEnvironment,
) -> None:
    task_queue = f"tq-{uuid.uuid4()}"

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[WaitingWorkflowOnBothConditionAndTimer],
    ):
        server_t0 = await ts_env.get_current_time()
        result = await ts_env.client.execute_workflow(
            WaitingWorkflowOnBothConditionAndTimer.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        server_end = await ts_env.get_current_time()

    assert result == "timer won"
    # 1h initial sleep + 1h wait_condition timeout = ~2h of virtual time.
    gap = server_end - server_t0
    assert timedelta(hours=1, minutes=55) <= gap <= timedelta(hours=2, minutes=5)
    logger.info("both/timer-won: virtual gap=%s", gap)


async def test_workflow_with_retry_policy_set_at_start_fails_after_retries(
    ts_env: WorkflowEnvironment,
) -> None:
    task_queue = f"tq-{uuid.uuid4()}"

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[AlwaysFailWorkflow],
    ):
        # Retry policy is set client-side at start; the server enforces it.
        # The workflow itself has no attempt-awareness.
        with pytest.raises(WorkflowFailureError):
            await ts_env.client.execute_workflow(
                AlwaysFailWorkflow.run,
                id=f"wf-{uuid.uuid4()}",
                task_queue=task_queue,
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=1),
                    backoff_coefficient=2.0,
                    maximum_attempts=3,
                ),
            )


async def test_child_workflow_completes_when_signaled(
    ts_env: WorkflowEnvironment,
) -> None:
    task_queue = f"tq-{uuid.uuid4()}"
    wf_id = f"wf-{uuid.uuid4()}"

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[SomeChildWorkflow],
    ):
        handle = await ts_env.client.start_workflow(
            SomeChildWorkflow.run,
            id=wf_id,
            task_queue=task_queue,
        )
        await handle.signal(SomeChildWorkflow.go)
        result = await handle.result()

    datetime.fromisoformat(result)
