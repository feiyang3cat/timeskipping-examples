import logging
import uuid
from datetime import datetime, timedelta

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from activities import dummy_activity
from workflow_child import (
    ParentResult,
    ParentWithChildWorkflow,
    WaitForSignalChildWorkflow,
)

logger = logging.getLogger(__name__)


# Child workflow is started with task_queue="your-task-queue" hardcoded in
# workflow_child.py, so the worker must listen on that queue for both
# parent and child to be picked up.
CHILD_TASK_QUEUE = "your-task-queue"


async def test_parent_and_child_complete_when_signals_arrive_before_timeout(
    ts_env: WorkflowEnvironment,
) -> None:
    parent_id = f"parent-{uuid.uuid4()}"
    child_id = f"child-{parent_id}"

    async with Worker(
        ts_env.client,
        task_queue=CHILD_TASK_QUEUE,
        workflows=[ParentWithChildWorkflow, WaitForSignalChildWorkflow],
        activities=[dummy_activity],
    ):
        server_t0 = await ts_env.get_current_time()

        handle = await ts_env.client.start_workflow(
            ParentWithChildWorkflow.run,
            child_id,
            id=parent_id,
            task_queue=CHILD_TASK_QUEUE,
        )

        # Advance past parent's first 1h sleep so the child exists.
        await ts_env.sleep(timedelta(hours=1, minutes=10).total_seconds())
        # Signal both. Signals persist until consumed, so they will be picked
        # up when each workflow reaches its wait_condition.
        await ts_env.client.get_workflow_handle(child_id).signal(
            WaitForSignalChildWorkflow.go
        )
        await handle.signal(ParentWithChildWorkflow.go)

        result: ParentResult = await handle.result()

    workflow_end = datetime.fromisoformat(result.end)
    gap = workflow_end - server_t0
    logger.info("signals-win: %s gap=%s", result, gap)

    assert result.parent_signal_won is True
    assert result.child_signal_won is True
    # Expected virtual timeline when both signals win:
    #   parent 1h sleep -> child runs (30min sleep, then signal wins instantly)
    #   -> parent 1h sleep -> parent signal wins instantly => ~2h30min
    assert timedelta(hours=2, minutes=25) <= gap <= timedelta(hours=2, minutes=35)


async def test_parent_and_child_complete_when_timers_expire_without_signals(
    ts_env: WorkflowEnvironment,
) -> None:
    parent_id = f"parent-{uuid.uuid4()}"
    child_id = f"child-{parent_id}"

    async with Worker(
        ts_env.client,
        task_queue=CHILD_TASK_QUEUE,
        workflows=[ParentWithChildWorkflow, WaitForSignalChildWorkflow],
        activities=[dummy_activity],
    ):
        server_t0 = await ts_env.get_current_time()

        result: ParentResult = await ts_env.client.execute_workflow(
            ParentWithChildWorkflow.run,
            child_id,
            id=parent_id,
            task_queue=CHILD_TASK_QUEUE,
        )

    workflow_end = datetime.fromisoformat(result.end)
    gap = workflow_end - server_t0
    logger.info("timers-win: %s gap=%s", result, gap)

    assert result.parent_signal_won is False
    assert result.child_signal_won is False
    # Expected virtual timeline when no signals arrive:
    #   parent 1h sleep -> child 30min sleep -> child 5min timer expires
    #   -> parent 1h sleep -> parent 5min timer expires => ~2h40min
    assert timedelta(hours=2, minutes=35) <= gap <= timedelta(hours=2, minutes=45)
