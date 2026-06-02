import logging
import time
import uuid
from datetime import datetime, timedelta

from temporalio.common import RetryPolicy
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from activities import record_completion, recorded_completions, slow_activity
from workflow_chain import ContinueAsNewWorkflow, CronTimerWorkflow
from workflow_child import ParentWithChildWorkflow, WaitForSignalChildWorkflow
from workflow_single import BusyWorkflow, FailAfterRecordingWorkflow

logger = logging.getLogger(__name__)


# using the current testing server


## case-0: if the workflow is busy until completion, automatic time skipping won't help
## Workflow: a workflow with 3 activities that runs for 300ms each, and then completes.
# setting time skipping with sleep 100ms won't return until the workflow completes.
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
        handle = await ts_env.client.start_workflow(
            BusyWorkflow.run,
            0.3,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        # Try to advance 100ms on the server clock. Activities are wall-clock work,
        # so the server can't fast-forward past them.
        await ts_env.sleep(0.1)
        wall_after_sleep_ms = (time.time() - wall_t0) * 1000

        result = await handle.result()
        wall_total_ms = (time.time() - wall_t0) * 1000

    assert result == "done"
    assert wall_total_ms >= 900
    logger.info(
        "wall after env.sleep(0.1)=%.2f ms; total wall=%.2f ms (3 × 300ms activities)",
        wall_after_sleep_ms,
        wall_total_ms,
    )


## Pattern 1: signals for long running worklfows
# Testing workflow: (1) activities that runs for 2s, (2) user timer runs for 10s


## case-2: continue-as-new chain — workflow-time gap should be ~2h (two 1h timers),
## even though wall-clock time is milliseconds.
async def test_continue_as_new_workflow_time_gap_is_about_two_hours(
    ts_env: WorkflowEnvironment,
) -> None:
    task_queue = f"tq-{uuid.uuid4()}"

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[ContinueAsNewWorkflow],
    ):
        server_t0 = await ts_env.get_current_time()
        wall_t0 = time.time()

        result = await ts_env.client.execute_workflow(
            ContinueAsNewWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )

        wall_elapsed_ms = (time.time() - wall_t0) * 1000

    workflow_end = result if isinstance(result, datetime) else datetime.fromisoformat(result)
    gap = workflow_end - server_t0

    logger.info(
        "server_t0=%s, workflow_end=%s, gap=%s, wall=%.2f ms",
        server_t0,
        workflow_end,
        gap,
        wall_elapsed_ms,
    )

    # Two 1h timers across the continue-as-new chain → gap should be ~2h.
    assert timedelta(hours=1, minutes=55) <= gap <= timedelta(hours=2, minutes=5)


## case-3: cron every 1 min with a 30s body — measure each run's completion deviation.
## (Original ask was 1s/500ms/3s, but the test server cron parser is UNIX 5-field, so
## scaled up 60× to the minimum supported granularity.)
async def test_cron_workflow_completion_times(
    ts_env: WorkflowEnvironment,
) -> None:
    recorded_completions.clear()
    task_queue = f"tq-{uuid.uuid4()}"

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[CronTimerWorkflow],
        activities=[record_completion],
    ):
        server_t0 = await ts_env.get_current_time()

        handle = await ts_env.client.start_workflow(
            CronTimerWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
            cron_schedule="* * * * *",
        )

        await ts_env.sleep(3 * 60)
        completions = list(recorded_completions)
        await handle.terminate()

    logger.info("Captured %d cron completions", len(completions))
    for i, actual in enumerate(completions):
        deviation_from_t0 = (actual - server_t0).total_seconds()
        logger.info(
            "run %d: actual=%s seconds_since_t0=%.2f",
            i + 1,
            actual.isoformat(),
            deviation_from_t0,
        )


## case-4: parent (1h timer → child(race 1h vs signal) → 1h timer → race(signal vs 5min timer)).
## Steps: (1) env.sleep(1h+10min) — past parent's 1h timer, child started + waiting.
##        (2) signal child — beats child's 1h timer.
##        (3) env.sleep(1h) — covers parent's second 1h timer.
##        (4) signal parent — beats parent's short 5min timer.
async def test_parent_child_workflow_final_virtual_time(
    ts_env: WorkflowEnvironment,
) -> None:
    task_queue = f"tq-{uuid.uuid4()}"
    parent_id = f"parent-{uuid.uuid4()}"
    child_id = f"child-{parent_id}"

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[ParentWithChildWorkflow, WaitForSignalChildWorkflow],
    ):
        server_t0 = await ts_env.get_current_time()

        handle = await ts_env.client.start_workflow(
            ParentWithChildWorkflow.run,
            child_id,
            id=parent_id,
            task_queue=task_queue,
        )

        # (1) Sleep 1h+10min — covers parent's 1h timer, leaves 10min buffer
        #     so the child workflow is guaranteed to exist.
        await ts_env.sleep(timedelta(hours=1, minutes=10).total_seconds())
        # (2) Signal child — beats child's 1h timer (which would fire at t=2h).
        await ts_env.client.get_workflow_handle(child_id).signal(
            WaitForSignalChildWorkflow.go
        )
        # (3) Sleep 1h — covers parent's second 1h timer
        #     (recording resumes from t≈1h10min after child completes).
        await ts_env.sleep(timedelta(hours=1).total_seconds())
        # (4) Signal parent — beats parent's 5min short timer.
        await handle.signal(ParentWithChildWorkflow.go)

        result = await handle.result()

    workflow_end = datetime.fromisoformat(result)
    gap = workflow_end - server_t0
    logger.info("server_t0=%s, workflow_end=%s, gap=%s", server_t0, workflow_end, gap)

