import asyncio
import logging

from datetime import timedelta, datetime
from temporalio import workflow, activity
from uuid import uuid4
from temporalio.common import RetryPolicy
logger = logging.getLogger(__name__)

default_retry_policy = RetryPolicy(
    maximum_attempts=1
)

@activity.defn
async def dummy_activity(name: str, activity_duration: timedelta = timedelta(seconds=0)):
    await asyncio.sleep(activity_duration.total_seconds())
    logger.info(f"Dummy activity completed for {name}")
    return f"done {name}"

@workflow.defn
class WorkflowWithUserTimer:
    @workflow.run
    async def run(self, wait_hours: int = 1) -> str:
        await workflow.execute_activity(
            dummy_activity,
            "preparation activity",
            start_to_close_timeout=timedelta(seconds=1),
            retry_policy=default_retry_policy,
        )
        await workflow.sleep(timedelta(hours=wait_hours))
        await workflow.execute_activity(
            dummy_activity,
            "cleanup activity",
            start_to_close_timeout=timedelta(seconds=1),
            retry_policy=default_retry_policy,
        )
        return workflow.now().isoformat()

@workflow.defn
class ParentChildWorkflowWithUserTimer:
    @workflow.run
    async def run(self) -> str:
        child_wf_1_id = f"child-wf-1-{workflow.uuid4()}"
        child_wf_2_id = f"child-wf-2-{workflow.uuid4()}"
        child_wf_1_handle = await workflow.start_child_workflow(
            WorkflowWithUserTimer.run,
            1,  # wait_hours
            id=child_wf_1_id,
        )
        child_wf_2_handle = await workflow.start_child_workflow(
            WorkflowWithUserTimer.run,
            2,  # wait_hours
            id=child_wf_2_id,
        )
        await asyncio.gather(
            child_wf_1_handle,
            child_wf_2_handle,
        )
        return workflow.now().isoformat()


# --- Activity-retry-backoff example -------------------------------------------
# Records the (virtual) scheduled time of each attempt so a test can verify that
# time-skipping fast-forwards the exponential retry backoff instead of waiting it
# out in wall-clock time. The worker runs in the same process as the test, so the
# test can read this list after the workflow completes.
attempt_scheduled_times: list[datetime] = []


@activity.defn
async def flaky_activity() -> str:
    info = activity.info()
    attempt_scheduled_times.append(info.current_attempt_scheduled_time)
    if info.attempt < 5:
        raise RuntimeError(f"deliberate failure on attempt {info.attempt}")
    return f"succeeded on attempt {info.attempt}"


@workflow.defn
class RetryingActivityWorkflow:
    """Runs an activity that fails attempts 1-4 and succeeds on attempt 5.

    The retry policy backs off 10s -> 20s -> 40s -> 80s between attempts; under
    time-skipping those backoffs are skipped, so ~150s of virtual time passes in
    near-zero wall-clock time.
    """

    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            flaky_activity,
            start_to_close_timeout=timedelta(seconds=10),
            schedule_to_close_timeout=timedelta(days=365),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=10),
                backoff_coefficient=2.0,
                maximum_attempts=5,
            ),
        )
