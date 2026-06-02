import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from activities import dummy_activity, retry_activity

default_retry_policy = RetryPolicy(maximum_attempts=1)


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



@workflow.defn
class WorkflowWithActivityRetires:
    @workflow.run
    async def run(self) -> int:
        return await workflow.execute_activity(
            retry_activity,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(hours=1),
                backoff_coefficient=2.0,
                maximum_attempts=3,
            ),
        )
