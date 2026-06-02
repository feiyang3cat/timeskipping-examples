import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import record_completion



@workflow.defn
class CronTimerWorkflow:
    """A short workflow run — 30s timer then records completion — used as a cron body.

    Note: the test server uses UNIX 5-field cron (minute granularity is the floor),
    so the original 1s/500ms/3s scenario is scaled up 60× to 1min/30s/3min.
    """
    @workflow.run
    async def run(self) -> None:
        await asyncio.sleep(30)
        await workflow.execute_activity(
            record_completion,
            workflow.now(),
            start_to_close_timeout=timedelta(seconds=10),
        )


## Around propagation
@workflow.defn
class ContinueAsNewWorkflow:
    """A workflow has a timer for 1hour and then continues as new and then has another timer for 1hour and then completes.
    We need to test if time skipping works for this workflow and its CAN run.
    """
    @workflow.run
    async def run(self, is_continuation: bool = False) -> str:
        await asyncio.sleep(timedelta(hours=1).total_seconds())
        if not is_continuation:
            workflow.continue_as_new(True)
        return workflow.now()

