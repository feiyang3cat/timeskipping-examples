"""Activity/timer workflows for V2's independent-clock tests."""

import asyncio
from datetime import timedelta

from activities import dummy_activity
from temporalio import workflow
from temporalio.common import RetryPolicy

default_retry_policy = RetryPolicy(maximum_attempts=1)


@workflow.defn
class WorkflowRacingActivityAndTimer:
    """Race an activity against a workflow timer and return the winner."""

    @workflow.run
    async def run(self, timer_seconds: float, activity_timeout_seconds: float) -> str:
        activity_task = asyncio.ensure_future(
            workflow.execute_activity(
                dummy_activity,
                "racing activity",
                start_to_close_timeout=timedelta(seconds=activity_timeout_seconds),
                retry_policy=default_retry_policy,
            )
        )
        timer_task = asyncio.ensure_future(asyncio.sleep(timer_seconds))
        done, pending = await workflow.wait(
            [activity_task, timer_task], return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        return "timer" if timer_task in done else "activity"


@workflow.defn
class WorkflowRunningLongActivity:
    """Hold an activity in flight while the test requests fast-forward."""

    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            dummy_activity,
            "long running activity",
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=default_retry_policy,
        )


@workflow.defn
class WorkflowWithTwoTimersAndActivity:
    """Record the completion order and virtual hours of two timers and an activity."""

    @workflow.run
    async def run(
        self,
        timer1_seconds: float,
        timer2_seconds: float,
        activity_timeout_seconds: float,
    ) -> list:
        start = workflow.now()
        tasks = {
            "timer1": asyncio.ensure_future(asyncio.sleep(timer1_seconds)),
            "timer2": asyncio.ensure_future(asyncio.sleep(timer2_seconds)),
            "activity": asyncio.ensure_future(
                workflow.execute_activity(
                    dummy_activity,
                    "racing activity",
                    start_to_close_timeout=timedelta(seconds=activity_timeout_seconds),
                    retry_policy=default_retry_policy,
                )
            ),
        }
        name_by_task = {task: name for name, task in tasks.items()}
        completion_order = []
        pending = set(tasks.values())
        while pending:
            done, pending = await workflow.wait(
                list(pending), return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                elapsed_hours = (workflow.now() - start).total_seconds() / 3600
                completion_order.append([name_by_task[task], round(elapsed_hours)])
        return completion_order
