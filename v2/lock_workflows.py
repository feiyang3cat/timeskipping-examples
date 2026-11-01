"""Workflow bodies shared by the four migrated lock exploration scenarios."""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError
from temporalio.exceptions import TimeoutError as TemporalTimeoutError

ACTIVITY_NAME = "lock_probe_activity"


@workflow.defn
class ThreeActivities:
    """Runs 3 activities concurrently; reports ok/timeout per activity."""

    @workflow.run
    async def run(self, start_to_close_seconds: float) -> list:
        async def one(i: int) -> str:
            try:
                await workflow.execute_activity(
                    ACTIVITY_NAME,
                    args=[f"act-{i}"],
                    start_to_close_timeout=timedelta(seconds=start_to_close_seconds),
                    retry_policy=RetryPolicy(maximum_attempts=1),
                )
                return "ok"
            except ActivityError as e:
                if isinstance(e.cause, TemporalTimeoutError):
                    return "timeout"
                return f"err:{type(e.cause).__name__}"

        return list(await asyncio.gather(*(one(i) for i in range(3))))


@workflow.defn
class ThreeActivitiesRacedVsTimer:
    """Runs 3 activities, each raced against its own user timer; reports the winner."""

    @workflow.run
    async def run(self, timer_seconds: float, start_to_close_seconds: float) -> list:
        async def one(i: int) -> str:
            act = asyncio.ensure_future(
                workflow.execute_activity(
                    ACTIVITY_NAME,
                    args=[f"act-{i}"],
                    start_to_close_timeout=timedelta(seconds=start_to_close_seconds),
                    retry_policy=RetryPolicy(maximum_attempts=1),
                )
            )
            timer = asyncio.ensure_future(asyncio.sleep(timer_seconds))
            done, pending = await workflow.wait(
                [act, timer], return_when=asyncio.FIRST_COMPLETED
            )
            for t in pending:
                t.cancel()
            return "timer" if timer in done else "activity"

        return list(await asyncio.gather(*(one(i) for i in range(3))))
