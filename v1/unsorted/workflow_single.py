import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from activities import record_completion, slow_activity



@workflow.defn
class AlwaysFailWorkflow:
    """Always raises — used to verify the client-side retry policy."""

    @workflow.run
    async def run(self) -> None:
        raise ApplicationError("boom")


@workflow.defn
class FailAfterRecordingWorkflow:
    """Records the current workflow time, then raises. Each retry produces one record."""

    @workflow.run
    async def run(self) -> None:
        await workflow.execute_activity(
            record_completion,
            workflow.now(),
            start_to_close_timeout=timedelta(seconds=5),
        )
        raise ApplicationError("boom")


@workflow.defn
class SomeChildWorkflow:
    """A child workflow that waits on a signal-driven condition."""

    def __init__(self) -> None:
        self._signal_count = 0

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self._signal_count >= 1)
        return workflow.now().isoformat()

    @workflow.signal
    def go(self) -> None:
        self._signal_count += 1
