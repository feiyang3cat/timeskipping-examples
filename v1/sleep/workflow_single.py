import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from activities import record_completion, slow_activity


@workflow.defn
class BusyWorkflow:
    """A busy workflow with no waiting — time-skipping has nothing to skip."""

    @workflow.run
    async def run(self, seconds_each: float) -> str:
        for _ in range(3):
            await workflow.execute_activity(
                slow_activity,
                seconds_each,
                start_to_close_timeout=timedelta(seconds=60),
            )
        return workflow.now().isoformat()


@workflow.defn
class WaitingWorkflowOnUserTimer:
    """A workflow waiting on a long timer — finishes in ms under time-skipping."""

    @workflow.run
    async def run(self) -> str:
        await workflow.sleep(timedelta(days=10))
        return "done"


@workflow.defn
class WaitingWorkflowOnCondition:
    """A workflow waiting on a signal-driven condition."""

    def __init__(self) -> None:
        self._signal_count = 0

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self._signal_count >= 1)
        return workflow.now().isoformat()

    @workflow.signal
    def go(self) -> None:
        self._signal_count += 1


@workflow.defn
class WaitingWorkflowOnBothConditionAndTimer:
    """Race a 1h timer against a `go` signal after an initial 1h sleep.

    Returns "timer won" if the wait_condition times out, "condition won" if the
    signal arrives in the race window.
    """

    def __init__(self) -> None:
        self._signal_count = 0

    @workflow.signal
    def go(self) -> None:
        self._signal_count += 1

    @workflow.run
    async def run(self) -> str:
        await workflow.sleep(timedelta(hours=1))
        try:
            await workflow.wait_condition(
                lambda: self._signal_count >= 1, timeout=timedelta(hours=1)
            )
            return "condition won"
        except asyncio.TimeoutError:
            return "timer won"


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
