import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Callable

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import dummy_activity


@dataclass
class ParentResult:
    end: str
    parent_signal_won: bool
    child_signal_won: bool


async def _timer_vs_signal(
    timer: timedelta, signaled: Callable[[], bool]
) -> bool:
    """Race a timer against a condition. Returns True if condition won."""
    try:
        await workflow.wait_condition(signaled, timeout=timer)
        return True
    except asyncio.TimeoutError:
        return False


@workflow.defn
class ParentWithChildWorkflow:
    APPROVAL_TIMEOUT = timedelta(minutes=5)

    def __init__(self) -> None:
        self._signaled = False

    @workflow.signal
    def go(self) -> None:
        self._signaled = True

    @workflow.run
    async def run(self, child_id: str) -> ParentResult:
        await workflow.execute_activity(
            dummy_activity,
            "sending an email",
            start_to_close_timeout=timedelta(seconds=5),
        )

        await workflow.sleep(timedelta(hours=1))

        child_signal_won = await workflow.execute_child_workflow(
            WaitForSignalChildWorkflow.run,
            id=child_id,
            task_queue="your-task-queue",
        )

        await workflow.sleep(timedelta(hours=1))

        parent_signal_won = await _timer_vs_signal(
            self.APPROVAL_TIMEOUT, lambda: self._signaled
        )
        workflow.logger.info(
            "Parent complete, parent_signal_won=%s child_signal_won=%s",
            parent_signal_won,
            child_signal_won,
        )
        return ParentResult(
            end=workflow.now().isoformat(),
            parent_signal_won=parent_signal_won,
            child_signal_won=child_signal_won,
        )


@workflow.defn
class WaitForSignalChildWorkflow:
    APPROVAL_TIMEOUT = timedelta(minutes=5)

    def __init__(self) -> None:
        self._signaled = False

    @workflow.signal
    def go(self) -> None:
        self._signaled = True

    @workflow.run
    async def run(self) -> bool:
        await workflow.sleep(timedelta(minutes=30))

        signal_won = await _timer_vs_signal(
            self.APPROVAL_TIMEOUT, lambda: self._signaled
        )
        workflow.logger.info("Child complete, signal_won=%s", signal_won)
        return signal_won
