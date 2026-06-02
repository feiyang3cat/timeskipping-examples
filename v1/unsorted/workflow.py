import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from activities import (
        GreetingInput,
        GreetingOutput,
        compose_greeting,
        send_farewell,
        slow_activity,
    )


@workflow.defn
class HelloTsWorkflow:
    """Hello-world variant exercised by the time-skipping test server.

    Includes a 1-hour durable timer between activities so we can prove the
    test server fast-forwards through timers in milliseconds of wall time.
    """

    @workflow.run
    async def run(self, name: str) -> str:
        workflow.logger.info("HelloTsWorkflow started for %s", name)

        greeting: GreetingOutput = await workflow.execute_activity(
            compose_greeting,
            GreetingInput(name=name),
            start_to_close_timeout=timedelta(seconds=30),
        )

        await asyncio.sleep(timedelta(hours=1).total_seconds())

        farewell: GreetingOutput = await workflow.execute_activity(
            send_farewell,
            GreetingInput(name=name),
            start_to_close_timeout=timedelta(seconds=30),
        )

        return f"{greeting.message} ... {farewell.message}"


@workflow.defn
class TwoTimerWorkflow:
    """Runs two concurrent timers (1h and 10h) and exposes which have fired."""

    def __init__(self) -> None:
        self._short_fired = False
        self._long_fired = False

    @workflow.run
    async def run(self) -> None:
        async def short_timer() -> None:
            await asyncio.sleep(timedelta(hours=1).total_seconds())
            self._short_fired = True

        async def long_timer() -> None:
            await asyncio.sleep(timedelta(hours=10).total_seconds())
            self._long_fired = True

        await asyncio.gather(short_timer(), long_timer())

    @workflow.query
    def short_fired(self) -> bool:
        return self._short_fired

    @workflow.query
    def long_fired(self) -> bool:
        return self._long_fired


@workflow.defn
class WaitForSignalWorkflow:
    """Blocks until `go` is signaled, then returns."""

    def __init__(self) -> None:
        self._signaled = False

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self._signaled)
        return "signaled"

    @workflow.signal
    def go(self) -> None:
        self._signaled = True


@workflow.defn
class FailFirstAttemptWorkflow:
    """Fails on attempt 1, succeeds on later attempts."""

    @workflow.run
    async def run(self) -> str:
        attempt = workflow.info().attempt
        if attempt == 1:
            raise ApplicationError("first attempt fails")
        return f"succeeded on attempt {attempt}"


@workflow.defn
class SlowActivityWorkflow:
    """Runs `slow_activity` for a caller-supplied duration and returns its result."""

    @workflow.run
    async def run(self, seconds: float) -> str:
        return await workflow.execute_activity(
            slow_activity,
            seconds,
            start_to_close_timeout=timedelta(seconds=60),
        )


@workflow.defn
class HelloLocalWorkflow:
    """Hello-world variant exercised by the full local dev server.

    Assumed to depend on a server feature the in-memory time-skipping test
    server doesn't implement (e.g. advanced visibility, schedules, search
    attributes), so its tests must boot a real Temporal server.
    """

    @workflow.run
    async def run(self, name: str) -> str:
        workflow.logger.info("HelloLocalWorkflow started for %s", name)

        greeting: GreetingOutput = await workflow.execute_activity(
            compose_greeting,
            GreetingInput(name=name),
            start_to_close_timeout=timedelta(seconds=30),
        )

        farewell: GreetingOutput = await workflow.execute_activity(
            send_farewell,
            GreetingInput(name=name),
            start_to_close_timeout=timedelta(seconds=30),
        )

        return f"{greeting.message} ... {farewell.message}"
