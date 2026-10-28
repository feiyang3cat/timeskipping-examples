"""Manual exploration of the time-skipping lock with 3 concurrent activities.

Not part of the CI suite (testpaths only runs test_workflow.py). Run explicitly:

    uv run pytest manual_env_sleep_lock_scenarios.py -s

The time-skipping server keeps a reference-counted lock; virtual time only
advances when the count is 0. Every in-flight activity increments it. These four
scenarios show how `env.sleep()` interacts with that lock depending on WHERE it
is called from.

    Scenario 1  env.sleep() OUTSIDE (from the test), 3 activities in flight
                -> cannot release the activities' locks; blocks in REAL time;
                   no skip, no timeout. Concurrency (1 vs 3) makes no difference.

    Scenario 2  env.sleep() INSIDE each of 3 activities, generous start_to_close
                -> each activity releases its own lock; count hits 0; virtual
                   time skips while all 3 are "in flight"; all complete OK.

    Scenario 3  env.sleep() INSIDE each of 3 activities, skip > start_to_close
                -> the in-flight skip advances virtual time past each activity's
                   own start_to_close_timeout; all 3 TIME OUT.

    Scenario 4  env.sleep() INSIDE each of 3 activities, raced vs a user timer
                (the repo 9++ pattern) -> the skip fires the shorter user timer
                first; the timer wins every race.

Gotcha baked into these workflows: activities are referenced BY STRING NAME.
Referencing a locally-defined (closure) activity function by object instead
raises NameError inside the workflow sandbox, which retries the workflow task
forever and looks exactly like a hang.
"""

import asyncio
import time
from datetime import timedelta
from uuid import uuid4

import pytest_asyncio
from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError
from temporalio.exceptions import TimeoutError as TemporalTimeoutError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

ACTIVITY_NAME = "lock_probe_activity"


@pytest_asyncio.fixture
async def ts_env():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        yield env


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


def _real_activity(hold_seconds: float):
    @activity.defn(name=ACTIVITY_NAME)
    async def real_activity(name: str) -> str:
        await asyncio.sleep(hold_seconds)  # real in-flight time, holds the lock
        return f"done {name}"

    return real_activity


def _skipping_activity(ts_env: WorkflowEnvironment, skip: timedelta):
    @activity.defn(name=ACTIVITY_NAME)
    async def skipping_activity(name: str) -> str:
        await asyncio.gather(
            asyncio.sleep(2),  # real in-flight work
            ts_env.sleep(skip.total_seconds()),  # manual skip from INSIDE the activity
        )
        return f"done {name}"

    return skipping_activity


# --- Scenario 1: env.sleep() OUTSIDE (from the test), 3 activities in flight ---

async def test_1_outside_env_sleep_blocks_in_real_time(ts_env: WorkflowEnvironment):
    task_queue = f"tq-outside-{uuid4()}"
    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[ThreeActivities],
        activities=[_real_activity(hold_seconds=10)],
    ):
        handle = await ts_env.client.start_workflow(
            ThreeActivities.run,
            timedelta(seconds=30).total_seconds(),  # start_to_close
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
        )
        await asyncio.sleep(1.0)  # let all 3 activities start and take the lock

        wall = time.monotonic()
        # 3 concurrent client-side skips of 1h each, while the 3 activities run.
        await asyncio.gather(
            *(ts_env.sleep(timedelta(hours=1).total_seconds()) for _ in range(3))
        )
        env_sleep_wall = time.monotonic() - wall
        results = await handle.result()

    # The client env.sleep() could NOT release the activity locks: it blocked in
    # real time (~the activities' remaining 9s) instead of skipping instantly.
    assert env_sleep_wall >= 2.5
    # No skip past the 30s start_to_close, so no activity timed out.
    assert results == ["ok", "ok", "ok"]


# --- Scenario 2: env.sleep() INSIDE 3 activities, generous start_to_close ---

async def test_2_inside_env_sleep_skips_while_in_flight(ts_env: WorkflowEnvironment):
    task_queue = f"tq-inside-ok-{uuid4()}"
    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[ThreeActivities],
        activities=[_skipping_activity(ts_env, skip=timedelta(hours=1))],
    ):
        wall = time.monotonic()
        results = await ts_env.client.execute_workflow(
            ThreeActivities.run,
            timedelta(hours=2).total_seconds(),  # start_to_close > 1h skip
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
        )
        elapsed = time.monotonic() - wall

    # Each in-flight activity released its own lock, so the 1h skip happened even
    # though all 3 were "running"; finished in ~real activity time, not 1h.
    assert results == ["ok", "ok", "ok"]
    assert elapsed < 5.0


# --- Scenario 3: env.sleep() INSIDE 3 activities, skip > start_to_close ---

async def test_3_inside_env_sleep_fires_start_to_close(ts_env: WorkflowEnvironment):
    task_queue = f"tq-inside-timeout-{uuid4()}"
    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[ThreeActivities],
        activities=[_skipping_activity(ts_env, skip=timedelta(hours=1))],
    ):
        results = await ts_env.client.execute_workflow(
            ThreeActivities.run,
            timedelta(seconds=30).total_seconds(),  # start_to_close < 1h skip
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
        )

    # The in-flight skip advanced virtual time past each activity's 30s
    # start_to_close_timeout, so all 3 timed out.
    assert results == ["timeout", "timeout", "timeout"]


# --- Scenario 4: env.sleep() INSIDE 3 activities, raced vs a user timer ---

async def test_4_inside_env_sleep_loses_race_to_user_timer(ts_env: WorkflowEnvironment):
    task_queue = f"tq-inside-race-{uuid4()}"
    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[ThreeActivitiesRacedVsTimer],
        activities=[_skipping_activity(ts_env, skip=timedelta(minutes=11))],
    ):
        results = await ts_env.client.execute_workflow(
            ThreeActivitiesRacedVsTimer.run,
            args=[
                timedelta(minutes=10).total_seconds(),  # user timer
                timedelta(minutes=30).total_seconds(),  # activity start_to_close
            ],
            id=f"wf-{uuid4()}",
            task_queue=task_queue,
        )

    # The 11-min in-activity skip fires the 10-min user timer first every time.
    assert results == ["timer", "timer", "timer"]
