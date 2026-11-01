"""V1 activity races and manual lock probes under V2 semantics.

V2 fast-forward cannot bypass in-flight activities. A call from an activity
must run in a separate task: awaiting it inline prevents the activity from
completing, which prevents time skipping. No caller can release a global lock.
"""

import asyncio
import time
from datetime import timedelta
from uuid import uuid4

import pytest
from temporalio import activity
from temporalio.worker import Worker

from activity_workflows import (
    WorkflowRacingActivityAndTimer,
    WorkflowRunningLongActivity,
    WorkflowWithTwoTimersAndActivity,
)
from lock_workflows import ACTIVITY_NAME, ThreeActivities, ThreeActivitiesRacedVsTimer


async def assert_blocked(task):
    # Shield preserves the in-flight RPC while observing that it hasn't finished.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(task), 0.3)


async def test_fast_forward_in_activity_cannot_fire_user_timer(ts_env):
    ready = asyncio.Event()
    requested = asyncio.Event()
    release = asyncio.Event()
    forwards = []
    handle = None

    @activity.defn(name="dummy_activity")
    async def slow_dummy(name: str, activity_duration=timedelta()) -> str:
        await ready.wait()
        forwards.append(
            asyncio.create_task(ts_env.fast_forward(handle, timedelta(minutes=11)))
        )
        requested.set()
        await release.wait()
        return f"done {name}"

    task_queue = f"tq-race-{uuid4()}"
    try:
        async with Worker(
            ts_env.client,
            task_queue=task_queue,
            workflows=[WorkflowRacingActivityAndTimer],
            activities=[slow_dummy],
        ):
            with ts_env.with_time_skipping_disabled():
                handle = await ts_env.client.start_workflow(
                    WorkflowRacingActivityAndTimer.run,
                    args=[600.0, 1800.0],
                    id=f"wf-{uuid4()}",
                    task_queue=task_queue,
                )
            ready.set()
            await requested.wait()
            try:
                await assert_blocked(forwards[0])
            finally:
                release.set()
            # V1's manual unlock fired the timer. V2 waits for the activity.
            assert await handle.result() == "activity"
            assert not await forwards[0]
    finally:
        for forward in forwards:
            forward.cancel()
        await asyncio.gather(*forwards, return_exceptions=True)


async def test_fast_forward_blocks_while_activity_in_flight(ts_env):
    started = asyncio.Event()
    release = asyncio.Event()

    @activity.defn(name="dummy_activity")
    async def held_dummy(name: str, activity_duration=timedelta()) -> str:
        started.set()
        await release.wait()
        return f"done {name}"

    task_queue = f"tq-held-{uuid4()}"
    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[WorkflowRunningLongActivity],
        activities=[held_dummy],
    ):
        with ts_env.with_time_skipping_disabled():
            handle = await ts_env.client.start_workflow(
                WorkflowRunningLongActivity.run,
                id=f"wf-{uuid4()}",
                task_queue=task_queue,
            )
        await started.wait()
        start = await ts_env.get_current_time(handle)
        wall = time.monotonic()
        forward = asyncio.create_task(ts_env.fast_forward(handle, 3.0))
        try:
            await assert_blocked(forward)
            elapsed = await ts_env.get_current_time(handle) - start
            assert elapsed.total_seconds() < 3.0
        finally:
            release.set()
        assert await handle.result() == "done long running activity"
        assert not await forward  # workflow completed before the target
        assert time.monotonic() - wall < 3.0


async def run_two_timer_probe(ts_env, *, inside):
    ready = asyncio.Event()
    started = asyncio.Event()
    release = asyncio.Event()
    forwards = []
    handle = None
    result = None

    @activity.defn(name="dummy_activity")
    async def held(name: str, activity_duration=timedelta()) -> str:
        await ready.wait()
        if inside:
            forwards.append(
                asyncio.create_task(ts_env.fast_forward(handle, timedelta(hours=1)))
            )
        started.set()
        await release.wait()
        return f"done {name}"

    task_queue = f"tq-two-timers-{uuid4()}"
    try:
        async with Worker(
            ts_env.client,
            task_queue=task_queue,
            workflows=[WorkflowWithTwoTimersAndActivity],
            activities=[held],
        ):
            with ts_env.with_time_skipping_disabled():
                handle = await ts_env.client.start_workflow(
                    WorkflowWithTwoTimersAndActivity.run,
                    args=[3600.0, 7200.0, 10800.0],
                    id=f"wf-{uuid4()}",
                    task_queue=task_queue,
                )
            # Preserve V1's background result wait: in V2 it cannot unlock
            # the held activity or change the behavior of fast_forward.
            result = asyncio.create_task(handle.result())
            ready.set()
            await started.wait()
            if not inside:
                forwards.append(
                    asyncio.create_task(ts_env.fast_forward(handle, timedelta(hours=1)))
                )
            try:
                await assert_blocked(forwards[0])
            finally:
                release.set()
            assert await forwards[0]
            # Both calls accumulate, but only after the activity releases its work.
            # The target coincides with timer2 and workflow completion. Either
            # transition can be observed first; verify the actual timer order.
            await ts_env.fast_forward(handle, timedelta(hours=1))
            assert await result == [
                ["activity", 0],
                ["timer1", 1],
                ["timer2", 2],
            ]
    finally:
        if result is not None:
            result.cancel()
            await asyncio.gather(result, return_exceptions=True)
        for forward in forwards:
            forward.cancel()
        await asyncio.gather(*forwards, return_exceptions=True)


async def test_two_fast_forwards_from_activity_fire_timers_after_activity(ts_env):
    await run_two_timer_probe(ts_env, inside=True)


async def test_fast_forward_from_test_waits_for_held_activity(ts_env):
    # Awaiting result concurrently does not let an outside caller bypass work.
    await run_two_timer_probe(ts_env, inside=False)


async def run_three_activity_probe(ts_env, *, inside, timeout_seconds, race=False):
    ready = asyncio.Event()
    release = asyncio.Event()
    started = asyncio.Event()
    count = 0
    handle = None
    forwards = []

    @activity.defn(name=ACTIVITY_NAME)
    async def held(name: str) -> str:
        nonlocal count
        await ready.wait()
        if inside and name == "act-0":
            # Concurrent fast-forwards on ONE workflow override each other;
            # one activity drives the workflow while all three remain in flight.
            forwards.append(
                asyncio.create_task(ts_env.fast_forward(handle, timedelta(hours=1)))
            )
        count += 1
        if count == 3:
            started.set()
        await release.wait()
        return name

    workflow_cls = ThreeActivitiesRacedVsTimer if race else ThreeActivities
    task_queue = f"tq-three-{uuid4()}"
    try:
        async with Worker(
            ts_env.client,
            task_queue=task_queue,
            workflows=[workflow_cls],
            activities=[held],
        ):
            with ts_env.with_time_skipping_disabled():
                handle = await ts_env.client.start_workflow(
                    workflow_cls.run,
                    args=[600.0, timeout_seconds] if race else [timeout_seconds],
                    id=f"wf-{uuid4()}",
                    task_queue=task_queue,
                )
            ready.set()
            await started.wait()
            if not inside:
                forwards.append(
                    asyncio.create_task(ts_env.fast_forward(handle, timedelta(hours=1)))
                )
            try:
                await assert_blocked(forwards[0])
            finally:
                release.set()
            result = await handle.result()
            assert not await forwards[0]
            return result
    finally:
        for forward in forwards:
            forward.cancel()
        await asyncio.gather(*forwards, return_exceptions=True)


async def test_1_outside_fast_forward_blocks_with_three_activities(ts_env):
    assert (
        await run_three_activity_probe(
            ts_env,
            inside=False,
            timeout_seconds=30.0,
        )
        == ["ok"] * 3
    )


async def test_2_inside_fast_forward_waits_for_activities(ts_env):
    assert (
        await run_three_activity_probe(
            ts_env,
            inside=True,
            timeout_seconds=7200.0,
        )
        == ["ok"] * 3
    )


async def test_3_inside_fast_forward_cannot_force_start_to_close(ts_env):
    # A 1h target cannot force the 30s timeout while activities are in flight.
    assert (
        await run_three_activity_probe(
            ts_env,
            inside=True,
            timeout_seconds=30.0,
        )
        == ["ok"] * 3
    )


async def test_4_inside_fast_forward_cannot_win_user_timer_race(ts_env):
    assert (
        await run_three_activity_probe(
            ts_env,
            inside=True,
            timeout_seconds=1800.0,
            race=True,
        )
        == ["activity"] * 3
    )
