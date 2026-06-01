import asyncio
import logging
import time
import uuid
from datetime import timedelta

import pytest
from temporalio.client import WorkflowFailureError
from temporalio.common import RetryPolicy
from temporalio.exceptions import TimeoutError as TemporalTimeoutError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from activities import compose_greeting, send_farewell, slow_activity
from workflow import (
    FailFirstAttemptWorkflow,
    HelloLocalWorkflow,
    HelloTsWorkflow,
    SlowActivityWorkflow,
    TwoTimerWorkflow,
    WaitForSignalWorkflow,
)

logger = logging.getLogger(__name__)


async def test_hello_ts_workflow(ts_env: WorkflowEnvironment) -> None:
    task_queue = f"tq-{uuid.uuid4()}"
    start_time = time.time()

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[HelloTsWorkflow],
        activities=[compose_greeting, send_farewell],
    ):
        result = await ts_env.client.execute_workflow(
            HelloTsWorkflow.run,
            "World",
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )

    elapsed_ms = (time.time() - start_time) * 1000
    assert result == "Hello, World! ... Goodbye, World!"
    assert elapsed_ms <= 1000
    logger.info("Time taken: %.2f ms", elapsed_ms)




async def test_only_short_timer_fires_after_partial_clock_advance(
    ts_env: WorkflowEnvironment,
) -> None:
    task_queue = f"tq-{uuid.uuid4()}"

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[TwoTimerWorkflow],
    ):
        handle = await ts_env.client.start_workflow(
            TwoTimerWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        # Manually advance the test server clock by 3h —
        # past the 1h timer but well short of the 10h timer.
        await ts_env.sleep(3 * 60 * 60)

        short_fired = await handle.query(TwoTimerWorkflow.short_fired)
        long_fired = await handle.query(TwoTimerWorkflow.long_fired)
        logger.info(
            "After 3h advance: short_fired=%s, long_fired=%s",
            short_fired,
            long_fired,
        )

        assert short_fired is True
        assert long_fired is False

        await handle.cancel()


async def test_unsignaled_workflow_times_out_past_run_timeout(
    ts_env: WorkflowEnvironment,
) -> None:
    task_queue = f"tq-{uuid.uuid4()}"

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[WaitForSignalWorkflow],
    ):
        handle = await ts_env.client.start_workflow(
            WaitForSignalWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
            run_timeout=timedelta(hours=1),
        )
        # Advance the test server clock past the 1h run timeout.
        await ts_env.sleep(2 * 60 * 60)

        with pytest.raises(WorkflowFailureError) as exc_info:
            await handle.result()

        assert isinstance(exc_info.value.cause, TemporalTimeoutError)


async def test_workflow_retries_and_succeeds_on_second_attempt(
    ts_env: WorkflowEnvironment,
) -> None:
    task_queue = f"tq-{uuid.uuid4()}"
    start_time = time.time()

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[FailFirstAttemptWorkflow],
    ):
        result = await ts_env.client.execute_workflow(
            FailFirstAttemptWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
            retry_policy=RetryPolicy(
                initial_interval=timedelta(hours=1),
                backoff_coefficient=1.0,
                maximum_attempts=3,
            ),
        )

    elapsed_ms = (time.time() - start_time) * 1000
    assert result == "succeeded on attempt 2"
    # 1h backoff was fast-forwarded by the test server.
    assert elapsed_ms <= 5000
    logger.info("Time taken: %.2f ms", elapsed_ms)


async def test_env_sleep_3s_while_activity_runs_5s(
    ts_env: WorkflowEnvironment,
) -> None:
    task_queue = f"tq-{uuid.uuid4()}"

    async with Worker(
        ts_env.client,
        task_queue=task_queue,
        workflows=[SlowActivityWorkflow],
        activities=[slow_activity],
    ):
        server_t0 = await ts_env.get_current_time()
        wall_t0 = time.time()

        handle = await ts_env.client.start_workflow(
            SlowActivityWorkflow.run,
            5.0,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
            execution_timeout=timedelta(days=1),
        )

        await ts_env.sleep(3)
        wall_after_first = time.time() - wall_t0
        server_after_first = (await ts_env.get_current_time() - server_t0).total_seconds()
        logger.info(
            "after first env.sleep(3): wall=%.2fs server=%.2fs",
            wall_after_first,
            server_after_first,
        )

        await ts_env.sleep(3)
        wall_after_second = time.time() - wall_t0
        server_after_second = (await ts_env.get_current_time() - server_t0).total_seconds()
        logger.info(
            "after second env.sleep(3): wall=%.2fs server=%.2fs",
            wall_after_second,
            server_after_second,
        )

        result = await handle.result()
        wall_done = time.time() - wall_t0
        server_done = (await ts_env.get_current_time() - server_t0).total_seconds()
        logger.info(
            "after handle.result(): wall=%.2fs server=%.2fs result=%s",
            wall_done,
            server_done,
            result,
        )


async def test_hello_local_workflow(local_env: WorkflowEnvironment) -> None:
    task_queue = f"tq-{uuid.uuid4()}"
    start_time = time.time()

    async with Worker(
        local_env.client,
        task_queue=task_queue,
        workflows=[HelloLocalWorkflow],
        activities=[compose_greeting, send_farewell],
    ):
        result = await local_env.client.execute_workflow(
            HelloLocalWorkflow.run,
            "World",
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )

    elapsed_ms = (time.time() - start_time) * 1000
    assert result == "Hello, World! ... Goodbye, World!"
    logger.info("Time taken: %.2f ms", elapsed_ms)


async def test_hello_local_workflow_runs_two_in_parallel(
    local_env: WorkflowEnvironment,
) -> None:
    async def run_one(name: str) -> str:
        task_queue = f"tq-{uuid.uuid4()}"
        async with Worker(
            local_env.client,
            task_queue=task_queue,
            workflows=[HelloLocalWorkflow],
            activities=[compose_greeting, send_farewell],
        ):
            return await local_env.client.execute_workflow(
                HelloLocalWorkflow.run,
                name,
                id=f"wf-{uuid.uuid4()}",
                task_queue=task_queue,
            )

    start_time = time.time()
    results = await asyncio.gather(run_one("Alice"), run_one("Bob"))
    elapsed_ms = (time.time() - start_time) * 1000

    assert results == [
        "Hello, Alice! ... Goodbye, Alice!",
        "Hello, Bob! ... Goodbye, Bob!",
    ]
    logger.info("Two parallel workflows took: %.2f ms", elapsed_ms)
