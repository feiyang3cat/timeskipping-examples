import logging
import time

import pytest_asyncio
from temporalio.testing import WorkflowEnvironment

logger = logging.getLogger(__name__)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def local_env():
    t0 = time.time()
    logger.info("Booting LOCAL Temporal dev server...")
    async with await WorkflowEnvironment.start_local() as env:
        logger.info(
            "LOCAL server ready in %.2f ms (target: %s)",
            (time.time() - t0) * 1000,
            env.client.service_client.config.target_host,
        )
        yield env


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def ts_env():
    t0 = time.time()
    logger.info("Booting TIME-SKIPPING test server...")
    async with await WorkflowEnvironment.start_time_skipping() as env:
        logger.info(
            "TS server ready in %.2f ms (target: %s)",
            (time.time() - t0) * 1000,
            env.client.service_client.config.target_host,
        )
        yield env
