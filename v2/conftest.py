"""Independent fixtures for Testing Env and Direct API tests."""

import os
from datetime import timedelta

import pytest
import pytest_asyncio
from temporalio.client import Client
from temporalio.testing import TimeSkipper, WorkflowEnvironment


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def env():
    # The SDK's default released CLI does not include the required branch.
    path = os.environ.get("TEMPORAL_CLI_PATH")
    if not path:
        raise pytest.UsageError(
            "Set TEMPORAL_CLI_PATH to a CLI built from temporalio/cli's "
            "fx/time-skipping-cli branch. See v2/README.md."
        )
    kwargs = {
        "dev_server_existing_path": path,
        "dev_server_extra_args": ["--enable-time-skipping"],
        "dev_server_startup_timeout": timedelta(seconds=30),
    }
    async with await WorkflowEnvironment.start_time_skipping_v2(**kwargs) as env:
        yield env


@pytest_asyncio.fixture
async def skipper(env: WorkflowEnvironment) -> TimeSkipper:
    # The env owns the test server; this connection uses the direct client API.
    client = await Client.connect(
        env.client.service_client.config.target_host,
        namespace=env.client.namespace,
    )
    return TimeSkipper(client)
