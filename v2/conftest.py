"""Run every scenario through both public V2 time-skipping entry points."""

import os
from datetime import timedelta

import pytest
import pytest_asyncio
from temporalio.testing import TimeSkipper, WorkflowEnvironment


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def local_server():
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


@pytest_asyncio.fixture(params=["environment", "time-skipper"])
async def ts_env(request, local_server):
    if request.param == "environment":
        yield local_server
    else:
        # A fresh, unwrapped client, just like one connected to OSS or Cloud.
        client = await local_server.connect_client()
        yield TimeSkipper(client)
