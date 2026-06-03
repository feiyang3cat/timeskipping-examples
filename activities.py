import asyncio
import logging
from datetime import timedelta

from temporalio import activity

logger = logging.getLogger(__name__)


@activity.defn
async def dummy_activity(
    name: str, activity_duration: timedelta = timedelta(seconds=0)
) -> str:
    await asyncio.sleep(activity_duration.total_seconds())
    logger.info("Dummy activity completed for %s", name)
    return f"done {name}"


@activity.defn
async def retry_activity() -> int:
    return activity.info().attempt
