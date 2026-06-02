import asyncio
import logging

from temporalio.client import Client
from temporalio.worker import Worker

from activities import compose_greeting, send_farewell
from workflow import HelloLocalWorkflow, HelloTsWorkflow

TASK_QUEUE = "hello-world-task-queue"


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    client = await Client.connect("localhost:7233")

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[HelloTsWorkflow, HelloLocalWorkflow],
        activities=[compose_greeting, send_farewell],
    )

    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
