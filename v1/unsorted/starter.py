import asyncio

from temporalio.client import Client

from worker import TASK_QUEUE
from workflow import HelloTsWorkflow


async def main() -> None:
    client = await Client.connect("localhost:7233")

    result = await client.execute_workflow(
        HelloTsWorkflow.run,
        "World",
        id="hello-world-workflow-id",
        task_queue=TASK_QUEUE,
    )

    print(result)


if __name__ == "__main__":
    asyncio.run(main())
