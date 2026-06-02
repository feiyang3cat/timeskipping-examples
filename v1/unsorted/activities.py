import asyncio
from dataclasses import dataclass
from datetime import datetime

from temporalio import activity


@activity.defn
async def slow_activity(seconds: float) -> str:
    await asyncio.sleep(seconds)
    return f"slept {seconds}s"


recorded_completions: list[datetime] = []


@activity.defn
async def record_completion(t: datetime) -> None:
    recorded_completions.append(t)


@activity.defn
async def dummy_activity(real_intent: str) -> None:
    activity.logger.info("Dummy activity executed: %s", real_intent)


@dataclass
class GreetingInput:
    name: str


@dataclass
class GreetingOutput:
    message: str


@activity.defn
async def compose_greeting(input: GreetingInput) -> GreetingOutput:
    activity.logger.info("Composing greeting for %s", input.name)
    return GreetingOutput(message=f"Hello, {input.name}!")


@activity.defn
async def send_farewell(input: GreetingInput) -> GreetingOutput:
    activity.logger.info("Sending farewell to %s", input.name)
    return GreetingOutput(message=f"Goodbye, {input.name}!")
