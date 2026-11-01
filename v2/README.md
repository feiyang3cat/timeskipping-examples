# Time skipping V2 (Python SDK main)

The dependency comes from [`temporalio/sdk-python` main](https://github.com/temporalio/sdk-python/tree/main).
`uv.lock` records the tested commit (`c1a79fb659ffab144d7949213664a584809089f7`).
Upgrade deliberately with `uv lock --directory v2 --upgrade-package temporalio`.
Building the SDK from Git requires Rust/Cargo and the SDK's native build prerequisites.

## Run

Use Python 3.13 and build the latest
[`temporalio/cli` `fx/time-skipping-cli` branch](https://github.com/temporalio/cli/tree/fx/time-skipping-cli).
This branch provides `server start-dev --enable-time-skipping`; the released CLI
that the SDK downloads by default is not sufficient. Install the Go toolchain
required by the branch's `go.mod` before building.

```sh
git clone --branch fx/time-skipping-cli --single-branch https://github.com/temporalio/cli.git /tmp/time-skipping-cli
(cd /tmp/time-skipping-cli && go build -mod=readonly -o /tmp/temporal-time-skipping ./cmd/temporal)
TEMPORAL_CLI_PATH=/tmp/temporal-time-skipping uv run --locked --directory v2 pytest -v
```

`TEMPORAL_CLI_PATH` is required; the fixture reports a setup error when it is
missing. Both entry points run against an ephemeral local server.
Each test has a 60-second wall-clock timeout so missing time skipping fails visibly.
The manual **Tests V2** GitHub Actions workflow checks out the latest
`fx/time-skipping-cli` branch, builds it, and runs the same suite.
All 40 V2 test cases passed with this CLI build:

- Repository: [temporalio/cli](https://github.com/temporalio/cli)
- Branch: [`fx/time-skipping-cli`](https://github.com/temporalio/cli/tree/fx/time-skipping-cli)
- Tested commit: [`440c1da397ae4520b3d8a25b44515b26028dc54a`](https://github.com/temporalio/cli/commit/440c1da397ae4520b3d8a25b44515b26028dc54a)
- Reported server version: `1.33.0`

To reproduce that exact CLI build, run `git checkout 440c1da397ae4520b3d8a25b44515b26028dc54a`
in the cloned CLI repository before `go build`. The branch and CI may advance
beyond this tested commit.

## Two ways to use V2

| Mode | API | Example |
| --- | --- | --- |
| Testing Env | `WorkflowEnvironment.start_time_skipping_v2()` | [Testing Env example](#1-testing-env) |
| Direct API | `Client.connect()` + `TimeSkipper(client)` | [Direct API example](#2-direct-api) |

### 1. Testing Env

Use `WorkflowEnvironment.start_time_skipping_v2(...)` when the test should own
its local server. The environment starts and stops the server, provides
`env.client`, and exposes time-skipping operations such as `env.fast_forward`.

The snippets use the shared `WorkflowWithUserTimer` and `dummy_activity` from
the repository root. Run them inside an async function with the root on the
Python import path.

```python
import os
from datetime import timedelta

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from activities import dummy_activity
from workflow import WorkflowWithUserTimer

async with await WorkflowEnvironment.start_time_skipping_v2(
    dev_server_existing_path=os.environ["TEMPORAL_CLI_PATH"],
    dev_server_extra_args=["--enable-time-skipping"],
) as env:
    async with Worker(
        env.client,
        task_queue="time-skipping-examples",
        workflows=[WorkflowWithUserTimer],
        activities=[dummy_activity],
    ):
        with env.with_time_skipping_disabled():
            handle = await env.client.start_workflow(
                WorkflowWithUserTimer.run,
                id="env-timer-example",
                task_queue="time-skipping-examples",
            )
        await env.fast_forward(handle, timedelta(minutes=30))
        await env.fast_forward(handle)  # resume skipping until completion
        result = await handle.result()
```

### 2. Direct API

Use `Client.connect(...)` to connect directly to an existing Temporal server,
then wrap that client with `TimeSkipper`. This mode does not require a
`WorkflowEnvironment` or start a server. `TimeSkipper` is the SDK's client-side
helper: it configures workflow starts and calls the server APIs to advance time.
The existing namespace must have the server-side time-skipping feature enabled.
For local use, start the CLI from the branch above with
`temporal server start-dev --enable-time-skipping`.

```python
from datetime import timedelta

from temporalio.client import Client
from temporalio.testing import TimeSkipper
from temporalio.worker import Worker

from activities import dummy_activity
from workflow import WorkflowWithUserTimer

client = await Client.connect("localhost:7233", namespace="default")
skipper = TimeSkipper(client)
async with Worker(
    skipper.client,
    task_queue="time-skipping-examples",
    workflows=[WorkflowWithUserTimer],
    activities=[dummy_activity],
):
    with skipper.with_time_skipping_disabled():
        handle = await skipper.client.start_workflow(
            WorkflowWithUserTimer.run,
            id="direct-client-timer-example",
            task_queue="time-skipping-examples",
        )
    await skipper.fast_forward(handle, timedelta(minutes=30))
    await skipper.fast_forward(handle)  # resume skipping until completion
    result = await handle.result()
```

### How the tests exercise both modes

All 20 scenarios run twice (40 cases). The `ts_env` fixture supplies either the
new environment (`environment`) or a `TimeSkipper` wrapping a fresh, directly
connected client (`time-skipper`). The direct client tests share the local server
as test infrastructure; the direct API client itself does not depend on an
environment. These tests exercise the SDK helper, rather than hand-written
low-level WorkflowService RPC requests.

Both stamp a `TimeSkippingConfig` on newly started workflows. By default skipping
is enabled at workflow start; awaiting `result()` no longer unlocks a global clock.
For interaction tests, start the workflow inside `with_time_skipping_disabled()`
and then call `fast_forward(handle, timedelta(...))`.
The context affects **new starts only**, not already-running workflows.

`fast_forward(handle, duration)` waits for the bounded advance and auto-disables
skipping at its target. It returns `True` if that target is reached, or `False`
if the workflow chain terminates first. Omitting duration enables unbounded
skipping and waits for a terminal event (returns `False`). A later fast-forward
on the same workflow overrides an earlier one; do not issue concurrent advances
against the same handle. `get_current_time(handle)` reads that workflow's clock.
`env.sleep()` and handle-free `env.get_current_time()` are not V2 APIs.

## V1 coverage mapping

Each row runs in both modes using the shared parameterized test:
`pytest -k environment` selects Testing Env, and `pytest -k time-skipper` selects Direct API.

The V2 suite covers all 16 V1 tests and 4 manual lock probes. It reuses the
base workflows and activities from the repository root; activity/timer probe
workflows live in `activity_workflows.py` and `lock_workflows.py`.
For the original V1 exploration, see the [report](../manual_time_skipping_report.md).

| V1 scenario | V2 Testing Env | V2 Direct API |
| --- | --- | --- |
| User timer | `test_workflow.py::test_time_skipping_in_workflow_with_user_timer` | `test_workflow.py::test_time_skipping_in_workflow_with_user_timer` |
| Parent / child user timers | `test_workflow.py::test_time_skipping_in_parent_child_workflow_with_user_timer` | `test_workflow.py::test_time_skipping_in_parent_child_workflow_with_user_timer` |
| Workflow retry backoff | `test_workflow.py::test_workflow_retry_backoff` | `test_workflow.py::test_workflow_retry_backoff` |
| Run timeout | `test_workflow.py::test_workflow_times_out_after_run_timeout` | `test_workflow.py::test_workflow_times_out_after_run_timeout` |
| Execution timeout | `test_workflow.py::test_workflow_times_out_after_execution_timeout` | `test_workflow.py::test_workflow_times_out_after_execution_timeout` |
| Start delay | `test_workflow.py::test_workflow_start_delay` | `test_workflow.py::test_workflow_start_delay` |
| Activity retry backoff | `test_workflow.py::test_time_skipping_activity_retry_backoff` | `test_workflow.py::test_time_skipping_activity_retry_backoff` |
| Cron | `test_workflow.py::test_cron_runs_on_schedule` | `test_workflow.py::test_cron_runs_on_schedule` |
| Two signals at different times | `test_workflow.py::test_workflow_waiting_for_signals` | `test_workflow.py::test_workflow_waiting_for_signals` |
| Two children + parent approval | `test_workflow.py::test_parent_with_child_and_wait_condition` | `test_workflow.py::test_parent_with_child_and_wait_condition` |
| Child and parent signal windows | `test_workflow.py::test_parent_child_both_wait_on_condition` | `test_workflow.py::test_parent_child_both_wait_on_condition` |
| Skip inside activity / timer race | `test_activity_time_skipping.py::test_fast_forward_in_activity_cannot_fire_user_timer` | `test_activity_time_skipping.py::test_fast_forward_in_activity_cannot_fire_user_timer` |
| Skip while activity is running | `test_activity_time_skipping.py::test_fast_forward_blocks_while_activity_in_flight` | `test_activity_time_skipping.py::test_fast_forward_blocks_while_activity_in_flight` |
| Two advances from activity | `test_activity_time_skipping.py::test_two_fast_forwards_from_activity_fire_timers_after_activity` | `test_activity_time_skipping.py::test_two_fast_forwards_from_activity_fire_timers_after_activity` |
| Advance from test / held activity | `test_activity_time_skipping.py::test_fast_forward_from_test_waits_for_held_activity` | `test_activity_time_skipping.py::test_fast_forward_from_test_waits_for_held_activity` |
| Advance / pending workflow task | `test_workflow.py::test_fast_forward_from_test_waits_for_pending_workflow_task` | `test_workflow.py::test_fast_forward_from_test_waits_for_pending_workflow_task` |
| Manual probe 1: outside / 3 activities | `test_activity_time_skipping.py::test_1_outside_fast_forward_blocks_with_three_activities` | `test_activity_time_skipping.py::test_1_outside_fast_forward_blocks_with_three_activities` |
| Manual probe 2: inside / generous timeout | `test_activity_time_skipping.py::test_2_inside_fast_forward_waits_for_activities` | `test_activity_time_skipping.py::test_2_inside_fast_forward_waits_for_activities` |
| Manual probe 3: inside / short timeout | `test_activity_time_skipping.py::test_3_inside_fast_forward_cannot_force_start_to_close` | `test_activity_time_skipping.py::test_3_inside_fast_forward_cannot_force_start_to_close` |
| Manual probe 4: inside / timer race | `test_activity_time_skipping.py::test_4_inside_fast_forward_cannot_win_user_timer_race` | `test_activity_time_skipping.py::test_4_inside_fast_forward_cannot_win_user_timer_race` |

The four manual probes are included in the default V2 suite.
Schedules and standalone activities have no V1 tests; they are outside this
migration's coverage.

## Changed behavior

V2 uses independent workflow clocks and waits for in-flight work, including
activities, workflow tasks and child workflows. There is no global reference
counter that another `sleep()` can unlock. Therefore the V1 lock-breaking
examples intentionally have different expected outcomes:

- A fast-forward from either the activity or test remains blocked while an
  activity is held. Awaiting it inline inside that activity would deadlock until
  the activity times out. Tests create a task and release the activity explicitly.
- An activity completes before its racing timer, and a fast-forward longer than
  start-to-close does not immediately force that activity to time out.
- Two advances still accumulate, but timers fire after the held activity completes.
- A pending workflow task needs a worker before fast-forward can proceed.
- A parent does not acquire its children's skipped duration when they finish.
  The two-child approval example therefore targets +1.5h on the parent's clock
  (1h cooldown + 30m into approval), rather than V1's shared +3.5h timeline.
- Drive a child's signal window using its own handle before resuming its parent.

The suite checks these changes instead of attempting to preserve V1 unlock tricks.
