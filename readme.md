[![Tests](https://github.com/feiyang3cat/timeskipping-examples/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/feiyang3cat/timeskipping-examples/actions/workflows/test.yml)

This repo contains examples of time-skipping best practices in testing, along with anti-patterns to avoid.

## Testing features/scenarios

1. Skip user timers in *workflows*
2. Test retry policies (backoff) for *workflows*
3. Test workflow execution and run timeouts
4. Test workflow start delay for *workflows*
5. Test retry policies (backoff) for *activities*
6. Test waiting time for *crons*
7. Test waiting time for *schedulers*
8. Test retry backoffs and start delays for *standalone activities (SAA)*
9. Set a duration for `env.sleep()` to pause time-skipping after some time. This is useful for:
    - Sleep with schedulers
    - Sleep with retries and cron
    - Sleep for interactions: single workflow, workflow with child workflows

## Examples

### By Features

| # | Execution Type | Feature | v1 Example | v2 |
|---|---------------|---------|------------|-----|
| 1 | workflow | User Timers | [workflow](workflow.py#L14) · [test](v1/test_workflow.py#L50) | 🔜 |
| 2 | workflow | Workflow Retry | [workflow](workflow.py#L59) · [test](v1/test_workflow.py#L100) | 🔜 |
| 3 | workflow | Workflow Execution/Run Timeouts | [workflow](workflow.py#L62) · [test (auto-skip, no sleep)](v1/test_workflow.py#L146) | 🔜 |
| 4 | workflow | Workflow Start Delay | [workflow](workflow.py#L79) · [test](v1/test_workflow.py#L168) | 🔜 |
| 5 | workflow | Activity Retry | [workflow](workflow.py#L88) · [test](v1/test_workflow.py#L186) | 🔜 |
| 6 | workflow | Cron | [workflow](workflow.py#L108) · [test using sleep](v1/test_workflow.py#L232) | 🔜 |
| 7 | scheduler | Scheduler | ❌ Schedules API not supported in the SDK testing server | 🔜 |
| 8 | SAA | Retry, Timeout, Start Delay | ❌ SAA not supported in the SDK testing server | 🔜 |
| 9 | workflow | wait for condition/signals/updates | [workflow](workflow.py#L129) · [test using sleep](v1/test_workflow.py#L274) | 🔜 |

> 🔜 coming soon · v1: SDK testing server · v2: Temporal OSS server and Cloud

### More examples with sleep

#### Some complicated examples
- parent workflow waiting on signals with timeouts: [[workflow](workflow.py#L161)] · [[test](v1/test_workflow.py#L307)]
- parent + child, both waiting on their own signal windows: [[workflow](workflow.py#L204)] · [[test](v1/test_workflow.py#L347)]

#### Time-skipping gotchas
- user timer racing an in-flight activity — while an activity is running the server keeps its clock at real wall-clock speed, so the timer would not fire for real minutes. The mocked activity calls `env.sleep()` from inside itself to manually skip the virtual clock past the (shorter) user timer while still in flight, so the timer fires *first*: [[workflow](workflow.py#L248)] · [[test](v1/test_workflow.py#L383)]
- `env.sleep()` does **not** skip while an activity is in flight — with a 10s activity running, does `env.sleep(3s)` return instantly or after 3s? *After ~3 real seconds.* An in-flight activity holds a time-skipping lock, so an `env.sleep()` overlapping it elapses in real wall-clock time; only the portion after the activity finishes is skipped instantly: [[workflow](workflow.py#L283)] · [[test](v1/test_workflow.py#L431)]
- two `env.sleep()` calls inside one activity fire two user timers — a workflow waits on two user timers (1h, 2h) and one activity. Calling `env.sleep()` twice inside the activity *accumulates*: the first skip (+1h) fires the 1h timer, the second (+1h → 2h) fires the 2h timer, then the activity returns — completing in order `timer1 @1h → timer2 @2h → activity @2h`, all in ~2 real seconds: [[workflow](workflow.py#L304)] · [[test](v1/test_workflow.py#L484)]
- any time-skipping lock can be broken by `env.sleep()` — even one called from the test, not inside the activity. The server only keeps a Time Locking Counter (starts at 1, `handle.result()` unlocks it, each in-flight activity or pending workflow task locks it) and skips whenever it reaches 0, no matter who unlocks. So the call just needs the right *timing* (while `result()` is already being awaited) and the right *duration* (long enough to reach the next timer; a shorter skip locks again):
    - activity held in flight, two `env.sleep(1h)` from the test fire the 1h and 2h timers: [[workflow](workflow.py#L304)] · [[test](v1/test_workflow.py#L537)]
    - pending workflow task (no worker), `env.sleep(30m)` is too short, a second `env.sleep(30m)` reaches the 1h execution timeout: [[workflow](workflow.py#L62)] · [[test](v1/test_workflow.py#L589)]

#### Some anti-patterns
- [busy workflow](workflow.py#L251) — if no failures, retries, or crons are in place, this workflow is always busy with workflow tasks/activities and doesn't need time-skipping or sleep
- [unnecessary `env.sleep()`](workflow.py#L269) — this workflow can just keep time-skipping to the end
