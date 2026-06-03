
This repo shares examples of time skipping best practice in testing, and also the anti-patterns that should be avoided.

## Testing scenarios and examples

- Skip user timers in testing *workflows*
- Testing workflow execution and run timeout
- Testing retry backoff and help test retry policy for  *workflows* and *activities*
- Testing retry backoffs and start delays for *standalone activities (SAA)*
- Testing waiting time for *crons* and *schedulers*
- The users can set a user-specified duration for one execution (e.g.`sleep(1hour)`) and pause time-skipping when virtual time reaching the end point of the sleep. This control is helpful in a bunch of different scenarios:
    - sleep for one 1hour to check how many runs the execution with a cron/retry policy has run in 1hour
    - sleep stops time skipping in the middle of a workflow so that users can interact with the workflow via signals, updates, and queries — rather than letting time-skipping run the workflow to completion uninterrupted

## the sleep method: best practice and anti-patterns

- sleep with schedulers
- sleep with retries and cron
- sleep for interactions: single workflow, workflow with child workflows
