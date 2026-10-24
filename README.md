# Distributed Task Scheduler

One coordinator loads the graph, and worker processes execute tasks over TCP.
Each worker has a small thread pool. This C++17 project practices DAGs, sockets,
threads, and retrying work after a connection is lost.

Tasks calculate a sum of squares over a range or simulate work by sleeping for
a specified number of milliseconds. A dependency only requires the predecessor
to succeed; its result is not passed to the next task. There are no shell
commands, automatic coordinator election, or coordinator state recovery after
a crash.

## Build

Windows, CMake 3.20+, a C++17 compiler, and Python 3 for integration tests.
Sockets use Winsock. The only external C++ library is nlohmann/json, already
included in `third_party`; the build downloads nothing. Python scripts use only
the standard library. Add `-DBUILD_TESTING=OFF` to build without tests; the
scheduler itself does not require Python.

The commands below use CLion's bundled MinGW. Run them from this directory
in PowerShell:

```powershell
$toolRoot = Join-Path $env:LOCALAPPDATA 'Programs\CLion\bin'
$env:PATH = "$toolRoot\mingw\bin;$env:PATH"
& "$toolRoot\cmake\win\x64\bin\cmake.exe" -S . -B build-clion -G "MinGW Makefiles" "-DCMAKE_CXX_COMPILER=$toolRoot\mingw\bin\g++.exe" "-DCMAKE_MAKE_PROGRAM=$toolRoot\mingw\bin\mingw32-make.exe" -DCMAKE_BUILD_TYPE=Debug
& "$toolRoot\cmake\win\x64\bin\cmake.exe" --build build-clion -j 2
& "$toolRoot\cmake\win\x64\bin\ctest.exe" --test-dir build-clion --output-on-failure
python tools/demo.py --bin build-clion
python tools/demo.py --bin build-clion --crash
```

MSYS2 GCC 14.2.0 from the system PATH failed during linking in the previous
project, so CLion's GCC 13.1.0 is selected explicitly here.

## Manual execution

Use three PowerShell terminals, starting the coordinator before the workers:

```powershell
.\build-clion\coordinator.exe --tasks examples/demo.json --port 9000 --workers 2 --report report.json
.\build-clion\worker.exe --id one --port 9000 --threads 2
.\build-clion\worker.exe --id two --port 9000 --threads 2
```

`--workers 2` waits for two registrations before scheduling begins. Once work has
started, the remaining worker continues even if the other disappears. Workers
have distinct IDs, and the OS selects their client ports automatically. The
coordinator listens only on `127.0.0.1`. The worker's `--host` option accepts a
numeric IPv4 address.

To demonstrate retries after task failures, use two terminals:

```powershell
.\build-clion\coordinator.exe --tasks examples/failure.json --report failure-report.json
.\build-clion\worker.exe --id one --threads 2 --fail-task A
```

`--fail-task` deliberately simulates a failure: the worker returns an error for
that ID. After three assignments, A becomes FAILED, C/D/E become SKIPPED, and B
finishes normally. The coordinator returns exit code 1, while the worker shuts
down cleanly with code 0.

## Input

```json
{
  "tasks": [
    {"id":"A", "type":"sum_squares", "args":{"from":1,"to":10}, "depends_on":[]},
    {"id":"B", "type":"sleep", "args":{"ms":100}, "depends_on":[]},
    {"id":"C", "type":"sleep", "args":{"ms":0}, "depends_on":["A","B"]}
  ]
}
```

All four task fields are required. An ID contains 1 to 64 ASCII letters, digits,
`_`, or `-`. At most 10,000 tasks are allowed. Duplicate IDs, duplicate edges,
self-dependencies, unknown dependencies, and cycles are rejected before opening
the listening port. Kahn's algorithm checks for cycles. An empty graph completes
immediately.

`sum_squares` includes both bounds. Each bound must be between -1,000,000 and
1,000,000, and the range may contain at most 1,000,000 numbers, keeping the sum
within `long long`. `sleep.ms` must be an integer from 0 to 30,000. Floating-point
numbers and Boolean values are not accepted for these arguments.

## Scheduling and failure

States are PENDING, READY, RUNNING, SUCCEEDED, FAILED, and SKIPPED. The coordinator
visits ready tasks in input order and selects workers in round-robin order among
those with free slots. It never assigns more tasks than a worker's declared
capacity.

The first assignment has `attempt_id=1`. A task error, failed send, EOF, or
heartbeat timeout consumes that attempt. If fewer than three attempts have been
made, the task returns to READY. Only the third failure makes it FAILED and skips
its descendants. A shared descendant of several failed branches is counted once;
independent branches continue.

Each TCP connection receives its own internal number. RESULT is accepted only
when the task ID, attempt_id, owning connection, and RUNNING state still match.
A duplicate or stale result logs IGNORE and does not release another slot.
Reusing a worker name after disconnection does not let the old connection
complete a new attempt.

By default, a worker sends a heartbeat every 1000 ms. The coordinator timeout is
5000 ms; intervals use `steady_clock`. Configure them with the worker's
`--heartbeat-ms` and coordinator's `--heartbeat-timeout-ms`. Choose a timeout
well above the heartbeat interval. Long tasks do not require a longer timeout:
heartbeats have their own thread.

`--idle-timeout-ms` (default 10000) limits waiting when there are no workers or
not enough initial workers for `--workers`. In that case the report is incomplete
and the exit code is 2; an unassigned task consumes no attempt. Otherwise, the
coordinator waits for every task to reach a terminal state.

At the end, the terminal shows each task's state, attempt count, result, and any
error. `--report` also writes JSON containing `completed`, `successful`, and a
`tasks` array. Exit codes are 0 when all tasks succeed, 1 for a completed graph
with failures, and 2 for input/network errors or an incomplete graph.

## Threads and locks

- `task_graph.*`: graph description and state. The graph has no locks; only the
  coordinator's main thread modifies it.
- `coordinator.cpp`: main loop, one reader thread per connection, and an event
  queue. Reader threads only receive messages and append events under
  `queue_mutex`. Only the main thread sends ASSIGN/SHUTDOWN and updates capacity,
  deadlines, and the DAG.
- `worker.cpp`: receiver thread (main), heartbeat thread, and pool. `send_mutex`
  covers an entire message, including prefix and body. `heartbeat_mutex` is only
  used for waiting/shutdown and is released before sending. These two locks are
  never held together. Active job count and connection status are atomic.
- `thread_pool.*`: function queue, mutex, and condition variable. Jobs run outside
  the queue lock; the worker lambda catches exceptions and turns them into RESULT.
- `net.*`, `protocol.*`: RAII sockets and message I/O. One thread reads a given
  socket. Its descriptor is closed only after the threads using it are joined.

On close, the pool drains its queue and joins its threads. The coordinator sends
SHUTDOWN, shuts down the write direction, and briefly waits for workers to close
the connection before shutting down reception. Readers check the shutdown flag
using select with a 100 ms timeout, so they do not depend on the peer voluntarily
closing its connection. Socket sends have a one-second timeout. `queue_mutex` is
not held during network sends or joins.

## Protocol

A message is a JSON object: four bytes of length in network byte order, followed
by that many UTF-8 bytes. Lengths from 1 to 1,048,576 bytes are allowed. Loops
handle partial sends/receives, multiple messages arriving together, and a
connection closing partway through a message.

| type | Important fields |
|---|---|
| REGISTER | worker_id, capacity |
| ASSIGN | task_id, attempt_id, task: {id, type, args} |
| RESULT | task_id, attempt_id, ok, result (success) or error (failure) |
| HEARTBEAT | no additional fields |
| SHUTDOWN | no additional fields |

A worker's first message must be REGISTER. The coordinator accepts up to 32
active workers with up to 64 threads each. To keep reader thread storage simple,
there is a limit of 256 total connections per run. Invalid connections are
terminated, and their running tasks follow the same retry path as a crash.

## Demo and tests

`examples/demo.json` contains 20 tasks with branching and merging. `tools/demo.py`
starts all three processes and saves logs and a JSON report in `demo-output`.
With `--crash`, it waits for A's first assignment to worker one, kills that
process, and checks that A succeeds on its second attempt. Change the output
directory with `--output`.

`scheduler_tests` uses a small CHECK runner without an external test framework.
`tests/integration.py` uses Python unittest, real processes, and fake workers to
control message ordering. It also checks parallel assignments before any RESULT,
capacity limits, heartbeats during work, process crashes, exhausted attempts,
stale results, and invalid/fragmented messages. All test waits have timeouts.

Local test results are recorded in `TESTING.md`.

Built-in jobs have no external side effects. Execution may occur more than once,
with at most three assignments; it is not exactly-once. An old worker may keep
running after its connection is lost, which is why attempts are checked. A
coordinator crash ends the entire run. There is no durable log, dynamic graph
modification, or file transfer.
