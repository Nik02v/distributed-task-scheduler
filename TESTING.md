# Verification

Windows, CLion MinGW GCC 13.1.0, C++17, Debug, CMake, and Python 3.12.7.
The build passed with `-Wall -Wextra -Wpedantic -Werror`. Tested executables are
in `build-clion`.

```text
ctest --test-dir build-clion --output-on-failure

1/2 Test #1: unit ............. Passed
2/2 Test #2: integration ...... Passed
100% tests passed, 0 tests failed out of 2
```

The unit executable has 7 groups: validation, calculations and input bounds,
diamond, retries/stale results, FAILED/SKIPPED with shared descendants, pool
draining, and coordinated parallel execution.

The integration unittest suite has 14 tests:

1. A 20-task demo with two real worker processes and dependency checks from logs.
2. Two assignments before the first RESULT, with capacity limits enforced.
3. Fragmented prefixes/bodies and multiple messages arriving together.
4. Stale and duplicate results do not overwrite a newer result.
5. A result from another connection is ignored.
6. A worker process is killed mid-task and another process continues the work.
7. A worker without heartbeats loses its assignment; a healthy worker continues.
8. A real worker returns three failures; descendants are SKIPPED and an independent
   branch succeeds.
9. Three consecutive disconnects consume three attempts.
10. Zero/oversized lengths, truncated bodies, and invalid JSON do not crash the
    coordinator.
11. Waiting without workers has a timeout and produces an incomplete report.
12. A cycle is rejected before opening the port.
13. An empty graph requires no workers.
14. A real worker receives fragmented jobs, sends heartbeats during a task, and
    shuts down cleanly on SHUTDOWN.

Both demonstration scenarios were also run separately:

```powershell
python tools/demo.py --bin build-clion --output demo-output/normal
python tools/demo.py --bin build-clion --crash --output demo-output/crash
```

Both finished with exit code 0 and all 20 tasks SUCCEEDED. In the crash scenario,
the log contains `ASSIGN A attempt=1 worker=one`, the loss of worker one, and then
`ASSIGN A attempt=2 worker=two`. A's final result is `slept 1200 ms`, with two
attempts. In the normal scenario, every task has one attempt. Logs and JSON
reports from these runs are kept in the listed directories.

## English text update verification

After removing comments and translating project text, the Debug build succeeded.
Both CTest entries passed: the 7 unit groups and all 14 integration tests.
The normal and worker-crash demos also passed, and their saved output was refreshed.
Source comparison confirmed that C++ and Python code structure was unchanged
apart from removing comments/docstrings and translating string literals.
