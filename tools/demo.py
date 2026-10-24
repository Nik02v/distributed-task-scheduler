import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
import time

def wait_log(process, path, pattern):
    end = time.monotonic() + 8
    while time.monotonic() < end:
        text = path.read_text(encoding="utf-8", errors="replace")
        found = re.search(pattern, text)
        if found:
            return found
        if process.poll() is not None:
            break
        time.sleep(0.01)
    raise RuntimeError("Missing expected log entry: " + pattern + "\n" + text)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin", default="build", help="directory containing the coordinator and worker executables")
    parser.add_argument("--output", default="demo-output")
    parser.add_argument("--crash", action="store_true", help="kill the first worker during task A")
    args = parser.parse_args()
    binaries = Path(args.bin).resolve()
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    project = Path(__file__).resolve().parents[1]
    graph = json.loads((project / "examples" / "demo.json").read_text())
    if args.crash:
        graph["tasks"][0]["args"]["ms"] = 1200
    source = output / "tasks.json"
    source.write_text(json.dumps(graph, indent=2), encoding="utf-8")
    report_file = output / "report.json"

    processes = []
    files = []
    def spawn(name, command):
        path = output / (name + ".log")
        log = path.open("wb")
        files.append(log)
        proc = subprocess.Popen([str(a) for a in command], stdout=log, stderr=subprocess.STDOUT)
        processes.append(proc)
        return proc, path

    coordinator_log = output / "coordinator.log"
    try:
        coordinator, coordinator_log = spawn("coordinator", [binaries / "coordinator.exe",
            "--tasks", source, "--port", 0, "--workers", 2, "--report", report_file,
            "--heartbeat-timeout-ms", 1500])
        port = wait_log(coordinator, coordinator_log, r"LISTENING (\d+)")[1]
        one, _ = spawn("one", [binaries / "worker.exe", "--id", "one",
                               "--port", port, "--threads", 2, "--heartbeat-ms", 200])
        wait_log(coordinator, coordinator_log, "REGISTER one")
        two, _ = spawn("two", [binaries / "worker.exe", "--id", "two",
                               "--port", port, "--threads", 2, "--heartbeat-ms", 200])
        if args.crash:
            wait_log(coordinator, coordinator_log, "ASSIGN A attempt=1 worker=one")
            print("Killing worker one during task A.", flush=True)
            one.kill()
            one.wait(timeout=5)
        if coordinator.wait(timeout=25) != 0:
            raise RuntimeError("Coordinator did not finish successfully")
        if two.wait(timeout=5) != 0 or (not args.crash and one.wait(timeout=5) != 0):
            raise RuntimeError("Worker did not shut down cleanly")
        report = json.loads(report_file.read_text())
        if not report["successful"] or len(report["tasks"]) != 20:
            raise RuntimeError("Invalid final report")
        if args.crash and report["tasks"][0]["attempts"] != 2:
            raise RuntimeError("A was not reassigned after the crash")
        print(coordinator_log.read_text(encoding="utf-8", errors="replace"))
        print("Report:", report_file)
        return 0
    except Exception as error:
        print("Demo:", error, file=sys.stderr)
        if coordinator_log.exists():
            print(coordinator_log.read_text(encoding="utf-8", errors="replace"), file=sys.stderr)
        return 1
    finally:
        for proc in processes:
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=5)
        for f in files:
            f.close()

if __name__ == "__main__":
    sys.exit(main())
