import argparse
import json
from pathlib import Path
import re
import select
import socket
import struct
import subprocess
import sys
import tempfile
import time
import unittest

def packet(message):
    data = json.dumps(message).encode("utf-8")
    return struct.pack("!I", len(data)) + data

def receive(sock):
    def exact(count):
        data = b""
        while len(data) < count:
            part = sock.recv(count - len(data))
            if not part:
                raise EOFError("Connection closed during a TCP message")
            data += part
        return data
    size, = struct.unpack("!I", exact(4))
    if not 0 < size <= 1024 * 1024:
        raise ValueError("Invalid length")
    return json.loads(exact(size))

def sleep_task(name, ms=0, deps=()):
    return {"id": name, "type": "sleep", "args": {"ms": ms}, "depends_on": list(deps)}

def response(assignment, value="ok", ok=True):
    msg = {"type": "RESULT", "task_id": assignment["task_id"],
           "attempt_id": assignment["attempt_id"], "ok": ok}
    msg["result" if ok else "error"] = value
    return msg

class Integration(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="scheduler-test-")
        self.root = Path(self.temp.name)
        self.processes = []
        self.files = []
        self.sockets = []

    def tearDown(self):
        for sock in self.sockets:
            sock.close()
        for proc, log in self.processes:
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=5)
        for f in self.files:
            f.close()

        self.temp.cleanup()

    def spawn(self, name, args):
        path = self.root / (name + ".log")
        out = path.open("wb")
        self.files.append(out)
        proc = subprocess.Popen([str(a) for a in args], stdout=out, stderr=subprocess.STDOUT)
        self.processes.append((proc, path))
        return proc, path

    def text(self, path):
        return path.read_text(encoding="utf-8", errors="replace")

    def wait_log(self, proc, path, pattern, timeout=5):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            text = self.text(path)
            found = re.search(pattern, text)
            if found:
                return found
            if proc.poll() is not None:
                break
            time.sleep(0.01)
        self.fail("Missing log entry: " + pattern + "\n" + self.text(path))

    def start_coordinator(self, tasks, workers=1, heartbeat=1000, idle=4000):
        source = self.root / "tasks.json"
        source.write_text(json.dumps({"tasks": tasks}), encoding="utf-8")
        self.report_path = self.root / "report.json"
        self.coordinator, self.coordinator_log = self.spawn("coordinator", [COORDINATOR,
            "--tasks", source, "--port", "0", "--workers", workers,
            "--heartbeat-timeout-ms", heartbeat, "--idle-timeout-ms", idle,
            "--report", self.report_path])
        found = self.wait_log(self.coordinator, self.coordinator_log, r"LISTENING (\d+)")
        self.port = int(found[1])

    def real_worker(self, name, threads=1, extra=()):
        return self.spawn(name, [WORKER, "--id", name, "--port", self.port,
                                "--threads", threads, "--heartbeat-ms", 50, *extra])

    def fake_worker(self, name, capacity=1, fragmented=False):
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=4)
        self.sockets.append(sock)
        data = packet({"type": "REGISTER", "worker_id": name, "capacity": capacity})
        if fragmented:

            for i in range(0, len(data), 3):
                sock.sendall(data[i:i + 3])
                time.sleep(0.001)
        else:
            sock.sendall(data)
        return sock

    def finish(self, code=0):
        try:
            actual = self.coordinator.wait(timeout=12)
        except subprocess.TimeoutExpired:
            self.fail("Coordinator did not exit\n" + self.text(self.coordinator_log))
        self.assertEqual(actual, code, self.text(self.coordinator_log))
        return json.loads(self.report_path.read_text(encoding="utf-8"))

    def worker_stopped(self, proc, path):
        self.assertEqual(proc.wait(timeout=5), 0, self.text(path))
        self.assertIn("STOPPED", self.text(path))

    def test_demo_twenty_tasks_two_real_workers(self):
        source = Path(__file__).resolve().parents[1] / "examples" / "demo.json"
        tasks = json.loads(source.read_text())["tasks"]
        self.start_coordinator(tasks, workers=2)
        one = self.real_worker("one", threads=2)
        two = self.real_worker("two", threads=2)
        report = self.finish()
        self.assertTrue(report["completed"] and report["successful"])
        self.assertEqual(len(report["tasks"]), 20)
        log = self.text(self.coordinator_log)
        self.assertRegex(log, r"ASSIGN .*worker=one")
        self.assertRegex(log, r"ASSIGN .*worker=two")
        for t, r in zip(tasks, report["tasks"]):
            self.assertEqual(r["state"], "SUCCEEDED")
            self.assertEqual(r["attempts"], 1)
            if t["type"] == "sum_squares":
                args = t["args"]
                self.assertEqual(r["result"], sum(x*x for x in range(args["from"], args["to"] + 1)))
            for parent in t["depends_on"]:
                self.assertLess(log.index("RESULT " + parent + " "), log.index("ASSIGN " + t["id"] + " "))
        self.worker_stopped(*one)
        self.worker_stopped(*two)

    def test_parallel_assignments_and_capacity(self):
        self.start_coordinator([sleep_task("A"), sleep_task("B"), sleep_task("C")], workers=2)
        one = self.fake_worker("one")
        two = self.fake_worker("two")
        a = receive(one)
        b = receive(two)
        self.assertEqual({a["task_id"], b["task_id"]}, {"A", "B"})

        self.assertFalse(select.select([one, two], [], [], 0.08)[0])
        one.sendall(packet(response(a)))
        c = receive(one)
        self.assertEqual(c["task_id"], "C")
        two.sendall(packet(response(b)))
        one.sendall(packet(response(c)))
        self.assertTrue(self.finish()["successful"])

    def test_fragmented_and_coalesced_frames(self):
        self.start_coordinator([sleep_task("A"), sleep_task("B")])
        sock = self.fake_worker("fragments", capacity=2, fragmented=True)
        a, b = receive(sock), receive(sock)
        sock.sendall(packet(response(a)) + packet({"type": "HEARTBEAT"}) + packet(response(b)))
        self.assertTrue(self.finish()["successful"])

    def test_stale_result_does_not_overwrite_retry(self):
        self.start_coordinator([sleep_task("A"), sleep_task("B", deps=["A"])])
        sock = self.fake_worker("one")
        first = receive(sock)
        sock.sendall(packet(response(first, "try again", ok=False)))
        second = receive(sock)
        self.assertEqual(second["attempt_id"], 2)
        sock.sendall(packet(response(first, 999)) + packet(response(second, 123)))
        child = receive(sock)
        sock.sendall(packet(response(second, 888)) + packet(response(child, "child")))
        report = self.finish()
        self.assertEqual(report["tasks"][0]["result"], 123)
        self.assertEqual(report["tasks"][0]["attempts"], 2)
        self.assertEqual(self.text(self.coordinator_log).count("IGNORE A"), 2)

    def test_result_from_wrong_worker_is_ignored(self):
        self.start_coordinator([sleep_task("A")], workers=2)
        one = self.fake_worker("owner")
        self.wait_log(self.coordinator, self.coordinator_log, "REGISTER owner")
        two = self.fake_worker("other")
        assignment = receive(one)
        two.sendall(packet(response(assignment, 999)))
        self.wait_log(self.coordinator, self.coordinator_log, "IGNORE A")
        one.sendall(packet(response(assignment, 55)))
        self.assertEqual(self.finish()["tasks"][0]["result"], 55)

    def test_killed_worker_is_retried_on_other_process(self):
        self.start_coordinator([sleep_task("A", ms=1200), sleep_task("B", deps=["A"])], workers=2)
        one, log_one = self.real_worker("one")
        self.wait_log(self.coordinator, self.coordinator_log, "REGISTER one")
        two = self.real_worker("two")
        self.wait_log(self.coordinator, self.coordinator_log, "ASSIGN A attempt=1 worker=one")
        one.kill()
        one.wait(timeout=5)
        report = self.finish()
        self.assertTrue(report["successful"])
        self.assertEqual(report["tasks"][0]["attempts"], 2)
        self.assertIn("ASSIGN A attempt=2 worker=two", self.text(self.coordinator_log))
        self.worker_stopped(*two)

    def test_heartbeat_timeout_reassigns_work(self):
        self.start_coordinator([sleep_task("A")], workers=2, heartbeat=350)
        silent = self.fake_worker("silent")
        self.wait_log(self.coordinator, self.coordinator_log, "REGISTER silent")
        healthy = self.real_worker("healthy")
        self.assertEqual(receive(silent)["attempt_id"], 1)
        report = self.finish()
        self.assertTrue(report["successful"])
        self.assertEqual(report["tasks"][0]["attempts"], 2)
        self.assertIn("LOST silent reason=heartbeat timeout", self.text(self.coordinator_log))
        self.worker_stopped(*healthy)

    def test_real_worker_failures_skip_descendants(self):
        self.start_coordinator([sleep_task("A"), sleep_task("B"),
            sleep_task("C", deps=["A"]), sleep_task("D", deps=["A", "B"]),
            sleep_task("E", deps=["C", "D"])])
        worker = self.real_worker("failing", threads=2, extra=["--fail-task", "A"])
        report = self.finish(code=1)
        self.assertTrue(report["completed"])
        self.assertFalse(report["successful"])
        self.assertEqual([r["state"] for r in report["tasks"]],
                         ["FAILED", "SUCCEEDED", "SKIPPED", "SKIPPED", "SKIPPED"])
        self.assertEqual([r["attempts"] for r in report["tasks"]], [3, 1, 0, 0, 0])
        self.worker_stopped(*worker)

    def test_three_disconnects_exhaust_attempts(self):
        self.start_coordinator([sleep_task("A"), sleep_task("B", deps=["A"])])
        for attempt in range(1, 4):
            sock = self.fake_worker("lost" + str(attempt))
            assignment = receive(sock)
            self.assertEqual(assignment["attempt_id"], attempt)
            sock.close()
            self.wait_log(self.coordinator, self.coordinator_log, "LOST lost" + str(attempt))
        report = self.finish(code=1)
        self.assertEqual(report["tasks"][0]["state"], "FAILED")
        self.assertEqual(report["tasks"][0]["attempts"], 3)
        self.assertEqual(report["tasks"][1]["state"], "SKIPPED")

    def test_bad_frames_do_not_stop_coordinator(self):
        self.start_coordinator([sleep_task("A")], workers=2)
        bad_frames = [struct.pack("!I", 1024 * 1024 + 1), struct.pack("!I", 0),
                      struct.pack("!I", 10) + b"{", struct.pack("!I", 1) + b"!"]
        for i, frame in enumerate(bad_frames):
            name = "bad" + str(i)
            sock = self.fake_worker(name)
            sock.sendall(frame)
            sock.shutdown(socket.SHUT_WR)
            self.wait_log(self.coordinator, self.coordinator_log, "LOST " + name)
            sock.close()
        one = self.real_worker("one")
        two = self.real_worker("two")
        report = self.finish()
        self.assertTrue(report["successful"])
        self.assertEqual(report["tasks"][0]["attempts"], 1)
        self.worker_stopped(*one)
        self.worker_stopped(*two)

    def test_no_worker_timeout_gives_incomplete_report(self):
        self.start_coordinator([sleep_task("A")], idle=150)
        report = self.finish(code=2)
        self.assertFalse(report["completed"])
        self.assertEqual(report["tasks"][0]["state"], "READY")
        self.assertEqual(report["tasks"][0]["attempts"], 0)

    def test_cycle_rejected_before_listening(self):
        source = self.root / "cycle.json"
        source.write_text(json.dumps({"tasks": [sleep_task("A", deps=["B"]),
                                                 sleep_task("B", deps=["A"])]}))
        proc, path = self.spawn("invalid", [COORDINATOR, "--tasks", source, "--port", 0])
        self.assertEqual(proc.wait(timeout=5), 2)
        text = self.text(path)
        self.assertIn("cycle", text)
        self.assertNotIn("LISTENING", text)

    def test_empty_graph_needs_no_workers(self):
        self.start_coordinator([])
        report = self.finish()
        self.assertTrue(report["completed"] and report["successful"])
        self.assertEqual(report["tasks"], [])

    def test_worker_receives_fragmented_jobs_and_sends_heartbeats(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            listener.settimeout(4)
            self.port = listener.getsockname()[1]
            worker = self.real_worker("one", threads=2)
            sock, _ = listener.accept()
            self.sockets.append(sock)
            sock.settimeout(4)
            register = receive(sock)
            self.assertEqual(register, {"type": "REGISTER", "worker_id": "one", "capacity": 2})
            assignments = []
            for name, ms in [("A", 400), ("B", 0)]:
                assignments.append({"type": "ASSIGN", "task_id": name, "attempt_id": 1,
                                    "task": sleep_task(name, ms=ms)})
            data = b"".join(packet(a) for a in assignments)
            for i in range(0, len(data), 7):
                sock.sendall(data[i:i + 7])
            results = {}
            heartbeat_before_a = False
            while len(results) < 2:
                msg = receive(sock)
                if msg["type"] == "HEARTBEAT" and "A" not in results:
                    heartbeat_before_a = True
                elif msg["type"] == "RESULT":
                    self.assertTrue(msg["ok"])
                    self.assertEqual(msg["attempt_id"], 1)
                    results[msg["task_id"]] = msg["result"]
            self.assertTrue(heartbeat_before_a)
            self.assertEqual(results, {"A": "slept 400 ms", "B": "slept 0 ms"})
            sock.sendall(packet({"type": "SHUTDOWN"}))
            self.worker_stopped(*worker)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--coordinator", required=True)
    parser.add_argument("--worker", required=True)
    args, rest = parser.parse_known_args()
    COORDINATOR = str(Path(args.coordinator).resolve())
    WORKER = str(Path(args.worker).resolve())
    unittest.main(argv=[sys.argv[0], *rest], verbosity=2)
