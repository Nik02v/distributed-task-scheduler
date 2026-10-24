#include "task_graph.h"
#include "thread_pool.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <utility>

using namespace std::chrono_literals;
#define CHECK(expr) do { if (!(expr)) throw std::runtime_error( \
    std::string(__FILE__) + ":" + std::to_string(__LINE__) + " " #expr); } while (false)

template<class F>
void rejected(F f)
{
    bool caught = false;
    try { f(); } catch (const std::exception&) { caught = true; }
    CHECK(caught);
}

json task(const std::string& id, json deps = json::array())
{
    return {{"id", id}, {"type", "sleep"}, {"args", {{"ms", 0}}}, {"depends_on", deps}};
}

void validation()
{
    TaskGraph empty(json{{"tasks", json::array()}});
    CHECK(empty.done() && empty.successful());
    CHECK(empty.report()["tasks"].empty());
    rejected([] { TaskGraph g(json{{"tasks", {task("a"), task("a")}}}); });
    rejected([] { TaskGraph g(json{{"tasks", {task("a", {"missing"})}}}); });
    rejected([] { TaskGraph g(json{{"tasks", {task("a", {"a"})}}}); });
    rejected([] { TaskGraph g(json{{"tasks", {task("a"), task("b", {"a", "a"})}}}); });
    rejected([] { TaskGraph g(json{{"tasks", {task("a", {"b"}), task("b", {"a"})}}}); });
    rejected([] { TaskGraph g(json{{"tasks", "not an array"}}); });
    rejected([] { TaskGraph g(json{{"tasks", {task("")}}}); });
    rejected([] { TaskGraph g(json{{"tasks", {task("bad id")}}}); });
    auto bad = task("a");
    bad["args"]["ms"] = -1;
    rejected([&] { parse_work(bad); });
    bad["args"]["ms"] = 1.5;
    rejected([&] { parse_work(bad); });
    bad["args"]["ms"] = true;
    rejected([&] { parse_work(bad); });
    bad["args"]["ms"] = 18446744073709551615ULL;
    rejected([&] { parse_work(bad); });
    bad["type"] = "shell";
    rejected([&] { parse_work(bad); });
}

void calculations()
{
    json sum = {{"id", "sum"}, {"type", "sum_squares"}, {"args", {{"from", -2}, {"to", 2}}}};
    CHECK(execute_work(parse_work(sum)) == 10);
    sum["args"] = {{"from", 1}, {"to", 10}};
    CHECK(execute_work(parse_work(sum)) == 385);
    sum["args"] = {{"from", 1000000}, {"to", 1000000}};
    CHECK(execute_work(parse_work(sum)) == 1000000000000LL);
    sum["args"] = {{"from", 2}, {"to", 1}};
    rejected([&] { parse_work(sum); });
    sum["args"] = {{"from", -1000000}, {"to", 1000000}};
    rejected([&] { parse_work(sum); });
    CHECK(execute_work(parse_work(task("sleep"))) == "slept 0 ms");
}

void diamond()
{
    TaskGraph g(json{{"tasks", {task("a"), task("b", {"a"}), task("c", {"a"}), task("d", {"b", "c"}), task("independent")}}});
    CHECK(g.tasks()[0].state == State::READY);
    CHECK(g.tasks()[1].state == State::PENDING);
    rejected([&] { g.assign(1, 0); });
    int attempt = g.assign(0, 0);
    CHECK(attempt == 1);
    rejected([&] { g.assign(0, 0); });
    CHECK(g.complete(0, 0, 1, true, 10, ""));
    CHECK(g.tasks()[1].state == State::READY && g.tasks()[2].state == State::READY);
    g.assign(1, 0);
    g.assign(2, 1);
    CHECK(g.complete(1, 0, 1, true, 20, ""));
    CHECK(g.tasks()[3].state == State::PENDING);
    CHECK(g.complete(2, 1, 1, true, 30, ""));
    CHECK(g.tasks()[3].state == State::READY);
    g.assign(3, 1);
    CHECK(g.complete(3, 1, 1, true, 40, ""));
    CHECK(!g.done());
    g.assign(4, 0);
    CHECK(g.complete(4, 0, 1, true, "ok", ""));
    CHECK(g.done() && g.successful());
    CHECK(!g.complete(4, 0, 1, true, "overwrite", ""));
    CHECK(g.report()["tasks"][4]["result"] == "ok");
}

void retry_and_stale()
{
    TaskGraph g(json{{"tasks", {task("a"), task("b", {"a"})}}});
    g.assign(0, 0);
    g.worker_lost(0, "worker died");
    CHECK(g.tasks()[0].state == State::READY);
    CHECK(g.tasks()[0].attempts == 1);
    CHECK(g.assign(0, 1) == 2);
    CHECK(!g.complete(0, 0, 1, true, "stale", ""));
    CHECK(!g.complete(0, 1, 1, true, "old attempt", ""));
    CHECK(!g.complete(0, 0, 2, true, "wrong owner", ""));
    CHECK(g.tasks()[1].state == State::PENDING);
    CHECK(g.complete(0, 1, 2, false, nullptr, "error"));
    CHECK(g.assign(0, 1) == 3);
    CHECK(g.complete(0, 1, 3, true, 123, ""));
    CHECK(g.tasks()[0].error.empty());
    CHECK(g.tasks()[0].result == 123);
    CHECK(g.tasks()[1].state == State::READY);
}

void failed_and_skipped()
{
    TaskGraph g(json{{"tasks", {task("a"), task("b"), task("c", {"a", "b"}), task("d", {"c"}), task("ok")}}});
    for (int attempt = 1; attempt <= 3; ++attempt) {
        CHECK(g.assign(0, 0) == attempt);
        CHECK(g.assign(1, 1) == attempt);
        g.worker_lost(0, "lost");
        CHECK(g.complete(1, 1, attempt, false, nullptr, "failed"));
    }
    CHECK(g.tasks()[0].state == State::FAILED);
    CHECK(g.tasks()[1].state == State::FAILED);
    CHECK(g.tasks()[2].state == State::SKIPPED);
    CHECK(g.tasks()[3].state == State::SKIPPED);
    CHECK(g.tasks()[2].attempts == 0);
    CHECK(!g.done());
    g.assign(4, 0);
    CHECK(g.complete(4, 0, 1, true, "ok", ""));
    CHECK(g.done() && !g.successful());
    CHECK(g.report()["tasks"].size() == 5);
    g.worker_lost(0, "again");
    CHECK(g.done());
}

void pool_drains()
{
    rejected([] { ThreadPool pool(0); });
    for (int threads : {1, 2, 4}) {
        std::atomic<int> calls{0};
        ThreadPool pool(threads);
        for (int i = 0; i < 200; ++i)
            pool.submit([&] { ++calls; });
        pool.close();
        CHECK(calls == 200);
        pool.close();
        rejected([&] { pool.submit([] {}); });
    }
    { ThreadPool unused(2); }
}

void pool_parallel()
{
    std::mutex m;
    std::condition_variable cv;
    int entered = 0;
    bool release = false;
    std::atomic<int> timeouts{0};
    ThreadPool pool(2);
    for (int i = 0; i < 2; ++i) {
        pool.submit([&] {
            std::unique_lock<std::mutex> lock(m);
            ++entered;
            cv.notify_all();
            if (!cv.wait_for(lock, 2s, [&] { return release; }))
                ++timeouts;
        });
    }
    bool parallel;
    {
        std::unique_lock<std::mutex> lock(m);
        parallel = cv.wait_for(lock, 2s, [&] { return entered == 2; });
        release = true;
    }
    cv.notify_all();
    pool.close();
    CHECK(parallel && timeouts == 0);
}

int main()
{
    const std::pair<const char*, void(*)()> tests[] = {
        {"validation", validation}, {"calculations and input bounds", calculations},
        {"diamond and disconnected node", diamond}, {"retry and stale results", retry_and_stale},
        {"failed and shared skipped descendants", failed_and_skipped},
        {"pool drains and closes", pool_drains}, {"pool parallel work", pool_parallel}
    };
    int failed = 0;
    for (const auto& test : tests) {
        try {
            test.second();
            std::cout << "[OK] " << test.first << '\n';
        } catch (const std::exception& e) {
            ++failed;
            std::cerr << "[FAIL] " << test.first << ": " << e.what() << '\n';
        }
    }
    return failed ? 1 : 0;
}
