#include "task_graph.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <queue>
#include <stdexcept>
#include <thread>
#include <utility>

int read_int(const json& object, const char* key, int low, int high)
{
    const auto& v = object.at(key);
    if (!v.is_number_integer())
        throw std::invalid_argument(std::string(key) + " must be an integer");

    if (v.is_number_unsigned() && v.get<unsigned long long>() > static_cast<unsigned long long>(high))
        throw std::invalid_argument(std::string(key) + " is out of range");
    long long n = v.get<long long>();
    if (n < low || n > high)
        throw std::invalid_argument(std::string(key) + " is out of range");
    return static_cast<int>(n);
}

std::string read_id(const json& object, const char* key)
{
    std::string s = object.at(key).get<std::string>();
    if (s.empty() || s.size() > 64)
        throw std::invalid_argument(std::string(key) + ": length must be 1..64");
    for (unsigned char c : s) {
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '_' || c == '-'))
            throw std::invalid_argument(std::string(key) + ": use letters, digits, _ or -");
    }
    return s;
}

Work parse_work(const json& value)
{
    Work work;
    work.id = read_id(value, "id");
    work.type = value.at("type").get<std::string>();
    work.args = value.at("args");
    if (!work.args.is_object())
        throw std::invalid_argument("args must be an object");
    if (work.type == "sleep") {
        read_int(work.args, "ms", 0, 30000);
    } else if (work.type == "sum_squares") {
        int from = read_int(work.args, "from", -1000000, 1000000);
        int to = read_int(work.args, "to", -1000000, 1000000);
        if (from > to || to - from + 1 > 1000000)
            throw std::invalid_argument("Invalid or oversized range");
    } else {
        throw std::invalid_argument("Unknown task type: " + work.type);
    }
    return work;
}

json execute_work(const Work& work)
{
    if (work.type == "sleep") {
        int ms = read_int(work.args, "ms", 0, 30000);
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        return "slept " + std::to_string(ms) + " ms";
    }
    if (work.type == "sum_squares") {
        int from = read_int(work.args, "from", -1000000, 1000000);
        int to = read_int(work.args, "to", -1000000, 1000000);
        if (from > to || to - from + 1 > 1000000)
            throw std::invalid_argument("Invalid range");
        long long sum = 0;
        for (long long x = from; x <= to; ++x)
            sum += x * x;
        return sum;
    }
    throw std::invalid_argument("Unknown job");
}

const char* state_name(State state)
{
    switch (state) {
    case State::PENDING: return "PENDING";
    case State::READY: return "READY";
    case State::RUNNING: return "RUNNING";
    case State::SUCCEEDED: return "SUCCEEDED";
    case State::FAILED: return "FAILED";
    case State::SKIPPED: return "SKIPPED";
    }
    return "?";
}

TaskGraph::TaskGraph(const json& input)
{
    const auto& list = input.at("tasks");
    if (!list.is_array() || list.size() > 10000)
        throw std::invalid_argument("tasks must be an array with at most 10000 tasks");
    for (const auto& value : list) {
        Task task;
        task.work = parse_work(value);
        if (ids.count(task.work.id))
            throw std::invalid_argument("Duplicate ID: " + task.work.id);
        ids[task.work.id] = nodes.size();
        nodes.push_back(std::move(task));
    }

    for (std::size_t i = 0; i < list.size(); ++i) {
        const auto& deps = list[i].at("depends_on");
        if (!deps.is_array())
            throw std::invalid_argument("depends_on must be an array");
        std::vector<std::size_t> seen;
        for (const auto& dep : deps) {
            std::size_t before = find(dep.get<std::string>());
            if (before == i)
                throw std::invalid_argument("A task cannot depend on itself");
            if (std::find(seen.begin(), seen.end(), before) != seen.end())
                throw std::invalid_argument("Duplicate dependency");
            seen.push_back(before);
            nodes[before].next.push_back(i);
            ++nodes[i].remaining;
        }
    }

    std::queue<std::size_t> q;
    std::vector<std::size_t> degree;
    for (std::size_t i = 0; i < nodes.size(); ++i) {
        degree.push_back(nodes[i].remaining);
        if (degree[i] == 0) {
            q.push(i);
            nodes[i].state = State::READY;
        }
    }
    std::size_t visited = 0;
    while (!q.empty()) {
        auto i = q.front();
        q.pop();
        ++visited;
        for (auto child : nodes[i].next) {
            if (--degree[child] == 0)
                q.push(child);
        }
    }
    if (visited != nodes.size())
        throw std::invalid_argument("Graph contains a cycle");
}

TaskGraph TaskGraph::load(const std::string& path)
{
    std::ifstream in(path);
    if (!in)
        throw std::runtime_error("Cannot open " + path);
    json input;
    in >> input;
    return TaskGraph(input);
}

std::size_t TaskGraph::find(const std::string& id) const
{
    auto it = ids.find(id);
    if (it == ids.end())
        throw std::invalid_argument("Unknown task: " + id);
    return it->second;
}

int TaskGraph::assign(std::size_t index, int worker)
{
    auto& task = nodes.at(index);
    if (task.state != State::READY || worker < 0)
        throw std::logic_error("Task is not ready or worker is invalid");
    task.state = State::RUNNING;
    task.owner = worker;
    return ++task.attempts;
}

void TaskGraph::skip_children(std::size_t index)
{
    std::vector<std::size_t> stack = {index};
    while (!stack.empty()) {
        auto i = stack.back();
        stack.pop_back();
        for (auto child : nodes[i].next) {
            if (nodes[child].state != State::PENDING)
                continue;
            nodes[child].state = State::SKIPPED;
            nodes[child].error = "A predecessor failed";
            ++finished;
            stack.push_back(child);
        }
    }
}

void TaskGraph::failed(std::size_t index, const std::string& error)
{
    auto& task = nodes[index];
    task.owner = -1;
    task.error = error;
    if (task.attempts < 3) {
        task.state = State::READY;
    } else {
        task.state = State::FAILED;
        ++finished;
        skip_children(index);
    }
}

bool TaskGraph::complete(std::size_t index, int worker, int attempt, bool ok,
                         const json& result, const std::string& error)
{
    auto& task = nodes.at(index);
    if (task.state != State::RUNNING || task.owner != worker || task.attempts != attempt)
        return false;
    if (!ok) {
        failed(index, error);
        return true;
    }
    task.state = State::SUCCEEDED;
    task.owner = -1;
    task.result = result;
    task.error.clear();
    ++finished;
    for (auto child : task.next) {
        if (nodes[child].state == State::PENDING && --nodes[child].remaining == 0)
            nodes[child].state = State::READY;
    }
    return true;
}

void TaskGraph::worker_lost(int worker, const std::string& reason)
{
    for (std::size_t i = 0; i < nodes.size(); ++i) {
        if (nodes[i].state == State::RUNNING && nodes[i].owner == worker)
            failed(i, reason);
    }
}

bool TaskGraph::successful() const
{
    if (!done())
        return false;
    for (const auto& task : nodes) {
        if (task.state != State::SUCCEEDED)
            return false;
    }
    return true;
}

json TaskGraph::report() const
{
    json list = json::array();
    for (const auto& task : nodes) {
        list.push_back({{"id", task.work.id}, {"state", state_name(task.state)},
                        {"attempts", task.attempts}, {"result", task.result}, {"error", task.error}});
    }
    return {{"completed", done()}, {"successful", successful()}, {"tasks", list}};
}
