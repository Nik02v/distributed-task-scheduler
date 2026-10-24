#pragma once

#include "json.hpp"

#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>

using json = nlohmann::json;

int read_int(const json& object, const char* key, int low, int high);
std::string read_id(const json& object, const char* key);

struct Work {
    std::string id;
    std::string type;
    json args;
};

Work parse_work(const json& value);
json execute_work(const Work& work);

enum class State { PENDING, READY, RUNNING, SUCCEEDED, FAILED, SKIPPED };
const char* state_name(State state);

struct Task {
    Work work;
    std::vector<std::size_t> next;
    std::size_t remaining = 0;
    State state = State::PENDING;
    int attempts = 0;
    int owner = -1;
    json result;
    std::string error;
};

class TaskGraph {
public:
    explicit TaskGraph(const json& input);
    static TaskGraph load(const std::string& path);

    const std::vector<Task>& tasks() const { return nodes; }
    std::size_t find(const std::string& id) const;
    int assign(std::size_t index, int worker);
    bool complete(std::size_t index, int worker, int attempt, bool ok,
                  const json& result, const std::string& error);
    void worker_lost(int worker, const std::string& reason);
    bool done() const { return finished == nodes.size(); }
    bool successful() const;
    json report() const;

private:
    void failed(std::size_t index, const std::string& error);
    void skip_children(std::size_t index);

    std::vector<Task> nodes;
    std::unordered_map<std::string, std::size_t> ids;
    std::size_t finished = 0;
};
