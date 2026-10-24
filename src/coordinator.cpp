#include "cli.h"
#include "protocol.h"

#include <chrono>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>

using Clock = std::chrono::steady_clock;

struct Options {
    std::string file;
    std::string report_file;
    int port = 9000;
    int workers = 1;
    int heartbeat_timeout = 5000;
    int idle_timeout = 10000;
};

struct Event {
    int connection;
    json message;
    std::string error;
    Clock::time_point received;
};

struct Connection {
    explicit Connection(Socket socket) : socket(std::move(socket)) {}
    Socket socket;
    std::thread reader;
    std::string id;
    int capacity = 0;
    int busy = 0;
    bool registered = false;
    bool alive = true;
    bool reader_finished = false;
    Clock::time_point last_seen = Clock::now();
};

class Coordinator {
public:
    Coordinator(TaskGraph graph, Options options)
        : graph(std::move(graph)), options(std::move(options)), listener(listen_local(this->options.port)) {}

    ~Coordinator()
    {

        for (auto& c : connections)
            c->socket.shutdown_all();
        for (auto& c : connections) {
            if (c->reader.joinable())
                c->reader.join();
        }
    }

    int run()
    {
        std::cout << "LISTENING " << local_port(listener) << std::endl;
        try {
            loop();
        } catch (...) {
            stop_workers();
            print_report();
            throw;
        }
        stop_workers();
        print_report();
        return graph.successful() ? 0 : 1;
    }

private:
    void push(Event event)
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        events.push_back(std::move(event));
    }

    void receive_loop(int index, Connection* connection)
    {
        try {
            json message;
            while (receive_message(connection->socket, message))
                push({index, std::move(message), "", Clock::now()});
            push({index, {}, "EOF", Clock::now()});
        } catch (const std::exception& e) {
            push({index, {}, e.what(), Clock::now()});
        }
        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            connection->reader_finished = true;
        }
        readers_cv.notify_all();
    }

    void accept_worker()
    {
        Socket socket = accept_one(listener);
        if (connections.size() >= 256) {

            socket.shutdown_all();
            return;
        }
        int index = static_cast<int>(connections.size());
        auto connection = std::make_unique<Connection>(std::move(socket));
        Connection* ptr = connection.get();
        connections.push_back(std::move(connection));

        ptr->reader = std::thread(&Coordinator::receive_loop, this, index, ptr);
    }

    int active_workers() const
    {
        int n = 0;
        for (const auto& c : connections) {
            if (c->alive && c->registered)
                ++n;
        }
        return n;
    }

    void lose_worker(int index, const std::string& reason)
    {
        auto& c = *connections[index];
        if (!c.alive)
            return;
        c.alive = false;
        c.socket.shutdown_all();
        graph.worker_lost(index, reason);
        c.busy = 0;
        std::cout << "LOST " << (c.id.empty() ? "unregistered" : c.id)
                  << " reason=" << reason << std::endl;
    }

    void handle(const Event& event)
    {
        auto& c = *connections[event.connection];
        if (!c.alive)
            return;
        if (!event.error.empty()) {
            lose_worker(event.connection, event.error);
            return;
        }
        try {
            const auto& msg = event.message;
            std::string type = msg.at("type").get<std::string>();
            if (!c.registered) {
                if (type != "REGISTER")
                    throw std::invalid_argument("The first message must be REGISTER");
                std::string id = read_id(msg, "worker_id");
                int capacity = read_int(msg, "capacity", 1, 64);
                if (active_workers() >= 32)
                    throw std::invalid_argument("At most 32 workers are allowed");
                for (const auto& other : connections) {
                    if (other->alive && other->registered && other->id == id)
                        throw std::invalid_argument("Duplicate worker ID");
                }
                c.id = id;
                c.capacity = capacity;
                c.registered = true;
                c.last_seen = event.received;
                std::cout << "REGISTER " << id << " capacity=" << capacity << std::endl;
            } else if (type == "HEARTBEAT") {
                c.last_seen = event.received;
            } else if (type == "RESULT") {
                std::string id = read_id(msg, "task_id");
                int attempt = read_int(msg, "attempt_id", 1, 3);
                auto index = graph.find(id);
                const auto& task = graph.tasks()[index];
                if (task.state != State::RUNNING || task.owner != event.connection || task.attempts != attempt) {
                    std::cout << "IGNORE " << id << " attempt=" << attempt << " worker=" << c.id << std::endl;
                    return;
                }
                bool ok = msg.at("ok").get<bool>();
                json result;
                std::string error;
                if (ok) {
                    result = msg.at("result");
                    if (!(result.is_number_integer() || result.is_string()) || result.dump().size() > 1024)
                        throw std::invalid_argument("Result must be a small integer or string");
                } else {
                    error = msg.at("error").get<std::string>();
                    if (error.size() > 1024)
                        throw std::invalid_argument("Error message is too long");
                }
                if (graph.complete(index, event.connection, attempt, ok, result, error)) {
                    --c.busy;
                    std::cout << "RESULT " << id << " attempt=" << attempt << " worker=" << c.id
                              << " state=" << state_name(graph.tasks()[index].state) << std::endl;
                }
            } else {
                throw std::invalid_argument("Unexpected message: " + type);
            }
        } catch (const std::exception& e) {
            lose_worker(event.connection, e.what());
        }
    }

    void schedule()
    {
        for (std::size_t i = 0; i < graph.tasks().size(); ++i) {
            if (graph.tasks()[i].state != State::READY)
                continue;
            int chosen = -1;
            for (std::size_t j = 0; j < connections.size(); ++j) {
                std::size_t candidate = (round_robin + j) % connections.size();
                auto& c = *connections[candidate];
                if (c.alive && c.registered && c.busy < c.capacity) {
                    chosen = static_cast<int>(candidate);
                    round_robin = (candidate + 1) % connections.size();
                    break;
                }
            }
            if (chosen < 0)
                return;

            auto& c = *connections[chosen];
            int attempt = graph.assign(i, chosen);
            ++c.busy;
            const auto& work = graph.tasks()[i].work;
            json message = {{"type", "ASSIGN"}, {"task_id", work.id}, {"attempt_id", attempt},
                            {"task", {{"id", work.id}, {"type", work.type}, {"args", work.args}}}};
            try {
                send_message(c.socket, message);
                std::cout << "ASSIGN " << work.id << " attempt=" << attempt << " worker=" << c.id << std::endl;
            } catch (const std::exception& e) {

                lose_worker(chosen, e.what());
            }
        }
    }

    void loop()
    {
        bool started = false;
        std::optional<Clock::time_point> waiting_since;
        while (!graph.done()) {
            if (wait_readable(listener, 20))
                accept_worker();

            std::deque<Event> batch;
            {
                std::lock_guard<std::mutex> lock(queue_mutex);
                batch.swap(events);
            }
            for (const auto& event : batch)
                handle(event);

            auto now = Clock::now();
            for (std::size_t i = 0; i < connections.size(); ++i) {
                auto& c = *connections[i];
                if (c.alive && now - c.last_seen > std::chrono::milliseconds(options.heartbeat_timeout))
                    lose_worker(static_cast<int>(i), "heartbeat timeout");
            }
            int active = active_workers();
            if (!started && active >= options.workers) {
                started = true;
                std::cout << "START workers=" << active << std::endl;
            }
            if (started)
                schedule();

            if (!started || active_workers() == 0) {
                if (!waiting_since)
                    waiting_since = now;
                if (!graph.done() && now - *waiting_since > std::chrono::milliseconds(options.idle_timeout))
                    throw std::runtime_error("Timed out waiting for workers; graph is incomplete");
            } else {
                waiting_since.reset();
            }
        }
    }

    void stop_workers()
    {
        for (auto& c : connections) {
            if (c->alive && c->registered) {
                try {
                    send_message(c->socket, {{"type", "SHUTDOWN"}});
                    c->socket.shutdown_write();
                } catch (const std::exception&) {
                    c->socket.shutdown_all();
                }
            } else {
                c->socket.shutdown_all();
            }
        }

        std::unique_lock<std::mutex> lock(queue_mutex);
        readers_cv.wait_for(lock, std::chrono::seconds(1), [this] {
            for (const auto& c : connections) {
                if (c->reader.joinable() && !c->reader_finished)
                    return false;
            }
            return true;
        });
        lock.unlock();
        for (auto& c : connections)
            c->socket.shutdown_all();
    }

    void print_report() const
    {
        for (const auto& task : graph.tasks()) {
            std::cout << task.work.id << " " << state_name(task.state)
                      << " attempts=" << task.attempts << " result=" << task.result.dump();
            if (!task.error.empty())
                std::cout << " error=" << task.error;
            std::cout << std::endl;
        }
        if (!options.report_file.empty()) {
            std::ofstream out(options.report_file);
            if (!out)
                throw std::runtime_error("Cannot open the report file");
            out << graph.report().dump(2) << '\n';
            out.close();
            if (!out)
                throw std::runtime_error("Failed to write the report file");
        }
    }

    TaskGraph graph;
    Options options;
    Socket listener;
    std::vector<std::unique_ptr<Connection>> connections;
    std::mutex queue_mutex;
    std::condition_variable readers_cv;
    std::deque<Event> events;
    std::size_t round_robin = 0;
};

int main(int argc, char** argv)
{
    try {
        Options options;
        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            if (arg == "--tasks") options.file = next_arg(i, argc, argv);
            else if (arg == "--report") options.report_file = next_arg(i, argc, argv);
            else if (arg == "--port") options.port = cli_int(next_arg(i, argc, argv), 0, 65535);
            else if (arg == "--workers") options.workers = cli_int(next_arg(i, argc, argv), 1, 32);
            else if (arg == "--heartbeat-timeout-ms") options.heartbeat_timeout = cli_int(next_arg(i, argc, argv), 50, 600000);
            else if (arg == "--idle-timeout-ms") options.idle_timeout = cli_int(next_arg(i, argc, argv), 50, 600000);
            else if (arg == "--help") {
                std::cout << "coordinator --tasks graph.json [--port 9000] [--workers 1]\n"
                             "  [--heartbeat-timeout-ms 5000] [--idle-timeout-ms 10000] [--report report.json]\n";
                return 0;
            } else throw std::invalid_argument("Unknown argument: " + arg);
        }
        if (options.file.empty())
            throw std::invalid_argument("--tasks graph.json is required");
        TaskGraph graph = TaskGraph::load(options.file);
        Network network;
        Coordinator coordinator(std::move(graph), options);
        return coordinator.run();
    } catch (const std::exception& e) {
        std::cerr << "coordinator: " << e.what() << std::endl;
        return 2;
    }
}
