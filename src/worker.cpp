#include "cli.h"
#include "protocol.h"
#include "thread_pool.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <iostream>
#include <mutex>
#include <thread>
#include <utility>

struct Options {
    std::string host = "127.0.0.1";
    std::string id;
    std::string fail_task;
    int port = 9000;
    int threads = 2;
    int heartbeat_ms = 1000;
};

void run_worker(const Options& options)
{
    Socket socket = connect_to(options.host, options.port);
    ThreadPool pool(options.threads);
    std::atomic<bool> online{true};
    std::atomic<int> in_flight{0};
    std::mutex send_mutex;
    std::mutex heartbeat_mutex;
    std::condition_variable heartbeat_cv;

    auto stop = [&] {

        {
            std::lock_guard<std::mutex> lock(heartbeat_mutex);
            online.store(false);
        }
        socket.shutdown_all();
        heartbeat_cv.notify_all();
    };
    auto send = [&](const json& message) {

        std::lock_guard<std::mutex> lock(send_mutex);
        if (!online.load())
            throw std::runtime_error("Connection is closed");
        send_message(socket, message);
    };

    send({{"type", "REGISTER"}, {"worker_id", options.id}, {"capacity", options.threads}});
    std::cout << "CONNECTED " << options.id << " threads=" << options.threads << std::endl;
    std::thread heartbeat([&] {
        std::unique_lock<std::mutex> lock(heartbeat_mutex);
        while (online.load()) {
            if (heartbeat_cv.wait_for(lock, std::chrono::milliseconds(options.heartbeat_ms),
                                      [&] { return !online.load(); }))
                break;
            lock.unlock();
            try {
                send({{"type", "HEARTBEAT"}});
            } catch (const std::exception&) {
                stop();
            }
            lock.lock();
        }
    });

    std::exception_ptr failure;
    try {
        bool shutdown = false;
        json message;
        while (online.load() && receive_message(socket, message)) {
            std::string type = message.at("type").get<std::string>();
            if (type == "SHUTDOWN") {
                shutdown = true;
                break;
            }
            if (type != "ASSIGN")
                throw std::invalid_argument("Worker expects ASSIGN or SHUTDOWN");
            std::string id = read_id(message, "task_id");
            int attempt = read_int(message, "attempt_id", 1, 3);
            Work task = parse_work(message.at("task"));
            if (task.id != id)
                throw std::invalid_argument("task_id does not match the job description");

            if (in_flight.fetch_add(1) >= options.threads) {
                --in_flight;
                throw std::invalid_argument("Coordinator exceeded worker capacity");
            }
            try {
                pool.submit([&, task, attempt] {
                    json response = {{"type", "RESULT"}, {"task_id", task.id}, {"attempt_id", attempt}};
                    try {
                        if (task.id == options.fail_task)
                            throw std::runtime_error("Simulated task failure");
                        response["result"] = execute_work(task);
                        response["ok"] = true;
                    } catch (const std::exception& e) {
                        response["ok"] = false;
                        response["error"] = std::string(e.what()).substr(0, 1024);
                    } catch (...) {
                        response["ok"] = false;
                        response["error"] = "Unknown task exception";
                    }

                    --in_flight;
                    try {
                        send(response);
                    } catch (const std::exception&) {
                        stop();
                    }
                });
            } catch (...) {
                --in_flight;
                throw;
            }
        }
        if (!shutdown)
            throw std::runtime_error("Connection to coordinator was lost");
    } catch (...) {
        failure = std::current_exception();
        stop();
    }

    pool.close();
    stop();
    heartbeat.join();
    if (failure)
        std::rethrow_exception(failure);
    std::cout << "STOPPED " << options.id << std::endl;
}

int main(int argc, char** argv)
{
    try {
        Options options;
        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            if (arg == "--id") options.id = next_arg(i, argc, argv);
            else if (arg == "--host") options.host = next_arg(i, argc, argv);
            else if (arg == "--port") options.port = cli_int(next_arg(i, argc, argv), 1, 65535);
            else if (arg == "--threads") options.threads = cli_int(next_arg(i, argc, argv), 1, 64);
            else if (arg == "--heartbeat-ms") options.heartbeat_ms = cli_int(next_arg(i, argc, argv), 10, 600000);
            else if (arg == "--fail-task") options.fail_task = next_arg(i, argc, argv);
            else if (arg == "--help") {
                std::cout << "worker --id name [--host 127.0.0.1] [--port 9000] [--threads 2]\n"
                             "  [--heartbeat-ms 1000] [--fail-task task_id]\n";
                return 0;
            } else throw std::invalid_argument("Unknown argument: " + arg);
        }
        read_id(json{{"id", options.id}}, "id");
        if (!options.fail_task.empty())
            read_id(json{{"id", options.fail_task}}, "id");
        Network network;
        run_worker(options);
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "worker: " << e.what() << std::endl;
        return 2;
    }
}
