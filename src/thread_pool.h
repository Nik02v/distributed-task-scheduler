#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class ThreadPool {
public:
    explicit ThreadPool(int count);
    ~ThreadPool();
    void submit(std::function<void()> job);
    void close();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

private:
    void loop();
    std::mutex mutex;
    std::condition_variable cv;
    std::queue<std::function<void()>> jobs;
    std::vector<std::thread> threads;
    bool stopping = false;
};
