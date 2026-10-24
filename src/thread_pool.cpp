#include "thread_pool.h"

#include <stdexcept>
#include <utility>

ThreadPool::ThreadPool(int count)
{
    if (count < 1 || count > 64)
        throw std::invalid_argument("Thread count must be 1..64");
    threads.reserve(count);
    try {
        for (int i = 0; i < count; ++i)
            threads.emplace_back(&ThreadPool::loop, this);
    } catch (...) {
        close();
        throw;
    }
}

ThreadPool::~ThreadPool()
{
    close();
}

void ThreadPool::submit(std::function<void()> job)
{
    if (!job)
        throw std::invalid_argument("Empty job");
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (stopping)
            throw std::logic_error("Pool is closed");
        jobs.push(std::move(job));
    }
    cv.notify_one();
}

void ThreadPool::close()
{
    {
        std::lock_guard<std::mutex> lock(mutex);
        stopping = true;
    }
    cv.notify_all();
    for (auto& thread : threads) {
        if (thread.joinable())
            thread.join();
    }
}

void ThreadPool::loop()
{
    while (true) {
        std::function<void()> job;
        {
            std::unique_lock<std::mutex> lock(mutex);
            cv.wait(lock, [this] { return stopping || !jobs.empty(); });
            if (jobs.empty())
                return;
            job = std::move(jobs.front());
            jobs.pop();
        }

        job();
    }
}
