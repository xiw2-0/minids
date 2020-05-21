/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi

#include <threadpool/thread_pool.hpp>

namespace minidfs {

ThreadPool::ThreadPool(size_t nThread)
    : nThread(nThread), running(true) {
  for (int i = 0; i < nThread; ++i) {
    pool.emplace_back(std::thread(&ThreadPool::work, this));
  }
}


ThreadPool::~ThreadPool() {
  running = false;
  condition.notify_all();

  for (auto& t : pool) {
    t.join();
  }
}

void ThreadPool::work() {
  while (running) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lockTasks(mutexTasks);
      if (tasks.empty() == true) {
        condition.wait(lockTasks, [this]{return this->tasks.empty() == false || this->running == false;});
      } 
      if (running) {
        /// fetch one task and remove it from the queue
        task = std::move(tasks.front());
        tasks.pop();
      } else {
        /// this is stopped
        return;
      }
    }
    if (task) {
      task();
    }
  }
}

}