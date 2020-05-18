/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief A simple thread pool.

#ifndef THREAD_POOL_H_
#define THREAD_POOL_H_

#include <thread>
#include <atomic>
#include <vector>
#include <queue>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <future>
#include <stdexcept>
#include <iostream>
#include <memory>

namespace minidfs {

class ThreadPool {

 private:
  /// set to false when shutting down
  std::atomic<bool> running;

  /// tasks to be executed by this thread pool
  std::queue<std::function<void()>> tasks;
  
  /// mutex for tasks
  std::mutex mutexTasks;

  /// condition variable for tasks
  std::condition_variable condition;

  /// number of threads
  size_t nThread;

  /// threads
  std::vector<std::thread> pool;



 public:
  /// Create a thread pool of nThread threads
  /// and start all the threads
  ThreadPool(size_t nThread);

  /// Add a task into the tasks queue
  template<class F, class... Args>
  auto enqueue(F&& f, Args&&... args) 
    -> std::future<typename std::result_of<F(Args...)>::type>;

  /// Wake all threads and stop them all
  ~ThreadPool();


 private:
  /// Worker thread method. Each worker runs this
  /// method to wait for available tasks.
  void work();
};



template<class F, class... Args>
auto ThreadPool::enqueue(F&& fn, Args&&... args) -> std::future<typename std::result_of<F(Args...)>::type> {
  using retType = typename std::result_of<F(Args...)>::type;
  
  /// bind the function and the return value
  auto task = std::make_shared<std::packaged_task<retType()>>(std::bind(std::forward<F>(fn), std::forward<Args>(args)...));
  auto res = task->get_future();
  if (running == false) {
    throw std::runtime_error("Add a new task to stopped thread pool");
  }
  {
    std::unique_lock<std::mutex> lockTasks(mutexTasks);
    tasks.emplace([task](){(*task)();});
    std::cerr << "[ThreadPool] " << "Number of tasks: " << tasks.size() << std::endl;
  }
  condition.notify_one();
  return res;    
}


} // namespace minidfs


#endif