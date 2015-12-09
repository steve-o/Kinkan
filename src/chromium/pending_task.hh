// Copyright (c) 2011 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CHROMIUM_PENDING_TASK_HH_
#define CHROMIUM_PENDING_TASK_HH_

#include <chrono>
#include <functional>
#include <queue>

namespace chromium {

// Contains data about a pending task. Stored in TaskQueue and DelayedTaskQueue
// for use by classes that queue and execute tasks.
struct PendingTask {
#if _MSC_VER >= 1700
  PendingTask();
#endif
  PendingTask(const std::function<void()>& task);
  PendingTask(const std::function<void()>& task, std::chrono::steady_clock::time_point delayed_run_time);
  ~PendingTask();

  // Used to support sorting.
  bool operator<(const PendingTask& other) const;

  // The task to run.
  std::function<void()> task;

  // Secondary sort key for run time.
  int sequence_num;

  // The time when the task should be run.
  std::chrono::steady_clock::time_point delayed_run_time;
};

// Wrapper around std::queue specialized for PendingTask which adds a Swap
// helper method.
class TaskQueue : public std::queue<PendingTask> {
 public:
  void Swap(TaskQueue* queue);
};

// PendingTasks are sorted by their |delayed_run_time| property.
typedef std::priority_queue<chromium::PendingTask> DelayedTaskQueue;

}  // namespace chromium

#endif  // CHROMIUM_PENDING_TASK_HH_
