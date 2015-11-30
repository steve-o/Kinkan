// Copyright (c) 2011 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "chromium/pending_task.hh"

namespace chromium {

#if _MSC_VER >= 1700
// This a temporary fix for compiling on VS2012. http://crbug.com/154744
PendingTask::PendingTask() : sequence_num(-1) {
}
#endif

PendingTask::PendingTask(const std::function<void()>& task)
    : task(task),
      sequence_num(0),
      delayed_run_time(TimeTicks()) {
}

PendingTask::PendingTask(const std::function<void()>& task,
                         TimeTicks delayed_run_time)
    : task(task),
      sequence_num(0),
      delayed_run_time(delayed_run_time) {
}

PendingTask::~PendingTask() {
}

bool PendingTask::operator<(const PendingTask& other) const {
  // Since the top of a priority queue is defined as the "greatest" element, we
  // need to invert the comparison here.  We want the smaller time to be at the
  // top of the heap.

  if (delayed_run_time < other.delayed_run_time)
    return false;

  if (delayed_run_time > other.delayed_run_time)
    return true;

  // If the times happen to match, then we use the sequence number to decide.
  // Compare the difference to support integer roll-over.
  return (sequence_num - other.sequence_num) > 0;
}

void TaskQueue::Swap(TaskQueue* queue) {
  c.swap(queue->c);  // Calls std::deque::swap.
}

}  // namespace chromium
