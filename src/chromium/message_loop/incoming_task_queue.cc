// Copyright 2013 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "chromium/message_loop/incoming_task_queue.hh"

#include "chromium/logging.hh"
#include "message_loop.hh"

namespace chromium {
namespace internal {

IncomingTaskQueue::IncomingTaskQueue(MessageLoop* message_loop)
    : message_loop_(message_loop),
      next_sequence_num_(0) {
}

bool IncomingTaskQueue::AddToIncomingQueue(
    const std::function<void()>& task,
    std::chrono::milliseconds delay) {
  AutoLock locked(incoming_queue_lock_);
  PendingTask pending_task(task, CalculateDelayedRuntime(delay));
  return PostPendingTask(&pending_task);
}

void IncomingTaskQueue::ReloadWorkQueue(TaskQueue* work_queue) {
  // Make sure no tasks are lost.
  DCHECK(work_queue->empty());

  // Acquire all we can from the inter-thread queue with one lock acquisition.
  AutoLock lock(incoming_queue_lock_);
  if (!incoming_queue_.empty())
    incoming_queue_.Swap(work_queue);  // Constant time

  DCHECK(incoming_queue_.empty());
}

void IncomingTaskQueue::WillDestroyCurrentMessageLoop() {
  AutoLock lock(incoming_queue_lock_);
  message_loop_ = NULL;
}

IncomingTaskQueue::~IncomingTaskQueue() {
  // Verify that WillDestroyCurrentMessageLoop() has been called.
  DCHECK(!message_loop_);
}

std::chrono::steady_clock::time_point IncomingTaskQueue::CalculateDelayedRuntime(std::chrono::milliseconds delay) {
  std::chrono::steady_clock::time_point delayed_run_time;
  if (delay.count() > 0) {
    delayed_run_time = std::chrono::steady_clock::now() + delay;
  } else {
    DCHECK_EQ(delay.count(), 0) << "delay should not be negative";
  }

  return delayed_run_time;
}

bool IncomingTaskQueue::PostPendingTask(PendingTask* pending_task) {
  // Warning: Don't try to short-circuit, and handle this thread's tasks more
  // directly, as it could starve handling of foreign threads.  Put every task
  // into this queue.

  // This should only be called while the lock is taken.
  incoming_queue_lock_.AssertAcquired();

  if (!message_loop_) {
    pending_task->task = nullptr;
    return false;
  }

  // Initialize the sequence number. The sequence number is used for delayed
  // tasks (to faciliate FIFO sorting when two tasks have the same
  // delayed_run_time value) and for identifying the task in about:tracing.
  pending_task->sequence_num = next_sequence_num_++;

  bool was_empty = incoming_queue_.empty();
  incoming_queue_.push(*pending_task);
  pending_task->task = nullptr;

  // Wake up the pump.
  message_loop_->ScheduleWork(was_empty);

  return true;
}

}  // namespace internal
}  // namespace chromium
