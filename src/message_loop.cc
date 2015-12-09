// Copyright 2013 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "message_loop.hh"

#include <algorithm>

#include "chromium/logging.hh"

namespace chromium {

MessageLoop::MessageLoop() {
  Init();
}

MessageLoop::~MessageLoop() {
  // Clean up any unprocessed tasks, but take care: deleting a task could
  // result in the addition of more tasks (e.g., via DeleteSoon).  We set a
  // limit on the number of times we will allow a deleted task to generate more
  // tasks.  Normally, we should only pass through this loop once or twice.  If
  // we end up hitting the loop limit, then it is probably due to one task that
  // is being stubborn.  Inspect the queues to see who is left.
  bool did_work;
  for (int i = 0; i < 100; ++i) {
    DeletePendingTasks();
    ReloadWorkQueue();
    // If we end up with empty queues, then break out of the loop.
    did_work = DeletePendingTasks();
    if (!did_work)
      break;
  }
  DCHECK(!did_work);

  // Tell the incoming queue that we are dying.
  incoming_task_queue_->WillDestroyCurrentMessageLoop();
  incoming_task_queue_ = NULL;
}

void MessageLoop::PostTask(
    const std::function<void()>& task) {
  DCHECK((bool)task);
  incoming_task_queue_->AddToIncomingQueue(task, std::chrono::milliseconds());
}

void MessageLoop::PostDelayedTask(
    const std::function<void()>& task,
    std::chrono::milliseconds delay) {
  DCHECK((bool)task);
  incoming_task_queue_->AddToIncomingQueue(task, delay);
}

//------------------------------------------------------------------------------

void MessageLoop::Init() {
  incoming_task_queue_.reset (new internal::IncomingTaskQueue(this));
}

bool MessageLoop::ProcessNextDelayedNonNestableTask() {
  if (deferred_non_nestable_work_queue_.empty())
    return false;

  PendingTask pending_task = deferred_non_nestable_work_queue_.front();
  deferred_non_nestable_work_queue_.pop();

  RunTask(pending_task);
  return true;
}

void MessageLoop::RunTask(const PendingTask& pending_task) {
  pending_task.task();
}

bool MessageLoop::DeferOrRunPendingTask(const PendingTask& pending_task) {
  {
    RunTask(pending_task);
    // Show that we ran a task (Note: a new one might arrive as a
    // consequence!).
    return true;
  }
}

void MessageLoop::AddToDelayedWorkQueue(const PendingTask& pending_task) {
  // Move to the delayed work queue.
  delayed_work_queue_.push(pending_task);
}

bool MessageLoop::DeletePendingTasks() {
  bool did_work = !work_queue_.empty();
  while (!work_queue_.empty()) {
    PendingTask pending_task = work_queue_.front();
    work_queue_.pop();
    if (pending_task.delayed_run_time > std::chrono::steady_clock::time_point()) {
      // We want to delete delayed tasks in the same order in which they would
      // normally be deleted in case of any funny dependencies between delayed
      // tasks.
      AddToDelayedWorkQueue(pending_task);
    }
  }
  did_work |= !deferred_non_nestable_work_queue_.empty();
  while (!deferred_non_nestable_work_queue_.empty()) {
    deferred_non_nestable_work_queue_.pop();
  }
  did_work |= !delayed_work_queue_.empty();

  // Historically, we always delete the task regardless of valgrind status. It's
  // not completely clear why we want to leak them in the loops above.  This
  // code is replicating legacy behavior, and should not be considered
  // absolutely "correct" behavior.  See TODO above about deleting all tasks
  // when it's safe.
  while (!delayed_work_queue_.empty()) {
    delayed_work_queue_.pop();
  }
  return did_work;
}

void MessageLoop::ReloadWorkQueue() {
  // We can improve performance of our loading tasks from the incoming queue to
  // |*work_queue| by waiting until the last minute (|*work_queue| is empty) to
  // load. That reduces the number of locks-per-task significantly when our
  // queues get large.
  if (work_queue_.empty())
    incoming_task_queue_->ReloadWorkQueue(&work_queue_);
}

void MessageLoop::ScheduleWork(bool was_empty) {
  if (was_empty)
    pump_->ScheduleWork();
}

bool MessageLoop::DoWork() {
  for (;;) {
    ReloadWorkQueue();
    if (work_queue_.empty())
      break;

    // Execute oldest task.
    do {
      PendingTask pending_task = work_queue_.front();
      work_queue_.pop();
      if (pending_task.delayed_run_time > std::chrono::steady_clock::time_point()) {
        AddToDelayedWorkQueue(pending_task);
        // If we changed the topmost task, then it is time to reschedule.
        if (delayed_work_queue_.top().task.target<void()>() == pending_task.task.target<void()>())
          pump_->ScheduleDelayedWork(pending_task.delayed_run_time);
      } else {
        if (DeferOrRunPendingTask(pending_task))
          return true;
      }
    } while (!work_queue_.empty());
  }

  // Nothing happened.
  return false;
}

bool MessageLoop::DoDelayedWork(std::chrono::steady_clock::time_point* next_delayed_work_time) {
  if (delayed_work_queue_.empty()) {
    recent_time_ = *next_delayed_work_time = std::chrono::steady_clock::time_point();
    return false;
  }

  // When we "fall behind," there will be a lot of tasks in the delayed work
  // queue that are ready to run.  To increase efficiency when we fall behind,
  // we will only call Time::Now() intermittently, and then process all tasks
  // that are ready to run before calling it again.  As a result, the more we
  // fall behind (and have a lot of ready-to-run delayed tasks), the more
  // efficient we'll be at handling the tasks.

  std::chrono::steady_clock::time_point next_run_time = delayed_work_queue_.top().delayed_run_time;
  if (next_run_time > recent_time_) {
    recent_time_ = std::chrono::steady_clock::now();  // Get a better view of Now();
    if (next_run_time > recent_time_) {
      *next_delayed_work_time = next_run_time;
      return false;
    }
  }

  PendingTask pending_task = delayed_work_queue_.top();
  delayed_work_queue_.pop();

  if (!delayed_work_queue_.empty())
    *next_delayed_work_time = delayed_work_queue_.top().delayed_run_time;

  return DeferOrRunPendingTask(pending_task);
}

bool MessageLoop::DoIdleWork() {
  if (ProcessNextDelayedNonNestableTask())
    return true;

  return false;
}

}  // namespace chromium
