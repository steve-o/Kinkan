// Copyright 2013 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CHROMIUM_MESSAGE_LOOP_INCOMING_TASK_QUEUE_HH_
#define CHROMIUM_MESSAGE_LOOP_INCOMING_TASK_QUEUE_HH_

#include <functional>

#include "chromium/pending_task.hh"
#include "chromium/synchronization/lock.hh"
#include "chromium/time/time.hh"

namespace chromium {

class MessageLoop;
class WaitableEvent;

namespace internal {

// Implements a queue of tasks posted to the message loop running on the current
// thread. This class takes care of synchronizing posting tasks from different
// threads and together with MessageLoop ensures clean shutdown.
class IncomingTaskQueue {
 public:
  explicit IncomingTaskQueue(MessageLoop* message_loop);

  // Appends a task to the incoming queue. Posting of all tasks is routed though
  // AddToIncomingQueue() or TryAddToIncomingQueue() to make sure that posting
  // task is properly synchronized between different threads.
  //
  // Returns true if the task was successfully added to the queue, otherwise
  // returns false. In all cases, the ownership of |task| is transferred to the
  // called method.
  bool AddToIncomingQueue(const std::function<void()>& task, TimeDelta delay);

  // Loads tasks from the |incoming_queue_| into |*work_queue|. Must be called
  // from the thread that is running the loop.
  void ReloadWorkQueue(TaskQueue* work_queue);

  // Disconnects |this| from the parent message loop.
  void WillDestroyCurrentMessageLoop();

 private:
  virtual ~IncomingTaskQueue();

  // Calculates the time at which a PendingTask should run.
  TimeTicks CalculateDelayedRuntime(TimeDelta delay);

  // Adds a task to |incoming_queue_|. The caller retains ownership of
  // |pending_task|, but this function will reset the value of
  // |pending_task->task|. This is needed to ensure that the posting call stack
  // does not retain |pending_task->task| beyond this function call.
  bool PostPendingTask(PendingTask* pending_task);

#if defined(_WIN32)
  TimeTicks high_resolution_timer_expiration_;
#endif

  // The lock that protects access to |incoming_queue_|, |message_loop_| and
  // |next_sequence_num_|.
  chromium::Lock incoming_queue_lock_;

  // An incoming queue of tasks that are acquired under a mutex for processing
  // on this instance's thread. These tasks have not yet been been pushed to
  // |message_loop_|.
  TaskQueue incoming_queue_;

  // Points to the message loop that owns |this|.
  MessageLoop* message_loop_;

  // The next sequence number to use for delayed tasks.
  int next_sequence_num_;
};

}  // namespace internal
}  // namespace chromium

#endif  // CHROMIUM_MESSAGE_LOOP_INCOMING_TASK_QUEUE_HH_
