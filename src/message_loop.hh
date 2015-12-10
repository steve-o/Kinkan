#ifndef CHROMIUM_MESSAGE_LOOP_HH_
#define CHROMIUM_MESSAGE_LOOP_HH_

#include <chrono>

#include "chromium/message_loop/incoming_task_queue.hh"
#include "chromium/message_loop/message_pump.hh"
#include "net/socket/socket_descriptor.hh"

namespace kinkan
{

class consumer_t;
class provider_t;

}

namespace chromium
{

// A MessageLoop is used to process events for a particular thread.  There is
// at most one MessageLoop instance per thread.
//
// Events include at a minimum Task instances submitted to PostTask and its
// variants.
//
// NOTE: Unless otherwise specified, a MessageLoop's methods may only be called
// on the thread where the MessageLoop's Run method executes.
	class MessageLoop : public MessagePump::Delegate {
	public:
		explicit MessageLoop();
		virtual ~MessageLoop();

// The "PostTask" family of methods call the task's Run method asynchronously
// from within a message loop at some point in the future.
//
// With the PostTask variant, tasks are invoked in FIFO order, inter-mixed
// with normal UI or IO event processing.  With the PostDelayedTask variant,
// tasks are called after at least approximately 'delay_ms' have elapsed.
//
// The MessageLoop takes ownership of the Task, and deletes it after it has
// been Run().
//
// PostTask(from_here, task) is equivalent to
// PostDelayedTask(from_here, task, 0).
//
// NOTE: These methods may be called on any thread.  The Task will be invoked
// on the thread that executes MessageLoop::Run().
		void PostTask(const std::function<void()>& task);
                
		void PostDelayedTask(const std::function<void()>& task, std::chrono::milliseconds delay);

	protected:
		std::shared_ptr<MessagePump> pump_;
       
	private: 
		friend class kinkan::consumer_t;
		friend class kinkan::provider_t;
		friend class internal::IncomingTaskQueue;

// Configures various members for the two constructors.
		void Init();

// Called to process any delayed non-nestable tasks.
		bool ProcessNextDelayedNonNestableTask();

// Runs the specified PendingTask.
		void RunTask(const PendingTask& pending_task);

// Calls RunTask or queues the pending_task on the deferred task list if it
// cannot be run right now.  Returns true if the task was run.
		bool DeferOrRunPendingTask(const PendingTask& pending_task);

// Adds the pending task to delayed_work_queue_.
		void AddToDelayedWorkQueue(const PendingTask& pending_task);

// Delete tasks that haven't run yet without running them.  Used in the
// destructor to make sure all the task's destructors get called.  Returns
// true if some work was done.
		bool DeletePendingTasks();

// Loads tasks from the incoming queue to |work_queue_| if the latter is
// empty.
		void ReloadWorkQueue();

// Wakes up the message pump. Can be called on any thread. The caller is
// responsible for synchronizing ScheduleWork() calls.
		void ScheduleWork(bool was_empty);

// MessagePump::Delegate methods:
		virtual bool DoWork() override;
		virtual bool DoDelayedWork(std::chrono::steady_clock::time_point* next_delayed_work_time) override;
		virtual bool DoIdleWork() override;

// A list of tasks that need to be processed by this instance.  Note that
// this queue is only accessed (push/pop) by our current thread.
		TaskQueue work_queue_;

// Contains delayed tasks, sorted by their 'delayed_run_time' property.
		DelayedTaskQueue delayed_work_queue_;

// A recent snapshot of Time::Now(), used to check delayed_work_queue_.
		std::chrono::steady_clock::time_point recent_time_;

// A queue of non-nestable tasks that we had to defer because when it came
// time to execute them we were in a nested message loop.  They will execute
// once we're out of nested message loops.
		TaskQueue deferred_non_nestable_work_queue_;

		std::shared_ptr<internal::IncomingTaskQueue> incoming_task_queue_;
	};

	class MessageLoopForIO : public MessageLoop
	{
	public:
// Used with WatchFileDescriptor to asynchronously monitor the I/O readiness
// of a file descriptor.
		class Watcher {
		public:
// Called from MessageLoop::Run when an FD can be read from/written to
// without blocking
			virtual void OnFileCanReadWithoutBlocking(net::SocketDescriptor fd) = 0;
			virtual void OnFileCanWriteWithoutBlocking(net::SocketDescriptor fd) = 0;

		protected:
			virtual ~Watcher() {}
		};

		enum Mode;

// Object returned by WatchFileDescriptor to manage further watching.
		class FileDescriptorWatcher {
		public:
			explicit FileDescriptorWatcher();
			~FileDescriptorWatcher();  // Implicitly calls StopWatchingFileDescriptor.

// Stop watching the FD, always safe to call.  No-op if there's nothing
// to do.
			bool StopWatchingFileDescriptor();

		private:
			friend class kinkan::provider_t;

			typedef std::pair<net::SocketDescriptor, Mode> event;

// Called by MessagePumpLibevent, ownership of |e| is transferred to this
// object.
			void Init(event* e);

// Used by MessagePumpLibevent to take ownership of event_.
			event* ReleaseEvent();

			void set_pump(kinkan::provider_t* pump) { pump_ = pump; }
			kinkan::provider_t* pump() const { return pump_; }

			void set_watcher(Watcher* watcher) { watcher_ = watcher; }

			void OnFileCanReadWithoutBlocking(net::SocketDescriptor fd, kinkan::provider_t* pump);
			void OnFileCanWriteWithoutBlocking(net::SocketDescriptor fd, kinkan::provider_t* pump);

/* pretend fd is a libevent event object */
			event* event_;
			Watcher* watcher_;
			kinkan::provider_t* pump_;
			std::shared_ptr<FileDescriptorWatcher> weak_factory_;
		};

		enum Mode {
			WATCH_READ = 1 << 0,
			WATCH_WRITE = 1 << 1,
			WATCH_READ_WRITE = WATCH_READ | WATCH_WRITE
		};

		virtual bool WatchFileDescriptor (net::SocketDescriptor fd, bool persistent, Mode mode, FileDescriptorWatcher* controller, Watcher* delegate) = 0;
	};

} /* namespace chromium */

#endif /* CHROMIUM_MESSAGE_LOOP_HH_ */

/* eof */
