/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 */

#include <stdlib.h>
#include "queue.h"
#include "threadpool.h"
#include "threadpool_macros.h"
#include "../kp_common.h"
#include "../kp_recovery.h"

struct threadpool_ {
	/* One mutex lock is used for all of the condition variables. */
	queue *task_queue;
	pthread_mutex_t *lock;
	pthread_cond_t *task_available;
	pthread_cond_t *exit_cond;
	bool worker_exit;
	unsigned int num_workers;
	unsigned int tasks_pending;
	unsigned int tasks_active;
	unsigned int tasks_completed;
	bool use_nvm;
};

typedef struct task_ {
	task_function *task_fn;
	void *arg;
	bool use_nvm;
} task;

/* Function to free a task struct. In the hopefully uncommon event that this
 * function is called by queue_destroy() (i.e. the threadpool is destroyed
 * while there are still tasks left in the queue), then it is possible that
 * the task's arg will never be freed and cause a memory leak, because the
 * task_function was never called on the arg. Oh well.
 */
void task_free_fn(void *arg) {
	task *dead_task;
	if (arg) {
		dead_task = (task *)arg;
		kp_free((void **)(&dead_task), dead_task->use_nvm);
	}
}

int threadpool_create(threadpool **tp, unsigned int num_workers,
		bool use_nvm) {
	int ret;
	unsigned int i, uret;

	if (!tp) {
		tp_error("got a NULL argument: tp=%p\n", tp);
		return -1;
	}
	if (num_workers == UINT32_MAX) {
		tp_error("invalid number of worker threads: %u\n", num_workers);
		return -1;
	}
	if (num_workers == 0) {
		tp_warn("creating a thread pool with 0 workers!\n");
	}
	tp_debug("creating new thread pool with %u workers\n", num_workers);

	kp_kpalloc((void **)tp, sizeof(threadpool), use_nvm);
	if (!(*tp)) {
		tp_error("malloc(threadpool) failed\n");
		return -1;
	}

	ret = queue_create(&((*tp)->task_queue), use_nvm);
	if (ret != 0) {
		tp_error("queue_create() returned error=%d\n", ret);
		goto free_pool;
	}
	ret = kp_mutex_create("thread pool mutex", &((*tp)->lock));
	if (ret != 0) {
		tp_error("kp_mutex_create() returned error=%d\n", ret);
		goto free_queue;
	}
	ret = kp_cond_create("task_available cond", &((*tp)->task_available));
	if (ret != 0) {
		tp_error("kp_cond_create(task_available) returned error=%d\n", ret);
		goto free_mutex;
	}
	ret = kp_cond_create("exit cond", &((*tp)->exit_cond));
	if (ret != 0) {
		tp_error("kp_cond_create(exit_cond) returned error=%d\n", ret);
		goto free_cond1;
	}
	(*tp)->worker_exit = false;
	(*tp)->use_nvm = use_nvm;

	/* Now that allocation is done, add the workers. threadpool_add_worker()
	 * will increment the num_workers count for us:
	 */
	(*tp)->num_workers = 0;
	for (i = 0; i < num_workers; i++) {
		uret = threadpool_add_worker(*tp);
		if (uret != i+1) {
			tp_error("threadpool_add_worker(%u) returned unexpected value "
					"%u\n", i, uret);
			for (; ; i--) {
				/* Don't wait, ignore return value: */
				threadpool_remove_worker(*tp, false);
				if (i == 0) {
					break;
				}
			}
			goto free_cond2;
		}
	}
	(*tp)->tasks_pending = 0;
	(*tp)->tasks_active = 0;
	(*tp)->tasks_completed = 0;

	tp_debug("successfully created new thread pool with %u threads\n",
			(*tp)->num_workers);
	return 0;

free_cond2:
	kp_cond_destroy("exit cond", &((*tp)->exit_cond));
free_cond1:
	kp_cond_destroy("task_available cond", &((*tp)->task_available));
free_mutex:
	kp_mutex_destroy("thread pool mutex", &((*tp)->lock));
free_queue:
	queue_destroy((*tp)->task_queue);
free_pool:
	kp_free((void **)tp, use_nvm);
	return -1;
}

void threadpool_destroy(threadpool *tp, bool wait) {
	unsigned int uret;

	if (!tp) {
		tp_error("got a NULL argument: tp=%p\n", tp);
		return;
	}
	tp_debug("destroying thread pool; wait=%s, thread pool currently has "
			"%u workers\n", wait ? "true" : "false", tp->num_workers);

	/* In this initial implementation, we don't bother to keep track of
	 * the worker thread IDs, so we can't force them to exit immediately
	 * by using pthread_cancel() - this is a TODO item.
	 *
	 * We destroy the thread pool by simply looping on the worker remove
	 * function until all of the threads have exited.
	 */
	if (!wait) {
		tp_error("not supported yet: forced cancellation of worker threads. "
				"Will wait for them all to finish instead\n");
	}
	while (tp->num_workers > 0) {
		uret = threadpool_remove_worker(tp, wait);
		if (uret == UINT32_MAX || uret != tp->num_workers) {
			tp_error("threadpool_remove_worker() returned unexpected "
					"value %u; all threads may not be cleaned up!\n", uret);
			break;
		}
	}
	tp_debug("done waiting, destroying threadpool with task counts "
			"pending=%u, active=%u, completed=%u\n", tp->tasks_pending,
			tp->tasks_active, tp->tasks_completed);

	/* Now free everything that we allocated: */
	kp_cond_destroy("exit cond", &(tp->exit_cond));
	kp_cond_destroy("task_available cond", &(tp->task_available));
	kp_mutex_destroy("thread pool mutex", &(tp->lock));
	queue_destroy(tp->task_queue);
	kp_free((void **)(&tp), tp->use_nvm);

	tp_debug("successfully destroyed thread pool\n");
	return;
}

int threadpool_add_task(threadpool *tp, task_function task_fn, void *arg) {
	int ret, retval;
	task *next_task;

	if (!tp || !task_fn) {
		tp_error("got a NULL argument: tp=%p, task_fn=%p\n", tp, task_fn);
		return -1;
	}

	kp_kpalloc((void **)(&next_task), sizeof(task), tp->use_nvm);
	if (!next_task) {
		tp_error("malloc(next_task) failed\n");
		return -1;
	}
	next_task->task_fn = task_fn;
	next_task->arg = arg;
	next_task->use_nvm = tp->use_nvm;

	retval = 0;
	kp_mutex_lock("add task", tp->lock);

	ret = queue_enqueue(tp->task_queue, (void *)next_task, task_free_fn);
	if (ret != 0) {
		tp_error("queue_enqueue returned error=%d\n", ret);
		retval = -1;
	} else {
		ret = pthread_cond_signal(tp->task_available);
		if (ret != 0) {
			tp_error("pthread_cond_signal(task_available) returned error=%d\n",
					ret);
			retval = -1;
		} else {
			tp_debug("successfully added a task and signaled task_available "
					"condition\n");
		}
	}
	tp->tasks_pending++;
#ifdef TP_ASSERT
	if (tp->tasks_pending != queue_length(tp->task_queue)) {
		tp_die("mismatch: tasks_pending=%u, but task_queue length=%u\n",
				tp->tasks_pending, queue_length(tp->task_queue));
	}
#endif

	kp_mutex_unlock("add task", tp->lock);

	return retval;
}

void threadpool_get_task_counts(threadpool *tp, unsigned int *pending,
		unsigned int *active, unsigned int *completed) {
	if (!tp) {
		tp_error("got a NULL argument: tp=%p; returning immediately\n", tp);
		return;
	}

	/* Lock the thread pool, so that all of the counts are consistent with
	 * each other (the sum of all of the counts should equal the number of
	 * times that threadpool_add_task() has ever been called). This could
	 * potentially hurt performance, of course; if this is the case, then
	 * we can disable these locks to sacrifice consistency for speed, or
	 * simply don't call this function while relying on the threadpool to
	 * complete tasks as quickly as possible.
	 */
	kp_mutex_lock("task counts lock", tp->lock);
	if (pending) {
		*pending = tp->tasks_pending;
	}
	if (active) {
		*active = tp->tasks_active;
	}
	if (completed) {
		*completed = tp->tasks_completed;
	}
	kp_mutex_unlock("task counts lock", tp->lock);

	return;
}

#if 0
unsigned int threadpool_get_task_count_active(threadpool *tp) {
	if (!tp) {
		tp_error("got a NULL argument: tp=%p\n", tp);
		return UINT32_MAX;
	}
	return queue_length(tp->task_queue);
}

unsigned int threadpool_get_task_count_completed(threadpool *tp) {
	if (!tp) {
		tp_error("got a NULL argument: tp=%p\n", tp);
		return UINT32_MAX;
	}
	return tp->completed;
}
#endif

unsigned int threadpool_get_worker_count(threadpool *tp) {
	if (!tp) {
		tp_error("got a NULL argument: tp=%p\n", tp);
		return UINT32_MAX;
	}
	return tp->num_workers;
}

/* The function that created worker threads will run. The argument is a
 * pointer to the threadpool struct itself, which contains all of the
 * necessary locks and condition variables.
 *
 * IMPORTANT: this function must always decrement the thread pool's
 * num_workers and signal its exit condition before returning; otherwise,
 * the client may deadlock in threadpool_destroy().
 *
 * Returns: NULL.
 */
void *threadpool_worker_fn(void *arg) {
	threadpool *tp;
	void *dequeued;
	task *next_task;
	int ret;
	bool outer_loop;
	bool move_active_to_complete;

	tp = (threadpool *)arg;
	if (!tp) {
		tp_error("got a NULL arg: tp=%p\n", tp);
		tp_error("Returning NULL now; deadlock may result in "
				"threadpool_destroy() because we can't decrement num_workers "
				"here!\n");
		return NULL;
	}

	tp_debug("new worker thread about to enter worker loop\n");

	/* Be careful with "break" statements here: there are two while loops!
	 * NOTE: when we break out of the OUTER loop, we will still hold the
	 * tp->lock!
	 */
	outer_loop = true;
	move_active_to_complete = false;
	while (outer_loop) {
		/* Get a task, waiting on the condition variable if one is not
		 * currently available. We grab the threadpool lock around this
		 * entire sequence (pthread_cond_wait releases the lock while
		 * waiting).
		 */
		kp_mutex_lock("worker loop", tp->lock);
		if (move_active_to_complete) {
			/* We check this here after we've already locked the threadpool,
			 * rather than adding another lock/unlock sequence at the end
			 * of the outer loop.
			 */
			move_active_to_complete = false;
			tp->tasks_active--;
			tp->tasks_completed++;
			tp_debug("worker thread moved a task from active (%u) to "
					"complete (%u)\n", tp->tasks_active, tp->tasks_completed);
		}
		while (queue_is_empty(tp->task_queue)) {
			/* Important: this check must come before the wait! Otherwise,
			 * a thread that was just created or that was just performing
			 * a task will miss the setting of worker_exit and the signal
			 * on task_available, and will wait forever.
			 */
			if (tp->worker_exit) {
				tp_debug("somebody set tp->worker_exit, breaking out of "
						"inner loop\n");
				outer_loop = false;
				break;
			}
			ret = pthread_cond_wait(tp->task_available, tp->lock);
			if (ret != 0) {
				tp_error("pthread_cond_wait() returned error=%d, breaking "
						"out of inner loop!\n", ret);
				outer_loop = false;
				break;
			}
		}
		/* Check worker_exit again here, in case somebody set it while we
		 * were waiting on task_available AND a new task arrived at the
		 * queue at/near the same time...
		 */
		if (!outer_loop || tp->worker_exit) {
#ifdef TP_DEBUG
			if (!outer_loop) {
				tp_debug("outer_loop set to false, so breaking out of "
						"outer loop too\n");
			} else if (tp->worker_exit) {
				tp_debug("somebody set tp->worker_exit, breaking out of "
						"outer loop\n");
			}
#endif
			break;  //still hold the tp->lock!
		}

		/* Move a task from "pending" to "active": */
		ret = queue_dequeue(tp->task_queue, &dequeued);
		if (ret == 1) {
			tp_warn("unexpected: dequeued an empty queue!\n");
			kp_mutex_unlock("worker loop", tp->lock);
			continue;  //after unlocking, outer loop again...
		} else if (ret != 0 || !dequeued) {
			tp_error("queue dequeue failed: ret=%d, dequeued=%p. Breaking "
					"out of outer loop\n", ret, dequeued);
			break;
		}
		tp->tasks_pending--;
		tp->tasks_active++;
		tp_debug("worker thread moved one task from pending (%u) to "
				"active (%u)\n", tp->tasks_pending, tp->tasks_active);
		kp_mutex_unlock("worker loop", tp->lock);

		/* Perform the task: */
		tp_debug("worker thread about to perform a new task\n");
		next_task = (task *)dequeued;
		next_task->task_fn(next_task->arg);
		  //todo: check/use return values?
		
		move_active_to_complete = true;  //see top of outer loop
		task_free_fn((void *)next_task);
		tp_debug("worker thread completed its task and freed it, looping "
				"again\n");
	}
	
	/* At this point, we should still hold the threadpool lock. We must
	 * decrement num_workers and signal the exit condition before returning!
	 * We also set tp->worker_exit to false, so that no other worker threads
	 * will see it set to true.
	 */
	tp_debug("worker thread here after outer worker loop\n");
	tp->num_workers -= 1;
	tp->worker_exit = false;
	tp_debug("decremented num_workers to %u and reset worker_exit to false\n",
			tp->num_workers);
	ret = pthread_cond_broadcast(tp->exit_cond);
	if (ret != 0) {
		tp_error("pthread_cond_broadcast() returned error=%d; oh well...\n",
				ret);
	}
	kp_mutex_unlock("worker exit", tp->lock);
	return NULL;
}

/* Note that this function is called by threadpool_create(), in addition
 * to being callable by the client.
 */
unsigned int threadpool_add_worker(threadpool *tp) {
	int ret;
	pthread_t new_thread_id;

	if (!tp) {
		tp_error("got a NULL argument: tp=%p\n", tp);
		return UINT32_MAX;
	}
	tp_debug("adding a worker to threadpool which currently has %u threads\n",
			tp->num_workers);
	
	kp_mutex_lock("threadpool add worker", tp->lock);
	/* Use default thread attributes. The argument to threadpool_worker_fn
	 * is a pointer to the threadpool struct itself.
	 */
	ret = pthread_create(&new_thread_id, NULL, threadpool_worker_fn,
			(void *)tp);
	if (ret != 0) {
		tp_error("pthread_create() returned error=%d\n", ret);
		kp_mutex_unlock("threadpool add worker", tp->lock);
		return UINT32_MAX;
	}
	tp->num_workers += 1;
	kp_mutex_unlock("threadpool add worker", tp->lock);

	tp_debug("successfully added a worker to threadpool, now has %u threads\n",
			tp->num_workers);
	return tp->num_workers;
}

unsigned int threadpool_remove_worker(threadpool *tp, bool wait) {
	unsigned int orig_num_workers;
	int ret;

	if (!tp) {
		tp_error("got a NULL argument: tp=%p\n", tp);
		return UINT32_MAX;
	}

	orig_num_workers = tp->num_workers;
	if (orig_num_workers == 0) {
		tp_error("no worker threads in pool right now, just returning\n");
		return 0;
	}
	tp_debug("removing one worker from thread pool: wait=%s, thread pool "
			"currently has %u workers\n", wait ? "true" : "false",
			orig_num_workers);

	/* In this initial implementation, we don't bother to keep track of
	 * the worker thread IDs, so we can't force them to exit immediately
	 * by using pthread_cancel() - this is a TODO item.
	 *
	 * Instead, we set worker_exit to true to tell some worker thread to
	 * exit on its next loop, then signal the task_available condition to
	 * wake up a thread, which will see that worker_exit is set, break out
	 * of its loop, decrement the number of workers, reset worker_exit back
	 * to false, and signal us here on the exit_cond before exiting.
	 */
	if (!wait) {
		tp_error("not supported yet: forced cancellation of worker threads. "
				"Will wait for a thread to finish instead\n");
	}
	kp_mutex_lock("remove worker", tp->lock);
	tp->worker_exit = true;
	while (orig_num_workers == tp->num_workers) {
		/* This broadcast must happen before we wait on the exit_cond!! */
		ret = pthread_cond_broadcast(tp->task_available);
		if (ret != 0) {
			tp_error("pthread_cond_broadcast() returned error=%d\n", ret);
			break;
		}
		ret = pthread_cond_wait(tp->exit_cond, tp->lock);
		if (ret != 0) {
			tp_error("pthread_cond_wait() returned error=%d\n", ret);
			break;
		}
	}
	kp_mutex_unlock("remove worker", tp->lock);

	if (ret != 0) {
		tp_error("ret is non-zero, returning error=UINT32_MAX\n");
		return UINT32_MAX;
	}
#ifdef TP_ASSERT
	else {
		if (orig_num_workers != tp->num_workers + 1) {
			tp_die("unexpected: num_workers was %u, but is now %u\n",
					orig_num_workers, tp->num_workers);
		}
		if (tp->worker_exit) {
			tp_die("unexpected: thread exited but did not reset worker_exit "
					"to false\n");
		}
	}
#endif

	tp_debug("successfully removed a worker thread from pool; now has %u "
			"workers\n", tp->num_workers);
	return tp->num_workers;
}

#if 0
void threadpool_destroy(threadpool *tp, bool wait) {
	if (!tp) {
		tp_error("got a NULL argument: tp=%p\n", tp);
		return;
	}
	tp_debug("destroying thread pool; wait=%s, thread pool currently has "
			"%u workers\n", wait ? "true" : "false", tp->num_workers);

	/* In this initial implementation, we don't bother to keep track of
	 * the worker thread IDs, so we can't force them to exit immediately
	 * by using pthread_cancel() - this is a TODO item.
	 *
	 * Instead, we set worker_exit to true to tell all of the worker
	 * threads to exit on their next loop, and then we wait for all of
	 * them to exit themselves. When each thread exits, it MUST decrement
	 * num_workers, and it must signal the exit_cond to wake up the thread
	 * waiting here. ADDITIONALLY, the thread here must signal (broadcast,
	 * really) the task_available condition on each loop, so that no worker
	 * threads keep waiting for tasks to arrive. Make sure that the broadcast
	 * happens BEFORE the wait!
	 */
	if (!wait) {
		tp_error("not supported yet: forced cancellation of worker threads. "
				"Will wait for them all to finish instead\n");
	}
	tp->worker_exit = true;
	kp_mutex_lock("threadpool_destroy", tp->lock);
	while (tp->num_workers > 0) {
		ret = pthread_cond_broadcast(tp->task_available);
		if (ret != 0) {
			tp_error("pthread_cond_broadcast() returned error=%d; skipping "
					"thread cleanup!\n", ret);
			break;
		}
		ret = pthread_cond_wait(tp->exit_cond, tp->lock);
		if (ret != 0) {
			tp_error("pthread_cond_wait() returned error=%d; skipping "
					"thread cleanup!\n", ret);
			break;
		}
	}
	kp_mutex_unlock("threadpool_destroy", tp->lock);

	/* Now free everything that we allocated: */
	kp_cond_destroy("exit cond", tp->exit);
	kp_cond_destroy("task_available cond", tp->task_available);
	kp_mutex_destroy("thread pool mutex", tp->lock);
	queue_destroy(tp->task_queue);
	kp_free((void **)(&tp), tp->use_nvm);

	tp_debug("successfully destroyed thread pool\n");
	return 0;
}
#endif

/*
 * Editor modelines  -  http://www.wireshark.org/tools/modelines.html
 *
 * Local variables:
 * c-basic-offset: 2
 * tab-width: 2
 * indent-tabs-mode: t
 * End:
 *
 * vi: set noexpandtab:
 * :noTabs=false:
 */
