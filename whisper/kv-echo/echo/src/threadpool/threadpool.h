/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 */

#ifndef THREADPOOL_H__
#define THREADPOOL_H__

#include <stdbool.h>
#include <stdint.h>

/* Opaque handle: */
struct threadpool_;
typedef struct threadpool_ threadpool;

/* Allocates and initializes an empty threadpool with num_workers threads
 * in it. The use_nvm argument indicates whether or not this threadpool will
 * be stored in non-volatile memory.
 * Returns: 0 on success, -1 on error. On success, *tp is set to point
 * to the new threadpool.
 */
int threadpool_create(threadpool **tp, unsigned int num_workers, bool use_nvm);

/* Destroys the threadpool. If the wait argument is true, then this function
 * will wait for all worker threads to finish their work before destroying
 * the thread pool (note that this could be a very long wait, of course). If
 * wait is false, then the worker threads will be cancelled immediately,
 * which of course may lead to corruption of the data that they are working
 * with.
 */
void threadpool_destroy(threadpool *tp, bool wait);

/* Function for performing work: takes a generic pointer to an argument
 * that the function should know what to do with.
 */
typedef void (task_function)(void *arg);

/* Adds a task to the thread pool to be performed by a worker thread.
 * Returns: 0 on success, -1 on error.
 */
int threadpool_add_task(threadpool *tp, task_function task_fn, void *arg);

/* Returns various task counts for the thread pool. Tasks that have been
 * added to the pool but not taken by any worker threads yet are "pending."
 * Tasks that are currently being executed by worker threads are "active."
 * Tasks that have been completed are "completed." This function will store
 * the current counts in any unsigned int whose pointer passed to this
 * function is non-NULL. This function takes a lock on the threadpool, so
 * its counts are all consistent with each other, but of course the counts
 * may be outdated as soon they are returned.
 */
void threadpool_get_task_counts(threadpool *tp, unsigned int *pending,
		unsigned int *active, unsigned int *completed);

#if 0
/* Returns: the number of tasks in the task queue. Note that this value
 * may be inaccurate as soon as it is returned. */
unsigned int threadpool_get_task_count_active(threadpool *tp);

/* Returns: the number of tasks that have been completed by worker threads
 * in this pool. Note that this value may be inaccurate as soon as it is
 * returned. Additionally, if more tasks than can be counted by an unsigned
 * integer are completed, then this value will wrap around.
 */
unsigned int threadpool_get_task_count_completed(threadpool *tp);
#endif

/* Returns: the number of worker threads currently in the thread pool.
 *   Note that if the threadpool is being managed by multiple threads,
 *   the number of workers returned could be out-of-date as soon as it is
 *   received by the caller!
 */
unsigned int threadpool_get_worker_count(threadpool *tp);

/* Creates another worker thread in the thread pool. 
 * Returns: the number of workers in the pool, or UINT32_MAX on error.
 *   Note that if the threadpool is being managed by multiple threads,
 *   the number of workers returned could be out-of-date as soon as it is
 *   received by the caller!
 */
unsigned int threadpool_add_worker(threadpool *tp);

/* Removes a worker thread from the pool. If all threads are currently
 * performing some work, then the wait argument determines if this function
 * will wait for a thread to finish, or if it will randomly choose a thread
 * to cancel. Use wait = false with caution!
 * Returns: the number of workers in the pool, or UINT32_MAX on error.
 *   Note that if the threadpool is being managed by multiple threads,
 *   the number of workers returned could be out-of-date as soon as it is
 *   received by the caller!
 */
unsigned int threadpool_remove_worker(threadpool *tp, bool wait);

#endif  //THREADPOOL_H__

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
