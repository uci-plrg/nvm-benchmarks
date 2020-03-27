/* Peter Hornyack
 * 2012-06-25
 * University of Washington
 */

#include <sched.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include "queue.h"
#include "threadpool.h"
#include "threadpool_macros.h"

bool use_nvm = true;

void short_task(void *arg) {
	unsigned int digit, value;
	unsigned long long sum;

	value = (unsigned int)arg;  //32-bit on burrard
	printf("starting short task, value is: %u\n", value);

	sum = 0;
	while (true) {
		digit = value % 10;
		sum += digit;
		value = value / 10;  //shift right
		if (value <= 0) {
			break;
		}
	}

	printf("finished short task, sum: %llu\n", sum);
	return;
}

void long_task(void *arg) {
	int i, j, inner, outer;
	unsigned long long sum;

	/* One thousand million is one billion, which is approx. one second
	 * on burrard... so ten seconds is ten thousand million? Actually,
	 * turns out to be about twice as long as that. */
	inner = 1000000;
	outer = 2000;
	sum = (unsigned long long)pthread_self();
	printf("starting long task, initial value: %llu\n", sum);

	for (i = 0; i < outer; i++) {
		for (j = 0; j < inner; j++) {
			sum += j;
		}
	}

	printf("long task complete, calculated sum: %llu\n", sum);
	return;
}

void test_queue(void) {
	int ret;
	queue *q;
	void *e, *e1, *e2, *e3, *e4;
	unsigned int uret;

	e1 = malloc(1);
	e2 = malloc(22);
	e3 = malloc(333);
	e4 = malloc(4444);
	if (!e1 || !e2 || !e3 || !e4) {
		tp_die("some malloc failed\n");
	}

	ret = queue_create(&q, use_nvm);
	tp_testcase_int("queue_create", 0, ret);
	if (ret != 0) {
		tp_die("queue_create() returned error=%d\n", ret);
	}

	ret = queue_dequeue(q, &e);
	tp_testcase_int("dequeue empty queue", 1, ret);
	uret = queue_length(q);
	tp_testcase_uint("queue length", 0, uret);
	ret = queue_enqueue(q, e1, free);
	tp_testcase_int("enqueue", 0, ret);
	uret = queue_length(q);
	tp_testcase_uint("queue length", 1, uret);
	ret = queue_dequeue(q, &e);
	tp_testcase_int("dequeue", 0, ret);
	tp_testcase_ptr("dequeue", e1, e);
	uret = queue_length(q);
	tp_testcase_uint("queue length", 0, uret);
	ret = queue_dequeue(q, &e);
	tp_testcase_int("dequeue empty", 1, ret);
	uret = queue_length(q);
	tp_testcase_uint("queue length", 0, uret);
	ret = queue_enqueue(q, e1, free);
	tp_testcase_int("enqueue", 0, ret);
	queue_destroy(q);
	tp_testcase_int("destroy non-empty queue", 0, 0);

	e1 = malloc(11111);  //should have been freed by queue_destroy()
	ret = queue_create(&q, use_nvm);
	tp_testcase_int("queue_create", 0, ret);
	if (ret != 0) {
		tp_die("queue_create() returned error=%d\n", ret);
	}
	ret = queue_enqueue(q, e1, free);
	tp_testcase_int("enqueue e1", 0, ret);
	ret = queue_enqueue(q, e2, free);
	tp_testcase_int("enqueue e2", 0, ret);
	ret = queue_enqueue(q, e3, free);
	tp_testcase_int("enqueue e3", 0, ret);
	ret = queue_enqueue(q, e4, free);
	tp_testcase_int("enqueue e4", 0, ret);
	uret = queue_length(q);
	tp_testcase_uint("queue length", 4, uret);
	tp_testcase_int("is_empty", queue_is_empty(q) ? 1 : 0, 0);
	ret = queue_dequeue(q, &e);
	tp_testcase_int("dequeue", 0, ret);
	tp_testcase_ptr("dequeue", e, e1);
	uret = queue_length(q);
	tp_testcase_uint("queue length", 3, uret);
	tp_testcase_int("is_empty", queue_is_empty(q) ? 1 : 0, 0);
	ret = queue_dequeue(q, &e);
	tp_testcase_int("dequeue", 0, ret);
	tp_testcase_ptr("dequeue", e, e2);
	uret = queue_length(q);
	tp_testcase_uint("queue length", 2, uret);
	tp_testcase_int("is_empty", queue_is_empty(q) ? 1 : 0, 0);
	ret = queue_dequeue(q, &e);
	tp_testcase_int("dequeue", 0, ret);
	tp_testcase_ptr("dequeue", e, e3);
	uret = queue_length(q);
	tp_testcase_uint("queue length", 1, uret);
	tp_testcase_int("is_empty", queue_is_empty(q) ? 1 : 0, 0);
	ret = queue_dequeue(q, &e);
	tp_testcase_int("dequeue", 0, ret);
	tp_testcase_ptr("dequeue", e, e4);
	uret = queue_length(q);
	tp_testcase_uint("queue length", 0, uret);
	tp_testcase_int("is_empty", queue_is_empty(q) ? 1 : 0, 1);

	ret = queue_enqueue(q, e4, free);
	tp_testcase_int("enqueue e4", 0, ret);
	ret = queue_enqueue(q, e3, free);
	tp_testcase_int("enqueue e3", 0, ret);
	ret = queue_enqueue(q, e2, free);
	tp_testcase_int("enqueue e2", 0, ret);
	ret = queue_enqueue(q, e1, free);
	tp_testcase_int("enqueue e1", 0, ret);
	ret = queue_dequeue(q, &e);
	tp_testcase_int("dequeue", 0, ret);
	tp_testcase_ptr("dequeue", e, e4);
	ret = queue_dequeue(q, &e);
	tp_testcase_int("dequeue", 0, ret);
	tp_testcase_ptr("dequeue", e, e3);
	ret = queue_dequeue(q, &e);
	tp_testcase_int("dequeue", 0, ret);
	tp_testcase_ptr("dequeue", e, e2);
	ret = queue_dequeue(q, &e);
	tp_testcase_int("dequeue", 0, ret);
	tp_testcase_ptr("dequeue", e, e1);
	ret = queue_dequeue(q, &e);
	tp_testcase_int("dequeue", 1, ret);
	queue_destroy(q);
	tp_testcase_int("destroy empty queue", 0, 0);

	free(e1);
	free(e2);
	free(e3);
	free(e4);

	return;
}

void test_threadpool(void) {
	int ret;
	unsigned int uret, pending, active, completed;
	bool wait = true;
	threadpool *tp;

	/* Create and destroy empty threadpool: */
	ret = threadpool_create(&tp, 0, use_nvm);
	tp_testcase_int("threadpool create", 0, ret);
	if (ret != 0) {
		tp_die("threadpool_create() failed\n");
	}
	uret = threadpool_get_worker_count(tp);
	tp_testcase_uint("worker count", 0, uret);
	threadpool_destroy(tp, wait);
	tp_testcase_int("threadpool destroy empty", 0, 0);

	/* Create empty threadpool, add a worker and destroy: */
	ret = threadpool_create(&tp, 0, use_nvm);
	tp_testcase_int("threadpool create", 0, ret);
	if (ret != 0) {
		tp_die("threadpool_create() failed\n");
	}
	uret = threadpool_add_worker(tp);
	tp_testcase_uint("add worker", 1, uret);
	threadpool_destroy(tp, wait);
	tp_testcase_int("threadpool destroy 1", 0, 0);

	/* Create empty threadpool, add a worker, remove a worker and destroy: */
	ret = threadpool_create(&tp, 0, use_nvm);
	tp_testcase_int("threadpool create", 0, ret);
	if (ret != 0) {
		tp_die("threadpool_create() failed\n");
	}
	uret = threadpool_add_worker(tp);
	tp_testcase_uint("add worker", 1, uret);
	uret = threadpool_get_worker_count(tp);
	tp_testcase_uint("worker count", 1, uret);
	threadpool_get_task_counts(tp, &pending, &active, &completed);
	tp_testcase_uint("task count", 0, pending + active + completed);
	uret = threadpool_remove_worker(tp, wait);
	tp_testcase_uint("remove worker", 0, uret);
	uret = threadpool_get_worker_count(tp);
	tp_testcase_uint("worker count", 0, uret);
	threadpool_destroy(tp, wait);
	tp_testcase_int("threadpool destroy 0", 0, 0);

	/* Two short tasks: */
	ret = threadpool_create(&tp, 2, use_nvm);
	tp_testcase_int("threadpool create", 0, ret);
	if (ret != 0) {
		tp_die("threadpool_create() failed\n");
	}
	uret = threadpool_get_worker_count(tp);
	tp_testcase_uint("worker count", 2, uret);
	ret = threadpool_add_task(tp, short_task, (void *)4123);
	tp_testcase_int("add task", 0, ret);
	ret = threadpool_add_task(tp, short_task, (void *)5142382);
	tp_testcase_int("add task", 0, ret);
	threadpool_get_task_counts(tp, &pending, &active, &completed);
	tp_testcase_uint("task count", 2, pending);  //not guaranteed!!
	tp_testcase_uint("task count", 2, pending + active + completed);
	tp_test("waiting for short tasks to complete: sleeping...\n");
	sleep(3);
	threadpool_get_task_counts(tp, &pending, &active, &completed);
	tp_testcase_uint("task count", 2, completed);  //not guaranteed!!!

	/* Two long tasks: */
	ret = threadpool_add_task(tp, long_task, NULL);
	tp_testcase_int("add task", 0, ret);
	ret = threadpool_add_task(tp, long_task, NULL);
	tp_testcase_int("add task", 0, ret);
	threadpool_get_task_counts(tp, &pending, &active, &completed);
	tp_testcase_uint("task count", 2, pending);  //not guaranteed!!
	tp_testcase_uint("task count", 4, pending + active + completed);
	tp_test("waiting for long tasks to complete: sleeping...\n");
	sleep(12);
	threadpool_get_task_counts(tp, &pending, &active, &completed);
	tp_testcase_uint("task count", 4, completed);  //not guaranteed!!

	/* More tasks than threads in pool: */
	ret = threadpool_add_task(tp, long_task, NULL);
	tp_testcase_int("add task", 0, ret);
	ret = threadpool_add_task(tp, short_task, (void *)4123);
	tp_testcase_int("add task", 0, ret);
	ret = threadpool_add_task(tp, short_task, (void *)5142382);
	tp_testcase_int("add task", 0, ret);
	ret = threadpool_add_task(tp, long_task, NULL);
	tp_testcase_int("add task", 0, ret);
	threadpool_get_task_counts(tp, &pending, &active, &completed);
	tp_testcase_uint("task count", 4, pending);  //not guaranteed!!
	tp_testcase_uint("task count", 8, pending + active + completed);

	threadpool_destroy(tp, wait);
	tp_testcase_int("threadpool destroy", 0, 0);

	return;
}

#define INITIAL_THREADS 44
#define LOOPS 50
#define ADDS_MIN 12
#define ADDS_MAX 15
#define SLEEPTIME 5
#define TASK_LOW 80
#define TASK_HIGH 100

void stress_test_threadpool()
{
	int i, j, ret;
	unsigned int total_tasks;
	unsigned int pending, active, completed;
	unsigned long tid;
	threadpool *tp;
	long int rand_int;

	if (ADDS_MAX <= ADDS_MIN) {
		tp_die("invalid ADDS_MAX %u and ADDS_MIN %u\n", ADDS_MAX,
				ADDS_MIN);
	}

	srandom((unsigned int)time(NULL));
	tid = pthread_self();
	ret = threadpool_create(&tp, INITIAL_THREADS, use_nvm);
	if (ret != 0) {
		tp_die("threadpool_create() failed\n");
	}

	total_tasks = 0;
	for (i = 0; i < LOOPS; i++) {
		/* First, add some tasks: */
		rand_int = random();
		rand_int = (rand_int % (ADDS_MAX - ADDS_MIN)) + ADDS_MIN;
		tp_test("adding %ld tasks to threadpool\n", rand_int);
		for (j = rand_int; j > 0; j--) {
			if (j % 2 == 0) {  //alternate between short and long
				ret = threadpool_add_task(tp, short_task, (void *)i+123);
			} else {
				ret = threadpool_add_task(tp, long_task, NULL);
			}
		}
		total_tasks += rand_int;
		threadpool_get_task_counts(tp, &pending, &active, &completed);
		tp_test("added %ld tasks, now task counts: pending=%u, "
				"active=%u, completed=%u\n", rand_int,
				pending, active, completed);
		tp_testcase_uint("total task count", total_tasks,
				pending + active + completed);

		/* Wait for just a little while for some tasks to complete. After
		 * sleeping, if the number of worker threads is much larger than
		 * the number of tasks, then we expect the debug statement below
		 * to always show 0 pending tasks. (for some reason this is not
		 * usualy true before we sleep, even after throwing some sched_yield
		 * calls in here.) */
		sleep(SLEEPTIME);
		threadpool_get_task_counts(tp, &pending, &active, &completed);
		tp_test("after sleeping, now task counts: pending=%u, "
				"active=%u, completed=%u\n", pending, active, completed);

		/* Now, either add a thread or remove a thread: */
		rand_int = random();
		if (rand_int % 2 == 0) {
			ret = threadpool_add_worker(tp);
			tp_test("added a worker to threadpool, now worker count=%u\n",
					threadpool_get_worker_count(tp));
		} else {
			ret = threadpool_remove_worker(tp, true);  //wait = true
			tp_test("removed a worker from threadpool, now worker count=%u\n",
					threadpool_get_worker_count(tp));
		}

		/* Monitor the number of pending tasks: */
		threadpool_get_task_counts(tp, &pending, NULL, NULL);
		if (pending > TASK_HIGH) {
			while (pending > TASK_LOW) {
				tp_test("too many tasks pending (%u), sleeping to wait for "
						"work to be done\n", pending);
				sleep(SLEEPTIME);
				threadpool_get_task_counts(tp, &pending, NULL, NULL);
				ret = threadpool_add_worker(tp);
				tp_test("added a worker to threadpool, now worker count=%u\n",
						threadpool_get_worker_count(tp));
			}
		}
	}

	/* Wait for threadpool to finish all of its tasks: */
	threadpool_get_task_counts(tp, &pending, &active, &completed);
	while (pending + active > 0) {
		tp_test("threadpool still has %u pending and %u active tasks; "
				"sleeping until they are all completed. NOTE: number of "
				"active tasks should match number of worker threads "
				"remaining!\n", pending, active);
		sleep(SLEEPTIME);
		threadpool_get_task_counts(tp, &pending, &active, &completed);
	}

	/* This is the critical test: */
	threadpool_get_task_counts(tp, &pending, &active, &completed);
	tp_testcase_uint("completed task count", total_tasks, completed);

	threadpool_destroy(tp, true);  //wait = true

	return;
}

int main(int argc, char *argv[])
{
//	test_queue();

#if 0
	short_task((void *)1);
	short_task((void *)12);
	short_task((void *)123);
	short_task((void *)1234);
	short_task((void *)51234);
	long_task(NULL);
#endif

//	test_threadpool();
	stress_test_threadpool();

	/* Things to look for with stress test (ignore DEBUG output):
	 *   While number of worker threads is greater than number of tasks,
	 *     make sure that no tasks are left pending (i.e. after the sleep).
	 *   When number of tasks starts to out-grow number of worker threads,
	 *     make sure that added worker threads are assigned a pending task
	 *     immediately (i.e. after the sleep, number of active tasks equals
	 *     number of worker threads).
	 *   Make sure that the total number of tasks that we add matches the
	 *     final number of completed tasks from the thread pool.
	 *   Anything else??
	 */

	printf("\nDon't forget to run this test file under valgrind "
			"--leak-check=full too!\n");

	return 0;
}
