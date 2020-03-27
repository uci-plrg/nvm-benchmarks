/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Peter Hornyack and Katelin Bailey
 * 2/19/12
 * University of Washington
 */

#include "../kp_macros.h"
#include "../kp_common.h"
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

#define MAX_THREADS 256
#define NUM_OPS 1000000
//#define SKIP_CAS
  /* If defined, then the tests below should fail pretty much every time,
   * but only if compiler optimizations are turned OFF - with -O3, an
   * atomic increment instruction is used or something, so the tests
   * rarely fail.
   */

typedef struct thread_args_struct {
	unsigned long *val_ptr;
	unsigned int num_ops;
	bool decrement;
} thread_args;

void *thread_fn(void *arg)
{
	thread_args *args = (thread_args *)arg;
	unsigned long *val_ptr = args->val_ptr;
	unsigned int num_ops = args->num_ops;
	bool decrement = args->decrement;
	unsigned int failed_cas = 0;
	unsigned int i;
	unsigned long prev_val, expected_val, new_val;

	for (i = 0; i < num_ops; ) {
		/* Only increment i if the CAS succeeds - this guarantees that this 
		 * particular thread will perform exactly num_ops increments or
		 * decrements.
		 */
		expected_val = *val_ptr;
		new_val = decrement ? expected_val - 1 : expected_val + 1;
#ifdef SKIP_CAS
		*val_ptr = new_val;
		i++;
#else
		prev_val = kp_cas(val_ptr, expected_val, new_val);
		if (prev_val == expected_val) {
			i++;
		} else {
			failed_cas++;
		}
#endif
	}
	kp_print("thread has finished executing %u %s; %u CAS operations failed "
			"along the way\n",
			num_ops, decrement ? "decrements" : "increments", failed_cas);

	return NULL;
}

void test_cas(unsigned int num_threads)
{
	unsigned int i;
	int ret;
	pthread_t threads[MAX_THREADS];
	unsigned long value = 0;
	thread_args args_array[MAX_THREADS];

	/* Run all worker threads: */
	for (i = 0; i < num_threads; i++) {
		args_array[i].val_ptr = &value;
		args_array[i].num_ops = NUM_OPS;
		args_array[i].decrement = i % 2;  //false for 0 / even threads
		ret = pthread_create(&(threads[i]), NULL, thread_fn,
				(void *)&(args_array[i]));
		if (ret != 0) {
			kp_die("pthread_create() returned error=%d\n", ret);
		}
	}
	for (i = 0; i < num_threads; i++) {
		ret = pthread_join(threads[i], NULL);
		if (ret != 0) {
			kp_die("pthread_join() returned error=%d\n", ret);
		}
	}

	/* Check the value stored is either 0 (even number of threads) or NUM_OPS
	 * (odd number of threads):
	 */
	if (num_threads % 2) {
		if (value == NUM_OPS) {
			printf("\nPASS: with odd number of threads, value is %lu\n",
					value);
		} else {
			printf("\nFAIL: with odd number of threads, value should be %u, "
					"but is %lu\n", NUM_OPS, value);
		}
	} else {
		if (value == 0) {
			printf("\nPASS: with even number of threads, value is %lu\n",
					value);
		} else {
			printf("\nFAIL: with even number of threads, value should be 0, "
					"but is %lu\n", value);
		}
	}
}

int main(int argc, char *argv[])
{
	if (argc != 2) {
		printf("usage: %s <num_threads>\n", argv[0]);
		return -1;
	}

	unsigned int num_threads = atoi(argv[1]);
	if (num_threads > MAX_THREADS) {
		num_threads = MAX_THREADS;
	} else if (num_threads == 0) {
		num_threads = 1;
	}
	kp_print("using num_threads=%u\n", num_threads);

	test_cas(num_threads);

	return 0;
}

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
