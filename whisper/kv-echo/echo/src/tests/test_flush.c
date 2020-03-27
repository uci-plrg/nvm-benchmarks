/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Peter Hornyack and Katelin Bailey
 * 2/19/12
 * University of Washington
 */

#include "../kp_macros.h"
#include "../kp_timing.h"
#include "../recovery.h"
#include <stdbool.h>
#include <stdint.h>

void test_range(int addr, int size)
{
	int *a = (int *)addr;
	size_t s = (size_t)size;
	flush_range((void *)a, s);
}

void test_ranges()
{
	/* CLFLUSH_SIZE is typically 64. */
	/* Start on cache line boundary: */
	test_range(64, 0);   //expect: {}
	test_range(64, 1);   //expect: {64}
	test_range(64, 63);  //expect: {64}
	test_range(64, 64);  //expect: {64}
	test_range(64, 65);  //expect: {64, 128}

	/* Start in middle of cache line: */
	test_range(96, 0);   //expect: {}
	test_range(96, 1);   //expect: {64}
	test_range(96, 31);  //expect: {64}
	test_range(96, 32);  //expect: {64}
	test_range(96, 33);  //expect: {64, 128}
	test_range(96, 63);  //expect: {64, 128}
	test_range(96, 64);  //expect: {64, 128}
	test_range(96, 65);  //expect: {64, 128}

	/* Try some large ranges: */
	test_range(0x9C400, 64);   //expect: {640000}
	test_range(0x9C400, 128);  //expect: {640000 640064}
	test_range(0x9C400, 129);  //expect: {640000 640064 640128}
	test_range(1234, 1234);    //expect: {1216 1280 1344 1408 1472 1536 1600 1664 1728 1792
	                           //         1856 1920 1984 2048 2112 2176 2240 2304 2368 2432}
	test_range(0xABCD, 512);   //expect: {43968 44032 44096 44160 44224 44288 44352 44416 44480}
}

#define INNER_OBJECTS (1)
//#define INNER_OBJECTS (1048576 / sizeof(int))
  //1 MB worth of integers
typedef struct _big_object {
	volatile int nums[INNER_OBJECTS];
} big_object;

void fill_big_object(big_object *object)
{
	int i;

	/* Bring all elements of object into cache: */
	for (i = 0; i < INNER_OBJECTS; i++) {
		(object->nums)[i] = i;
	}
}

void update_big_object(big_object *object, bool flush_each_update)
{
	int i;
	volatile int tmp;

	for (i = 0; i < INNER_OBJECTS; i++) {
		tmp = (object->nums)[i];
		(object->nums)[i] = tmp + 1;
		if (flush_each_update) {
			flush_range((void *)(&((object->nums)[i])), sizeof(int));
		}
	}
}

//volatile int int1;

void test_objects()
{
	big_object *object;
	kp_timers timers;
	uint64_t start, end;
//	volatile int int2;

	object = (big_object *)malloc(sizeof(big_object));
	if (object == NULL) {
		kp_error("malloc(object) failed\n");
		return;
	}
	kp_print("sizeof(big_object): %u bytes\n", sizeof(big_object));

	/* Fill up the object, then time how long it takes to read-then-write
	 * each item in the object:
	 */
	fill_big_object(object);
//	int1 = 1234;
	start_timing(&timers);
	start = rdtsc();
//	int2 = int1;
	update_big_object(object, false);
	end = rdtsc();
	stop_timing(&timers);
	kp_print("Update big object, no flushing: %llu ticks\n", end - start);
	print_timing("Update big object, no flushing", &timers, INNER_OBJECTS);

	/* Now, flush in-between filling and updating: */
	fill_big_object(object);
//	int1 = 1234;
	start_timing(&timers);
	start = rdtsc();
	flush_range((void *)object, sizeof(big_object));
//	flush_range(&int1, sizeof(int));	
	end = rdtsc();
	stop_timing(&timers);
	kp_print("Flush big object: %llu ticks\n", end - start);
	print_timing("Flush big object", &timers, 1);

	start_timing(&timers);
	start = rdtsc();
	update_big_object(object, false);
//	int2 = int1;
	end = rdtsc();
	stop_timing(&timers);
	kp_print("Update big object after flushing: %llu ticks\n", end - start);
	print_timing("Update big object after flushing", &timers, INNER_OBJECTS);

	/* Now, flush each individual update: */
	fill_big_object(object);
	start_timing(&timers);
	start = rdtsc();
	update_big_object(object, true);
	end = rdtsc();
	stop_timing(&timers);
	kp_print("Update big object, flushing each update: %llu ticks\n", end - start);
	print_timing("Update big object, flushing each update", &timers, INNER_OBJECTS);

	free(object);
}

int main(int argc, char *argv[])
{
	/* Need to comment out the actual clflush asm block to run this test,
	 * otherwise a segfault will occur.
	 */
	//test_ranges();

	test_objects();

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
