/* Copied from: http://stackoverflow.com/questions/6041271/is-there-a-way-to-check-whether-the-processor-cache-has-been-flushed-recently
 * Compile with: gcc -O0 -o test_flush2 test_flush2.c
 */

#include <stdio.h>
#include <stdint.h>

//#define USE_VOLATILE

inline void
clflush(volatile void *p)
{
	asm volatile ("clflush (%0)" :: "r"(p));
}

inline uint64_t
rdtsc()
{
	unsigned long a, d;
	asm volatile ("rdtsc" : "=a" (a), "=d" (d));
	return a | ((uint64_t)d << 32);
}

#ifdef USE_VOLATILE
volatile int i;
#else
int i;
#endif

inline void
test()
{
	uint64_t start, end;
#ifdef USE_VOLATILE
	volatile int j;
#else
	int j;
#endif

	start = rdtsc();
	j = i;
	end = rdtsc();
	printf("took %llu ticks\n", end - start);
}

int
main(int ac, char **av)
{
	test();
	test();
	printf("flush: ");
	clflush(&i);
	test();
	test();
	return 0;
}
