/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Katelin Bailey & Peter Hornyack
 * 2/19/12
 * University of Washington
 *
 */

#include "kp_timing.h"
#include "kp_macros.h"
#include <stdio.h>

void start_timing(kp_timers *timers) {
	int ret;

	ret = clock_gettime(CLOCK_REALTIME, &(timers->realtime_start));
	if (ret != 0) {
		kp_error("startclock_gettime(CLOCK_REALTIME) returned %d\n", ret);
	}

	ret = clock_gettime(CLOCK_MONOTONIC, &(timers->monotonic_start));
	if (ret != 0) {
		kp_error("startclock_gettime(CLOCK_MONOTONIC) returned %d\n", ret);
	}

	ret = clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &(timers->proc_cputime_start));
	if (ret != 0) {
		kp_error("startclock_gettime(CLOCK_PROCESS_CPUTIME_ID) returned %d\n",
				ret);
	}
}

void stop_timing(kp_timers *timers) {
	int ret;

	ret = clock_gettime(CLOCK_REALTIME, &(timers->realtime_stop));
	if (ret != 0) {
		kp_error("stop clock_gettime(CLOCK_REALTIME) returned %d\n", ret);
	}

	ret = clock_gettime(CLOCK_MONOTONIC, &(timers->monotonic_stop));
	if (ret != 0) {
		kp_error("stop clock_gettime(CLOCK_MONOTONIC) returned %d\n", ret);
	}

	ret = clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &(timers->proc_cputime_stop));
	if (ret != 0) {
		kp_error("stop clock_gettime(CLOCK_PROCESS_CPUTIME_ID) returned %d\n",
				ret);
	}

}

void print_timing(const char *prefix, kp_timers *timers, int divisor) {
	struct timespec ts_diff;
	unsigned long total_usec;
	float avg_usec;

	ts_subtract(&(timers->realtime_stop), &(timers->realtime_start), &ts_diff);
	total_usec = (ts_diff.tv_sec)*1000000 + (ts_diff.tv_nsec)/1000;
	avg_usec = (float)total_usec / (float)divisor;
	kp_print("%s:\n", prefix);
	kp_print("\t CLOCK_REALTIME \t %lu usec, average cost %f usec\n",
			total_usec, avg_usec);

	ts_subtract(&(timers->monotonic_stop), &(timers->monotonic_start), &ts_diff);
	total_usec = (ts_diff.tv_sec)*1000000 + (ts_diff.tv_nsec)/1000;
	avg_usec = (float)total_usec / (float)divisor;
	kp_print("\t CLOCK_MONOTONIC \t %lu usec, average cost %f usec\n",
			total_usec, avg_usec);

	ts_subtract(&(timers->proc_cputime_stop), &(timers->proc_cputime_start), 
			&ts_diff);
	total_usec = (ts_diff.tv_sec)*1000000 + (ts_diff.tv_nsec)/1000;
	avg_usec = (float)total_usec / (float)divisor;
	kp_print("\t CLOCK_PROC_CPUTIME_ID \t %lu usec, average cost %f usec\n",
			total_usec, avg_usec);
	printf("\n");
}

inline uint64_t rdtsc()
{
	unsigned long a, d;
	asm volatile ("rdtsc" : "=a" (a), "=d" (d));
	return a | ((uint64_t)d << 32);
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
