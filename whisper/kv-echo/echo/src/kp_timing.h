/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Katelin Bailey & Peter Hornyack
 * 2/19/12
 * University of Washington
 *
 */

#include <stdint.h>
#include <sys/time.h>
#include <time.h>

typedef struct _kp_timers {
	/* Struct timespec returns timing information at second and nanosecond
	 * granularity. On host burrard, clock_getres() confirms that the
	 * resolution of these timers is indeed 1 nanosecond.
	 */
	struct timespec realtime_start;      //clock_gettime(CLOCK_REALTIME)
	struct timespec realtime_stop;
	struct timespec monotonic_start;     //clock_gettime(CLOCK_MONOTONIC)
	struct timespec monotonic_stop;
	struct timespec proc_cputime_start;  //clock_gettime(CLOCK_PROCESS_CPUTIME_ID)
	struct timespec proc_cputime_stop;
} kp_timers;


/* Starts various timers.
*/
void start_timing(kp_timers *timers);

/* Stops various timers. The kp_timers pointer that is passed to
 * this function must have been passed to start_timing() before
 * it is used here, or undefined behavior may result.
 */
void stop_timing(kp_timers *timers);

/* Subtracts struct timespec *b from struct timespec *a. The tv_nsec member
 * of struct timespec is a long, so we know it's signed and we can check
 * if the difference is negative in this code.
 * http://stackoverflow.com/questions/1858050/how-do-i-compare-two-timestamps-in-c
 * http://www.gnu.org/s/hello/manual/libc/Elapsed-Time.html
 */
#define ts_subtract(a, b, result) \
	do { \
		(result)->tv_sec = (a)->tv_sec - (b)->tv_sec; \
		(result)->tv_nsec = (a)->tv_nsec - (b)->tv_nsec; \
		if ((result)->tv_nsec < 0) { \
			(result)->tv_sec -= 1; \
			(result)->tv_nsec += 1000000000; \
		} \
	} while (0)

/* Prints out timing information. The kp_timers pointer that is passed
 * to this function must have first been passed to start_timing() and
 * stop_timing(), or undefined behavior may result. The prefix argument
 * will be used to identify the operations whose timing information
 * we are printing here. divisor is the number of iterations, so that
 * average operation latency can be computed.
 */
void print_timing(const char *prefix, kp_timers *timers, int divisor);

/* This code was copied from:
 *   http://stackoverflow.com/questions/6041271/is-there-a-way-to-check-whether-the-processor-cache-has-been-flushed-recently
 * NOTE that there are some problems inherent in using cycle counters,
 * e.g. they need to be calibrated across cores and so on. Use at your
 * own risk...
 */
inline uint64_t rdtsc();

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
