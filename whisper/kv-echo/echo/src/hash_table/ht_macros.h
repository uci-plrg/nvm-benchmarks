/*
 * Peter Hornyack & Katelin Bailey
 * Created: 8/28/2011
 * University of Washington
 */

#ifndef HT_MACROS_H
#define HT_MACROS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

/* HT_ASSERT should always be defined for sanity-checking purposes, and
 * only un-defined when we want to disable sanity-checking code to get
 * maximum performance (it will probably make very little difference
 * though).
 * HT_DEBUG enables verbose debug messages.
 * HT_DEBUG_LOCK enables verbose messages for mutex and rwlock lock/unlock.
 */
//#define HT_ASSERT
//#define HT_DEBUG
//#define HT_DEBUG_LOCK

/* Note that printing the result of pthread_self() in this way is not
 * portable. On systems lab machines, /usr/include/bits/pthreadtypes.h
 * contains: "typedef unsigned long int pthread_t;".
 */

/* Print normal and debug output to stdout, warning and error output to
 * stderr. Always flush after printing; this makes debugging etc. easier,
 * but possibly causes slowdown.
 */
#define ht_print(f, a...)  do { \
	fprintf(stdout, "KP: %lu: " f, pthread_self() % 10000, ##a); \
	fflush(stdout); \
	} while(0)
#define ht_warn(f, a...)  do { \
	fprintf(stderr, "**WARNING**: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stderr); \
	} while(0)
#define ht_error(f, a...)  do { \
	fprintf(stderr, "ERROR: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stderr); \
	} while(0)
#define ht_test(f, a...) fprintf(stdout, "TEST: %lu: " f, pthread_self() % 10000, ##a)

#ifdef HT_DEBUG
#define ht_debug(f, a...)  do { \
	fprintf(stdout, "DEBUG: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stdout); \
	} while(0)
#else
#define ht_debug(f, a...)  do { ; } while(0)
#endif
#define ht_debug2(f, a...)  do { \
	fprintf(stdout, "DEBUG: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stdout); \
	} while(0)

/* die by abort()ing; is exit(-1) better? */
#define ht_die(f, a...)  do { \
	fprintf(stderr, "KP: Fatal error (%lu: %s): " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stderr); \
	abort(); \
	} while(0)

#ifdef HT_DEBUG_LOCK
#define ht_debug_lock(f, a...)  do { \
	fprintf(stdout, "DEBUG: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stderr); \
	} while(0)
#else
#define ht_debug_lock(f, a...)  do { ; } while(0)
#endif

#define ht_testcase_int(description, expected, actual) do { \
	fprintf(stdout, "TEST %s: %lu: %s: expected=%d, actual=%d\n", \
			(expected == actual) ? "PASS" : "FAIL", \
			pthread_self() % 10000, description, expected, actual); \
} while(0)

/* Importantly, this macro doesn't dereference the pointers that is checks. */
#define ht_testcase_ptr(description, expected, actual) do { \
	fprintf(stdout, "TEST %s: %lu: %s: expected=%p, actual=%p\n", \
			(expected == actual) ? "PASS" : "FAIL", \
			pthread_self() % 10000, description, expected, actual); \
} while(0)

#define ht_testcase_uint64(description, expected, actual) do { \
	fprintf(stdout, "TEST %s: %lu: %s: expected=%llu, actual=%llu\n", \
			(expected == actual) ? "PASS" : "FAIL", \
			pthread_self() % 10000, description, expected, actual); \
} while(0)

#define ht_testcase_uint64_not(description, not_expected, actual) do { \
	fprintf(stdout, "TEST %s: %lu: %s: not_expected != %llu, actual=%llu\n", \
			(not_expected != actual) ? "PASS" : "FAIL", \
			pthread_self() % 10000, description, not_expected, actual); \
} while(0)

#define ht_testcase_string(description, expected, actual) do { \
	fprintf(stdout, "TEST %s: %lu: %s: expected=%s, actual=%s\n", \
			(expected == NULL) ? "FAIL" : \
			(actual == NULL) ? "FAIL" : \
			(strcmp(expected, actual) == 0) ? "PASS" : "FAIL", \
			pthread_self() % 10000, description, expected, actual); \
} while(0)


#if 0
void ht_testcase_int(char *description, int expected, int actual)
{
	fprintf(stdout, "TEST %s: %s: expected=%d, actual=%d\n",
			(expected == actual) ? "PASS" : "FAIL",
			description, expected, actual);
	//if (expected != actual) {
	//	exit(1);
	//}
}

void ht_testcase_string(char *description, char *expected, char *actual)
{
	fprintf(stdout, "TEST %s: %s: expected=%s, actual=%s\n",
			(strcmp(expected, actual) == 0) ? "PASS" : "FAIL",
			description, expected, actual);
	//if (expected != actual) {
	//	exit(1);
	//}
}
#endif
#endif  //HT_MACROS_H
