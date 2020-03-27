/*
 * Peter Hornyack
 * Created: 8/28/2011
 * University of Washington
 */

#ifndef THREADPOOL_MACROS_H
#define THREADPOOL_MACROS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

/* TP_ASSERT should always be defined for sanity-checking purposes,
 * and only un-defined when we want to disable sanity-checking code to
 * get maximum performance (it will probably make very little difference
 * though).
 * TP_DEBUG enables verbose debug messages.
 * TP_DEBUG_LOCK enables verbose messages for mutex and rwlock lock/unlock.
 */
#define TP_ASSERT
//#define TP_DEBUG
//#define TP_DEBUG_LOCK

/* Note that printing the result of pthread_self() in this way is not
 * portable. On systems lab machines, /usr/include/bits/pthreadtypes.h
 * contains: "typedef unsigned long int pthread_t;".
 */

/* Print normal and debug output to stdout, warning and error output to
 * stderr. Always flush after printing; this makes debugging etc. easier,
 * but possibly causes slowdown.
 */
#define tp_print(f, a...)  do { \
	fprintf(stdout, "TP: %lu: " f, pthread_self() % 10000, ##a); \
	fflush(stdout); \
	} while(0)
#define tp_warn(f, a...)  do { \
	fprintf(stderr, "**WARNING**: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stderr); \
	} while(0)
#define tp_error(f, a...)  do { \
	fprintf(stderr, "ERROR: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stderr); \
	} while(0)
#define tp_test(f, a...) fprintf(stdout, "TEST: %lu: " f, pthread_self() % 10000, ##a)

//	fprintf(stdout, "DEBUG: %lu: %s: " f, pthread_self() % 10000, __func__, ##a);
#ifdef TP_DEBUG
#define tp_debug(f, a...)  do { \
	fprintf(stdout, "DEBUG: %llu: %s: " f, (unsigned long long)pthread_self() % 10000, __func__, ##a); \
	fflush(stdout); \
	} while(0)
#else
#define tp_debug(f, a...)  do { ; } while(0)
#endif
#define tp_debug2(f, a...)  do { \
	fprintf(stdout, "DEBUG: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stdout); \
	} while(0)

/* die by abort()ing; is exit(-1) better? */
#define tp_die(f, a...)  do { \
	fprintf(stderr, "TP: Fatal error (%lu: %s): " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stderr); \
	abort(); \
	} while(0)

#ifdef TP_DEBUG_LOCK
#define tp_debug_lock(f, a...)  do { \
	fprintf(stdout, "DEBUG: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stderr); \
	} while(0)
#else
#define tp_debug_lock(f, a...)  do { ; } while(0)
#endif

#define tp_testcase_int(description, expected, actual) do { \
	fprintf(stdout, "TEST %s: %llu: %s: expected=%d, actual=%d\n", \
			(expected == actual) ? "PASS" : "FAIL", \
			(unsigned long long)pthread_self() % 10000, description, \
			expected, actual); \
} while(0)

#define tp_testcase_uint(description, expected, actual) do { \
	fprintf(stdout, "TEST %s: %llu: %s: expected=%u, actual=%u\n", \
			(expected == actual) ? "PASS" : "FAIL", \
			(unsigned long long)pthread_self() % 10000, description, \
			expected, actual); \
} while(0)

#define tp_testcase_ptr(description, expected, actual) do { \
	fprintf(stdout, "TEST %s: %llu: %s: expected=%p, actual=%p\n", \
			(expected == actual) ? "PASS" : "FAIL", \
			(unsigned long long)pthread_self() % 10000, description, \
			expected, actual); \
} while(0)

#define tp_testcase_uint64(tid, description, expected, actual) do { \
	fprintf(stdout, "TEST %s: %lu: %s: expected=%llu, actual=%llu\n", \
			(expected == actual) ? "PASS" : "FAIL", \
			tid, description, expected, actual); \
} while(0)

#define tp_testcase_uint64_not(tid, description, expected, actual) do { \
	fprintf(stdout, "TEST %s: %lu: %s: expected != %llu, actual=%llu\n", \
			(expected != actual) ? "PASS" : "FAIL", \
			tid, description, expected, actual); \
} while(0)

#define tp_testcase_string(tid, description, expected, actual) do { \
	fprintf(stdout, "TEST %s: %lu: %s: expected=%s, actual=%s\n", \
			(expected == NULL) ? "FAIL" : \
			(actual == NULL) ? "FAIL" : \
			(strcmp(expected, actual) == 0) ? "PASS" : "FAIL", \
			tid, description, expected, actual); \
} while(0)

#endif  //THREADPOOL_MACROS_H

