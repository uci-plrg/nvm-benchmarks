/*
 * Peter Hornyack
 * Created: 8/28/2011
 * University of Washington
 */

#ifndef VECTOR_MACROS_H
#define VECTOR_MACROS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

/* VECTOR_ASSERT should always be defined for sanity-checking purposes,
 * and only un-defined when we want to disable sanity-checking code to
 * get maximum performance (it will probably make very little difference
 * though).
 * VECTOR_DEBUG enables verbose debug messages.
 * VECTOR_DEBUG_LOCK enables verbose messages for mutex and rwlock lock/unlock.
 */
#define VECTOR_ASSERT
//#define VECTOR_DEBUG
//#define VECTOR_DEBUG2
//#define VECTOR_DEBUG_LOCK

/* Note that printing the result of pthread_self() in this way is not
 * portable. On systems lab machines, /usr/include/bits/pthreadtypes.h
 * contains: "typedef unsigned long int pthread_t;".
 */

/* Print normal and debug output to stdout, warning and error output to
 * stderr. Always flush after printing; this makes debugging etc. easier,
 * but possibly causes slowdown.
 */
#define v_print(f, a...)  do { \
	fprintf(stdout, "VECTOR: %lu: " f, pthread_self() % 10000, ##a); \
	fflush(stdout); \
	} while(0)
#define v_warn(f, a...)  do { \
	fprintf(stderr, "**WARNING**: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stderr); \
	} while(0)
#define v_error(f, a...)  do { \
	fprintf(stderr, "ERROR: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stderr); \
	} while(0)
#define v_test(f, a...) fprintf(stdout, "TEST: %lu: " f, pthread_self() % 10000, ##a)

#ifdef VECTOR_DEBUG
#define v_debug(f, a...)  do { \
	fprintf(stdout, "DEBUG: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stdout); \
	} while(0)
#else
#define v_debug(f, a...)  do { ; } while(0)
#endif

#ifdef VECTOR_DEBUG2
#define v_debug2(f, a...)  do { \
	fprintf(stdout, "DEBUG: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stdout); \
	} while(0)
#else
#define v_debug2(f, a...)  do { ; } while(0)
#endif

/* die by abort()ing; is exit(-1) better? */
#define v_die(f, a...)  do { \
	fprintf(stderr, "VECTOR: Fatal error (%lu: %s): " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stderr); \
	abort(); \
	} while(0)

#ifdef VECTOR_DEBUG_LOCK
#define v_debug_lock(f, a...)  do { \
	fprintf(stdout, "DEBUG: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stdout); \
	} while(0)
#else
#define v_debug_lock(f, a...)  do { ; } while(0)
#endif

#define v_testcase_int(tid, description, expected, actual) do { \
	fprintf(stdout, "TEST %s: %lu: %s: expected=%d, actual=%d\n", \
			(expected == actual) ? "PASS" : "FAIL", \
			tid, description, expected, actual); \
} while(0)

#define v_testcase_ul(tid, description, expected, actual) do { \
	fprintf(stdout, "TEST %s: %lu: %s: expected=%lu, actual=%lu\n", \
			(expected == actual) ? "PASS" : "FAIL", \
			tid, description, (unsigned long)expected, actual); \
} while(0)

#define v_testcase_ul_not(tid, description, expected, actual) do { \
	fprintf(stdout, "TEST %s: %lu: %s: expected != %lu, actual=%lu\n", \
			(expected != actual) ? "PASS" : "FAIL", \
			tid, description, (unsigned long)expected, actual); \
} while(0)

#define v_testcase_uint64(tid, description, expected, actual) do { \
	fprintf(stdout, "TEST %s: %lu: %s: expected=%ju, actual=%ju\n", \
			(expected == actual) ? "PASS" : "FAIL", \
			tid, description, expected, actual); \
} while(0)

#define v_testcase_uint64_not(tid, description, expected, actual) do { \
	fprintf(stdout, "TEST %s: %lu: %s: expected != %ju, actual=%ju\n", \
			(expected != actual) ? "PASS" : "FAIL", \
			tid, description, expected, actual); \
} while(0)

#define v_testcase_string(tid, description, expected, actual) do { \
	fprintf(stdout, "TEST %s: %lu: %s: expected=%s, actual=%s\n", \
			(expected == NULL) ? "FAIL" : \
			(actual == NULL) ? "FAIL" : \
			(strcmp(expected, actual) == 0) ? "PASS" : "FAIL", \
			tid, description, expected, actual); \
} while(0)

#endif  //VECTOR_MACROS_H

