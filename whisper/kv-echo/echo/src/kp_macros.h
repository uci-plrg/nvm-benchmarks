/*
 * Peter Hornyack & Katelin Bailey
 * Created: 8/28/2011
 * University of Washington
 */

#ifndef KP_MACROS_H
#define KP_MACROS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <pm_instr.h>

/* KP_ASSERT should always be defined for sanity-checking purposes, and
 * only un-defined when we want to disable sanity-checking code to get
 * maximum performance (it will probably make very little difference
 * though).
 * KP_DEBUG enables verbose debug messages.
 * KP_DEBUG_RECOVERY enables debug messages specifically related to recovery.
 * KP_DEBUG_LOCK enables verbose messages for mutex and rwlock lock/unlock.
 */
#define KP_ASSERT
//#define KP_EVAL_LOG
//#define KP_DEBUG
//#define KP_DEBUG2
//#define KP_DEBUG_GC
//#define KP_DEBUG_RECOVERY
//#define KP_DEBUG_LOCK
//#define KP_DEBUG_LOCK2
#define KP_SUPPRESS_TODOS

#define do_forked_run( function, ...)					\
  fflush(NULL);								\
  c = fork();								\
  if(c == 0){								\
    function(__VA_ARGS__);						\
    fflush(NULL);							\
    exit(0);								\
  }									\
  else{									\
    while (waitpid(c, 0, 0) == -1 && errno == EINTR)			\
      /* loop */;							\
  }									



/* If defined, versioning will not be performed, and all puts will overwrite
 * the previous (if any) value in the store. Disabling versioning will
 * eliminate some, but not entirely all, of the costs associated with
 * versioning. Some of the costs that will not (currently) be eliminated
 * include:
 *   Merging: currently we "reset" the local store by allocating a new
 *     one and freeing the previous one, but it might be more efficient
 *     to explicitly reset the values in the existing store (although,
 *     this may be true even with versioning in use...).
 *   
 */
//#define DISABLE_VERSIONING

/* If defined, garbage collection will not be performed. Note that
 * DISABLE_VERSIONING supercedes DISABLE_GC, because garbage collection
 * obviously isn't necessary when there's no versioning of the values.
 *   Well, maybe not so obvious: if we try to delete a kv pair, then without
 *   garbage collection running this probably doesn't happen...
 */
//#define DISABLE_GC

#define KP_MSG_SIZE 512
  /* Used for printing stats messages */

/* Note that printing the result of pthread_self() in this way is not
 * portable. On systems lab machines, /usr/include/bits/pthreadtypes.h
 * contains: "typedef unsigned long int pthread_t;".
 */

/* Print normal and debug output to stdout, warning and error output to
 * stderr. Always flush after printing; this makes debugging etc. easier,
 * but possibly causes slowdown.
 */
#ifdef KP_EVAL_LOG
#define kp_log(fp, f, a...)  do {			     \
	fprintf(fp, "KP: %lu: " f, pthread_self() % 10000, ##a); \
	fflush(stdout); \
	} while(0)
#else
#define kp_log(fp, f, a...)  do { ; } while(0)
#endif
#define kp_print(f, a...)  do { \
	fprintf(stdout, "KP: %lu: " f, pthread_self() % 10000, ##a); \
	fflush(stdout); \
	} while(0)
#define kp_warn(f, a...)  do { \
	fprintf(stderr, "**WARNING**: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stderr); \
	} while(0)
#ifdef KP_SUPPRESS_TODOS
#define kp_todo(f, a...)  do { ; } while(0)
#else
#define kp_todo(f, a...)  do { \
	fprintf(stdout, "TODO here: %s: " f, __func__, ##a); \
	fflush(stdout); \
	} while(0)
#endif
#define kp_error(f, a...)  do { \
	fprintf(stderr, "ERROR: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stderr); \
	} while(0)
#define kp_test(f, a...) fprintf(stdout, "TEST: %lu: " f, pthread_self() % 10000, ##a)

#ifdef KP_DEBUG
#define kp_debug(f, a...)  do { \
	fprintf(stdout, "DEBUG: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stdout); \
	} while(0)
#else
#define kp_debug(f, a...)  do { ; } while(0)
#endif

#ifdef KP_DEBUG2
#define kp_debug2(f, a...)  do { \
	fprintf(stdout, "DEBUG2: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stdout); \
	} while(0)
#else
#define kp_debug2(f, a...)  do { ; } while(0)
#endif

#ifdef KP_DEBUG_GC
#define kp_debug_gc(f, a...)  do { \
	fprintf(stdout, "DEBUG GC: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stdout); \
	} while(0)
#else
#define kp_debug_gc(f, a...)  do { ; } while(0)
#endif

#ifdef KP_DEBUG_RECOVERY
#define kp_debug_r(f, a...)  do { \
	fprintf(stdout, "DEBUG_R: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stdout); \
	} while(0)
#else
#define kp_debug_r(f, a...)  do { ; } while(0)
#endif

/* die by abort()ing; is exit(-1) better? */
#define kp_die(f, a...)  do { \
	fprintf(stderr, "KP: Fatal error (%lu: %s): " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stderr); \
	abort(); \
	} while(0)

#ifdef KP_DEBUG_LOCK
#define kp_debug_lock(f, a...)  do { \
	fprintf(stdout, "DEBUGL: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stdout); \
	} while(0)
#else
#define kp_debug_lock(f, a...)  do { ; } while(0)
#endif
#ifdef KP_DEBUG_LOCK2
#define kp_debug_lock2(f, a...)  do { \
	fprintf(stdout, "L: %lu: %s: " f, pthread_self() % 10000, __func__, ##a); \
	fflush(stdout); \
	} while(0)
#else
#define kp_debug_lock2(f, a...)  do { ; } while(0)
#endif

#define KP_TESTCASE
#ifdef KP_TESTCASE
#define kp_testcase_int(tid, description, expected, actual) do { \
	fprintf(stdout, "TEST %s: %lu: %s: expected=%d, actual=%d\n", \
			(expected == actual) ? "PASS" : "FAIL", \
			tid, description, expected, actual); \
} while(0)
#else
#define kp_testcase_int(tid, description, expected, actual) \
	do { ; } while(0)
#endif

#ifdef KP_TESTCASE
#define kp_testcase_size_t(tid, description, expected, actual) do { \
	fprintf(stdout, "TEST %s: %lu: %s: expected=%zu, actual=%zu\n", \
			(expected == actual) ? "PASS" : "FAIL", \
			tid, description, expected, actual); \
} while(0)
#else
#define kp_testcase_size_t(tid, description, expected, actual) \
	do { ; } while(0)
#endif

#ifdef KP_TESTCASE
#define kp_testcase_uint64(tid, description, expected, actual) do { \
	fprintf(stdout, "TEST %s: %lu: %s: expected=%ju, actual=%ju\n", \
			(expected == actual) ? "PASS" : "FAIL", \
			tid, description, expected, actual); \
} while(0)
#else
#define kp_testcase_uint64(tid, description, expected, actual) \
	do { ; } while(0)
#endif

#ifdef KP_TESTCASE
#define kp_testcase_uint64_not(tid, description, not_expected, actual) do { \
	fprintf(stdout, "TEST %s: %lu: %s: not_expected != %ju, actual=%ju\n", \
			(not_expected != actual) ? "PASS" : "FAIL", \
			tid, description, not_expected, actual); \
} while(0)
#else
#define kp_testcase_uint64_not(tid, description, not_expected, actual) \
	do { ; } while(0)
#endif

#ifdef KP_TESTCASE
#define kp_testcase_string(tid, description, expected, actual) do { \
	fprintf(stdout, "TEST %s: %lu: %s: expected=%s, actual=%s\n", \
			(expected == NULL) ? "FAIL (expected=NULL)" : \
			(actual == NULL) ? "FAIL (actual=NULL)" : \
			(strcmp(expected, actual) == 0) ? "PASS" : "FAIL", \
			tid, description, expected, actual); \
} while(0)
#else
#define kp_testcase_string(tid, description, expected, actual) \
	do { ; } while(0)
#endif

#define debug_print_retval(ret)  do { \
	kp_debug("ret is %d: %s\n", ret, \
			ret == 0 ? "success" : \
			ret == 1 ? "key not-found" : \
			ret == 2 ? "version not-found" : \
			ret == 3 ? "tombstone" : \
			ret == -1 ? "ERROR" : "UNKNOWN"); \
} while (0)

#define kp_retval_to_str(ret)  ret == 0 ? "success" : \
		ret == 1 ? "key not-found" : \
		ret == 2 ? "version not-found" : \
		ret == 3 ? "tombstone" : \
		ret == -1 ? "ERROR" : "UNKNOWN"

#if 0
void kp_testcase_int(char *description, int expected, int actual)
{
	fprintf(stdout, "TEST %s: %s: expected=%d, actual=%d\n",
			(expected == actual) ? "PASS" : "FAIL",
			description, expected, actual);
	//if (expected != actual) {
	//	exit(1);
	//}
}

void kp_testcase_string(char *description, char *expected, char *actual)
{
	fprintf(stdout, "TEST %s: %s: expected=%s, actual=%s\n",
			(strcmp(expected, actual) == 0) ? "PASS" : "FAIL",
			description, expected, actual);
	//if (expected != actual) {
	//	exit(1);
	//}
}
#endif

#endif  //KP_MACROS_H


