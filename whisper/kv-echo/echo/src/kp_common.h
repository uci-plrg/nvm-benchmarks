/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Peter Hornyack & Katelin Bailey
 * 3/22/12
 *
 * Contains "utility" functions that are common to kp_kvstore, kp_kv_local,
 * kp_kv_master, and other modules in our code.
 */

#ifndef KP_COMMON_H__
#define KP_COMMON_H__

#include "kp_macros.h"
#include <pthread.h>

/* Defining UNSAFE_COMMIT will skip the lock/unlock on the master kvstore's
 * snapshot_lock when commits are appended to the commit log. When this
 * setting is enabled, we must also define and pre-allocate the size of
 * the entire commit log, or else we will get segfaults when vector resizing
 * occurs. */
#define UNSAFE_COMMIT_LOG_SIZE (16 * 5000)

/* When this is defined, we'll skip adding and removing snapshots from
 * the garbage collector's snapshots_in_use list - we definitely don't
 * actually use it right now, and we may be able to do without it
 * altogether at some point. */
#define DISABLE_GC_SNAPSHOT_TRACKING

/* defining REHASHING_ENABLED will allow rehashing to happen in the
 * hash table, and will perform read-write locking to ensure that lookups
 * aren't corrupted by rehashing, but which causes rehashing to halt
 * all other hash table operations. Note that in hash.c, we only take
 * the rwlock for _writing_ when we're about to rehash - this means
 * that when rehashing is disabled, we can also eliminate the rwlock
 * entirely (which the code now does). */
//#define REHASHING_ENABLED

/* Defining each of these will save one pthreads init + destroy for every
 * mutex, rwlock and condition variable that is allocated in the kvstore.
 * These must be undefined if non-default attributes will be used for these
 * synchronization primitives. See kp_common.c for more detail. */
#define DEFAULT_MUTEX_ATTR
#define DEFAULT_RWLOCK_ATTR
#define DEFAULT_COND_ATTR

/* Allocates a mutex using a set of attributes appropriate for our kvstore.
 * The name argument is only used for printing messages.
 * Returns: 0 on success, -1 on failure.
 */
int kp_mutex_create(const char *name, pthread_mutex_t **mutex);

/* Destroys a mutex that was created by kp_mutex_create(). The name argument
 * is only used for printing messages.
 * This function sets *mutex to NULL on success.
 */
void kp_mutex_destroy(const char *name, pthread_mutex_t **mutex);

/* Locks a mutex. The name argument is only used for printing messages. */
void kp_mutex_lock(const char *name, pthread_mutex_t *lock);

/* Unlocks a mutex. The name argument is only used for printing messages. */
void kp_mutex_unlock(const char *name, pthread_mutex_t *lock);

/* Allocates a read-write lock using a set of attributes appropriate for
 * our kvstore. The name argument is only used for printing messages.
 * Returns: 0 on success, -1 on failure.
 */
int kp_rwlock_create(const char *name, pthread_rwlock_t **rwlock);

/* Destroys a read-write lock that was created by kp_rwlock_create(). The
 * name argument is only used for printing messages.
 * This function sets *rwlock to NULL on success.
 * Returns: 0 on success, -1 on failure.
 */
void kp_rwlock_destroy(const char *name, pthread_rwlock_t **rwlock);

/* Read-locks a rwlock. The name argument is only used for printing messages. */
void kp_rdlock(const char *name, pthread_rwlock_t *rwlock);

/* Write-locks a rwlock. The name argument is only used for printing messages. */
void kp_wrlock(const char *name, pthread_rwlock_t *rwlock);

/* Read-unlocks a rwlock. The name argument is only used for printing messages. */
void kp_rdunlock(const char *name, pthread_rwlock_t *rwlock);

/* Write-unlocks a rwlock. The name argument is only used for printing messages. */
void kp_wrunlock(const char *name, pthread_rwlock_t *rwlock);

/* Allocates a condition variable using a set of attributes appropriate for
 * our kvstore. The name argument is only used for printing messages.
 * Returns: 0 on success, -1 on failure.
 */
int kp_cond_create(const char *name, pthread_cond_t **cond);

/* Destroys a condition variable that was created by kp_cond_create(). The
 * name argument is only used for printing messages.
 * This function sets *cond to NULL on success.
 * Returns: 0 on success, -1 on failure.
 */
void kp_cond_destroy(const char *name, pthread_cond_t **cond);

/* Wrapper function for GCC built-in compare-and-swap atomic function.
 * It uses the unsigned long type because it should match the word length
 * of x86 processors (32-bits on 32-bit processors, 64-bits on 64-bit
 * processors). Takes as input the address of a word in memory, the expected
 * value of the word, and the value to store in that word. This function
 * will ATOMICALLY check that the word in memory still matches the expected
 * value and, if so, write the new value into the word. This function will
 * then return the value that it read from the word - if the returned value
 * matches the expected value, then the swap was successful, otherwise the
 * value of the word changed before the CAS executed and the new value was
 * not written.
 *
 * Note that this function does not deal with the "ABA" problem that can
 * occur with basic compare-and-swap.
 */
unsigned long kp_cas(unsigned long *ptr, unsigned long expected_val,
		unsigned long new_val);

typedef struct kp_stats_struct {
	/* All of these are signed, rather than unsigned, so that the difference
	 * between two stats structs can be calculated.
	 */
	long int mem_data;
	long int mem_metadata;
} kp_stats;

/* Fills in *stats with statistics about allocated memory, garbage collection,
 * etc. - see the definition of kp_stats.
 */
void kp_stats_get(kp_stats *stats);

/* Fills in *result with the difference in statistics between *later and
 * *earlier. It is ok for result to be the same as earlier or later.
 */
void kp_stats_subtract(kp_stats *earlier, kp_stats *later, kp_stats *result);

/* Prints statistics info. */
void kp_stats_print(kp_stats *stats, const char *description);

#endif  //KP_COMMON_H

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
