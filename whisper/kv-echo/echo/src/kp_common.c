/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Peter Hornyack & Katelin Bailey
 * 3/22/12
 *
 */

#include "kp_common.h"
#include "kp_macros.h"
#include "kp_recovery.h"
#include <stdarg.h>

/* Tracing infrastructure */
__thread struct timeval mtm_time;
__thread int mtm_tid = -1;

__thread char tstr[TSTR_SZ];
__thread unsigned long long tsz = 0;
__thread unsigned long long tbuf_ptr = 0;

/* Can we make these thread local ? */
char *tbuf;
pthread_spinlock_t tbuf_lock, tot_epoch_lock;
unsigned long long tbuf_sz;
unsigned long long n_tentry = 0;
int mtm_enable_trace = 0, tmp_enable_trace = 0;
int trace_marker = -1, tracing_on = -1;
int mtm_debug_buffer = 1;
struct timeval glb_time;
unsigned long long start_buf_drain = 0, end_buf_drain = 0, buf_drain_period = 0;
unsigned long long glb_tv_sec = 0, glb_tv_usec = 0, glb_start_time = 0;
unsigned long long tot_epoch = 0;

__thread int reg_write = 0;
__thread unsigned long long n_epoch = 0;


void __pm_trace_print(int unused, ...)
{
        va_list __va_list;
        va_start(__va_list, unused);
        va_arg(__va_list, int); /* ignore first arg */
        char* marker = va_arg(__va_list, char*);
        unsigned long long addr = 0;

        if(!strcmp(marker, PM_FENCE_MARKER) ||
                !strcmp(marker, PM_TX_END)) {
                /*
                 * Applications are notorious for issuing
                 * fences, even when they didn't write to 
                 * PM. For eg., a fence for making a store
                 * to local, volatile variable visible.
                 */

                if(reg_write) {
                        n_epoch += 1;
                        pthread_spin_lock(&tot_epoch_lock);
                        tot_epoch += 1;
                        pthread_spin_unlock(&tot_epoch_lock);
                }
                reg_write = 0;

        } else if(!strcmp(marker, PM_WRT_MARKER) ||
                !strcmp(marker, PM_DWRT_MARKER) ||
                !strcmp(marker, PM_DI_MARKER) ||
                !strcmp(marker, PM_NTI)) {
                addr = va_arg(__va_list, unsigned long long);
                if((PSEGMENT_RESERVED_REGION_START < addr &&
                        addr < PSEGMENT_RESERVED_REGION_END))
                {
                        reg_write = 1;
                }
        } else {;}
        va_end(__va_list);
}

unsigned long long get_epoch_count(void)
{
        return n_epoch;
}

unsigned long long get_tot_epoch_count(void)
{
        return tot_epoch;
}

int kp_mutex_create(const char *name, pthread_mutex_t **mutex)
{
	int ret;
	int retval = 0;
#ifndef DEFAULT_MUTEX_ATTR
	pthread_mutexattr_t attr;

	ret = pthread_mutexattr_init(&attr);  /* fills attr with defaults */
	if (ret != 0) {
		kp_error("%s: pthread_mutexattr_init() returned error: %d\n",
				name, ret);
		return -1;
	}
	/* PTHREAD_MUTEX_ERRORCHECK attempts to detect deadlock and other
	 * errors; we should eventually disable it for better performance!
	 * Requires -D_GNU_SOURCE flag... bah  todo
	 */
	//ret = pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
	//if (ret != 0) {
	//	kp_error("pthread_mutexattr_settype() returned error: %d\n", ret);
	//	return -1;
	//}
#endif

	kp_malloc((void **)mutex, sizeof(pthread_mutex_t), false);
	  //CHECK: mutexes are ALWAYS in volatile memory, right?!? so use_nvm = false
	if (*mutex == NULL) {
		kp_error("%s: malloc(pthread_mutex_t) failed\n", name);
		retval = -1;
	} else {
#ifdef DEFAULT_MUTEX_ATTR
		ret = pthread_mutex_init(*mutex, NULL);
#else
		ret = pthread_mutex_init(*mutex, &attr);
#endif
		if (ret != 0) {
			kp_error("%s: pthread_mutex_init() returned error: %d\n",
					name, ret);
			kp_free((void **)mutex, false);
			retval = -1;
		}
	}

#ifndef DEFAULT_MUTEX_ATTR
	ret = pthread_mutexattr_destroy(&attr);
	if (ret != 0) {
		kp_error("%s: pthread_mutexattr_destroy() returned error: %d\n",
				name, ret);
		retval = -1;
	}
#endif

	return retval;
}

void kp_mutex_destroy(const char *name, pthread_mutex_t **mutex)
{
	int ret;

	if (!mutex) {
		kp_error("%s: mutex is null!\n", name);
		return;
	}
	if (*mutex) {
		ret = pthread_mutex_destroy(*mutex);
		if (ret != 0) {
			kp_error("%s: pthread_mutex_destroy() returned error: %d\n",
					name, ret);
		}

		kp_free((void **)mutex, false);
		*mutex = NULL;
	}
}

void kp_mutex_lock(const char *name, pthread_mutex_t *lock)
{
	int ret;
	kp_debug_lock("locking mutex %s (%p)\n", name, lock);
#ifdef KP_ASSERT
	if (!lock) {
		kp_die("locking mutex %s: lock is NULL!\n", name);
	}
#endif
	ret = pthread_mutex_lock(lock);
	kp_debug_lock("locked mutex %s (%p)\n", name, lock);
	if (ret != 0) {
		kp_die("pthread_mutex_lock() for %s returned error %d\n", name, ret);
	}
}

void kp_mutex_unlock(const char *name, pthread_mutex_t *lock)
{
	int ret;
	kp_debug_lock("unlocking mutex %s (%p)\n", name, lock);
#ifdef KP_ASSERT
	if (!lock) {
		kp_die("locking mutex %s: lock is NULL!\n", name);
	}
#endif
	ret = pthread_mutex_unlock(lock);
	kp_debug_lock("unlocked mutex %s (%p)\n", name, lock);
	if (ret != 0) {
		kp_die("pthread_mutex_unlock() for %s returned error %d\n", name, ret);
	}
}

int kp_rwlock_create(const char *name, pthread_rwlock_t **rwlock)
{
	int ret;
	int retval = 0;
#ifndef DEFAULT_RWLOCK_ATTR
	pthread_rwlockattr_t attr;

	ret = pthread_rwlockattr_init(&attr);  /* fills attr with defaults */
	if (ret != 0) {
		kp_error("%s: pthread_rwlockattr_init() returned error: %d\n",
				name, ret);
		return -1;
	}
#endif

	kp_malloc((void **)rwlock, sizeof(pthread_rwlock_t), false);
	  //CHECK: rwlocks are ALWAYS in volatile memory, right?!? so use_nvm = false.
	if (*rwlock == NULL) {
		kp_error("%s: malloc(pthread_rwlock_t) failed\n", name);
		retval = -1;
	} else {
#ifdef DEFAULT_RWLOCK_ATTR
		ret = pthread_rwlock_init(*rwlock, NULL);
#else
		ret = pthread_rwlock_init(*rwlock, &attr);
#endif
		if (ret != 0) {
			kp_error("%s: pthread_rwlock_init() returned error: %d\n",
					name, ret);
			kp_free((void **)rwlock, false);
			retval = -1;
		}
	}

#ifndef DEFAULT_RWLOCK_ATTR
	ret = pthread_rwlockattr_destroy(&attr);
	if (ret != 0) {
		kp_error("%s: pthread_rwlockattr_destroy() returned error: %d\n",
				name, ret);
		retval = -1;
	}
#endif

	return retval;
}

void kp_rwlock_destroy(const char *name, pthread_rwlock_t **rwlock)
{
	int ret;

	if (!rwlock) {
		kp_error("%s: rwlock is null!\n", name);
		return;
	}
	if (*rwlock) {
		ret = pthread_rwlock_destroy(*rwlock);
		if (ret != 0) {
			kp_error("%s: pthread_rwlock_destroy() returned error: %d\n",
					name, ret);
		}

		kp_free((void **)rwlock, false);
		*rwlock = NULL;
	}
}

void kp_rdlock(const char *name, pthread_rwlock_t *rwlock)
{
	int ret;
	kp_debug_lock("READ-locking %s\n", name);
	ret = pthread_rwlock_rdlock(rwlock);
	kp_debug_lock("READ-locked %s\n", name);
	if (ret != 0) {
		kp_die("pthread_rwlock_rdlock() on %s returned error %d\n", name, ret);
	}
}

void kp_wrlock(const char *name, pthread_rwlock_t *rwlock)
{
	int ret;
	kp_debug_lock("WRITE-locking %s\n", name);
	ret = pthread_rwlock_wrlock(rwlock);
	kp_debug_lock("WRITE-locked %s\n", name);
	if (ret != 0) {
		kp_die("pthread_rwlock_wrlock() on %s returned error %d\n", name, ret);
	}
}

void kp_rdunlock(const char *name, pthread_rwlock_t *rwlock)
{
	int ret;
	kp_debug_lock("READ-unlocking %s\n", name);
	ret = pthread_rwlock_unlock(rwlock);
	kp_debug_lock("READ-unlocked %s\n", name);
	if (ret != 0) {
		kp_die("pthread_rwlock_rdlock() on %s returned error %d\n", name, ret);
	}
}

void kp_wrunlock(const char *name, pthread_rwlock_t *rwlock)
{
	int ret;
	kp_debug_lock("WRITE-unlocking %s\n", name);
	ret = pthread_rwlock_unlock(rwlock);
	kp_debug_lock("WRITE-unlocked %s\n", name);
	if (ret != 0) {
		kp_die("pthread_rwlock_wrunlock() on %s returned error %d\n", name, ret);
	}
}

int kp_cond_create(const char *name, pthread_cond_t **cond)
{
	int ret;
	int retval = 0;
#ifndef DEFAULT_COND_ATTR
	pthread_condattr_t attr;

	ret = pthread_condattr_init(&attr);  /* fills attr with defaults */
	if (ret != 0) {
		kp_error("%s: pthread_condattr_init() returned error: %d\n",
				name, ret);
		return -1;
	}
#endif

	kp_malloc((void **)cond, sizeof(pthread_cond_t), false);
	  //CHECK: condition variables are ALWAYS in volatile memory,
	  //right?!? so use_nvm = false.
	if (*cond == NULL) {
		kp_error("%s: malloc(pthread_cond_t) failed\n", name);
		retval = -1;
	} else {
#ifdef DEFAULT_COND_ATTR
		ret = pthread_cond_init(*cond, NULL);
#else
		ret = pthread_cond_init(*cond, &attr);
#endif
		if (ret != 0) {
			kp_error("%s: pthread_cond_init() returned error: %d\n",
					name, ret);
			kp_free((void **)cond, false);
			retval = -1;
		}
	}

#ifndef DEFAULT_COND_ATTR
	ret = pthread_condattr_destroy(&attr);
	if (ret != 0) {
		kp_error("%s: pthread_condattr_destroy() returned error: %d\n",
				name, ret);
		retval = -1;
	}
#endif

	return retval;
}

void kp_cond_destroy(const char *name, pthread_cond_t **cond)
{
	int ret;

	if (!cond) {
		kp_error("%s: cond is null!\n", name);
		return;
	}
	if (*cond) {
		ret = pthread_cond_destroy(*cond);
		if (ret != 0) {
			kp_error("%s: pthread_cond_destroy() returned error: %d\n",
					name, ret);
		}

		kp_free((void **)cond, false);
		*cond = NULL;
	}
}

unsigned long kp_cas(unsigned long *ptr, unsigned long expected_val,
		unsigned long new_val)
{
	/* http://gcc.gnu.org/onlinedocs/gcc-4.6.3/gcc/Atomic-Builtins.html#Atomic-Builtins */
	return __sync_val_compare_and_swap(ptr, expected_val, new_val);
}

void kp_stats_get(kp_stats *stats)
{
	uint64_t mem_data, mem_metadata;

	if (!stats) {
		kp_die("got a NULL stats pointer\n");
	}
	
	kp_get_mem_accounting(&mem_data, &mem_metadata);
	if (mem_data > INT64_MAX || mem_metadata > INT64_MAX) {
		kp_error("either mem_data=%ju or mem_metadata=%ju is greater than "
				"INT64_MAX=%ju, but we're about to cast these values to "
				"_signed_ long ints - this will result in nonsense stats\n",
				mem_data, mem_metadata, INT64_MAX);
	}

	stats->mem_data = (long int)mem_data;
	stats->mem_metadata = (long int)mem_metadata;
}

void kp_stats_subtract(kp_stats *earlier, kp_stats *later, kp_stats *result)
{
	if (!earlier || !later || !result) {
		kp_die("got a NULL argument: earlier=%p, later=%p, result=%p\n",
				earlier, later, result);
	}

	result->mem_data     = later->mem_data - earlier->mem_data;
	result->mem_metadata = later->mem_metadata - earlier->mem_metadata;
}

void kp_stats_print(kp_stats *stats, const char *description)
{
	if (stats && description) {
		printf("\tSTATS: %s:\n"
				"\t\tData (bytes): %ju\n"
				"\t\tMetadata (bytes): %ju\n"
				"\n",
				description, stats->mem_data, stats->mem_metadata);
		//kp_print("STATS: %s:\n", description);
		//kp_print("\tData (bytes): %ju\n", stats->mem_data);
		//kp_print("\tMetadata (bytes): %ju\n", stats->mem_metadata);
	} else {
		kp_error("null argument: stats=%p, description=%p\n", stats,
				description);
	}
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
