/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 * 
 * Katelin Bailey & Peter Hornyack
 * Created: 11/16/2011
 * University of Washington
 * 
 * After the addition of "checkpoints" to the kp_kvstore on 12/7/11, this
 * file contains no references (except in comments) to "gvns"; it should
 * probably/hopefully stay this way, let gvns be solely an internal part
 * of the kp_kvstore.
 */

#include "../include/kp_kv_master.h"
#include "kp_kv_master_internal.h"
#include "kp_kvstore_internal.h"
#include "kp_macros.h"
#include "kp_common.h"
#include "kp_recovery.h"
#include "PIN_Hooks.h"
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <string.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdarg.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

/* If LOCK_MASTER is defined, then the read-write lock on the master store
 * will be used - however, right now the lock is only taken for reading
 * anyway, so this just costs us a bit of time on every get from the master.
 * In other words, the master struct is currently immutable, so we don't
 * need to lock its fields. */
//#define LOCK_MASTER

uint32_t kp_master_id_count = 0;

/* External structures: */
struct kp_kv_master_struct {
	/* Note: we don't store a "use_nvm" flag in the master struct; we just
	 * have the one inside of the master's kvstore. */
	uint32_t id;
	kp_kvstore *kv;
	consistency_mode mode;
	/* Do we ever need to lock this struct? The id, kv and mode should all
	 * be immutable once the master has been created, so I don't think so.
	 * The kv does locking internally to handle concurrent accesses.
	 * HOWEVER, if we want to prevent concurrent accesses to the underlying
	 * kvstore from happening from the master (i.e. concurrent merges, or
	 * gets concurrent with a merge), we DO need to lock!
	 */
#ifdef LOCK_MASTER
	pthread_rwlock_t *rwlock;  //lock for this entire struct (not for the kp_kvstore)
	  /* read lock should be taken for general get() access to ->kv; write
	   * lock should be taken for exclusive access, if/when necessary.
	   */
#endif
	ds_state state;
};

int kp_kv_master_create(kp_kv_master **master, consistency_mode mode,
		size_t expected_max_keys, bool do_conflict_detection, bool use_nvm)
{
	int ret;

	if (!use_nvm) {  //master should pretty much always be on nvm...
		kp_die("got use_nvm = false, unexpectedly!\n");
	}

	/* Allocate a new kp_kv_master struct: use calloc because it's on nvm
	 * and we want it to be "CDDS"... */
	kp_kpalloc((void **)master, sizeof(kp_kv_master), use_nvm);
	if (*master == NULL) {
		kp_error("kp_calloc(kp_kv_master) returned error\n");
		return -1;
	}

	/* Initialize its members: */
	PM_EQU(((*master)->id), (kp_master_id_count));  //minor race condition... // persistent
	kp_master_id_count++;
	if (kp_master_id_count == UINT32_MAX) {
		kp_error("hit maximum id count for kp_masters: %u\n", kp_master_id_count);
		kp_free((void **)master, use_nvm);  //CHECK - TODO: call _destroy() here?
		return -1;
	}

	kp_debug("creating master kp_kvstore; expected_max_keys=%zu, "
			"do_conflict_detection=%s\n", expected_max_keys,
			do_conflict_detection ? "true" : "false");
	ret = kp_kvstore_create(&((*master)->kv), true, expected_max_keys,
			do_conflict_detection, true);
	  //First true: MASTER store
	  //Second true: use_nvm for master store!
	if (ret != 0 || (*master)->kv == NULL) {
		kp_error("kp_kvstore_create() returned error\n");
		kp_free((void **)master, use_nvm);  //CHECK - TODO: call _destroy() here?
		return -1;
	}
	PM_EQU(((*master)->mode), (mode)); // persistent
	  //TODO: need to check that the mode is valid? or does enum do this for us??

#ifdef LOCK_MASTER
	ret = kp_rwlock_create("master->rwlock", &((*master)->rwlock));
	if (ret != 0) {
		kp_error("kp_rwlock_create() returned error=%d\n", ret);
		kp_kvstore_destroy(&((*master)->kv));
		kp_free((void **)master, use_nvm);  //CHECK - TODO: call _destroy() here?
		return -1;
	}
#endif

	kp_reset_mem_accounting();

	/* For "CDDS": flush, set state, and flush again. These flushes will
	 * only have an effect if FLUSH_IT is defined. */
	kp_flush_range(*master, sizeof(kp_kv_master) - sizeof(ds_state), use_nvm);
	PM_EQU(((*master)->state), (STATE_ACTIVE)); // persistent
	kp_flush_range(&((*master)->state), sizeof(ds_state), use_nvm);

	kp_debug("created a new kp_kv_master struct with id=%u and mode=%s\n",
			(*master)->id,
			mode == MODE_WEAK ? "WEAK" : 
			mode == MODE_SNAPSHOT ? "SNAPSHOT" : 
			mode == MODE_SEQUENTIAL ? "SEQUENTIAL" : "unknown!");

	return 0;
}

/* What happens to local stores that link to this master???
 *   Whatever, user should have destroyed them already.
 */
int kp_kv_master_destroy(kp_kv_master *master)
{
	int ret, retval;
	bool use_nvm;

	kp_debug2("skipping kp_kv_master_destroy() for now!\n");
	return 0;

	if (master == NULL) {
		kp_error("master is NULL, doing nothing\n");
		return -1;
	}
	retval = 0;
	use_nvm = kp_uses_nvm(master->kv);
	kp_debug("got use_nvm = %s from master->kv\n", use_nvm ? "true" : "false");
	if (!use_nvm) {
		kp_die("use_nvm is unexpectedly false\n");
	}

#ifdef LOCK_MASTER
	kp_rwlock_destroy("master->rwlock", &(master->rwlock));
#endif

	kp_kvstore_destroy(&(master->kv));
	if (ret != 0) {
		kp_error("kp_kvstore_destroy() returned error: %d\n", ret);
		retval = -1;
	}
	kp_free((void **)&master, use_nvm);
	kp_debug("freed master store\n");

	return retval;
}

uint32_t kp_master_get_id(kp_kv_master *master)
{
	uint32_t ret;
	if (master == NULL) {
		kp_error("master is NULL\n");
		return -1;
	}

	ret = master->id;
	return ret;
}

consistency_mode kp_master_get_mode(kp_kv_master *master)
{
	consistency_mode ret;
	if (master == NULL) {
		kp_error("master is NULL\n");
		return -1;
	}

	ret = master->mode;
	return ret;
}

int kp_master_get(kp_kv_master *master, const kp_ht_entry *lookup_entry,
		void **value, size_t *size)
{
	int ret;

	if (master == NULL) {
		kp_error("master is NULL\n");
		return -1;
	}

	/* Just pass-through to underlying kp_kvstore's get(). We return the
	 * same value that the get() returns; on success, *value and *size
	 * will have been set to point to a new value and size.
	 */
	kp_debug("calling kp_get(master->kv) and returning whatever it does\n");
#ifdef LOCK_MASTER
	kp_rdlock("master->rwlock", master->rwlock);
#endif
	ret = kp_get(master->kv, lookup_entry, value, size);
#ifdef LOCK_MASTER
	kp_rdunlock("master->rwlock", master->rwlock);
#endif
	kp_debug("kp_get() returned %d: %s\n", ret, 
			ret == 0 ? "success" :
			ret == 1 ? "key not-found" :
			ret == 2 ? "version not-found" :
			ret == 3 ? "tombstone" :
			ret == -1 ? "ERROR" : "UNKNOWN");
	if (ret < 0) {
		kp_error("kp_get() returned error=%d, passing it back up\n", ret);
	} else if ((ret != 0) && (ret != 1) && (ret != 3)) {
		/* We expect key not-found or tombstone, but not version not-found
		 * (since we're always getting latest) or other return codes:
		 */
		kp_error("kp_get() returned unexpected value=%d, returning -1\n",
				ret);
		ret = -1;
	}

	return ret;
}

int kp_master_get_by_snapshot(kp_kv_master *master, uint64_t snapshot,
		const kp_ht_entry *lookup_entry, void **value, size_t *size)
{
	int ret;

	if (!master || !lookup_entry) {
		kp_error("got a null argument: master=%p, lookup_entry=%p\n",
				master, lookup_entry);
		return -1;
	}

	/* With the addition of "checkpoints" to the underlying kp_kvstore,
	 * "snapshots" are the same as checkpoints (and the master store can
	 * ignore "gvns" here). So, just pass-through the snapshot to the
	 * underlying kvstore's get_version_snapshot(). We return the same
	 * value that the get() returns; on success, *value and *size will
	 * have been set to point to a new value and size.
	 */
	kp_debug2("calling kp_get_version_snapshot(master->kv, lookup_entry->"
			"key=%s, snapshot=%ju) and returning whatever it does\n",
			lookup_entry->key, snapshot);
#ifdef LOCK_MASTER
	kp_rdlock("master->rwlock", master->rwlock);
	  /* Lock the kv_master while using master->kv; note that this is
	   * NOT the same as locking the master kvstore, which is done inside
	   * of the get function that we're calling.
	   */
#endif

	/* Currently, kp_get_version_snapshot() will copy the value that is
	 * looked up, but will put it on VOLATILE memory, not NVM. This is
	 * because we think that all gotten values should be kept in volatile
	 * memory for better performance; they can easily be just gotten
	 * again if a failure happens.
	 */
	ret = kp_get_version_snapshot(master->kv, lookup_entry, snapshot,
			value, size);

#ifdef LOCK_MASTER
	kp_rdunlock("master->rwlock", master->rwlock);
#endif
	if (ret < 0) {
		kp_error("kp_get_version_snapshot() returned error=%d, passing it "
				"back up\n", ret);
	}
	kp_debug2("kp_get_version_snapshot(key=%s, snapshot=%ju) returned %d "
			"(value: %s)\n", lookup_entry->key, snapshot, ret,
			ret == 0 ? value ? (char *)(*value)
			                 : "value is NULL"
			         : "not-found or error");

	return ret;
}

uint64_t kp_master_get_latest_snapshot(kp_kv_master *master)
{
	uint64_t ret;
	if (!master) {
		kp_error("master is NULL\n");
		return UINT64_MAX;
	}
	ret = kp_get_latest_snapshot(master->kv);
	return ret;
}

#if OLD_CODE
int kp_master_print_stats(kp_kv_master *master, FILE *stream, bool detailed)
{
	kp_die("not implemented yet!\n");
	return -1;
}
#endif

uint64_t kp_master_get_initial_snapshot(kp_kv_master *master)
{
	uint64_t ret = kp_increment_snapshot_mark_in_use(master->kv);
	return ret;
}

bool kp_master_uses_nvm(kp_kv_master *master)
{
	bool ret = kp_uses_nvm(master->kv);
	return ret;
}

int kp_master_merge_commit(kp_kv_master *master, kp_commit_record *cr,
		        void **conflict_list, uint64_t *new_snapshot)
{
	/* This function and the functions that it calls are where most of the
	 * NVRAM work takes place: */

	int ret;

	if (!master || !cr) {
		kp_error("got a null argument: master=%p, cr=%p\n", master, cr);
		return -1;
	}
	kp_debug("entered; got conflict_list=%p (probably nil). Beginning "
			"the process of merging in this commit record\n", conflict_list);

	/* Eventually, we'd like this function to simply take the commit record
	 * from the local store, append it to a queue, and be done - some other
	 * master thread would come along, dequeue the commit record, and merge
	 * it in. However, for now, this function just merges in each commit
	 * synchronously.
	 *   When we do make this change, I'm not sure if it will make sense
	 *   to have a separate queue for this (in which case it seems like
	 *   it would be conceptually nicer to have the queue-of-pending-
	 *   commit-records in the master, and not in the kvstore itself),
	 *   or if we will just use the commit log itself as the queue (and
	 *   master threads will monitor/scan the commit log for commit records
	 *   that aren't complete or in-progress).
	 *
	 * The data structures that are needed to perform a merge - commit log,
	 * locks, global snapshot number, etc. - are all located inside of the
	 * kvstore itself. So, this function doesn't do much, and mostly just
	 * passes the commit record to the internal kvstore.
	 */
	ret = kp_commit(master->kv, cr, conflict_list, new_snapshot);
	if (ret == -1) {
		kp_error("kp_commit(master->kv) returned error!\n");
		return -1;
	} else if (ret == 1) {
		kp_debug2("kp_commit() returned 1: there was a conflict! "
				"We're returning 1 too; kp_commit() set *new_snapshot=%ju\n",
				*new_snapshot);
#ifdef KP_ASSERT
		if (*new_snapshot == UINT64_MAX) {
			kp_die("invalid *new_snapshot here: %ju\n", *new_snapshot);
		}
#endif
		return 1;
	}

#ifdef KP_ASSERT
	if (*new_snapshot == UINT64_MAX) {
		kp_die("invalid *new_snapshot here: %ju\n", *new_snapshot);
	}
#endif
	kp_debug("returning the new snapshot number for the local store that "
			"was set by kp_commit(): %ju\n", *new_snapshot);
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
