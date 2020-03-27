/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 * 
 * Katelin Bailey & Peter Hornyack
 * Created: 11/15/2011
 * University of Washington
 * 
 */

#include "../include/kp_kv_local.h"
#include "../include/kp_kv_master.h"
#include "kp_kvstore_internal.h"
#include "kp_kv_master_internal.h"
#include "kp_macros.h"
#include "kp_recovery.h"
#include "kp_common.h"
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

/* Global variables: */
uint32_t kp_local_id_count = 0;

/* External structures: */
struct kp_kv_local_struct {
	uint32_t id;                //not guaranteed to be unique _across failures_
	uint64_t snapshot;          //snapshot number that this store is working off of
	kp_kvstore *kv;             //each local index is just its own kp_kvstore, for now
	kp_kv_master *master;       //master store for this local store
	kp_ht_entry *lookup_entry;  //pre-allocated entry used for hash table lookup
	kp_iterator *iter;          //pre-allocated iterator (for merges)
	kp_iter_item *item;         //pre-allocated iterator item (for merges)
	consistency_mode mode;      //copy of master's consistency_mode
	size_t expected_max_keys;   //used to optimize creation of resizeable vectors
	pthread_rwlock_t *rwlock;   //lock for this entire struct (not for the kp_kvstore)
	  /* Basically, read lock should be taken anywhere ->kv or ->snapshot
	   * are accessed, write lock should be taken if/when they are modified;
	   * locking for ->master shouldn't be necessary, as it shouldn't
	   * ever change.
	   */
	ds_state state;
};

int kp_kv_local_create(kp_kv_master *master, kp_kv_local **local,
		size_t expected_max_keys, bool use_nvm)
{
	int ret;
	uint64_t ret64;

	if (master == NULL) {
		kp_error("master is NULL\n");
		return -1;
	}
	if (use_nvm) {
		kp_warn("got use_nvm = true; is this expected?\n");
	}

	/* FAIL anywhere in this function: we don't care about any changes
	 * to the local kvstore itself or anything we've allocated here.
	 * The master store should take care of all of the snapshot number
	 * stuff (i.e. making sure our current snapshot number is no longer
	 * marked as in-use) during recovery. After the master store has
	 * recovered, the local workers should be able to just call this
	 * function again and restart from scratch. We'll lose the
	 * kp_local_id_count, but nobody pays attention to that anyway.
	 */

	/* We only care about zero-allocation if use_nvm is true: */
	kp_kpalloc((void **)local, sizeof(kp_kv_local), use_nvm);
	if (*local == NULL) {
		kp_error("kp_kpalloc(kp_kv_local) failed\n");
		return -1;
	}

    (*local)->id = kp_local_id_count;  //minor race condition... // persistent
	kp_local_id_count++;
	if (kp_local_id_count == UINT32_MAX) {
		kp_error("hit maximum id count for kp_locals: %u\n", kp_local_id_count);
		kp_free((void **)local, use_nvm);  //CHECK - TODO: call _destroy() here instead!
		return -1;
	}

	kp_debug("creating initial local kp_kvstore; expected_max_keys=%zu\n",
			expected_max_keys);
	ret = kp_kvstore_create(&((*local)->kv), false, expected_max_keys, false,
			use_nvm);
	  /* First false: LOCAL store. Second false: don't perform conflict 
	   * detection in local store. */
	if (ret != 0 || (*local)->kv == NULL) {
		kp_error("kp_kvstore_create() failed\n");
		kp_free((void **)local, use_nvm);  //CHECK - TODO: call _destroy() here instead!
		return -1;
	}

	kp_debug("creating lookup_entry for kp_kv_local\n");
	ret = kp_ht_entry_create(&((*local)->lookup_entry), NULL, false, use_nvm);
	  //CHECK: even if use_nvm is true, isn't lookup_entry on the "list of
	  //  things that we'll always reset and re-allocate / reconstruct on
	  //  recovery"?
	  //  SAME THING for local->iter and local->item!?
	if (ret != 0) {
		kp_error("kp_ht_entry_create() returned error=%d\n", ret);
		kp_kvstore_destroy(&((*local)->kv));
		kp_free((void **)local, use_nvm);  //CHECK - TODO: call _destroy() here instead!
		return -1;
	}
	/* Ensure that if we call kp_ht_entry_destroy() below, it won't try to
	 * free some random address in memory! */
	(*local)->lookup_entry->key = NULL; // persistent

	ret = kp_iterator_create(&((*local)->iter), use_nvm);
	if (ret != 0) {
		kp_error("kp_iterator_create() returned error=%d\n", ret);
		kp_ht_entry_destroy(&((*local)->lookup_entry), use_nvm);
		kp_kvstore_destroy(&((*local)->kv));
		kp_free((void **)local, use_nvm);  //CHECK - TODO: call _destroy() here instead!
		return -1;
	}

	ret = kp_iter_item_create_internal(&((*local)->item), use_nvm);
	if (ret != 0) {
		kp_error("kp_iter_item_create() returned error=%d\n", ret);
		kp_iterator_destroy(&((*local)->iter));
		kp_ht_entry_destroy(&((*local)->lookup_entry), use_nvm);
		kp_kvstore_destroy(&((*local)->kv));
		kp_free((void **)local, use_nvm);  //CHECK - TODO: call _destroy() here instead!
		return -1;
	}

	/* Ask the master for an initial snapshot number: */
	(*local)->master = master;	// persistent
	(*local)->mode = kp_master_get_mode(master);	// persistent
	ret64 = kp_master_get_initial_snapshot((*local)->master);	// persistent
	if (ret64 == UINT64_MAX) {
		kp_error("kp_master_get_initial_snapshot() returned error\n");
		kp_iter_item_destroy(&((*local)->item));
		kp_iterator_destroy(&((*local)->iter));
		kp_ht_entry_destroy(&((*local)->lookup_entry), use_nvm);
		kp_kvstore_destroy(&((*local)->kv));
		kp_free((void **)local, use_nvm);  //CHECK - TODO: call _destroy() here instead!
		return -1;
	}
	(*local)->snapshot = ret64;	// persistent
	kp_debug("initialized local snapshot to %ju (got from master)\n",
			(*local)->snapshot);
	(*local)->expected_max_keys = expected_max_keys;	// persistent

	ret = kp_rwlock_create("local->rwlock", &((*local)->rwlock));
	if (ret != 0) {
		kp_error("kp_rwlock_create() returned error: %d\n", ret);
		kp_iter_item_destroy(&((*local)->item));
		kp_iterator_destroy(&((*local)->iter));
		kp_ht_entry_destroy(&((*local)->lookup_entry), use_nvm);
		kp_kvstore_destroy(&((*local)->kv));
		kp_free((void **)local, use_nvm);  //CHECK - TODO: call _destroy() here instead!
		return -1;
	}

	kp_debug("created a new kp_kv_local struct with id=%u, snapshot=%ju\n",
			(*local)->id, (*local)->snapshot);

	/* CDDS: flush, set state, and flush again. Will only actually do
	 * any flushing if both use_nvm and FLUSH_IT are true/defined. */
	kp_flush_range(*local, sizeof(kp_kv_local) - sizeof(ds_state), use_nvm);
	(*local)->state = STATE_ACTIVE;	// persistent
	kp_flush_range(&((*local)->state), sizeof(ds_state), use_nvm);

	return 0;
}

void kp_kv_local_destroy(kp_kv_local **local)
{
	bool use_nvm;

	if (!local) {
		kp_error("local is NULL\n");
		return;
	}
	if (*local) {
		use_nvm = kp_uses_nvm((*local)->kv);
		(*local)->state = STATE_DEAD;
		kp_flush_range(&((*local)->state), sizeof(ds_state), use_nvm);

		kp_rwlock_destroy("(*local)->rwlock", &((*local)->rwlock));
		kp_iter_item_destroy(&((*local)->item));
		kp_iterator_destroy(&((*local)->iter));
		/* IMPORTANT: set lookup_entry's key pointer to NULL, so that
		 * kp_ht_entry_destroy() doesn't free it! (wait for kp_kvstore_destroy()
		 * to free it.) */
		(*local)->lookup_entry->key = NULL;
		kp_ht_entry_destroy(&((*local)->lookup_entry), use_nvm);
		kp_kvstore_destroy(&((*local)->kv));
		
		/* Don't free the master, obviously */
		kp_free((void **)local, use_nvm);
		kp_debug("freed local store\n");
	}
}

uint64_t kp_local_put(kp_kv_local *local, const char *key, const void *value,
		size_t size)
{
	int ret;
	uint64_t retval;

	if (local == NULL) {
		kp_error("local is NULL\n");
		return -1;
	}

	/* Set our lookup_entry to point to the same key that the user passed
	 * to us. This code assumes that there is only a single thread accessing
	 * the local store at a time, so we have exclusive access to lookup_entry.
	 * Forget about kp_ht_entry_set(), just do it directly:
	 */
	local->lookup_entry->key = key;
	local->lookup_entry->vt = NULL;
	kp_debug("set local->lookup_entry key=%s, vt=%p\n",
			local->lookup_entry->key, local->lookup_entry->vt);

	/* Just pass-through to kvstore's put, which returns the current
	 * local version number for the key, or UINT64_MAX on error. This
	 * is what we return as well. We have to take the local store's
	 * read-lock around the put, since we're reading the pointer to
	 * the kp_kvstore!
	 *   Actually, removed this for now, since we're assuming that local
	 *   workers are the only accessor of their local store.
	 */
#ifdef LOCK_LOCAL
	kp_rdlock("local->rwlock", local->rwlock);
#endif
	retval = kp_put(local->kv, local->lookup_entry, value, size,
			local->snapshot);
#ifdef LOCK_LOCAL
	kp_rdunlock("local->rwlock", local->rwlock);
#endif
	if (retval == UINT64_MAX) {
		kp_error("kp_put returned error\n");
	}

	/* In SEQUENTIAL mode, we want to merge after every put! In other
	 * modes, we only merge when user explicitly performs a merge (for
	 * now).
	 */
	if (local->mode == MODE_SEQUENTIAL) {
		kp_debug("mode is SEQUENTIAL, performing a merge!\n");
		ret = kp_local_commit(local, NULL);  // TODO: fix this!?
		if (ret != 0) {
			kp_error("kp_local_merge() returned error=%d\n", ret);
			return UINT64_MAX;
		}
	}

	return retval;
}

/* Gets the current version of the key's value from the LOCAL store. This
 * function does not go to the master store if the key is not found in
 * the local store.
 * This function makes a copy of the value, so it is the caller's
 * responsibility to free the data.
 * Returns: If the key is found, 0 is returned, the value argument
 * is set to point to the new copy of the value, and the size argument
 * is set to the size of the value. If the key is not found in the
 * store, 1 is returned. If the key is found but the version we got
 * is a deletion tombstone (value is NULL), 3 is returned. If an error
 * occurred, -1 is returned. This function should never return 2 (version-
 * not-found).
 */
int kp_local_get_from_local(kp_kv_local *local, const char *key,
		void **value, size_t *size)
{
	int ret;

	if (local == NULL) {
		kp_error("local is NULL\n");
		return -1;
	}

	/* Set our lookup_entry to point to the same key that the user passed
	 * to us. This code assumes that there is only a single thread accessing
	 * the local store at a time, so we have exclusive access to lookup_entry.
	 * Forget about kp_ht_entry_set(), just do it directly:
	 */
	local->lookup_entry->key = key;
	local->lookup_entry->vt = NULL;
	kp_debug("set local->lookup_entry key=%s, vt=%p\n",
			local->lookup_entry->key, local->lookup_entry->vt);

	/* Then, just pass-through to underlying kp_kvstore's get(),
	 * after taking the rdlock on the local struct. We return the same
	 * value that the get() returned; on success, *value and *size will
	 * have been set to point to a new value and size.
	 */
	kp_debug("calling kp_get(local->kv) and returning whatever it does\n");
#ifdef LOCK_LOCAL
	kp_rdlock("local->rwlock", local->rwlock);
#endif
	ret = kp_get(local->kv, local->lookup_entry, value, size);
#ifdef LOCK_LOCAL
	kp_rdunlock("local->rwlock", local->rwlock);
#endif
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

/* Gets the current version of the key's value from the MASTER store. This
 * function does not ever return a value from the local store.
 * This function makes a copy of the value, so it is the caller's
 * responsibility to free the data.
 * Returns: If the key is found, 0 is returned, the value argument
 * is set to point to the new copy of the value, and the size argument
 * is set to the size of the value. If the key is not found in the
 * store, 1 is returned. If the key is found but the version we got
 * is a deletion tombstone (value is NULL), 3 is returned. If an error
 * occurred, -1 is returned. This function should never return 2 (version-
 * not-found).
 */
int kp_local_get_from_master(kp_kv_local *local, const char *key,
		void **value, size_t *size)
{
	int ret;

	if (local == NULL) {
		kp_error("local is NULL\n");
		return -1;
	}

	kp_die("this is kind of dead code: only called in WEAK or SEQUENTIAL "
			"consistency modes!\n");

	/* Set our lookup_entry to point to the same key that the user passed
	 * to us. This code assumes that there is only a single thread accessing
	 * the local store at a time, so we have exclusive access to lookup_entry.
	 * Forget about kp_ht_entry_set(), just do it directly:
	 */
	local->lookup_entry->key = key;
	local->lookup_entry->vt = NULL;
	kp_debug("set local->lookup_entry key=%s, vt=%p\n",
			local->lookup_entry->key, local->lookup_entry->vt);

	/* Just pass-through to the master kvstore's get(). We don't need to
	 * take the rdlock, since local->master should be immutable for the
	 * lifetime of the local struct. We return the same value that the
	 * get() returned; on success, *value and *size will have been set
	 * to point to a new value and size.
	 */
	kp_debug("calling kp_master_get(local->master) and returning "
			"whatever it does\n");
	ret = kp_master_get(local->master, local->lookup_entry, value, size);
	if (ret < 0) {
		kp_error("kp_master_get() returned error=%d, passing it back up\n", ret);
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

/* Gets a snapshot version of the key's value from the MASTER store.
 * Right now, this is only internally visible, but it could be made
 * externally visible to workers if we eventually want to.
 * This function makes a copy of the value, so it is the caller's
 * responsibility to free the data.
 * Returns: If the key is found, 0 is returned, the value argument
 * is set to point to the new copy of the value, and the size argument
 * is set to the size of the value. If the key is not found in the
 * store, 1 is returned. If the key is found but the version we got
 * is a deletion tombstone (value is NULL), 3 is returned. If an error
 * occurred, -1 is returned. This function should never return version-
 * not-found (2).
 */
int kp_local_get_from_master_snapshot(kp_kv_local *local, uint64_t snapshot,
		const char *key, void **value, size_t *size)
{
	int ret;

	if (local == NULL) {
		kp_error("local is NULL\n");
		return -1;
	}
	if (snapshot == UINT64_MAX) {
		kp_error("invalid snapshot number: %ju\n", snapshot);
		return -1;
	}

	kp_debug2("entered: snapshot=%ju, key=%s\n", snapshot, key);

	/* Even when getting from the master store, we use the local worker's
	 * pre-allocated lookup_entry. Set it to point to the same key that the
	 * user passed to us. This code assumes that there is only a single thread
	 * accessing the local store at a time, so we have exclusive access to
	 * lookup_entry. Forget about kp_ht_entry_set(), we just do it directly:
	 */
	local->lookup_entry->key = key;
	local->lookup_entry->vt = NULL;
	kp_debug("set local->lookup_entry key=%s, vt=%p\n",
			local->lookup_entry->key, local->lookup_entry->vt);

	/* Pass-through to the master kvstore's get_by_snapshot(). We don't need
	 * to take the rdlock, since local->master should be immutable for the
	 * lifetime of the local struct. We return the same value that the
	 * get() returned; on success, *value and *size will have been set
	 * to point to a new value and size.
	 */
	kp_debug("calling kp_master_get_by_snapshot(local->master) and returning "
			"whatever it does\n");
	ret = kp_master_get_by_snapshot(local->master, snapshot,
			local->lookup_entry, value, size);
	if (ret < 0) {
		kp_error("kp_master_get_by_snapshot() returned error=%d, "
				"passing it back up\n", ret);
	} else if (ret > 3) {
		/* We expect key not-found or tombstone or version not-found
		 * (since we're getting from snapshot), but not other return codes:
		 */
		kp_error("kp_get() returned unexpected value=%d, returning -1\n",
				ret);
		ret = -1;
	} else if (ret == 2) {
		kp_debug("got version-not-found for snapshot=%ju, transforming "
				"return code to just not-found\n", snapshot);
		ret = 1;
	}

	return ret;
}

/* Looks for the key in the LOCAL store first, and if it is found, returns
 * the latest/current version. If the key is not found in the local store,
 * this function will then look in the MASTER store, and return the version
 * of the value that was present in the designated snapshot (GVN?).
 * This function makes a copy of the value, so it is the caller's
 * responsibility to free the data.
 * Returns: If the key is found, 0 is returned, the value argument
 * is set to point to the new copy of the value, and the size argument
 * is set to the size of the value. If the key is not found in the
 * store, 1 is returned. If the key is found but the version we got
 * is a deletion tombstone (value is NULL), 3 is returned. If an error
 * occurred, -1 is returned. This function should never return 2 (version-
 * not-found).
 */
int kp_local_get_from_snapshot(kp_kv_local *local, const char *key,
		void **value, size_t *size)
{
	int ret;

	if (local == NULL) {
		kp_error("local is NULL\n");
		return -1;
	}

	//MAKE SURE TO: take local's rdlock while getting from local->kv
	//MAKE SURE TO: take local's rdlock while getting from local->master?
	/* First, try to get from local kvstore (we may have done a put to
	 * the key that hasn't been merged yet, or perhaps we're caching
	 * gets locally for the current snapshot):
	 */
	kp_debug("trying to get_from_local()\n");
	ret = kp_local_get_from_local(local, key, value, size);
	kp_debug2("kp_local_get_from_local() returned ret=%d (0=found, 1=not-"
			"found, 3=tombstone); my snapshot number=%ju\n", ret,
			local->snapshot);
	kp_debug("kp_local_get_from_local() returned %d\n", ret);
	if (ret == 1) {
		/* If not found in local kvstore, get the SNAPSHOT version from
		 * the master kvstore:
		 */
		kp_debug2("not found in local, trying to get from master using "
				"my snapshot number=%ju\n", local->snapshot);
		ret = kp_local_get_from_master_snapshot(local, local->snapshot,
				key, value, size);
		kp_debug2("kp_local_get_from_master_snapshot() returned %d; "
				"*value=%s\n", ret, value ? (char *)(*value) : "undefined");
		if (ret == 2) {
			kp_error("kp_local_get_from_master_snapshot() returned "
					"unexpected value=%d, returning error\n", ret);
			return -1;
		} else if (ret == 3) {
			kp_debug_gc("kp_local_get_from_master_snapshot() returned 3: we "
					"found a tombstone value for this key in the master "
					"store for the local worker's current snapshot number. "
					"We'll eventually return just \"not-found\" "
					"(rather than \"tombstone\") to the local worker.\n");
		}
	} else if (ret == 2) {
		kp_error("kp_local_get_from_local returned unexpected value %d, "
				"returning error\n", ret);
		return -1;
	} else if (ret == 3) {
		kp_debug_gc("kp_local_get_from_local() returned 3: we found a "
				"tombstone value for this key in the local store. This "
				"means that the local worker has deleted the key during "
				"its current commit! So, we don't try to get from the "
				"master store; we'll eventually return just \"not-found\" "
				"(rather than \"tombstone\") to the local worker.\n");
	}

	/* todo: we could add code here to "cache" gotten kv pairs for the
	 * current snapshot in our local kvstore. We can't just simply
	 * put() them though; we need to remember which ones are cached so
	 * that they don't get written again in the next merge. This could
	 * be done by adding a flag inside of the kp_kvstore itself, or by
	 * passing a list of these pairs to the master at merge time. Also,
	 * we would have to clear the cache on various actions (e.g. when
	 * snapshot number is changed, after a merge, ...)
	 */

	/* We don't have to do anything, just return the same return value. On
	 * success, *value and *size should have been set, and ret could be
	 * either 0 (usually) or 3 (for a tombstone value). On not-found, ret
	 * will be 1 (key not-found) or 2 (version not-found). On failure, ret
	 * will be -1.
	 */
	debug_print_retval(ret);
	return ret;
}

int kp_local_get(kp_kv_local *local, const char *key, void **value,
		size_t *size)
{
	int ret;
	consistency_mode mode;
	
	if (local == NULL) {
		kp_error("local is NULL\n");
		return -1;
	}

	/* Call the right get function(s) depending on the consistency mode
	 * that we're in. No need for locking here: local->master should
	 * be immutable while we read its mode, and the local kvstore is
	 * locked if necessary by each of the kp_local_get_*() functions.
	 * IMPORTANT: all of the get functions should return the same
	 * values, for simplicity: 0 if found, 1 if key not-found, 2 if
	 * version not-found (only for getting-from-snapshot functions),
	 * 3 if found a tombstone, -1 if error.
	 * HOWEVER, this function should never return 2 for version-not-found;
	 * the local worker should have no concept of "versions", only
	 * checkpoint/snapshot numbers, and if the internal kvstore returns
	 * version-not-found for this worker's checkpoint/snapshot number,
	 * then we should just return not-found to the worker.
	 */
	mode = kp_master_get_mode(local->master);
	switch (mode) {
	case MODE_WEAK:
		kp_debug("in MODE_WEAK, calling kp_local_get_from_local() first\n");
		ret = kp_local_get_from_local(local, key, value, size);
		/* If not-found in local, check master (for LATEST version); all
		 * other cases fall-through. kp_local_get_from_local() itself should
		 * never return 2, because it's always getting "current" version
		 * from the local store.
		 */
		if (ret == 1) {
			kp_debug("kp_local_get_from_local returned not-found, checking "
					"master\n");
			ret = kp_local_get_from_master(local, key, value, size);
			if (ret == 1) {
				kp_debug("kp_local_get_from_master returned not-found\n");
				return ret;
			} else if (ret == 2) {
				/* don't expect version-not-found (2), always getting
				 * "current" in master:
				 */
				kp_error("kp_local_get_from_master returned unexpected "
						"value %d (version-not-found), returning error\n",
						ret);
				return -1;
			}
		} else if (ret == 2) {
			kp_error("kp_local_get_from_local returned unexpected value %d, "
					"version-not-found\n", ret);
			return -1;
		}
		debug_print_retval(ret);
		return ret;
		break;
	case MODE_SNAPSHOT:
		kp_debug("in MODE_SNAPSHOT, calling kp_local_get_from_snapshot()\n");
		ret = kp_local_get_from_snapshot(local, key, value, size);
		debug_print_retval(ret);
		if (ret == 0) {
#ifdef KP_ASSERT
			if (!*value || *size == 0) {
				kp_die("*value=%p and *size=%zu, but didn't get expected "
						"return value (3) for tombstone!\n", *value, *size);
			}
#endif
			/* value and size should now be set. */
			kp_debug2("kp_local_get_from_snapshot returned success (%d); "
					"*value=%s\n", ret, value ? (char *)(*value) :
					"undefined");
			return ret;
		} else if (ret == 1 || ret == 3) {
			if (ret == 3) {
				kp_debug_gc("kp_local_get_from_snapshot() returned 3 - "
						"a tombstone value was found, in either the "
						"local store or the master store (we don't know "
						"which right here in this function). We treat this "
						"in the same was as key-not-found!\n");
			} else {
				kp_debug("kp_local_get_from_snapshot returned key not-found\n");
			}
			return 1;
		} else if (ret == 2) {
			kp_debug("kp_local_get_from_snapshot returned version "
					"not-found; this is unexpected, returning error\n");
			return -1;
		} else {
			kp_error("kp_local_get_from_snapshot returned error=%d\n", ret);
			return ret;
		}
		break;
	case MODE_SEQUENTIAL:
		/* In sequential mode, we always get LATEST from the master
		 * (right??):
		 */
		kp_debug("in MODE_SEQUENTIAL, calling kp_local_get_from_master()\n");
		ret = kp_local_get_from_master(local, key, value, size);
		debug_print_retval(ret);
		if ((ret == 0) || (ret == 3)) {
			/* value and size should now be set. */
			kp_debug("kp_local_get_from_master returned success (%d)\n",
					ret);
			return ret;
		} else if (ret == 1) {
			kp_debug("kp_local_get_from_master returned not-found\n");
			return ret;
		} else if (ret == 2) {
			/* Don't expect version-not-found (2), always getting "current"
			 * in master:
			 */
			kp_error("kp_local_get_from_master unexpectedly returned "
					"version-not-found (%d)\n", ret);
			return -1;
		} else { 
			kp_error("kp_local_get_from_master returned error=%d\n", ret);
			return ret;
		}
		break;
	default:
		kp_error("unrecognized mode (%d)!\n", mode);
		return -1;
		break;
	}

	kp_die("reached unreachable code!\n");
	return -1;
}

/* Note: this function is nearly identical to kp_local_put(). */
int kp_local_delete_key(kp_kv_local *local, const char *key)
{
	int ret;

	if (local == NULL) {
		kp_error("local is NULL\n");
		return -1;
	}

	/* Set our lookup_entry to point to the same key that the user passed
	 * to us. This code assumes that there is only a single thread accessing
	 * the local store at a time, so we have exclusive access to lookup_entry.
	 * Forget about kp_ht_entry_set(), just do it directly:
	 */
	local->lookup_entry->key = key;
	local->lookup_entry->vt = NULL;
	kp_debug("set local->lookup_entry key=%s, vt=%p\n",
			local->lookup_entry->key, local->lookup_entry->vt);

#ifdef LOCK_LOCAL
	kp_rdlock("local->rwlock", local->rwlock);
#endif
	ret = kp_delete_key(local->kv, local->lookup_entry, local->snapshot);
#ifdef LOCK_LOCAL
	kp_rdunlock("local->rwlock", local->rwlock);
#endif
	if (ret != 0) {
		kp_error("kp_put returned error %d\n", ret);
	}

	/* In SEQUENTIAL mode, we want to merge after every put! */
	if (local->mode == MODE_SEQUENTIAL) {
		kp_debug("mode is SEQUENTIAL, performing a merge!\n");
		ret = kp_local_commit(local, NULL);
		if (ret != 0) {
			kp_error("kp_local_merge() returned error=%d\n", ret);
		}
	}

	return (ret == 0 ? 0 : -1);
}

/* Performs cleanup steps after a single commit has completed. Currently,
 * when commit records are constructed, the iteration process frees every
 * version table entry as it goes along, but the hash table entries and
 * the version table structs that they point to remain. So, to clean these
 * up, this function calls a hash table clear function that calls our
 * callback function on every entry in the table.
 *
 * If/when we move to a model where a local worker can perform multiple
 * commits before performing a merge, I'm not exactly sure how this function
 * will change and where exactly the entries / vts / vtes should be cleaned
 * up (if at all...).
 *
 * Returns: 0 on success, -1 on error. */
int kp_local_cleanup_after_commit(kp_kv_local *local)
{
	int ret;

	if (!local) {
		kp_error("got a null argument: local=%p\n", local);
		return -1;
	}

	kp_todo("Make this function CDDS, add flushing, etc.?\n");

	/* In kp_kvstore.c, local hash tables are initialized with the hash
	 * freer function kp_ht_entry_free_local(). That function is called
	 * on every kp_ht_entry in the hash table when we call hash_clear().
	 * kp_kvstore_reset() calls hash_clear() on the internal hash table,
	 * and resets other internal kvstore state. */
	ret = kp_kvstore_reset(local->kv);
	if (ret != 0) {
		kp_error("kp_kvstore_reset() returned error=%d\n", ret);
		return -1;
	}

	/* Looking at the kp_kv_local struct, I don't see anything else that
	 * needs to be reset / cleaned up here: our caller will set the new
	 * snapshot number, the pre-allocated items don't need to change or
	 * be reset, and the master doesn't change. */
	kp_debug("done cleaning up local kvstore after merge\n");
	return 0;
}

#if OLD_CODE
/* Performs cleanup steps after a merge has completed. Currently unused... */
int kp_local_cleanup_after_merge(kp_kv_local *local)
{
	kp_die("call kp_local_cleanup_after_commit(), not "
			"kp_local_cleanup_after_merge() ??\n");
}
#endif

/* Puts a new commit record on the local store's "commit queue", which
 * currently doesn't exist. So, instead this function immediately calls
 * kp_master_merge_commit() with the commit record that it is passed.
 *
 * Returns: 0 on success, 1 on conflict, -1 on error. On success or conflict,
 * *new_snapshot is set to a new snapshot number for the client to work with.
 */
int kp_local_enqueue_commit(kp_kv_local *local, kp_commit_record *cr,
		void **conflict_list, uint64_t *new_snapshot)
{
	int ret, ret_merge;

	if (!local || !cr) {
		kp_error("got a null argument: local=%p, cr=%p\n", local, cr);
		return -1;
	}

	kp_debug("eventually, we can create a queue here that stores the local "
			"worker's commits that are waiting to be merged. For now though, "
			"we couple commits and merges together and merge every individual "
			"commit.\n");

	/* See the comments in kp_kv_master_internal.h for all of the steps
	 * that kp_master_merge_commit() performs.
	 *
	 * NOTE: currently, this function operates synchronously: this thread
	 * that is calling the function will also perform the merge work
	 * immediately, and the function will not return until the commit
	 * has either been committed or aborted. In the future, we will probably
	 * change this functionality in two ways: first, this function call
	 * will just append the commit record to the master queue, and one or more
	 * dedicated master threads will actually perform the work of merging
	 * the commit record in. Second, the local thread here will be able
	 * to optionally continue working and receive an asynchronous signal /
	 * callback / something that tells it whether the commit record was
	 * committed or aborted. */
	kp_debug("calling kp_master_merge_commit() to pass off our single "
			"commit to be merged\n");
	ret_merge = kp_master_merge_commit(local->master, cr, conflict_list,
			new_snapshot);
	if (ret_merge == -1) {
		kp_error("kp_master_merge_commit() returned error!\n");
	} else if (ret_merge == 1) {
		kp_debug2("kp_master_merge_commit() returned 1: conflict! "
				"We must also return 1, but *new_snapshot has still been "
				"set to %ju.\n", *new_snapshot);
	} else {
		kp_debug("kp_master_merge_commit() returned new snapshot number "
				"for local worker to use: %ju\n", *new_snapshot);
	}

	/* As described above, the following code will also eventually be
	 * moved into the "kp_local_merge_commits()" function! (or replaced
	 * with a single call to kp_local_cleanup_after_merge(), or something) */
	kp_debug("calling kp_local_cleanup_after_commit() to clean up "
			"hash table entries and version tables\n");
	ret = kp_local_cleanup_after_commit(local);
	if (ret != 0) {
		kp_error("kp_local_cleanup_after_commit() returned error: %d\n", ret);
		return -1;
	}

#ifdef KP_ASSERT
	if (*new_snapshot == UINT64_MAX && ret_merge != -1) {
		kp_die("invalid *new_snapshot: %ju\n", *new_snapshot);
	}
#endif
	return ret_merge;
}

int kp_local_commit(kp_kv_local *local, void **conflict_list)
{
	int ret, retval = -1;
	bool use_nvm_master;
	kp_commit_record *cr;
	size_t num_processed = 0;
	uint64_t new_snapshot;

	if (!local) {
		kp_error("got a null argument: local=%p\n", local);
		return -1;
	}
	kp_debug("entered; got conflict_list=%p (probably null for now). "
			"Beginning commit process\n", conflict_list);

	/* Write-lock the entire local kvstore; this will be necessary if
	 * we allow multiple local worker threads per local kvstore, but we
	 * currently assume that we don't do this. */
#ifdef LOCK_LOCAL
	kp_wrlock("local->rwlock", local->rwlock);
#endif

	kp_debug("entered, beginning commit process\n");

	/* Steps:
	 *   - Create commit record: allocate and initialize ON PCM!
	 *   - Fill in commit record: walk over the local kvstore!
	 *     ~ (Create a function to do this, with two callback functions:
	 *        one for not-latest version of a key, and one for latest
	 *        version of a key?)
	 *       # Create a function that iterates over all of the CURRENT
	 *         versions, and skips anything that's not the latest?
	 *     ~ If not the latest version of a key, delete it!
	 *         (for now; if there's a conflict, might want to see this
	 *          history though)
	 *     ~ For the latest version of a key, append it to the commit log!
	 *       Then delete the data too.
	 *   - Append commit record to _commit log_
	 *     ~ (Do this first, _before_ filling in commit record? Does
	 *        it matter?? If we're not worrying about reachability, then
	 *        no.)
	 *   - Get back a new snapshot number from the commit-record append!
	 *   - Return the new snapshot number, so that it can be used to merge
	 *     (right?)
	 * Right now, don't know if commit succeeded or failed until after the
	 * merge happens (that's when conflict detection is performed), so this
	 * function is deleting data that could potentially be useful to the
	 * client after a conflicted merge that failed. Oh well - we can adjust
	 * this later on to save this information and clean it up later if we
	 * want?
	 */

	/* We decide whether or not to store
	 * the commit record in non-volatile memory based on how the master
	 * store was created (but generally, we expect use_nvm to be true
	 * here!) */
	use_nvm_master = kp_master_uses_nvm(local->master);
	kp_debug("got use_nvm_master=%s from master store\n",
			use_nvm_master ? "true" : "false");

	/* Allocate a new commit record.
	 * kp_commit_record_create() allocates the vector for the kvpairs,
	 * sets the begin_snapshot to the snapshot number that the local
	 * kvstore is currently using, and sets the commit's state to CREATED. */
	ret = kp_commit_record_create(&cr, local->snapshot,
			local->expected_max_keys, use_nvm_master);
	if (ret != 0) {
		kp_error("kp_commit_record_create() returned error=%d\n", ret);
		retval = -1;
		goto unlock_and_return;
	}

	/* Iterate over the entire local kvstore by calling the hash table's
	 * for-each function and passing it a processor function. Our processor
	 * function iterates over the version table for each entry in the hash
	 * table and appends the latest versions to the commit record (which is
	 * passed as extra data to the processor function); all vtes in the
	 * version table (latest or not) are freed along the way.
	 *
	 * This still leaves the version table vectors and the kp_ht_entrys
	 * themselves to be freed. We can free the former during our traversal,
	 * but not the latter, because the hash table for-each function will
	 * go crazy if the hash table changes while it's running. So, we free
	 * all of this later... (see further down in this function...).
	 */
	num_processed = kp_kvstore_do_for_each(local->kv, kp_process_commit_entries,
			(void *)cr);
	if (num_processed == 0) {
		kp_debug("didn't process ANY entries in hash_do_for_each(); this "
				"means that we tried to commit while our local store is "
				"empty. Returning 2 for empty-commit.\n");
		kp_commit_record_destroy(&cr, true, true);  // free keys and values
		retval = 2;
		goto unlock_and_return;
	} else if (num_processed == SIZE_MAX) {
		kp_error("kp_kvstore_do_for_each() returned error=%zu\n",
				num_processed);
		kp_commit_record_destroy(&cr, true, true);  // free keys and values
		retval = -1;
		goto unlock_and_return;
	}

	/* The commit record has members use_nvm, begin_snapshot, end_snapshot,
	 * kvpairs, and state. use_nvm and begin_snapshot were set and flushed
	 * at creation time, and kvpairs was flushed during
	 * kp_kvstore_do_for_each(). So, all that we have left to do is set
	 * end_snapshot, update the state, and append the commit record to the
	 * commit log.
	 *
	 * IMPORTANT: we leave the commit record's state as just CREATED, for
	 * now; we only update it to WRITTEN at the point when it is appended
	 * to the commit log (in case anything unexpected happens along the
	 * way and we want to recover it (clean it up); the state change to
	 * WRITTEN is the "point of no return", where the commit must either
	 * be committed or aborted, I think). */
	kp_debug("kp_kvstore_do_for_each() is done, has written the entire "
			"commit record to memory (but its state is still CREATED).\n");

	/* At this point, the local worker's work is done; we just need to
	 * pass the pointer to the commit record to the master, which will
	 * fill in the rest of the commit record and actually perform the
	 * merge.
	 *
	 * We may want to add support for a local worker to make multiple
	 * commits before merging them all at once, so we call this function
	 * to enqueue the commit record that we just created onto a local
	 * queue of commits.
	 *
	 * However, currently the kp_local_enqueue_commit() function just
	 * calls kp_master_merge_commit() as well - commits and merges are
	 * coupled together. In the future, we may require that the client
	 * thread itself calls kp_local_merge_commits() to explicitly merge
	 * the commit(s) is has just made.
	 * freud : I knew it was all git ! Ha !
	 * This is all analogous to git's separation of "git commit" and
	 * "git push". */
	new_snapshot = UINT64_MAX;
	ret = kp_local_enqueue_commit(local, cr, conflict_list, &new_snapshot);
	kp_debug("kp_local_enqueue_commit() returned ret=%d, new_snapshot=%ju, "
			"and conflict_list=%p\n", ret, new_snapshot, conflict_list);
	if (ret == -1) {
		kp_error("kp_local_enqueue_commit() returned error! Destroying "
				"commit record.\n");
		kp_commit_record_destroy(&cr, true, true);
		    // Free keys and values - kp_process_commit_entries() made copies
		retval = -1;
		goto unlock_and_return;
	} else if (ret == 1) {
		kp_debug2("kp_local_enqueue_commit() returned 1: there was a "
				"conflict! However, new_snapshot was still set to %ju\n",
				new_snapshot);
		kp_debug2("not touching the commit record, still needed by master "
				"kvstore; it will garbage-collect it at some point.\n");
		retval = 1;
	} else {
		kp_debug2("kp_local_enqueue_commit() succeeded, setting our retval "
				"to 0\n");
		retval = 0;
	}
#ifdef KP_ASSERT
	if (new_snapshot == UINT64_MAX) {
		kp_die("invalid new_snapshot at this point!\n");
	}
#endif

	/* Update the local worker's snapshot number! The end_timestamp of our
	 * previous commit/merge will become the begin_timestamp of our next
	 * commit (the next time the local worker calls this function).
	 * This is the only part of the kp_kv_local struct that we update in
	 * this function, so go ahead and flush it now.
	 * Currently, we do this even if there was a conflict. */
	kp_todo("adjust this function's API so that it returns the end_snapshot "
			"of the commit record to the client - they have to check on it "
			"or wait on it to find out if the commit succeeded or aborted!\n");
	local->snapshot = new_snapshot;
	kp_flush_range(&(local->snapshot), sizeof(uint64_t),
			kp_uses_nvm(local->kv));
	kp_debug2("after %s commit, local store (id=%d)'s snapshot number has "
			"been set to %ju\n", retval == 0 ? "COMMITTED" : "ABORTED",
			local->id, local->snapshot);

	/* Note that kp_local_enqueue_commit() calls kp_local_cleanup_after_commit(),
	 * which resets the hash table and any other necessary local kvstore state
	 * (nothing, at the moment). So, now that we've updated our local snapshot
	 * number, we're done. */
	kp_debug("commit process complete, returning %d\n", retval);

unlock_and_return:
#ifdef LOCK_LOCAL
	kp_wrunlock("local->rwlock", local->rwlock);
#endif
	return retval;
}

#if OLD_CODE
int kp_local_merge_commits(kp_kv_local *local, void **conflict_list)
{
	/* Add code here:
	 *   - Dequeue commit records from local's queue (to-be-implemented)
	 * Move code here from kp_local_enqueue_commit():
	 *   - Call(s) to kp_merge_commit()
	 *   - Call to kp_local_cleanup_after_merge()
	 */
	kp_die("not implemented yet!\n");
	return -1;
}
#endif

#if 0
int kp_local_merge(kp_kv_local *local, void **conflict_list_or_something)
{
	int ret, retval;

	if (local == NULL) {
		kp_error("got a NULL argument: local=%p\n", local);
		return -1;
	}

	/* For now, this function is a wrapper around a single commit, followed
	 * by a merge of that commit. In the future we can expose the commit
	 * function to clients and allow clients to perform multiple commits
	 * before merging them all at once. */

	/* Write-lock the entire local kvstore; this will be necessary if
	 * we allow multiple local worker threads per local kvstore, but we
	 * currently assume that we don't do this. */
#ifdef LOCK_LOCAL
	kp_wrlock("local->rwlock", local->rwlock);
#endif

	ret = kp_local_commit(local);
	if (ret != 0) {
		kp_error("kp_local_commit() returned error=%d\n", ret);
		retval = -1;
		goto unlock_and_return;
	}

	/* FAIL at this point: ...? */

#ifdef DISABLED_TEMPORARILY
	ret = kp_local_merge_internal(local);
	if (ret == 1) {
		kp_debug("kp_local_merge_internal() returned conflict!\n");
		kp_die("implement me: post-conflict stuff!\n");
		//...
		retval = 1;
		goto unlock_and_return;
	} else if (ret != 0) {
		kp_error("kp_local_merge_internal() returned error=%d\n", ret);
		//CHECK - TODO: undo the commit somehow??
		retval = -1;
		goto unlock_and_return;
	}
	kp_debug("kp_local_merge_internal() returned success\n");

	kp_warn("need to update local kvstore's snapshot number here??\n");

	/* Perform cleanup steps after a successful (non-conflict) merge!
	 * For now, this means walking over the entire local kvstore and
	 * freeing all of the data (while maintaining the hash table itself).
	 * As a future optimization, we may want to "cache" the most-recent
	 * data (values that were just merged into the master) while freeing
	 * everything else. */
	ret = kp_local_cleanup_after_merge(local);
	if (ret != 0) {
		kp_error("kp_local_cleanup_after_merge() returned error: %d\n", ret);
		retval = -1;
		goto unlock_and_return;
	}
#endif

	kp_warn("need to update local kvstore's snapshot number here??\n");

	retval = 0;
	/* fall-through into unlock_and_return: */

unlock_and_return:
#ifdef LOCK_LOCAL
	kp_wrunlock("local->rwlock", local->rwlock);
#endif
	return retval;
}
#endif

uint64_t kp_local_get_current_snapshot(kp_kv_local *local)
{
	if (!local) {
		kp_error("local is NULL\n");
		return UINT64_MAX;
	}
	return local->snapshot;
}

uint64_t kp_local_get_latest_snapshot(kp_kv_local *local)
{
	if (!local) {
		kp_error("local is NULL\n");
		return UINT64_MAX;
	}
	return kp_master_get_latest_snapshot(local->master);
}

uint64_t kp_local_set_snapshot(kp_kv_local *local, uint64_t snapshot)
{
	uint64_t max_snapshot, prev_snapshot;

#ifdef DISABLE_VERSIONING
	kp_warn("DISABLE_VERSIONING: this function is not supported, "
			"changing the snapshot number will have no effect.\n");
#endif

	if (local == NULL) {
		kp_error("local is NULL\n");
		return UINT64_MAX;
	}
	if (snapshot == 0 || snapshot == UINT64_MAX) {
		/* 0 is an invalid snapshot number; initial snapshot is 1 */
		kp_error("invalid snapshot number: %ju\n", snapshot);
		return UINT64_MAX;
	}

	kp_die("broken code! Need to repair this so that garbage collector's "
			"cps_* vectors are adjusted here as well!\n");

	max_snapshot = kp_master_get_latest_snapshot(local->master);
	if (max_snapshot == UINT64_MAX) {
		kp_error("kp_master_get_lastest_snapshot() returned error=%ju\n",
				max_snapshot);
		return UINT64_MAX;
	}
	if (snapshot > max_snapshot) {
		kp_error("invalid snapshot number %ju, greater than master's latest "
				"snapshot number %ju\n", snapshot, max_snapshot);
		return UINT64_MAX;
	}

	/* We're assuming that only one local worker accesses a local kvstore,
	 * so disabled lock code for now. */
#ifdef LOCK_LOCAL
	kp_wrlock("local->rwlock", local->rwlock);
#endif
	prev_snapshot = local->snapshot;
	local->snapshot = snapshot;  //FLUSH!
#ifdef LOCK_LOCAL
	kp_wrunlock("local->rwlock", local->rwlock);
#endif
	kp_debug("set local->snapshot to %ju (previous snapshot: %ju)\n",
			local->snapshot, prev_snapshot);

	return prev_snapshot;
}

uint64_t kp_local_update(kp_kv_local *local)
{
	//MAKE SURE TO: take local's wrlock while changing local->snapshot!
	kp_die("not implemented yet\n");
	if (local == NULL) {
		kp_error("local is NULL\n");
		return -1;
	}
	return UINT64_MAX;
}

int kp_local_print_stats(kp_kv_local *local, FILE *stream, bool detailed)
{
	char msg[KP_MSG_SIZE];

	if (local == NULL) {
		kp_error("local is NULL (detailed=%d)\n", detailed);
		return -1;
	}

#ifdef LOCK_LOCAL
	kp_rdlock("local->rwlock", local->rwlock);
#endif

	/* Introduction */
	snprintf(msg, KP_MSG_SIZE, "Printing memory usage for local copy #%lu\n",
					 (long unsigned int)local->id);
	kp_write_stream(stream, msg);

	snprintf(msg, KP_MSG_SIZE, "\t Operates off snapshot %ju\n",
			local->snapshot);
	kp_write_stream(stream, msg);

	snprintf(msg, KP_MSG_SIZE, "\t Points to Master %lu\n",
					 (long unsigned int)kp_master_get_id(local->master));
	kp_write_stream(stream, msg);


	//	kp_die("core print stats not implemented yet\n");
	//	int rc = kp_print_stats(local->kv, stream, detailed);


#ifdef LOCK_LOCAL
	kp_rdunlock("local->rwlock", local->rwlock);
#endif
	return -1;
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
