/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Katelin Bailey & Peter Hornyack
 * Created: 11/16/2011
 * University of Washington
 *
 * This file contains definitions for the master table that need to be
 * visible to other internal .c files (i.e. kp_kv_local.c), but should
 * not be externally visible to users/clients.
 */

#ifndef KP_KV_MASTER_INTERNAL_H
#define KP_KV_MASTER_INTERNAL_H

#include "../include/kp_kv_local.h"
#include "../include/kp_kv_master.h"
#include "kp_kvstore_internal.h"
#include <stdint.h>
#include <stdbool.h>

/* Gets the current version of the value for the key. This function
 * makes a copy of the value, so it is the caller's responsibility
 * to free the data.
 * Returns: If the key is found, 0 is returned, the value argument
 * is set to point to the new copy of the value, and the size argument
 * is set to the size of the value. If the key is not found in the
 * store, 1 is returned. If the key is found but the version we got
 * is a deletion tombstone (value is NULL), 3 is returned. If an error
 * occurred, -1 is returned.
 */
int kp_master_get(kp_kv_master *master, const kp_ht_entry *lookup_entry,
		void **value, size_t *size);

/* Gets the value for the key from a past snapshot. The caller (a local
 * worker) must pre-allocate a kp_ht_entry and set its key member to point
 * to the key to be looked up. This function makes a copy of the value,
 * so it is the caller's responsibility to free the data.
 * Returns: If the key is found, 0 is returned, the value argument
 * is set to point to the new copy of the value, and the size argument
 * is set to the size of the value. If the key is not found in the
 * store, 1 is returned. If the key is found but the snapshot version
 * is not found, 2 is returned. If the key is found but the version we got
 * is a deletion tombstone (value is NULL), 3 is returned. If an error
 * occurred, -1 is returned.
 */
int kp_master_get_by_snapshot(kp_kv_master *master, uint64_t snapshot,
		const kp_ht_entry *lookup_entry, void **value, size_t *size);

/* This function should only be visible to the kp_kv_local.c code;
 * it should not be externally visible!! (clients initiate merges on
 * their local tables, not using the master table)
 * Merges a kp_kvstore (i.e. from a local store) into the master's store.
 * The store that is being merged in must be frozen!
 * This function operates synchronously (for now...): the worker thread
 * that calls this function performs the work to merge in its local store.
 * If the second-to-last arg is true (which it always should be...), this
 * function will free the kvstore after it has been merged in.
 * Also, the caller (a local worker) must pass in its current snapshot
 * number, so that the snapshot can be removed from the list of snapshots
 * that are in use and should be kept (not garbage-collected).
 *
 * Returns: a snapshot number that represents the current global state
 * of the store: a worker working off of this snapshot number will see
 * the values that they just merged in, along with any other values
 * that were merged in by other workers to other keys since the last
 * snapshot this worker was using. The snapshot number probably only
 * makes sense in MODE_SNAPSHOT. On error, UINT64_MAX is returned.
 * (todo: always return 0 on success when we're not in MODE_SNAPSHOT??)
 */
uint64_t kp_kv_merge_into_master(kp_kv_master *master, kp_kvstore *frozen_kv,
		bool free_kvstore, uint64_t current_snapshot);

/* Use this function to set the initial snapshot number for a local worker.
 * The master's global snapshot counter will be copied, incremented and
 * returned. Additionally, the initial snapshot number will be added to
 * the list of in-use snapshots, so that it will not be garbage-collected.
 * Returns: a unique snapshot number, or UINT64_MAX on error.
 */
uint64_t kp_master_get_initial_snapshot(kp_kv_master *master);

/* This function is basically just a wrapper around the kvstore's
 * kp_commit(); see that function's definition for detailed
 * notes.
 *
 * Note: on success, the master takes ownership of the commit record
 * that is passed here; the local worker should not do anything with
 * it anymore. On failure, the local worker still owns the commit record,
 * and should probably just free it and fail itself (but it could
 * potentially retry the master commit, etc.).
 *
 * Returns: 0 on success, 1 if there was a conflict, or -1 on error. On
 * success or conflict, a new snapshot number for the local worker to use
 * is stored in *new_snapshot. */
int kp_master_merge_commit(kp_kv_master *master, kp_commit_record *cr,
		void **conflict_list, uint64_t *new_snapshot);

#endif  // KP_KV_MASTER_INTERNAL_H

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
