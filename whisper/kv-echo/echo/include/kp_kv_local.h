/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Katelin Bailey & Peter Hornyack
 * Created: 11/15/2011
 * University of Washington
 *
 * IMPORTANT: right now, there is at least one place where we assume that
 * only a single thread is accessing the local store at a given time: in
 * kp_lookup_versiontable(), we take advantage of a pre-allocated data
 * structure (kp_ht_entry) that will only have one copy per local worker.
 * This method in this function can be changed easily, but for now it
 * serves as an optimization that avoids an additional memory allocation
 * on each get().
 *
 * Each local store keeps track of its "current" snapshot number. The
 * snapshot is only relevant when the master kvstore was created with
 * MODE_SNAPSHOT. The snapshot number should update (in a
 * monotonically-increasing manner) when:
 *   kp_local_merge() is called
 *   kp_local_set_snapshot() is called
 *   kp_local_update() is called
 * (What if we want asynchronous merges? Then the snapshot number can't
 *  be updated until the asynchronous merge has completed, but in the
 *  meantime, can/should we still use the old snapshot number? I guess
 *  we could, but values returned from a get() would need to have some
 *  kind of flag indicating that the merge still hasn't completed and
 *  the value is still coming from the previous snapshot...?)
 * (What if we want multiple worker threads per core? Should we change
 *  kp_kv_local so that each worker can hold its own snapshot number?
 *  Should each worker just create its own local store, even if it
 *  means multiple local kvstores per core?)
 * (If/when we decide to support simultaneous merges, we need to carefully
 *  think about / define what get() behavior on a particular checkpoint/
 *  snapshot will be: if we can end up with multiple versions of a key-value
 *  pair that were inserted during the same checkpoint, then which one do
 *  we return when we get()? Right now, we'll return the last one in the
 *  array, due to the implementation of our search function... is this what
 *  we want?)
 */

#ifndef KP_KV_LOCAL_H
#define KP_KV_LOCAL_H

#include "kp_kv_master.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/* Opaque handle to a local portion of the master key-value store.
 */
struct kp_kv_local_struct;
typedef struct kp_kv_local_struct kp_kv_local;

/* Initializes a local portion for the KV store. This means allocating
 * and initializing a kp_kv_local struct, which should be used by one
 * (or more?) worker threads. The kp_kv_local struct should later be
 * freed (by just ONE thread) by passing it to kp_kv_local_destroy().
 * If the kvstore will be stored on non-volatile memory, then use_nvm
 * should be set to true so that the store will handle the memory
 * appropriately (allocating to 0, flushing, etc.).
 * Returns: 0 on success, -1 on error. On success, *kv_local is set to
 * point to a newly-allocated kp_kv_local struct.
 */
int kp_kv_local_create(kp_kv_master *master, kp_kv_local **local,
		size_t expected_max_keys, bool use_nvm);

/* Deallocates a kp_kv_local struct and performs other cleanup tasks.
 */
void kp_kv_local_destroy(kp_kv_local **local);

/* Puts a key-value pair into the local store.
 * This function takes a pointer to the key and the value, and makes
 * a copy of them both; the caller can free the key and the value after
 * this function returns. The key and the value cannot be NULL.
 * Sizes must be positive (not zero). key must be a NULL-terminated
 * string! Note that if the value is also a NULL-terminated string, the
 * size should account for the null-zero (i.e. size = strlen()+1).
 * Returns: the current local version number for the key, or UINT64_MAX
 * on error (same as kp_put()).
 */
uint64_t kp_local_put(kp_kv_local *local, const char *key, const void *value,
		size_t size);

/* Gets the version of the key's value corresponding to the local worker's
 * current snapshot number.
 *
 * Returns: If the key is found, 0 is returned, the value argument
 * is set to point to the new copy of the value, and the size argument
 * is set to the size of the value. If a version of the key is not found
 * in the store for the current snapshot number, 1 is returned.
 * If an error occurred, -1 is returned.
 */
int kp_local_get(kp_kv_local *local, const char *key, void **value,
		size_t *size);

/* Deletes the specified key. When a local worker deletes a key, the deletion
 * will take effect on its next commit. When the commit is successfully
 * merged into the master store (not if it aborts), gets from that key for
 * snapshot numbers in the future will return not-found (unless another put
 * is done to the key again).
 *
 * Deleting a key does not immediately eliminate all (or even any) of the
 * key's historical versions; old versions will continue to be garbage-
 * collected as usual. However, note that the most-recent version of a key's
 * value will never be garbage collected; so, the memory used by a key
 * will never be entirely freed until it is passed to this function.
 *
 * Note that this function does not check if the key is actually present
 * in the store; if it is not present, then when this local worker's next
 * commit is performed, the key / delete operation will just be ignored.
 *
 * Returns: 0 on success, -1 on failure.
 */
int kp_local_delete_key(kp_kv_local *local, const char *key);

/* This function constructs a commit record that contains the latest
 * version of all uncommitted key-value pairs in the local store. This
 * commit record is then merged into the master store as well. In the
 * future, committing and merging may be de-coupled, to allow a local
 * worker to make multiple commits before merging them all at once.
 *
 * After the merge, the local store will be reset (all keys and values
 * will be freed), and the local worker's snapshot number will have
 * been updated. Currently, this is true whether the commit COMMITTED
 * or ABORTED; however, if an error occurs, the snapshot number may
 * not be updated and the local store may not be reset.
 *
 * When conflict detection is added, we will somehow return a list of
 * conflicting key-value pairs to the caller; the conflict_list argument
 * is a placeholder for this. Eventually, when merges are de-coupled
 * from commits, this argument will be removed from this function.
 *
 * This function assumes that the local store is being accessed by only
 * a single local worker thread!!
 * 
 * Returns: 0 on success, 1 if there was a conflict, -1 on error. 2 is
 * returned if the local store is empty and the caller is trying to perform
 * an empty commit. */
int kp_local_commit(kp_kv_local *local, void **conflict_list);

/* Not supported yet: merge all commits that have been taken but not yet
 * merged into the master. We will somehow return the 
 *
 * Returns: 0 on success, -1 on error. */
int kp_local_merge_commits(kp_kv_local *local, void **conflict_list);

/* For now, this function currently performs both a commit and a merge
 * of that commit into the master. These two functions may be de-coupled
 * in the future to allow multiple commits before merging.
 * Currently merges are synchronous: the thread that calls this function
 * will perform the merge itself, and this function will not return until
 * the merge has either completed or failed.
 *
 * This function now performs conflict detection! If a conflict does occur,
 * then this function will somehow return a list of conflicting key-value
 * pairs (or just the first key-value pair that conflicted).
 *
 * If the merge succeeds, then this function will also update the local
 * store's snapshot number. The new snapshot number will represent both
 * the "commit timestamp" of the merged commit that just took place, and
 * also represents the "start timestamp" of the local store's next commit.
 * This snapshot number is guaranteed to be unique, and will never be
 * "assigned" to any other commit.
 *
 * Returns: 0 on success, 1 if the merge failed due to a conflict, and
 * -1 if the merge failed due to an error.  */
//int kp_local_merge(kp_kv_local *local, void **conflict_list_or_something);

/* Returns: the current snapshot number that this local worker is working
 * off of.
 */
uint64_t kp_local_get_current_snapshot(kp_kv_local *local);

/* Returns: the latest/maximum snapshot number that has been taken in
 * the system, or UINT64_MAX on error (the only possible error is a null
 * pointer, so it is probably ok to not check the return value).
 * NOTE that the latest snapshot number is the one that is still being
 * modified; depending on the use case, the caller of this function may
 * want to subtract 1 from it!
 */
uint64_t kp_local_get_latest_snapshot(kp_kv_local *local);

/* Switches the local kvstore to get values from the indicated snapshot.
 * The worker can then put values as usual, but note that working off
 * of a snapshot that is not recent will raise the probability of conflicts
 * at merge time.
 * This function will return an error if the master kvstore was not created
 * in MODE_SNAPSHOT.
 * Returns: the previous snapshot number on success, or UINT64_MAX on error.
 */
uint64_t kp_local_set_snapshot(kp_kv_local *local, uint64_t snapshot);

/* Takes a snapshot of the master store's current state. This function is
 * like a "cvs update", "svn update", or "git pull": a worker can call this
 * to indicate that they want to access the most recently put values in the
 * master store.
 * If the local store is "dirty" and has keys that have been put to since
 * the last merge, this function will: ???
 *   Return an error?
 *   Return an error only if there is a "conflict"?
 *   Return the key-value pairs that conflict, and let the worker handle it?
 *   ???
 * Returns: a new snapshot number for the worker to work off of, or
 * UINT64_MAX on error ????
 */
uint64_t kp_local_update(kp_kv_local *local);

/* Prints statistics about the current state of the local KV store to the
 * specified stream. The stream argument should be a valid, open file
 * pointer; "stdout" or "stderr" can be used for printing to standard
 * output/error. The stream is not closed by this function. This function
 * flushes the data that it writes to the stream (fflush(stream)), but
 * does not sync it to disk (fsync(stream)).
 * http://www.chemie.fu-berlin.de/chemnet/use/info/libc/libc_7.html
 * Returns: 0 on success, -1 on error.
 */
int kp_local_print_stats(kp_kv_local *local, FILE *stream, bool detailed);

#endif  // KP_KV_LOCAL_H

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
