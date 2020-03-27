/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Katelin Bailey & Peter Hornyack
 * Created: 11/22/2011
 * University of Washington
 *
 * API for a versioned key-value store. The internal kp_kvstore should
 * only be visible to the local stores and the master store; clients
 * should not directly call any of the functions in this interface.
 *  
 * Keys are null-terminated strings.
 * Values are arbitrary bytes in memory. A pointer to these bytes and
 * the size of the value are passed to the key-value store for insertion.
 * All version numbers are unsigned 64-bit integers and start from 0. The
 * GLOBAL version number (GVN) for the store is incremented on every put
 * and delete. Every key also has a LOCAL version number (LVN) which is
 * incremented every time a new value is put for the key. If a key is
 * deleted and then re-inserted, its LVN will reset to 0 (right?).
 *
 * TODO: we should probably remove references to "gvns" from the kp_kvstore
 *   interface; they've probably been superceded by "checkpoints", and
 *   should now probably only be used internally in the kp_kvstore.
 */

#ifndef KP_KVSTORE_INTERNAL_H
#define KP_KVSTORE_INTERNAL_H

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include "hash.h"

/* Opaque handle to the master key-value store.
 * Actual structure definition is in the .c file, since the user should
 * only use the functions defined below to update the key-value store.
 * http://stackoverflow.com/questions/2672015/hiding-members-in-a-c-struct
 */
struct kp_kvstore_struct;
typedef struct kp_kvstore_struct kp_kvstore;
struct kp_vt_struct;
typedef struct kp_vt_struct kp_vt;

/* The hash table stores pointers to these structs, which hold the key
 * for the key-value pair (which we perform hashing on), and a pointer
 * to the version table for the key (which holds the values). In version
 * 3.0 of the kvstore, these structs "own" the key (keys that are copied
 * from the user are first allocated when this struct is created).
 *
 * This struct is located in kp_kvstore_internal.h because local and master
 * stores need to know about it too: local workers pre-allocate a lookup_entry
 * at creation time, the master may allocate just one lookup_entry for an
 * entire merge, and so on.
 */
typedef struct kp_ht_entry_struct {
	const char *key;  //MUST be null-terminated, or hash function will fail!
	kp_vt *vt;
} kp_ht_entry;

/* Version iterator stuff: eventually we want to expose this to clients,
 * which means adding functions in kp_kv_local that call these functions
 * and return opaque kp_version_iter handles to the client. */
typedef struct kp_version_iter_struct {
	bool use_nvm;
	kp_vt *vt;
	uint64_t vt_ver_count;  //initial version count of the vt: used to check for invalidation
	uint64_t vt_len;        //number of entries in vt when this iterator was created
	uint64_t vt_idx;        //current index into vt
	bool is_empty;          //has the iterator reached the end of the vt?
	bool is_invalidated;    //has a vt append invalidated this iterator?
	bool is_set;            //has kp_version_iter_set() been called on this iter yet?
} kp_version_iter;

/* We used to make this struct opaque, but now we've made it visible here
 * so that local stores can modify iterators directly while committing /
 * merging. */
typedef struct kp_iterator_struct {
	bool use_nvm;             //is this iterator stored durably?
	kp_version_iter *v_iter;  //iterator for current version table
	kp_kvstore *kv;           //back-pointer to "parent"
	kp_ht_entry *cur_entry;   //hash table iterator, points to current entry
	bool is_internal;         //is this iterator used internally or by the client?
	bool is_empty;            //no more items to return?
	bool is_invalidated;      //invalidated by a put?
} kp_iterator;

/* An item that is returned from a kp_iterator: */
typedef struct kp_iter_item_struct {
	bool use_nvm;
	bool is_internal;      //if true, key and value point to internal memory!
	const char *key;
	const void *value;
	size_t size;
	uint64_t lvn;
	uint64_t snapshot;
	bool is_last_version;  //true if this is the last version/value for this key
} kp_iter_item;

int kp_ht_entry_create(kp_ht_entry **entry, const char *key, bool copy_key,
		bool use_nvm);
void kp_ht_entry_destroy(kp_ht_entry **entry, bool use_nvm);

/* Creates a new key-value store and initializes. The is_master flag should
 * be set to true if this store is being used for the master store, otherwise
 * it should be false for local worker stores. If the kvstore will be stored
 * on non-volatile memory, then use_nvm should be set to true so that the
 * store will handle the memory appropriately (allocating to 0, flushing,
 * etc.).
 * Returns: 0 on success, -1 on error. On success, *kv is set to point to
 * the new key-value store.
 */
int kp_kvstore_create(kp_kvstore **kv, bool is_master, size_t expected_max_keys,
		bool do_conflict_detection, bool use_nvm);

/* Destroys the entire key-value store.
 * Returns: 0 on success, -1 on failure.
 */
void kp_kvstore_destroy(kp_kvstore **kv);

/* Gets the specified LOCAL version of the value for the key. This
 * function makes a copy of the value, so it is the caller's
 * responsibility to free the data.
 * Returns: If the key is found, 0 is returned, the value argument
 * is set to point to the new copy of the value, and the size argument
 * is set to the size of the value. If the key is not found in the
 * store, 1 is returned. If the key is found but the specified version
 * is not, 2 is returned. If the key is found but the version we got
 * is a deletion tombstone (value is NULL), 3 is returned. If an error
 * occurred, -1 is returned.
 */
//DISABLED for version 3.0
//int kp_get_version_local(kp_kvstore *kv, const char *key, uint64_t version,
//		void **value, size_t *size);

/* Gets the specified GLOBAL version of the value for the key (the version
 * of this key's value that was present in the kvstore when the specified
 * global version was created - this function performs a range lookup, so
 * the EXACT gvn in which the value was created does not have to be
 * used). This function makes a copy of the value, so it is the caller's
 * responsibility to free the data.
 * Returns: If the key is found, 0 is returned, the value argument
 * is set to point to the new copy of the value, and the size argument
 * is set to the size of the value. If the key is not found in the
 * store, 1 is returned. If the key is found but the specified version
 * is not, 2 is returned. If the key is found but the version we got
 * is a deletion tombstone (value is NULL), 3 is returned. If an error
 * occurred, -1 is returned.
 */
//DISABLED for version 3.0
//int kp_get_version_global(kp_kvstore *kv, const char *key, uint64_t version,
//		void **value, size_t *size);

/* Checks if a key is present in the store.
 * Returns: the current local version number if present, or -1 if not present.
 */
uint64_t kp_is_present(kp_kvstore *kv, const char *key);

/* Returns: the latest/maximum checkpoint number for the kvstore, or
 * UINT64_MAX on error (the only possible error is a null
 * pointer, so it is probably ok to not check the return value).
 * NOTE that the latest snapshot number is the one that is still being
 * modified; depending on the use case, the caller of this function may
 * want to subtract 1 from it!
 */
uint64_t kp_get_latest_snapshot(kp_kvstore *kv);

/* Returns: true if this kvstore was allocated with the use_nvm flag set
 * (for use on non-volatile memory), or else false. */
bool kp_uses_nvm(kp_kvstore *kv);

/* Runs one pass of the garbage collector. For now, the garbage collector
 * runs synchronously, so this function will not return until it is
 * finished. Eventually, garbage collection will probably become invisible
 * to the user and this function will disappear.
 * This function is (currently...) intended to be run after every checkpoint
 * that is taken, so cp_just_taken should be the checkpoint/snapshot
 * number that was just returned by kp_take_checkpoint().
 * Returns: the number of DATA bytes collected, or UINT64_MAX on error.
 */
uint64_t kp_run_gc(kp_kvstore *kv, uint64_t cp_just_taken);

/* Prints statistics about the current state of the KV store to the
 * specified stream. The stream argument should be a valid, open file
 * pointer; "stdout" or "stderr" can be used for printing to standard
 * output/error. The stream is not closed by this function. This function
 * flushes the data that it writes to the stream (fflush(stream)), but
 * does not sync it to disk (fsync(stream)).
 * http://www.chemie.fu-berlin.de/chemnet/use/info/libc/libc_7.html
 * Returns: 0 on success, -1 on error.
 */
int kp_print_stats(kp_kvstore *kv, FILE *stream, bool detailed);

/* Gets an iterator for the versioned key-value pairs in the kp_kvstore.
 * The iterator may return the keys in any order, but the versions of the
 * values for each key are guaranteed to be returned in the order that
 * they were inserted (earliest version first). If this iterator will
 * be used only by a local / master store, then is_internal should be
 * true; if the iterator will be passed back to an external client, then
 * is_internal should be false.
 *
 * If this iterator should be stored on non-volatile memory, then use_nvm
 * should be set to true. This is currently unsupported; iterators should
 * be "reconstructable" state, so I think they should always go on volatile
 * memory.
 *
 * IMPORTANT: currently, this function does not take a read-lock on the
 * kvstore or any of its version tables, so bad behavior (e.g. indexing
 * out-of-bounds in the vt vectors) may result if the kvstore that is
 * being iterated over is not frozen!!
 * Returns: 0 on success, -1 on error. On success, *iter is set to point
 * to the newly-allocated iterator.
 */
//OLD_CODE:
//int kp_iterator_get(kp_kvstore *kv, kp_iterator **iter, bool is_internal,
//		bool use_nvm);

/* Resets *iter. After this function returns successfully, iter is ready
 * to be passed to kp_iterator_next2().
 * This function doesn't change (*iter)->use_nvm; the caller is expected
 * to have already set it (i.e. when the iterator was created by
 * kp_iterator_create()).
 * Returns: 0 on success, -1 on error.
 */
int kp_iterator_set(kp_kvstore *kv, kp_iterator *iter, bool is_internal);

/* Returns: true if the iterator has more items, otherwise false. If the
 * iterator was invalidated while items were remaining, this function will
 * still return true! (the return value from kp_iterator_next() must be
 * checked for invalidation).
 */
//bool kp_iterator_has_next(kp_iterator *iter);
  /* This function seems unnecessary for now; just call kp_iterator_next()
   * until it returns 1. */

/* Gets the next item from the iterator. This function allocates a new
 * kp_iter_item struct, which should be freed by passing it to
 * kp_iter_item_destroy().
 * If the iterator encounters a tombstone value (representing a deletion)
 * in a version table, it still returns an item here, but its value will
 * be NULL.
 * Returns: 0 on success, 1 if no more items are available, -1 on error,
 * or -2 if the iterator has been invalidated. On success, *item is set to
 * point to a newly-allocated iterator.
 */
int kp_iterator_next(kp_iterator *iter, kp_iter_item **item);

/* Creates a string describing an iterator item, which must be freed by
 * the caller. The string will contain strange results if the item's value
 * is not a char string, and could even cause a crash.
 * Returns: pointer to the string, or NULL on error.
 */
char *kp_iter_item_to_string(kp_iter_item *item);

/* Creates a new iterator item. This should only be used internally to
 * pre-allocate an iterator item struct that is then filled in by
 * kp_iterator_next() (or something...)
 * Returns: 0 on success, -1 on error.
 */
int kp_iter_item_create_internal(kp_iter_item **item, bool use_nvm);

/* Destroys an iterator item and sets *item to NULL. If the item was
 * created with use_nvm set to true, then this function will flush before
 * returning.
 */
void kp_iter_item_destroy(kp_iter_item **item);

/* Creates a kvstore iterator. After creation, this iterator must be
 * initialized by passing it to kp_iterator_set().
 * Returns: 0 on success, -1 on error.
 */
int kp_iterator_create(kp_iterator **iter, bool use_nvm);

/* Destroys a kvstore iterator. */
void kp_iterator_destroy(kp_iterator **iter);

int kp_write_stream(FILE *stream, char *msg);

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
int kp_get(kp_kvstore *kv, const kp_ht_entry *lookup_entry, void **value,
		size_t *size);

#if 0  //old preliminary changes...
/* Recovers a kvstore from any operations that may have been in progress.
 * For now: 0 = no operation (invalid?), 1 = put, 2 = get, 3 = merge.
 *
 * Returns: 0 if the in-progress operation was RESUMED and run to completion;
 * 1 if the in-progress operation had to be UNDONE / aborted; -1 on an error.
 */
int kp_kvstore_recover(kp_kvstore *kv, int operation);
  //CHECK
#endif

/* Returns the current checkpoint number, then increments the checkpoint
 * number so that subsequent puts are assigned the incremented number.
 * The caller must pass in their current/active checkpoint/snapshot number,
 * so that the kvstore can remove it from the list of checkpoints to keep
 * around!
 * NOTE: if no items have been put into the kvstore since the last time
 * that this function was called, the checkpoint number will not be
 * advanced, and this function will return the current/active checkpoint
 * number!
 * Returns: the checkpoint number, or UINT64_MAX on error.
 */
uint64_t kp_take_snapshot(kp_kvstore *kv, uint64_t worker_previous_cp);

/* This is the put function called by local workers for their own local store.
 * Puts a key-value pair into the store. If the key is not already
 * present, creates version 0; otherwise, puts the new value and
 * increments the local version number.
 *
 * This function takes a snapshot as an argument: this should be the local
 * worker's current working snapshot!! And if the current working snapshot
 * is not the same as the "most recent" snapshot that the local worker has
 * seen, then this means that the local worker is writing the past, which
 * will result in bad bad behavior.
 *
 * This function takes a pointer to the key and the value, and makes
 * a copy of them both; the caller can free the key and the value after
 * this function returns. The key and the value cannot be NULL.
 * Sizes must be positive (not zero). key must be a NULL-terminated
 * string! Note that if the value is also a NULL-terminated string, the
 * size should account for the null-zero (i.e. size = strlen()+1).
 * Returns: the current local version number for the key, or UINT64_MAX
 * on error.
 */
uint64_t kp_put(kp_kvstore *kv, const kp_ht_entry *lookup_entry,
		const void *value, size_t size, uint64_t snapshot);

/* Deletes the specified key from the store. Note that this function will
 * only be called for local kvstores; delete operations in the local store
 * become put operations performed during the merge of the local store's
 * next commit.
 *
 * See kp_kv_local.h for a description of the behavior of this function from
 * the client / worker's point of view. This function does not require that
 * the key actually exist in either the local or master kvstores.
 *
 * Returns: 0 on success, or -1 on failure.
 */
int kp_delete_key(kp_kvstore *kv, const kp_ht_entry *lookup_entry,
		uint64_t snapshot);

/* Gets the version of the key's value that was present in the kvstore
 * during the given checkpoint.
 * This function makes a copy of the value, so it is the caller's
 * responsibility to free the data.
 * Returns: If the key is found, 0 is returned, the value argument
 * is set to point to the new copy of the value, and the size argument
 * is set to the size of the value. If the key is not found in the
 * store, 1 is returned. If the key is found but the specified version
 * is not, 2 is returned. If the key is found but the version we got
 * is a deletion tombstone (value is NULL), 3 is returned. If an error
 * occurred, -1 is returned.
 */
//int kp_get_version_snapshot(kp_kvstore *kv, const char *key,
//		uint64_t snapshot, void **value, size_t *size);
int kp_get_version_snapshot(kp_kvstore *kv, const kp_ht_entry *lookup_entry,
		uint64_t snapshot, void **value, size_t *size);

/* Internal version of destroy function: frees memory for the kp_kvstore
 * data structures, but only frees the values that are pointed to if the
 * free_values argument is true.
 * Returns: 0 on success, -1 on failure.
 */
void kp_kvstore_destroy_internal(kp_kvstore **kv, bool free_values);

/* Gets the next item from the iterator. This function allocates a new
 * kp_iter_item struct, which should be freed by passing it to
 * kp_iter_item_destroy_internal() (NOT kp_iter_item_destroy()). The
 * item returned by this function will contain pointers to memory internal
 * to the KV store, so the memory that is pointed to should not be
 * manipulated by the caller of this function.
 * If the iterator encounters a tombstone value (representing a deletion)
 * in a version table, it still returns an item here, but its value will
 * be NULL.
 * Returns: 0 on success, 1 if no more items are available, -1 on error,
 * or -2 if the iterator has been invalidated. On success, *item is set to
 * point to a newly-allocated iterator.
 */
int kp_iterator_next_internal(kp_iterator *iter, kp_iter_item **item);


/* This function currently assumes that *item points to a pre-allocated
 * kp_iter_item. (in the future, this function will allocate a new item
 * if *item is NULL?)
 * Returns: 0 on success, 1 if no more items are available, -1 on error,
 * or -2 if the iterator has been invalidated. On success, *item is set to
 * point to ...
 */
int kp_iterator_next2(kp_iterator *iter, kp_iter_item **item);

/* Destroys an iterator item that has been allocated by
 * kp_iterator_next_internal(). Calling this function on a item allocated
 * by kp_iterator_next() will result in a memory leak.
 * Returns: 0 on success, -1 on error.
 */
int kp_iter_item_destroy_internal(kp_iter_item *item);

/* Copies the snapshot counter, increments it, adds the copied snapshot
 * to the list of in-use snapshots that should not be garbage-collected,
 * and returns the copied snapshot. Every snapshot number (starting from
 * 0) will only ever be returned once by this function. This function
 * is concurrency-safe (there is an internal lock that is taken on the
 * snapshot number).
 * The new internal snapshot value is guaranteed to have been flushed
 * to PCM before this function returns.
 * Returns: a unique snapshot number, or UINT64_MAX on error.
 */
uint64_t kp_increment_snapshot_mark_in_use(kp_kvstore *kv);

/* Recovers a key-value store after a failure.
 * ...
 * Returns: 0 on success, -1 on failure.
 */
int kp_kvstore_recover_master(kp_kvstore *kv);


/* Gets an iterator for all of the versions for the specified key.
 * ...
 * Returns: 0 on success, 1 if the key was not found in the kvstore, or -1
 * on failure.
 */
int kp_get_version_iter(kp_kvstore *kv, const kp_ht_entry *lookup_entry,
		kp_version_iter **v_iter, bool use_nvm);

/* Gets the next version from the version iterator. Currently, the caller
 * is responsible for pre-allocating a kp_iter_item and setting *item to
 * point to it (in the future, this can be changed so that if *item is
 * NULL, this function will allocate a new kp_iter_item before continuing).
 * This function will not do any allocations for any members of the *item,
 * it will just directly set its members. *item->use_nvm should be already
 * set by the caller, and will not be changed by this function.
 * 
 * If the item was created with use_nvm set to true, then *item will be
 * flushed before this function returns. If the version iterator was created
 * with use_nvm set to true, then the modified *v_iter will be flushed.
 * 
 * Currently, this function does some things that are specific to local
 * stores (i.e. during a commit/merge) and to _internal_ vt iterators;
 * if we expose version iterators to external clients, or use them over
 * the master store, then this function needs to be re-examined!
 *
 * Returns: 0 on success, 1 if no more items are available, -1 on error,
 * or -2 if the iterator has been invalidated. On success, *item is set to
 * point to a newly-allocated iterator. */
int kp_version_iter_next(kp_version_iter *v_iter, kp_iter_item **item);

/* Destroys a kp_version_iter and sets *v_iter to NULL. */
void kp_version_iter_destroy(kp_version_iter **v_iter);

/* "Processor" function for use with hash_do_for_each(). The first argument
 * passed to this function is an entry from the hash table, which can be
 * cast to a kp_ht_entry*. The second argument is auxiliary data, which
 * we use to pass the current commit record (cast to kp_commit_record*).
 *
 * NOTE: if this function returns false, then the local kvstore may be
 * left in an unusable state! For example, if appending a value to the
 * commit record fails, then the value and all of the values that have
 * been iterated over previously will have been freed already.
 *
 * This function lives here, rather than in kp_kv_local, because it may
 * need to access the internals of various data structures that aren't
 * exposed to the local / master stores.
 *
 *
 * Returns: true if the entry was processed successfully, false otherwise.
 */
bool kp_process_commit_entries(void *entry_ptr, void *cr_ptr);

/* Just a wrapper that enables the owner of a kvstore to call a processor
 * function on each entry in the hash table.
 * fn probably points to kp_process_commit_entries() (elsewhere in this file).
 * Returns: the number of entries processed, or SIZE_MAX on error.
 */
size_t kp_kvstore_do_for_each(kp_kvstore *kv, Hash_processor fn, void *aux);


/* Enumerates the states that a commit record can be in. The state is
 * CREATED when the structure is first allocated. When all of the kvpairs
 * have been appended to the commit record, the state is set to WRITTEN.
 * When the commit begins to be merged into the master kvstore, its state
 * is set to INPROGRESS. If the commit succeeds, its state is set to
 * COMMITTED once all of its kvpairs have been put into the master store.
 * If the commit fails due to a conflict (or other problem?), then its
 * state is set to ABORTED.
 */
typedef enum commit_state_enum {
	ALLOCATED = 0,
	CREATED,
	CREATED_OK,
	INPROGRESS,
	INPROGRESS_OK,
	WAIT_TO_COMMIT,
	COMMITTED,
	WAIT_TO_ABORT,
	ABORTED,
	UNDEFINED,  //bad input to state transition function
	DEAD
} commit_state;
#define commit_state_to_string(state)                \
	((state) == ALLOCATED      ? "ALLOCATED"      :  \
	 (state) == CREATED        ? "CREATED"        :  \
	 (state) == CREATED_OK     ? "CREATED_OK"     :  \
	 (state) == INPROGRESS     ? "INPROGRESS"     :  \
	 (state) == INPROGRESS_OK  ? "INPROGRESS_OK"  :  \
	 (state) == WAIT_TO_COMMIT ? "WAIT_TO_COMMIT" :  \
	 (state) == COMMITTED      ? "COMMITTED"      :  \
	 (state) == WAIT_TO_ABORT  ? "WAIT_TO_ABORT"  :  \
	 (state) == ABORTED        ? "ABORTED"        :  \
	 (state) == UNDEFINED      ? "UNDEFINED"      :  \
	 (state) == DEAD           ? "DEAD"           : "UNKNOWN!")

struct kp_commit_record_struct;
typedef struct kp_commit_record_struct kp_commit_record;
struct kp_kvpair_struct;
typedef struct kp_kvpair_struct kp_kvpair;


/* Allocates a new commit record and initializes its begin_snapshot, and
 * sets the commit's state to CREATED. The begin_snapshot passed to this
 * function should be the current snapshot that the local worker is working
 * off of.
 * Returns: 0 on success, -1 on error. */
int kp_commit_record_create(kp_commit_record **cr, uint64_t begin_snapshot,
		size_t expected_max_keys, bool use_nvm);

/* Appends a kvpair to a commit record.
 * Returns: 0 on success, -1 on failure. */
int kp_commit_record_append(kp_commit_record *cr, const kp_kvpair *pair);

/* Destroys a kp_commit_record. */
void kp_commit_record_destroy(kp_commit_record **cr, bool free_keys,
		bool free_values);

/* If use_nvm is true, then this function always COPIES the key and the value
 * into the created kp_kvpair. If use_nvm is false, well that isn't supported
 * yet.
 *
 * This function will handle tombstone values (value == NULL, size == 0)
 * just fine.
 *
 * Returns: 0 on success, -1 on failure. */
int kp_kvpair_create(kp_kvpair **pair, const char *key, const void *value,
		size_t size, bool use_nvm);

/* Destroys a kp_kvpair. */
void kp_kvpair_destroy(kp_kvpair **pair);

/* Calls hash_free() on the kvstore's internal hash table and resets
 * other kvstore state. The vtes stored in the store's version tables
 * are assumed to have been freed already (i.e. during a local commit).
 *
 * Returns: 0 on success, -1 on error.
 */
int kp_kvstore_reset(kp_kvstore *kv);

/* Takes a commit record and puts it in the "merge queue." Actually,
 * at the moment this function is synchronous: the calling thread
 * itself will perform the merge right away, and this function will
 * not return until the commit has either committed or aborted!
 *
 * The kvstore will perform all of these steps:
 *   Lock its "snapshot lock"
 *   Append the commit record to the commit log
 *   Increment and flush the global snapshot/timestamp number
 *   Write the new snapshot number as the commit record's end_snapshot
 *   Set the commit record's state to WRITTEN
 *   Flush the commit record's new snapshot + state
 *   Add the new snapshot number to the list of snapshots in use
 *   Unlock the snapshot lock
 *   Call a merge function that:
 *     Locks the "merge lock"
 *     Performs the merge
 *     Unlocks the merge lock
 *   Return the end_snapshot to us to be used as the local worker's new
 *     working snapshot.
 *
 * Returns: 0 on success, 1 if there was a conflict, -1 on error. On
 * success OR CONFLICT, *new_snapshot is set to the new snapshot number. */
int kp_commit(kp_kvstore *kv, kp_commit_record *cr,
		void **conflict_list, uint64_t *new_snapshot);

#endif  // KP_KVSTORE_INTERNAL_H
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
