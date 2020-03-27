/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 * 
 * Katelin Bailey & Peter Hornyack
 * Created: 8/26/2011
 * University of Washington
 * 
 * Implementation of a versioned key-value store.
 * Abbreviations used in this file:
 *   vt  = version table
 *   vte = version table entry
 *   ht  = hash table
 *   gc  = garbage collection/collector
 *   lvn = local version number
 *   gvn = global version number
 *   ss  = snapshot  (and ss_num = snapshot_number)
 *   cs  = changeset
 *   CDDS = "consistent and durable data structure"
 *   
 * Various TODOs:
 *   Limit number of versions to UINT64_MAX
 *   Add a debug function that prints out the entire kv store?
 *   ...
 */

#include "kp_kvstore_internal.h"
#include "kp_macros.h"
#include "kp_recovery.h"
#include "vector.h"
#include "vector64.h"
#include "kp_common.h"
#include "PIN_Hooks.h"
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <unistd.h>

#define INITIAL_GLOBAL_SNAPSHOT 444
  // arbitrary
#define GC_VECTOR_SIZE ((unsigned long)16)

/* Forward declarations: */
struct kp_gc_struct;
typedef struct kp_contour_rec_struct kp_contour_rec;
typedef struct kp_gc_struct kp_gc;
void kp_vt_destroy(kp_vt **vt, bool vtes_already_freed);
uint64_t kp_vt_delete_version(kp_vt *vt, uint64_t idx);
int kp_version_iter_set(kp_version_iter *v_iter, kp_vt *vt);
int kp_version_iter_reset(kp_version_iter *v_iter);
void kp_debug_print_cr(const char *description, const kp_commit_record *cr);

/* External structures:
 * NOTE: if any of these structures are changed, probably have to change
 * calculations in kp_print_sweep_vt()!
 */
struct kp_kvstore_struct {
	int id;
	uint64_t pairs_count;      //NOTE: check that this is reliable before using...
	kp_gc *gc;                 //holds garbage collection info
	Hash_table *ht;            //hashes (string) keys to version tables
	pthread_rwlock_t *rwlock;  //kv store-wide lock
	bool is_master;            //is this the master store?
	bool use_nvm;              //is this kvstore stored in non-volatile memory?
	bool detect_conflicts;
	uint64_t global_snapshot;        //global snapshot number - master only!!!
	                                 //this number should only ever be incremented,
									 //never written to!!
	vector *commit_log;
	pthread_mutex_t *snapshot_lock;  //lock for the snapshot number
	ds_state state;
};

/* Global variables: */
static int kp_kvstore_count = 0;  /* unique identifier for kp_kvstores */

/* Internal structures:
 * NOTE: if any of these structures are changed, probably have to change
 * calculations in kp_print_sweep_vt()!
 */
/* A version table (vt) consists of a vector of pointers to version table
 * entries (vte's):
 */
typedef struct kp_vte_ {
	/* The snapshot stored in the kp_vte is either the local worker's
	 * current snapshot when they put the value ("start timestamp") OR
	 * the snapshot when the local worker performed the merge into the
	 * master ("commit timestamp").
	 */
	const void *value;        //pointer to the actual data
	size_t size;        //size of the value
	uint64_t lvn;       //local version number
	uint64_t ttl;       //number of snapshots that reference this version
	uint64_t snapshot;     //LOCAL only: working snapshot when inserted!
	kp_commit_record *cr;  //MASTER only: pointer to commit record!
	ds_state state;
} kp_vte;

/* Version tables: right now, the version table keeps track of what
 * its "current" version is, the idea being that the current version
 * can be set to a previous version. The eventual programming model
 * that we use may or may not need this.
 * LVNs start from 0, so the most-recent LVN is ver_count-1!
 */
struct kp_vt_struct {
	kp_kvstore *parent;    //back-pointer to parent kvstore
	const char *key;       //key that identifies this version table
	uint64_t ver_count;    //total number of versions ever added
	uint64_t len;          //number of entries stored in version table
	/* EXPLANATION: ver_count is the total number of versions that have ever
	 * been appended to this version table; it will never decrease. len is
	 * the number of entries that are currently stored in the table (which
	 * is the same as the vector_count() of all of the vector members below);
	 * it is incremented every time a version is added, and decremented
	 * every time a version is collected. To get the "current"/latest version
	 * of a value, index into the vectors using vt->len - 1.
	 */
	vector *vtes;    //vector of version table entries
	pthread_mutex_t *lock;  //lock for this version table
	ds_state state;
};

/* Commit records serve several purposes: ...
 * IMPORTANT: in kp_append_to_commit_log(), it is assumed that end_snapshot
 * comes before state in the layout of this struct! */
struct kp_commit_record_struct {
	bool use_nvm;
	uint64_t begin_snapshot;  //written into record by local worker
	uint64_t end_snapshot;    //written into record by master
	vector *kvpairs;
	/* The state_lock should be held whenever the state variable is changed.
	 * The state_lock is associated with the state_condition variable; 
	 * readers should lock the state_lock, check the state, and then wait
	 * on the condition variable if they need to be notified of a state
	 * change. */
	pthread_mutex_t *state_lock;
	pthread_cond_t *state_cond;
	const char *conflict_key;  //if we don't make a copy, then isn't this dangerous?
	kp_commit_record *next_cr;  //see kp_append_to_commit_log()
	commit_state state;
	bool debug_signalled;
};

/* Commit records store a vector of these structs. */
struct kp_kvpair_struct {
	/* Why not a "use_nvm" flag here? Because these are always stored in a
	 * commit record, which does have use_nvm.
	 * But actually, just makes it simpler to store it in here anyway. */
	bool use_nvm;
	const char *key;
	const void *value;
	size_t size;
	ds_state state;
};

/* Stores info used for garbage collection. Only one garbage collector
 * struct needs to be allocated for each master store; it is unused for
 * local stores.
 *
 * The garbage collector stores several "lists" of snapshot numbers.
 * Currently, these are implemented using vectors that are kept sorted,
 * but other data structures could be more efficient. The good thing
 * about using vectors is that they provide efficient "set comparison"
 * or "range comparison", e.g. checking if the values 123 - 135 are all
 * present in the list can be performed by finding the index containing
 * 123, then simply checking if the index 12 ahead contains 135 (assuming
 * there are no duplicates). The bad thing about using vectors is that
 * keeping them sorted means insertion operations require O(n) time.
 * Other data structures provide other tradeoffs for these operations;
 * a balanced tree would have more efficient insertions, but might
 * require traversal of part of the tree to perform range checking
 * (although maybe some kind of tree/heap-kept-in-an-array would not?).
 * Anyway, this vector was the data structure that I had most readily
 * available, so that's what we've got for now.
 *
 * The vector64 data structure works with unsigned long integers, which
 * should work with uint64_t's without casting on 64-bit systems. On 32-bit
 * systems, it's possible that using uint64_t with vector64 will cause
 * warnings at compile time and errors at runtime...
 */
struct kp_gc_struct {
	bool use_nvm;
	uint64_t last_gc_snapshot;   //snapshot from when we last ran GC
	vector64 *snapshots_in_use;  //snapshots in use by a local worker
	vector64 *cps_to_keep;       //checkpoints in use or marked as keep-forever
	vector64 *cps_collectable;   //checkpoints marked collectable (by telescoping), but still in-use!
	vector64 *cps_to_collect;    //checkpoints that will be collected in next GC run
	vector64 *cps_collected;     //checkpoints that we've collected
	pthread_mutex_t *lock;       //disallow concurrent GC
	ds_state state;
};

typedef struct stat_accumulator stat_accumulator_t;

struct stat_accumulator{
	FILE *stream;
	bool detailed;

	int vt_touched;
	int max_vt_versions;
	int min_vt_versions;
	float avg_vt_versions;

	int max_gvn_alive;
	int min_gvn_alive;

	int total_versions_alive;
	long bytes_in_current_versions;
	long bytes_in_old_versions;
	long bytes_in_metadata;
	long bytes_wasted;

};

/********************************/
/* Hash table helper functions: */
/********************************/
/* This hash function was found by googling for "c string hash function,"
 * and viewing this result: http://www.cse.yorku.ca/~oz/hash.html.
 * There are many, many other possibilities for a hash function; search
 * stackoverflow.com for much more information. MD5, SHA, etc. are other
 * potential good bets?
 */
size_t kp_ht_hasher_sdbm(const void *entry_ptr, size_t table_size)
{
	int c;
	const char *key;
	unsigned long hash;
	kp_ht_entry *entry = (kp_ht_entry *)entry_ptr;

	if (!entry) {
		kp_error("entry is NULL! Returning 0 (all NULL pointers will "
				"hash to this slot)\n");
		return 0;
	}

	//kp_debug("hashing entry: key=%s, vt=%p\n", entry->key, entry->vt);
	key = entry->key;  //"unsigned char *key" on hash function web page...
	hash = 0;

	/* "sdbm" algorithm: apparently this is used in gawk, BerkelyDB, etc. */
	c = *key;
	while (c != '\0') {
		hash = c + (hash << 6) + (hash << 16) - hash;
		key++;
		c = *key;
	}
	hash = hash % table_size;  //don't forget this!
	//kp_debug("returning hash=%u for entry->key=%s\n", (size_t)hash, entry->key);
	return (size_t)hash;
}

size_t kp_ht_hasher_djb2(const void *entry_ptr, size_t table_size)
{
	int c;
	const char *key;
	unsigned long hash;
	kp_ht_entry *entry = (kp_ht_entry *)entry_ptr;

	if (!entry) {
		kp_error("entry is NULL! Returning 0 (all NULL pointers will "
				"hash to this slot)\n");
		return 0;
	}

	//kp_debug("hashing entry: key=%s, vt=%p\n", entry->key, entry->vt);
	key = entry->key;  //"unsigned char *key" on hash function web page...
	hash = 5381;

	while ( (c = *key++) ) {
		hash = ((hash << 5) + hash) + c;  /* hash * 33 + c */
	}

	hash = hash % table_size;  //don't forget this!
	//kp_debug("returning hash=%u for entry->key=%s\n", (size_t)hash, entry->key);
	return (size_t)hash;
}

//Hash_hasher kp_ht_hasher = kp_ht_hasher_sdbm;
Hash_hasher kp_ht_hasher = kp_ht_hasher_djb2;

/* Simple string-comparison comparator. Returns true if the keys that
 * are stored in each entry match; returns false if they do not, or if
 * either or both pointers are NULL.
 */
bool kp_ht_comparator(const void *entry1_ptr, const void *entry2_ptr)
{
	kp_ht_entry *entry1 = (kp_ht_entry *)entry1_ptr;
	kp_ht_entry *entry2 = (kp_ht_entry *)entry2_ptr;
	if (!entry1 || !entry2) {
		kp_error("got a null ptr: %p %p\n", entry1, entry2);
		return false;
	}
	//kp_debug("comparing entry1 [%s] to entry2 [%s]\n",
	//		entry1->key, entry2->key);
	if (strcmp(entry1->key, entry2->key) == 0) {
		return true;
	}
	return false;
}

/* This is a callback function used with the hash table code to lock the
 * version table returned by a hash table lookup while the read-lock is
 * still held on the hash table. This ensures that the caller of the hash
 * lookup "owns" the version table that is looked up and it cannot be
 * deleted until the caller has released the vt lock.
 * Matches Hash_lookup_action typedef in hash.h.
 */
bool kp_ht_lock_vt(const void *entry_ptr)
{
	kp_ht_entry *entry = (kp_ht_entry *)entry_ptr;  //de-constification...

	/* Don't put this code behind KP_ASSERT - due to concurrency and racey
	 * conditions, may actually expect to hit it in "normal" operation. */
	if (!entry) {
		kp_error("got a NULL entry!\n");
		return false;
	}
	if (!entry->vt) {
		/* Depending on how deletions and garbage collection and removals
		 * from the hash table are implemented, this case may not be
		 * abnormal, i.e. if this thread happens to try to lookup an entry
		 * that is in the process of being removed (i.e. its vt has already
		 * been destroyed).
		 */
		kp_error("got a NULL vt!\n");
		return false;
	}

	kp_debug("got looked-up ht entry for key=%s; locking its version "
			"table while the rdlock on the hash table is still held!\n",
			entry->key);
	kp_debug_lock("%s: locking version table\n", entry->vt->key);
	kp_mutex_lock("entry->vt->lock", entry->vt->lock);
	return true;
}

/* Returns true if the entry is non-NULL and its pointers are null and
 * its free_key is false. Otherwise, returns false.
 */
bool kp_ht_entry_is_reset(kp_ht_entry *entry)
{
	if (entry) {
		if (entry->key == NULL && entry->vt == NULL) {
//		    && entry->free_key == false) {
			return true;
		}
	}
	return false;
}

/* Fills in the members of a kp_ht_entry. If the copy_key argument is true,
 * then this function will perform a string copy of the key argument;
 * otherwise, the pointer to the key argument will just be copied. The vt
 * pointer argument is always just copied; it is ok for the vt pointer to be
 * NULL, which will be the case when the entry will only be used for lookup
 * purposes (rather than actually being inserted into the hash table). The
 * key pointer must not be NULL, and the key string must not be empty.
 *
 * NOTE: kp_ht_entry_create() relies on this function!!
 *
 * If the use_nvm argument is set, then this function will use the
 * nvram memory allocation functions (if copy_key is also true), BUT it
 * will not flush the entire struct after it finishes! (right now the
 * caller(s) of this function does that).
 *
 * Returns: 0 on success, -1 on error.
 */
int kp_ht_entry_set(kp_ht_entry *entry, const char *key, kp_vt *vt,
		bool copy_key, bool use_nvm)
{
	size_t len = 0;

	if (!entry || !key) {
		kp_error("got a null argument: entry=%p, key=%p\n", entry, key);
		return -1;
	}

	len = strlen(key);
	if (len == 0) {
		kp_error("key length is 0\n");
		return -1;
	}

	/* In the code below, we un-cast the const qualifier on the key; this
	 * is kind of a no-no, but ok here because this is a set() function,
	 * and the caller knows that we're either just copying the key, or the
	 * caller has set copy_key to false and so it knows that it's handing
	 * over the pointer to us. */
	if (copy_key) {
		kp_debug("copy_key is true, allocating new string and performing "
				"strncpy\n");
		kp_kpalloc((void **)&(entry->key), len+1, use_nvm);
		if (entry->key == NULL) {
			kp_error("kp_kpalloc(&(entry->key)) failed\n");
			return -1;
		}
		kp_strncpy((char *)(entry->key), key, len+1, use_nvm);
	} else {
		kp_die("I think this is dead code: callers can just set key directly, "
				"rather than the overhead of calling this function\n");
	}

	entry->vt = vt;
	kp_debug("set kp_ht_entry: key=%p [%s], vt=%p (key=[%s])\n",
			entry->key, entry->key, entry->vt,
			entry->vt == NULL ? "NULL" : entry->vt->key);

	return 0;
}

/* Allocates a new kp_ht_entry. If the key argument is NULL, then the
 * entry struct will just be pre-allocated, but its members (key and vt)
 * must be filled in later by calling kp_ht_entry_set(). If the key
 * argument is non-NULL, then this function will call kp_ht_entry_set()
 * before returning. If the copy_key argument is true, then
 * this function will perform a string copy of the key argument; otherwise,
 * the pointer to the key argument will just be copied. The vt pointer
 * argument is always just copied; it is ok for the vt pointer to be NULL,
 * but this should (probably) only be done if this entry is being
 * allocated in order to perform a hash table lookup (rather than inserted
 * into the hash table). The key pointer must not be NULL, and the key
 * string must not be empty.
 *
 * If the use_nvm argument is true, then this function will allocate
 * the entry using the appropriate allocation function and will flush
 * things appropriately.
 *
 * Returns: 0 on success, -1 on error. On success, *entry is set to point
 * to the new entry.
 */
int kp_ht_entry_create_internal(kp_ht_entry **entry, const char *key,
		bool copy_key, kp_vt *vt, bool use_nvm)
{
	int ret;

	if (!entry) {
		kp_error("got a null argument! entry=%p, key=%p\n", entry, key);
		return -1;
	}

	kp_kpalloc((void **)entry, sizeof(kp_ht_entry), use_nvm);
	if (*entry == NULL) {
		kp_error("kp_kpalloc(kp_ht_entry) failed\n");
		return -1;
	}

	if (key) {
		kp_debug("key is non-NULL, calling kp_ht_entry_set\n");
		if (copy_key) {
			ret = kp_ht_entry_set(*entry, key, vt, copy_key, use_nvm);
			if (ret != 0) {
				kp_error("kp_ht_entry_set() returned error=%d\n", ret);
				kp_ht_entry_destroy(entry, use_nvm);  //will destroy key we just copied
				return -1;
			}
		} else {
			/* Optimization: if not copying key, just skip call to 
			 * kp_ht_entry_set(), set key and vt pointers directly.
			 * Really, we should just delete kp_ht_entry_set() altogether... */
			PM_EQU(((*entry)->key), (key));
			PM_EQU(((*entry)->vt), (vt));
		}
	} else {
		kp_debug("key is NULL, just setting members to null/false\n");
		PM_EQU(((*entry)->key), (NULL));
		PM_EQU(((*entry)->vt), (NULL));
	}
	kp_flush_range((void *)*entry, sizeof(kp_ht_entry), use_nvm);

	kp_debug("allocated new kp_ht_entry, key=%p [%s], vt=%p (vt->key=[%s]); "
			"%s copy key for the entry!\n",
			(*entry)->key, (*entry)->key, (*entry)->vt,
			(*entry)->vt == NULL ? "NULL vt" : (*entry)->vt->key,
			copy_key ? "did" : "did not");
	return 0;
}

/* Wrapper around kp_ht_entry_create_internal(); used by kp_kv_local and
 * kp_kv_master because they don't know what a kp_vt is, so the vt pointer
 * is always set to NULL.
 */
int kp_ht_entry_create(kp_ht_entry **entry, const char *key, bool copy_key,
		bool use_nvm)
{
	return kp_ht_entry_create_internal(entry, key, copy_key, NULL, use_nvm);
}

/* Resets the entry's values to null/false. This function DOES NOT free the
 * key nor the vt that the entry points to, so the caller should free these if
 * this is necessary!! Also, this function DOES NOT flush the pointer values
 * after seting them to NULL.
 * CHECK: will this ever be used in a place where it DOES need to be flushed?
 *   If so, add a "use_nvm" argument and change the code.
 */
void kp_ht_entry_reset(kp_ht_entry *entry)
{
	if (entry) {
		entry->key = NULL;
		entry->vt = NULL;
	} else {
		kp_warn("got a NULL entry pointer, doing nothing\n");
	}
}

/* Frees a kp_ht_entry that was allocated with kp_ht_entry_create_internal().
 * This function ALWAYS frees the key that the entry points to; if the caller
 * does not want the key to be freed, then (*entry)->key should be set to
 * NULL before calling this function! After freeing the entry,
 * this function will set *entry to NULL. If use_nvm is true, then this
 * function will flush *entry as well before returning.
 */
void kp_ht_entry_destroy(kp_ht_entry **entry, bool use_nvm)
{
    //CHECK - TODO: shouldn't use_nvm be internal to the kp_ht_entry??
	//  ignored this for now... wouldn't be used currently anyway.
	if (!entry) {
		kp_error("got null kp_ht_entry **entry!\n");
		return;
	}

	if (*entry != NULL) {
		if ((*entry)->key) {
			kp_debug("freeing key in kp_ht_entry!: %s\n", (*entry)->key);
			kp_free((void **)&((*entry)->key), use_nvm);
		}
		kp_free((void **)entry, use_nvm);  //sets *entry = NULL, always
	}
}

/* Internal version of kp_ht_entry_free_callback(). free_key tells us
 * whether or not the key that this entry contains should be freed. free_values
 * tells us whether or not the values that this entry's version table points to
 * should be freed. If the vt pointer inside of the entry is NULL, then
 * free_values doesn't matter.
 * This function should only be called on kp_ht_entrys that were previously
 * stored in the hash table; for kp_ht_entrys used only for lookup, free
 * them using kp_ht_entry_free().
 *
 * Currently only used with master store.
 */
void _kp_ht_entry_free_callback(kp_ht_entry *entry, bool free_key,
		bool free_values)
{
	if (!entry) {
		kp_error("entry is NULL! Returning.\n");
		return;
	}
	kp_debug("freeing kp_ht_entry: key=[%s], vt=%p (key=[%s]); free_values=%s\n",
			entry->key,	entry->vt, entry->vt ? entry->vt->key : "NULL",
			free_values ? "true" : "false");

	kp_die("old code: need to update / remove!\n");

	if (entry->key) {
		if (free_key) {
			/* Not sure we'll ever need this code branch! */
			kp_die("not implemented yet! (free_nv or free_v?!?)\n");
			kp_debug("free_key is true, so freeing it now!\n");
			kp_free((void **)&(entry->key), true);  //BUG: no idea if true or false here
		} else {
			kp_debug("free_key is false, so leaving key alone! (this is "
					"either a lookup entry, or the key will be freed when "
					"the version table is freed).\n");
		}
	} else {
		kp_warn("entry->key is NULL! This is unexpected.\n");
	}

	if (entry->vt) {
		/* Free the version table. This function will free the vtes that
		 * the version table contains because the second argument here is
		 * false, but kp_vt_destroy() will never free the actual values
		 * that the vtes point to. This is what we want for a master store,
		 * because the commit records own the values, not the version
		 * tables.
		 */
		kp_vt_destroy(&(entry->vt), false);
		  //CHECK: nvram or not? If local, then not; if master, then yes. Right?
	} else {
		kp_debug("entry->vt is NULL! This may be expected, e.g. if this "
				"entry was only used for performing a lookup.\n");
	}

	kp_warn("this code is about to free the key - is this the right location?\n");
	kp_ht_entry_destroy(&entry, true);  //CHECK: use_nvm here?!??
	  //CHECK: if entry->key is non-NULL, then kp_ht_entry_destroy() will
	  //  ALWAYS free it here!
	kp_debug("successfully destroyed the kp_ht_entry\n");
}

/* Hash table entry "freer" function for local kvstores. This function is
 * called either by hash_clear() (after the local store has merged its
 * commit into the master and needs to be reset) or by hash_free() (when
 * the local store is being destroyed).
 *
 * This function is passed a kp_ht_entry* as an argument, which contains
 * a pointer to a key and a version table. In version 3.0, keys are owned
 * by the hash table entries, so this function will always free the key.
 * When called by hash_clear(), all of the vtes that are pointed to by
 * the vte have already been freed, so this function just needs to free
 * the vt itself. Finally, the hash table entry struct itself is freed.
 *
 * This function determines whether or not to "use_nvm" by following the
 * pointer from each vt back to its parent kvstore, which was either
 * created to be stored on nvm or not. */
void kp_ht_entry_free_local(void *entry_ptr)
{
	bool use_nvm;

	kp_ht_entry *entry = (kp_ht_entry *)entry_ptr;
	if (!entry) {
		kp_error("entry is NULL! Returning.\n");
		return;
	}

	if (!entry->key || !entry->vt) {
		kp_error("got a null key=%p or vt=%p; returning\n", entry->key,
				entry->vt);
		return;
	}
	use_nvm = entry->vt->parent->use_nvm;
	kp_debug("freeing kp_ht_entry: key=[%s], vt=%p (key=[%s]), use_nvm=%s\n",
			entry->key,	entry->vt, (char *)entry->vt->key,
			use_nvm ? "true" : "false");

	/* Free the entry's version table, its key, and then the entry itself.
	 * All of these are copied / allocated in kp_put_local() (grep for
	 * "new_entry" in that function). kp_put_local() calls
	 * kp_ht_entry_create_internal() to allocate the entry and copy the
	 * key, so we call kp_ht_entry_destroy() to free them both (the destroy
	 * function always frees the entry's key if the pointer is non-NULL).
	 * kp_vt_create() created the vt, so we call kp_vt_destroy() to free it.
	 * Simple...
	 *
	 * We set the "vtes_already_freed" parameter to kp_vt_destroy() to true,
	 * because when hash_clear() is called on the local store's hash table,
	 * leading to this function, the local store has just finished iterating
	 * over its version tables to create a commit record, and along the way
	 * it destroyed all of the version table entries (see
	 * kp_process_commit_entries() - it calls kp_vte_destroy() on EVERY vte
	 * it traverses). So, we don't need to destroy them here, just destroy
	 * the version table itself.
	 */
	kp_todo("calling kp_vt_destroy() on a local store's version table; "
			"most of the time this is happening while the local store "
			"is being reset after a commit, so we don't need to free any "
			"VTEs here (because we already did during the commit), but "
			"won't we sometimes have to free VTEs here when this "
			"function is called due to a local store being entirely "
			"destroyed (not just reset/cleared)? AND ALSO: in this case, "
			"shouldn't we also free any values "
			"remaining in the local store (because kp_vt_destroy() "
			"never frees values)? Maybe we just need a separate function "
			"for cleaning up local stores that does all of these "
			"things...\n");
	kp_vt_destroy(&(entry->vt), true);
#ifdef KP_ASSERT
	if (entry->vt) {
		kp_die("kp_vt_destroy() didn't set entry->vt = NULL as expected!\n");
	}
#endif
	kp_ht_entry_destroy(&(entry), use_nvm);
#ifdef KP_ASSERT
	if (entry) {
		kp_die("kp_ht_entry_destroy() didn't set entry = NULL as expected!\n");
	}
#endif

	kp_debug("called kp_vt_destroy() and kp_ht_entry_destroy() to clean "
			"up this hash table entry\n");
	return;
}

/* Callback function for hash_clear() and hash_free(). Sometimes when the
 * hash table is freed we will want to destroy all of the values in the
 * version tables (e.g. when the master kvstore is freed), but sometimes
 * we will want to leave the values intact (e.g. when a local kvstore that
 * has just been merged is freed). Therefore, this function checks the
 * kvstore's "free_values" member to determine whether or not to free
 * the values in the version tables; make sure free_values is set
 * appropriately before calling this function, or hash_clear() or hash_free()!!
 *
 * Currently only used with master store.
 */
void kp_ht_entry_free_callback(void *entry_ptr)
{
	bool free_values = false;
	kp_ht_entry *entry = (kp_ht_entry *)entry_ptr;
	if (!entry) {
		kp_error("entry is NULL! Returning.\n");
		return;
	}

	kp_die("old code: need to update / remove!\n");
	/* TODO: make sure that our common kp_free(), etc. functions are used
	 * here! */

	///* The vt pointer may be NULL (e.g. if this is an entry used for lookup);
	// * in this case, free_values will be set to false, but it doesn't really
	// * matter because the internal _kp_ht_entry_free_callback() will ignore it anyway.
	// */
	//if (entry->vt && entry->vt->parent) {
	//	free_values = entry->vt->parent->free_values;
	//}
	kp_debug("set free_values to %s; vt is %p\n",
			free_values ? "true" : "false", entry->vt);

	/* This is just a wrapper function. Set the second argument to false to
	 * indicate that the key for this entry should NOT be freed (I think the
	 * vt free function will take tare of this...).
	 */
	_kp_ht_entry_free_callback(entry, false, free_values);
}

/* Function passed to hash_do_for_each(); for now, just prints each entry,
 * and ignores the "extra data" pointer. hash_do_for_each() will keep
 * calling this function with subsequent entries from the hash table until
 * this function returns false.
 */
bool kp_ht_print_entry(void *entry_ptr, void *extra_data)
{
	kp_ht_entry *entry;

	entry = (kp_ht_entry *)entry_ptr;
	if (!entry) {
		kp_debug("got NULL kp_ht_entry, halting processing\n");
		return false;
	}

	kp_print("printing kp_ht_entry: key=[%s], vt->key=[%s]; extra_data=%p\n",
			entry->key, entry->vt ? entry->vt->key : "NULL!",
			extra_data);  //added extra_data just to eliminate not-used warning
	return true;
}

/* Function used with hash_do_for_each() that calculates statistics for
 * each entry (version table) in the hash table. The data pointer is
 * expected to point to a stat_accumulator_t struct.
 * Returns: true on success, false on failure or null entry.
 */
bool kp_ht_process_stats(void *entry_ptr, void *data)
{
	//kp_vte *get_vte;
	kp_vte *get_vte1;

	kp_die("re-do this function!\n");

	kp_ht_entry *entry = (kp_ht_entry *)entry_ptr;
	if (!entry) {
		kp_debug("got NULL kp_ht_entry, halting processing\n");
		return false;  //stop calling this function on hash table entries.
	}

	kp_vt *vt = entry->vt;  //(kp_vt *)ht_value;
	stat_accumulator_t *stats = (stat_accumulator_t *)data;
	FILE *fp = stats->stream;
	char msg[KP_MSG_SIZE];

	kp_warn("haven't checked this code in version 3.0, may be possibility "
			"of deadlock!\n");
	kp_debug_lock("%s: locking version table\n", vt->key);
	kp_mutex_lock("vt->lock", vt->lock);

	kp_warn("this function probably needs updating after the addition of "
			"cp_nums!\n");

	if(stats->detailed){
		snprintf(msg, KP_MSG_SIZE,"This is key %s\n", vt->key);
		kp_write_stream(fp, msg);
		snprintf(msg, KP_MSG_SIZE,"\t...sanity check: we have added %ju "
				"versions, %ju of them are still stored in version table\n",
				vt->ver_count, vt->len);
		kp_write_stream(fp, msg);
	}

	// PJH: commented out to eliminate signed vs. unsigned comparison
	// warnings; this code is probably mostly obsolete anyway.
	//if(vt->ver_count > stats->max_vt_versions)
	//	stats->max_vt_versions = vt->ver_count;
	//if(vt->ver_count < stats->min_vt_versions)
	//	stats->min_vt_versions = vt->ver_count;

	stats->vt_touched++;
	stats->total_versions_alive += vt->ver_count;
	stats->avg_vt_versions =  stats->total_versions_alive/
		(float)stats->vt_touched;


	//int small_index = (int)vector64_get(vt->gvns, 0);
	//int big_index = (int)vector64_get(vt->gvns, vt->len-1);
	//if(small_index < stats->min_gvn_alive)
	//	stats->min_gvn_alive = small_index;
	//if(big_index > stats->max_gvn_alive)
	//	stats->max_gvn_alive = big_index;

	vector_get(vt->vtes, vt->len-1, (void **)(&get_vte1));
	int current_size = (int)(get_vte1->size);
	stats->bytes_in_current_versions += current_size;
	stats->bytes_in_metadata += sizeof(size_t) + //size
		sizeof(uint64_t) + // gvn 
		sizeof(uint64_t); //lvn

	// PJH: commented out to eliminate signed vs. unsigned comparison
	// warnings; this code is probably mostly obsolete anyway.
	//int i;
	//for(i = 0; i < vt->len-1; i++){
	//	vector_get(vt->vtes, i, (void **)(&get_vte));
	//	current_size = (int)(get_vte->size);
	//	stats->bytes_in_old_versions += current_size;
	//	stats->bytes_in_metadata += sizeof(size_t) + //size
	//		sizeof(uint64_t) + // gvn 
	//		sizeof(uint64_t); //lvn
	//}

	stats->bytes_in_metadata += strlen(vt->key) +1 +
		sizeof(pthread_mutex_t) +     //lock
		vector_struct_size() +        //values
		(4*vector64_struct_size()) +  //sizes, gvns, lvns, cp_nums
		sizeof(kp_vt);

	kp_debug_lock("%s: unlocking version table\n", vt->key);
	kp_mutex_unlock("vt->lock", vt->lock);
	return true;  //call this function on the next entry.
}

/***************************/
/*** Internal functions: ***/
/***************************/
/* This function doesn't do any error checking. */
kp_commit_record *kp_get_commit_log_record(vector *commit_log, uint64_t idx)
{
	void *e;
	vector_get(commit_log, idx, &e);
	return (kp_commit_record *)e;
}

/* (Currently unused, but might want it later...) */
/* This function checks if the commit record has been "finalized", which
 * at the moment means that it has entered either the COMMITTED or ABORTED
 * state. Importantly, commit records that have entered one of these states
 * can never leave these states (unless we have a bug somewher). That means
 * that we can check for finalization and the caller of this function can
 * actually use this information without worrying too much about race
 * conditions; of course, the caller should be aware that the CR's state
 * could become finalized immediately after this function returns false,
 * etc. */
bool kp_cr_is_finalized(kp_commit_record *cr)
{
	/* Currently, a commit record's state is only _written_ in the
	 * kp_commit_state_transition() function; while the state is updated
	 * with the state_lock mutex held, the actual write to cr->state should
	 * be atomic (just a mov instruction), so we always see a consistent
	 * state of the commit record here even though we don't have the lock. */
	return (cr->state == COMMITTED || cr->state == ABORTED);
}

/* Implements a state machine for commit records whose transitions are
 * defined by four parameters:
 *   The current state of the commit.
 *   Whether the transition is caused by a failure (a conflict detected
 *     for this commit record, or some other error) or a "success" (e.g.
 *     a completed merge, or ...).
 *       INVARIANT: I think that success should only ever be set to true
 *       by the owner of the commit, or by the just-previous commit when
 *       it also sets prev_is_finalized to be true. Otherwise, non-owners
 *       will only ever signal a commit record when success is false (for
 *       an abort).
 *   Whether or not this commit still has to wait for its previous commit
 *     to finish. The commit owner itself can set this condition to true
 *     in exactly one location: when it first appends the commit record
 *     to the log and checks if the previous commit has already been
 *     finalized (in which case we don't have to wait for it). Likewise,
 *     non-owners can set this condition to true in exactly one other
 *     location: in kp_merge_commit_record(), after a commit has just
 *     finalized and is signaling / transitioning its next_cr. In all
 *     other cases (namely when in-progress commits are looping forwards
 *     through their inserted version tables and sending failed transitions
 *     to other commits, in addition to a commit's self-transitions),
 *     this condition must be false. We call this condition
 *     "prev_is_finalized".
 *       INVARIANT: I think this means that no commit should ever be
 *         transitioned here more than once with prev_is_finalized set
 *         to true!!!!!!!!!!!!!!!!!! If you find that this is happening,
 *         then there had better be a good reason.
 *  Is the caller the "owner" of the commit or not (the thread that actually
 *    is performing or has performed the merge into the master store). This
 *    is only used at the end of this function for deciding whether or not
 *    to wait on the cr's condition variable when the state is WAIT_TO_COMMIT
 *    or WAIT_TO_ABORT (only the owner will ever want to wait, at least in
 *    this function anyway). This parameter is not identical to
 *    prev_is_finalized!
 *
 * This function always operates by first acquiring the state_lock of the
 * target commit record. In certain cases (when this commit has tentatively
 * succeeded, but is waiting for earlier commits to complete) this function
 * will wait on the commit record's condition variable; another (non-owner)
 * thread will eventually complete their commit and signal ours, and we'll
 * wake up and return.
 *
 * Before returning, this function ALWAYS signals the commit record's
 * condition variable, because the commit record's state will always be
 * changed by this function. Additionally, this function always flushes
 * the state after changing it; this seems like a good idea, but I haven't
 * thought carefully about whether this is necessary every time.
 *
 * Returns: the new state of the commit. NOTE that if this state is not a
 * final state (ABORTED or COMMITTED), then the caller should not rely on
 * it and should not take any actions based on the state without locking
 * the state_lock again. If something went wrong in this function, then
 * UNDEFINED will be returned. */
commit_state kp_commit_state_transition(kp_commit_record *cr,
		bool prev_is_finalized, bool success, bool is_owner,
		bool detect_conflicts, bool use_nvm)
{
	int ret;
	bool skip_transition = false;
	commit_state old_state, new_state, ret_state;
	if (!cr) {
		kp_error("got null argument cr=%p\n", cr);
		return UNDEFINED;
	}

	kp_debug2("entered: cr (%p) has range [%ju, %ju] and state=%s; "
			"prev_is_finalized=%s, success=%s, is_owner=%s, use_nvm=%s\n",
			cr, cr->begin_snapshot, cr->end_snapshot,
			commit_state_to_string(cr->state),
			prev_is_finalized ? "true" : "false",
			success ? "true" : "false",
			is_owner ? "true" : "false",
			use_nvm ? "true" : "false");
	//kp_todo("should this function set conflict_key if the caller passes "
	//		"it to us??\n");
	//  forget about conflict_key for now...
	
	/* To avoid the cost of a mutex lock & unlock here, we skip all of
	 * the transition code in the case where we enter this function but
	 * somebody has already set the state of this commit record to COMMITTED
	 * or ABORTED, or the commit record is already WAIT_TO_ABORT
	 * but we are not the previous commit record coming in
	 * to signal the next commit record here. Importantly, we don't ever
	 * skip the transition if we're the owner, because we may need to
	 * wait on the condition variable (which requires locking first), even
	 * if we're not actually going to transition out of one of these states.
	 * Similarly, we don't ever skip the transition if prev_is_finalized is
	 * set to true, because this transition must definitely signal the
	 * condition variable and possibly transition from a WAIT_TO_ state to
	 * a final state.
	 *
	 * Also, note that we can't skip the transition on WAIT_TO_COMMIT here,
	 * because even though the owner may have completed the merge successfully
	 * so far, prior commits may come in here with a conflict and transition
	 * to WAIT_TO_ABORT. One in WAIT_TO_ABORT, however, we can skip any
	 * transitions from non-owners and non-previous-commit threads; these
	 * threads are the only target of this optimization.
	 *
	 * This is purely an optimization on top of an already-working transition
	 * process; I think it should work fine, but if it causes any problems
	 * then we can just disable this. Because this is an optimization, we
	 * don't take the lock here before reading cr->state; if somebody else
	 * changes the state so that one of these conditions is true immediately
	 * after we've checked them and found them to be false, then this is
	 * fine, it will be worked out in the transition code below. */
#define OPTIMIZE_TRANSITION
#ifdef OPTIMIZE_TRANSITION
	if (!is_owner && !prev_is_finalized && 
			(cr->state == COMMITTED || cr->state == ABORTED ||
			 cr->state == WAIT_TO_ABORT)) {
		kp_debug2("setting skip_transition = true to optimize this "
				"transition function; somebody has already set this "
				"commit record's state to %s\n",
				commit_state_to_string(cr->state));
		skip_transition = true;
	}
#else
	skip_transition = false;
#endif

	/* A tricky scenario to keep track of: imagine that T8 is in the middle
	 * of merging its commit (it is INPROGRESS), when some earlier,
	 * concurrently merging commit T6 causes a conflict for T8. T6 will
	 * enter this function, and I'm pretty sure it should transition to
	 * WAIT_TO_ABORT (this was coded to begin with). Now, a few things
	 * could happen:
	 *   T6 completes, then T7 completes and signals T8 (which is still
	 *   merging its commit) with prev_is_finalized and success. At this
	 *   point I think that T8's commit record goes from WAIT_TO_ABORT
	 *   to ABORTED, even if T8 is still trying to perform its merge. Two
	 *   comments:
	 *     1. Below, when T8 tries to transition itself, we should probably
	 *        expect that it may find itself in ABORTED already and just
	 *        remain in ABORTED without error. Note that T8 could signal
	 *        itself with either !success or success!!!
	 *          I see a potential issue with this: if T8 doesn't try to
	 *          transition itself for some time, then T9 may be appended
	 *          to the commit log and initialize itself to CREATED_OK
	 *          because it thinks that T8 is finalized (and later on T8
	 *          will signal T9 anyway). This is the same "benign race
	 *          condition" that is described in several places below,
	 *          but possibly amplified. To "mitigate" this, see comment
	 *          2 below.
	 *          If this really becomes a problem, then I think it would
	 *          have to be solved by adding another state bit somewhere -
	 *			e.g. inside of the commit record itself, we store a flag
	 *			that says if the owner has recognized the abort yet or
	 *			not, or add another state to the machine, or something.
	 *			  The more and more I'm seeing this in the state transitions
	 *			  below, the more and more I don't really like that a
	 *			  non-owner can finalize the commit state..... gulp.
	 *     2. Really, T8 should check if it has been ABORTED in every
	 *        iteration of the loop through kvpairs. This wouldn't /
	 *        shouldn't eliminate any race conditions that already exist,
	 *        but it should prevent unnecessary work and perhaps unnecessary
	 *        conflicts (because T8 will stop inserting its kvpairs).
	 *   
	 *   T8 completes its merge and signals itself (i.e. because it thinks
	 *   it should try to move from INPROGRESS to WAIT_TO_COMMIT). At this
	 *   point, the commit record is actually in the WAIT_TO_ABORT, and a
	 *   transition will arrive with !prev_is_finalized, success, and
	 *   is_owner. I think the correct action is to remain in WAIT_TO_ABORT;
	 *   the owner should wait in this state for T7 to come along and tell
	 *   it prev_is_finalized.
	 *
	 *   T8 discovers a conflict itself and signals itself, i.e. to try to
	 *   move from INPROGRESS to WAIT_TO_ABORT. At this point, the commit
	 *   record's state is WAIT_TO_ABORT, and a transition with
	 *   !prev_is_finalized, !success, and is_owner will arrive. Again, I
	 *   think the correct action is to remain in WAIT_TO_ABORT, so that
	 *   the owner will wait in this state for T7 to come along and tell
	 *   it prev_is_finalized.
	 */

	/* More thoughts: in order to avoid deadlock, we must only transition
	 * into the WAIT_TO_ABORT state when we are in a non-OK state and
	 * we receive a transition without prev_is_finalized. If we are in
	 * an OK state and we move to WAIT_TO_ABORT, or if we move into
	 * WAIT_TO_ABORT on a transition with prev_is_finalized, then we will
	 * never receive another prev_is_finalized transition and we'll never
	 * leave the WAIT_TO_ABORT state!!!
	 */

	if (!skip_transition) {
		if (detect_conflicts) {
			kp_mutex_lock("cr->state_lock for transition", cr->state_lock);
		} else {
			/* When we don't do conflict detection, only the commit record's
			 * owner will call this function on it, so no need for locking.
			 *
			 * Actually, if/when we allow merges to complete asynchronously,
			 * and when we'll allow clients to wait for their commits to
			 * complete or abort, then we may need to put these locks back
			 * in place (because they protect the state associated with the
			 * condition variables the clients will wait on). Same thing for
			 * the condition variable broadcast below. */
			kp_debug2("skipping cr->state_lock lock, not needed when "
					"detect_conflicts is false\n");
		}
	
		new_state = UNDEFINED;
		old_state = cr->state;
		if (!success) {  //failed transition
			/* Failed transitions: we'll either transition to ABORTED or
			 * WAIT_TO_ABORT!!! (and sometimes we'll be UNDEFINED for
			 * something that we don't expect). */
			switch(old_state) {
			case CREATED:
				new_state = prev_is_finalized ? ABORTED : WAIT_TO_ABORT;
				break;
			case CREATED_OK:
				/* Somebody already told us that prev_is_finalized, so we
				 * don't expect it again! */
				new_state = prev_is_finalized ? UNDEFINED : ABORTED;
				break;
			case INPROGRESS:
				new_state = prev_is_finalized ? ABORTED : WAIT_TO_ABORT;
				break;
			case INPROGRESS_OK:
				/* Somebody already told us that prev_is_finalized, so we
				 * don't expect it again! */
				new_state = prev_is_finalized ? UNDEFINED : ABORTED;
				break;
			case WAIT_TO_COMMIT:
				/* If we're WAIT_TO_COMMIT, then only non-owner threads will be
				 * transitioning us here. If we get a non-success transition,
				 * then we know that we must abort; prev_is_finalized tells
				 * us if we can abort immediately, or if we still have to
				 * wait for the just-previous commit.
				 *
				 * Actually, I think that it's impossible for a failed (!success)
				 * transition to happen here with prev_is_finalized set to true;
				 * if the previous commit found a conflict with this commit
				 * record, then it already signaled it during the merge/insert
				 * function and so this commit record should already be in
				 * an ABORT state. When a commit record finalizes itself and
				 * signals its next_cr in kp_merge_commit_record, it always
				 * sets success to true! So, I changed this code line from this:
				 *    new_state = prev_is_finalized ? ABORTED : WAIT_TO_ABORT;
				 * to this:
				 *    new_state = prev_is_finalized ? UNDEFINED : WAIT_TO_ABORT;
				 */
#ifdef KP_ASSERT
				if (prev_is_finalized) {
					kp_die("don't ever expect this transition, I think; see "
							"comments in this function\n");
				}
#endif
				new_state = prev_is_finalized ? UNDEFINED : WAIT_TO_ABORT;
				break;
			case WAIT_TO_ABORT:
				/* It's possible that more than one non-owner will tell us
				 * to abort before the previous commit is finalized; if that
				 * happens, then we remain in the WAIT_TO_ABORT state until
				 * the previous commit tells us to go to ABORTED for sure.
				 */
				new_state = prev_is_finalized ? ABORTED : WAIT_TO_ABORT;
				break;
			case ABORTED:
				/* I don't think that this code path can / should ever be
				 * hit - if we've moved into our final ABORTED state, then
				 * all prior commits must have completed too, so the only
				 * commits that could be telling us to abort would be commits
				 * that come after ours, but I don't think that LATER commits
				 * ever signal / transition EARLIER commits in our code!
				 *
				 * Actually, this is not true - it's currently possible for
				 * the previous commit to finalize its next_cr's state to
				 * ABORTED! See the "tricky scenarios" notes further above. We
				 * do expect that a commit owner may then transition itself
				 * after it has already been ABORTED; if this happens, we just
				 * remain ABORTED.
				 *   Of course, this is only true if prev_is_finalized is false;
				 *   this commit record can't already be in a finalized state
				 *   and then get a new transition from its previous commit
				 *   record.
				 *
				 * The merge process checks if the current commit has ever been
				 * aborted (by somebody else) on every iteration through the list
				 * of kvpairs; currently, if this happens then the owner will
				 * signal itself with success = true, but I could also envision
				 * it signaling itself with success = false instead, in which
				 * case we would hit this code path. */
				new_state = prev_is_finalized ? UNDEFINED : ABORTED;
				break;
			case COMMITTED:
				/* Similarly to the ABORTED block just above, once we're in a
				 * final state of ABORTED or COMMITTED, I don't think that
				 * anybody should ever transition us again. But it's possible
				 * that it could happen due to a "benign" race condition in
				 * the code..... */
				kp_error("got a failed state transition but state is already "
						"COMMITTED\n");
				new_state = UNDEFINED;
				break;
			case UNDEFINED:
				kp_error("got a failed state transition but state is already "
						"UNDEFINED\n");
				new_state = UNDEFINED;
				break;
			default:
				kp_error("got bad or unexpected old_state=%s\n",
						commit_state_to_string(old_state));
				new_state = UNDEFINED;
				break;
			}
		} else {  //success transition
			switch(old_state) {
			case CREATED:
				/* Commit record was just-appended to commit log: if the commit
				 * before us finishes (or was already finished when we were
				 * appended to the log), we transition to CREATED_OK, otherwise
				 * the owner is starting the merge and goes to INPROGRESS. */
				new_state = prev_is_finalized ? CREATED_OK : INPROGRESS;
				break;
			case CREATED_OK:
				/* If the owner transitions here with prev_is_finalized set to
				 * false, then it is beginning the merge and so we go to
				 * INPROGRESS_OK. If prev_is_finalized is true, we may have
				 * hit the harmless (I hope) race condition where both the owner
				 * and the just-finished prior commit transition us to OK, so
				 * we stay in the CREATED_OK state (see comments in
				 * kp_append_to_commit_log()). */
#ifdef KP_ASSERT
				if (prev_is_finalized) {
					kp_debug2("hit benign race condition where both the commit "
							"owner and the prior commit set our state to OK; "
							"this should be normal, expect it to happen "
							"sometimes\n");
				}
#endif
				new_state = prev_is_finalized ? CREATED_OK : INPROGRESS_OK;
				break;
			case INPROGRESS:
				/* The commit owner is currently performing the merge. If the
				 * owner finishes and prev_is_finalized is false, then we go to
				 * the WAIT_TO_COMMIT state and will wait for the commit prior to us
				 * to signal us (see below). If the thread before us signals
				 * us with prev_is_finalized, then nobody has conflicted with us
				 * so far and we go to INPROGRESS_OK. */
				new_state = prev_is_finalized ? INPROGRESS_OK : WAIT_TO_COMMIT;
				break;
			case INPROGRESS_OK:
				/* The commit owner has finished merging and the previous commit
				 * has told us that we're ok, so we can go to COMMITTED. We can
				 * expect to hit the same race condition here that we hit in the
				 * CREATED_OK state: the CR owner can see that the previous commit
				 * record has finalized its state _before_ the previous commit
				 * has a chance to signal us, so we move ourselves to CREATED_OK
				 * above, then we yet again move ourselves to INPROGRESS_OK
				 * before the previous commit can signal us. This isn't terribly
				 * uncommon because in-between the previous commit finalizing
				 * its state and its signaling us, there is a flush / fence
				 * and a function return. */
				if (prev_is_finalized) {
					kp_debug2("hit benign race condition where both the commit "
							"owner and the prior commit set our state to OK; "
							"this should be normal, expect it to happen "
							"sometimes\n");
				}
				new_state = prev_is_finalized ? INPROGRESS_OK : COMMITTED;
				break;
			case WAIT_TO_COMMIT:
				/* Only the previous commit should be able to transition us
				 * out of this state with a prev_is_finalized transition;
				 * because we're waiting, the owner can't transition itself
				 * here, and we don't ever expect non-previous threads to
				 * signal / transition us with success set to true. */
				new_state = prev_is_finalized ? COMMITTED : UNDEFINED;
				break;
			case WAIT_TO_ABORT:
				/* In the case where a previous commit has aborted this commit,
				 * but before the commit just-previous to this one has finalized,
				 * we may find ourself in this state if the owner tries to signal
				 * itself successfully at the end of its merge. Surprise - you've
				 * been aborted! In this case, we want to remain in the
				 * WAIT_TO_ABORT state, because we still need the previous commit
				 * to tell us that prev_is_finalized (we'll go wait on the
				 * condition variable below).
				 *
				 * In the case where some previous commit has aborted this commit,
				 * and then the just-previous commit tells us that it is finalized,
				 * we move immediately to the ABORTED state. This means that a non-
				 * owner is moving this commit to a finalized state!! I think that
				 * this is ok, but it does have at least minor repercussions (see
				 * the "tricky scenario" notes above).
				 *   The alternative is to add another dimension or state bit
				 *   somewhere: there needs to be a difference between
				 *   OWNER_WAITING_TO_ABORT and OWNER_SHOULD_ABORT_WHEN_IT_
				 *   NOTICES_THIS. */
				new_state = prev_is_finalized ? ABORTED : WAIT_TO_ABORT;
				break;
			case ABORTED:
				/* We can hit this case in the following scenario: T5 ABORTS
				 * due to a conflict that it finds itself, and then T6 ABORTS
				 * due to a conflict that it finds itself, before T5 has a
				 * chance to signal T6 (with prev_is_finalized and success) when
				 * T5 is finalizing itself. There are probably other instances
				 * where things like this can occur as well.
				 * 
				 * Should we worry about this and try to prevent it? Meh; it
				 * might waste a few cycles (ok, perhaps many cycles, due to
				 * the mutex in this function), but it _seems_ harmless to
				 * just remain in the ABORTED state when we receive a second
				 * transition while we're already ABORTED.
				 *   Instead, our state machine can guarantee: if _at least
				 *   one_ failure transition is received, or _at least one_
				 *   prev_is_finalized transition is received, then the cr will
				 *   definitely transition to an ABORTED or a committable
				 *   state. I'm pretty sure this is true in the current state
				 *   machine.
				 *
				 * ALSO, I'm pretty sure that the "benign race condition" bug
				 * described in the COMMITTED state below could hit us right
				 * here too (see the comments at the end of kp_merge_commit_record()
				 * too).
				 *
				 * For the time being, we only expect prev_is_finalized transitions
				 * to come in while we're already aborted; we don't expect
				 * "progress" transitions (from the commit's owner) when we're
				 * already ABORTED.
				 *   To ensure that the commit owner doesn't try to do this, it
				 *   must check if somebody else has ABORTED it every time it
				 *   loops through a kvpair in the commit record... right?
				 * Actually, see the "tricky scenarios" notes further above. We
				 * do expect that a commit owner may transition itself after it
				 * has already been ABORTED; if this happens, we just remain
				 * ABORTED.
				 */
				kp_debug2("received a transition while already in the ABORTED "
						"state; is this expected? Probably (see comments "
						"in this code)...\n");
				new_state = prev_is_finalized ? ABORTED : ABORTED;
				break;
			case COMMITTED:
				/* If we're already COMMITTED, then we don't really expect any
				 * more state transitions. However, it's possible that the
				 * "benign" race condition that happens for CREATED_OK and
				 * INPROGRESS_OK could also happen for the COMMITTED state!
				 * So, if we get a prev_is_finalized transition here, we just
				 * remain COMMITTED; we don't ever expect to get a
				 * non-prev_is_finalized transition here (because it would
				 * be coming from ourself...).
				 *   Is there some kind of cascading effect here??? If we
				 *   hit this code block, then it means that this commit
				 *   record finalized (and perhaps signaled its next commit
				 *   record, and so on...) before the previous commit has
				 *   finalized. Actually, I think this is fine - it's not
				 *   that the previous commit hasn't _finalized_, it's just
				 *   that the previous commit hasn't signaled us yet. We
				 *   always ensure that the previous commit's state is finalized
				 *   before signaling; that's what started this race condition
				 *   (when this commit checked the previous commit's state) in
				 *   the first place.
				 * It looks like I actually hit this race condition in a test
				 * run once (with only 4 threads in test_local_master), although
				 * I caught it with an assertion just before this function was
				 * called; see the comment at the end of kp_merge_commit_record().
				 *
				 *   Oof. */
				if (prev_is_finalized) {
					kp_debug2("hit benign race condition where both the commit "
							"owner and the prior commit set our state to OK; "
							"this should be normal, expect it to happen "
							"sometimes\n");
				}
				new_state = prev_is_finalized ? COMMITTED : UNDEFINED;
				break;
			case UNDEFINED:
				/* Should never be called with UNDEFINED state; only case
				 * I can see is if pthread_cond_wait() below returns an
				 * error. */
				kp_error("got a success state transition but state is already "
						"UNDEFINED\n");
				new_state = UNDEFINED;
				break;
			default:
				kp_error("got bad or unexpected old_state=%s\n",
						commit_state_to_string(old_state));
				new_state = UNDEFINED;
				break;
			}
		}

		/* Update and flush, every time: */
		PM_EQU((cr->state), (new_state));
		kp_flush_range(&(cr->state), sizeof(commit_state), use_nvm);
		kp_debug2("Commit [%ju,%ju] TRANSITION: %s -> %s\n",
				cr->begin_snapshot, cr->end_snapshot, 
				commit_state_to_string(old_state),
				commit_state_to_string(cr->state));
	} else {
		kp_debug2("skip_transition is true, means that we already found "
				"that this commit record is already or waiting to be "
				"committed or aborted. To be safe, we'll still signal "
				"the condition variable now; we'll skip the condition "
				"variable wait because we're not the owner, and we'll "
				"skip the mutex unlock below.\n");
#ifdef KP_ASSERT
		if (is_owner) {
			kp_die("should never skip the transition if we are the owner!!\n");
		}
#endif
	}

	/* The condition variable effectively represents "commit record state
	 * change" - the owner waits on it when it needs somebody else to make
	 * the state change, and so right here we should ALWAYS signal the
	 * condition variable to tell potential waiters (often there will be
	 * none) that the state has changed. According to the pthread_signal
	 * man page and some research on the web, it's probably a good idea
	 * to signal while we still hold the mutex, for "predictable scheduling."
	 *
	 * Does it make sense to do this signal first, before potentially
	 * waiting on the condition below? It seems ok, and it seems like if
	 * we didn't signal the condition before waiting on it, there could
	 * be deadlock if (for some reason...) the thread that WE are expecting
	 * to change the state now and signal us is currently waiting on the
	 * condition itself.
	 *
	 * We actually broadcast rather than signal here - it's possible that
	 * there could be multiple threads waiting on a commit record's state
	 * change, especially when we allow asynchronous commits and worker
	 * threads can request to block until their particular commit has
	 * completed.
	 */
	if (detect_conflicts) {
		ret = pthread_cond_broadcast(cr->state_cond);
		if (ret != 0) {
			kp_error("pthread_cond_broadcast() returned error=%d\n", ret);
			cr->state = UNDEFINED;  //return error
			kp_flush_range(&(cr->state), sizeof(commit_state), use_nvm);
		}
		/* Don't unlock quite yet... */
	} else {
		kp_debug2("skipping pthread_cond_broadcast(cr->state_cond) because "
				"detect_conflicts is false\n");
	}

	/* If we transitioned into the WAIT_TO_COMMIT or WAIT_TO_ABORT state, then
	 * we actually 
	 * don't want to leave this function yet; we wait on the condition
	 * variable here, and the thread that owns the previous commit
	 * will call this function on the same commit record, transition
	 * the state out of WAIT_TO_COMMIT / WAIT_TO_ABORT and into either ABORTED
	 * or COMMITTED,
	 * and then signal us so that we can wake up and proceed with
	 * whatever we were doing (i.e. finalizing our commit, or abort).
	 * IMPORTANT:
	 * note that after waking up, we don't have to actually check the
	 * state (besides that it's not WAIT_TO_COMMIT / WAIT_TO_ABORT still) or
	 * do anything
	 * more; the signaller is the one that will transition the state
	 * to ABORTED or COMMITTED, and we just need to return to our
	 * caller.
	 * We hold the state_lock now, but remember how condition variables
	 * work: we'll release the mutex when we start waiting, and will
	 * automatically acquire it again before we start running this code
	 * again. */
	/* Remember: while loop, not if statement!! (see man page for wait). */
	while (cr->state == WAIT_TO_COMMIT || cr->state == WAIT_TO_ABORT) {
		if (!is_owner) {
			/* I grepped for this debug statement after adding this if
			 * block, and it actually is hit every once in a while. Looks
			 * like it's working; immediately after the thread that hits
			 * this code unlocks the mutex and leaves this function, an
			 * owner thread on this commit record wakes up, sees that the
			 * state has moved from WAIT_TO_ABORT to WAIT_TO_ABORT, and
			 * then waits again (and a little while later somebody moves
			 * it to ABORTED and the thread wakes up and continues; hooray!)
			 */
			kp_debug2("is_owner is false, so not waiting on the condition "
					"here even though state is %s\n",
					commit_state_to_string(cr->state));
			break;
		}
#ifdef KP_ASSERT
		if (prev_is_finalized) {
			kp_die("prev_is_finalized should never be true here; it it is, "
					"this means that our previous commit is about to wait "
					"for something to happen to this commit, which will "
					"almost certainly cause deadlock!\n");
		}
		if (!detect_conflicts) {
			kp_die("detect_conflicts is false, but we're about to wait "
					"on the condition variable - nobody will ever wake "
					"us up!!\n");
		}
#endif
		kp_debug2("WAITING on cr->state_cond now (is_owner is true!)\n");
		ret = pthread_cond_wait(cr->state_cond, cr->state_lock);
		kp_debug2("WOKE UP from cr->state_cond wait, cr->state is "
				"now %s\n", commit_state_to_string(cr->state));
		if (ret != 0) {
			kp_error("pthread_cond_wait() returned error=%d\n", ret);
			/* Override state that may have just been set by non-owner */
			cr->state = UNDEFINED;
			kp_flush_range(&(cr->state), sizeof(commit_state), use_nvm);
		}
	}

	/* Make a returnable copy of the commit record's state BEFORE we unlock;
	 * I learned the hard way with the stupid hash table what can happen if
	 * you don't copy before returning. */
	ret_state = cr->state;

	if (!skip_transition && detect_conflicts) {
		kp_mutex_unlock("cr->state_lock for transition", cr->state_lock);
	} else {
		kp_debug2("skipping cr->state_lock unlock, not needed when "
				"skip_transition is true (%s) or detect_conflicts is "
				"false (%s)\n", skip_transition ? "true" : "false",
				detect_conflicts ? "true" : "false");
	}

#ifdef KP_ASSERT
	if (ret_state == UNDEFINED) {
		kp_die("transition to UNDEFINED state; aborting here\n");
	}
#endif
	return ret_state;
}

/* Allocates and initializes a garbage collector struct. The use_nvm value
 * that is passed to this function should match what is used for the master
 * store. However, even if use_nvm is true, that does not mean that this
 * function will allocate all (or any) of its data structures in non-volatile
 * memory; most (all?) of the GC state can be reconstructed upon failure
 * and recovery, so we don't have to waste time flushing it etc. here.
 *
 * Returns: 0 on success, -1 on error. On success, *gc is set to point
 * to the new gc struct.
 */
int kp_gc_create(kp_gc **gc, bool use_nvm)
{
	int ret;
	bool use_nvm_gc;

	kp_debug_gc("entered: gc=%p, use_nvm=%s\n",
			gc, use_nvm ? "true" : "false");

	use_nvm_gc = false;
	kp_debug_gc("even though use_nvm=%s, using use_nvm_gc=%s throughout "
			"this function, because I think all (?) of the GC state can "
			"just be reconstructed during recovery\n",
			use_nvm ? "true" : "false",
			use_nvm_gc ? "true" : "false");

	kp_kpalloc((void **)gc, sizeof(kp_gc), use_nvm_gc);
	if (*gc == NULL) {
		kp_error("kp_kpalloc(kp_gc) failed!\n");
		return -1;
	}

	(*gc)->use_nvm = use_nvm_gc;
	(*gc)->last_gc_snapshot = INITIAL_GLOBAL_SNAPSHOT;
	kp_debug_gc("set gc's use_nvm to %s\n", (*gc)->use_nvm ? "true" : "false");
	kp_debug_gc("set gc's last_gc_snapshot to INITIAL_GLOBAL_SNAPSHOT=%ju; "
			"this means that the initial snapshot will never be checked "
			"for collection. Is this ok?\n", (*gc)->last_gc_snapshot);

	kp_debug_gc("using GC_VECTOR_SIZE=%lu for initial size of gc vectors; "
			"using a larger size (closer to expected number of commits...) "
			"could reduce number of vector resizes\n", GC_VECTOR_SIZE);
	ret = vector64_create(&((*gc)->snapshots_in_use), GC_VECTOR_SIZE,
			use_nvm_gc);
	if (ret != 0) {
		kp_error("vector64_create(snapshots_in_use) failed: %d\n", ret);
		kp_free((void **)gc, use_nvm_gc);
		return -1;
	}
	ret = vector64_create(&((*gc)->cps_to_keep), GC_VECTOR_SIZE,
			use_nvm_gc);
	if (ret != 0) {
		kp_error("vector64_create(cps_to_keep) failed: %d\n", ret);
		vector64_destroy(&((*gc)->snapshots_in_use));
		kp_free((void **)gc, use_nvm_gc);
		return -1;
	}
	ret = vector64_create(&((*gc)->cps_collectable), GC_VECTOR_SIZE,
			use_nvm_gc);
	if (ret != 0) {
		kp_error("vector64_create(cps_collectable) failed: %d\n", ret);
		vector64_destroy(&((*gc)->cps_to_keep));
		vector64_destroy(&((*gc)->snapshots_in_use));
		kp_free((void **)gc, use_nvm_gc);
		return -1;
	}
	ret = vector64_create(&((*gc)->cps_to_collect), GC_VECTOR_SIZE,
			use_nvm_gc);
	if (ret != 0) {
		kp_error("vector64_create(cps_to_collect) failed: %d\n", ret);
		vector64_destroy(&((*gc)->cps_collectable));
		vector64_destroy(&((*gc)->cps_to_keep));
		vector64_destroy(&((*gc)->snapshots_in_use));
		kp_free((void **)gc, use_nvm_gc);
		return -1;
	}
	ret = vector64_create(&((*gc)->cps_collected), GC_VECTOR_SIZE,
			use_nvm_gc);
	if (ret != 0) {
		kp_error("vector64_create(cps_collected) failed: %d\n", ret);
		vector64_destroy(&((*gc)->cps_to_collect));
		vector64_destroy(&((*gc)->cps_collectable));
		vector64_destroy(&((*gc)->cps_to_keep));
		vector64_destroy(&((*gc)->snapshots_in_use));
		kp_free((void **)gc, use_nvm_gc);
		return -1;
	}

	ret = kp_mutex_create("gc->lock", &((*gc)->lock));
	if (ret != 0) {
		kp_error("kp_mutex_create() returned error %d\n", ret);
		vector64_destroy(&((*gc)->cps_collected));
		vector64_destroy(&((*gc)->cps_to_collect));
		vector64_destroy(&((*gc)->cps_collectable));
		vector64_destroy(&((*gc)->cps_to_keep));
		vector64_destroy(&((*gc)->snapshots_in_use));
		kp_free((void **)gc, use_nvm_gc);
		return -1;
	}

	/* "CDDS": flush, set state, and flush again.
	 *
	 * Most/all of these steps will be noops for the gc struct, because
	 * for now we're not allocating it in non-volatile memory. We put
	 * the code here anyway though to match all of our other data structures.
	 */
	kp_flush_range((void *)*gc, sizeof(kp_gc) - sizeof(ds_state),
			(*gc)->use_nvm);
	(*gc)->state = STATE_ACTIVE;
	kp_flush_range((void *)&((*gc)->state), sizeof(ds_state),
		(*gc)->use_nvm);

	kp_debug_gc("successfully allocated new gc_struct (%p)\n", *gc);
	return 0;
}

/* Frees a garbage collector struct.
 * Returns: 0 on success, or -1 on error.
 */
void kp_gc_destroy(kp_gc **gc)
{
	if (!gc) {
		kp_error("got a null argument: gc=%p\n", gc);
		return;
	}

	kp_debug_gc("entered - destroying gc struct, including any vector64s "
			"which might still contain snapshot numbers...\n");
	if (*gc) {
		(*gc)->state = STATE_DEAD;
		kp_flush_range((void *)&((*gc)->state), sizeof(ds_state),
				(*gc)->use_nvm);

		if ((*gc)->snapshots_in_use) {
			vector64_destroy(&((*gc)->snapshots_in_use));
		}
		if ((*gc)->cps_to_keep) {
			vector64_destroy(&((*gc)->cps_to_keep));
		}
		if ((*gc)->cps_collectable) {
			vector64_destroy(&((*gc)->cps_collectable));
		}
		if ((*gc)->cps_to_collect) {
			vector64_destroy(&((*gc)->cps_to_collect));
		}
		if ((*gc)->cps_collected) {
			vector64_destroy(&((*gc)->cps_collected));
		}
		if ((*gc)->lock) {
			kp_mutex_destroy("gc->lock", &((*gc)->lock));
		}

		kp_free((void **)gc, (*gc)->use_nvm);  //flushes and sets *gc = NULL
	}
}

/* Checks that vt's len member matches length of all of the vectors in
 * the version table. This function will die if the sanity check fails.
 */
void kp_vt_sanity_check_lengths(kp_vt *vt)
{
#ifdef KP_ASSERT
	if (vt->len != vector_count(vt->vtes)) {  //sanity check:
		kp_die("vt->len != vector_count(vt->vtes): len=%ju, count=%llu\n",
			  vt->len, vector_count(vt->vtes));
	}
	if (vt->len == UINT64_MAX) {
		kp_die("vt->len is UINT64_MAX\n");
	}
#endif
	return;
}

/* This function just directly copies its arguments into the newly-allocated
 * kp_vte; it doesn't perform any validity checking on the values. Currently
 * kp_vt_append_value() is the only caller of this function (kp_vt_append_value
 * is unified for both local and master stores (but actually, is only called
 * by local stores as of version 3.0)). This function accepts tombstone values
 * (NULL value and 0 size) for deletions.
 *
 * Returns: 0 on success, -1 on error. On success, *vte is set to point to
 * the new kp_vte, and the struct and its pointer have been flushed to memory.
 * On failure, *vte is set to NULL. */
int kp_vte_create(kp_vte **vte, const void *value, uint64_t size, uint64_t lvn,
		uint64_t ttl, uint64_t snapshot, kp_commit_record *cr, bool use_nvm)
{
	if (!vte) {
		kp_error("got a null argument: vte=%p\n", vte);
		return -1;
	}

	/* kp_kpalloc: use zero-initialized memory when on nvm, but normal
	 * malloc'd memory when not. */
	kp_kpalloc((void **)vte, sizeof(kp_vte), use_nvm);
	if (*vte == NULL) {
		kp_error("kp_kpalloc(kp_vte) failed\n");
		return -1;
	}

	/* Here it is: a tombstone value for a deletion is represented by a
	 * vte with a NULL value pointer and 0 size.
	 */
	PM_EQU(((*vte)->value), (value));
	PM_EQU(((*vte)->size), (size));
	PM_EQU(((*vte)->lvn), (lvn));
	PM_EQU(((*vte)->ttl), (ttl));
	PM_EQU(((*vte)->snapshot), (snapshot));
	PM_EQU(((*vte)->cr), (cr));
	if ((*vte)->value == NULL) {
		kp_debug_gc("created a new tombstone vte (%p): its value pointer "
				"is NULL and its size is %zu\n", *vte, (*vte)->size);
	}

	/* "CDDS": flush, set state, and flush again. The flushes will only actually
	 * occur if use_nvm is true. */
	kp_flush_range((void *)vte, sizeof(kp_vte) - sizeof(ds_state), use_nvm);
	PM_EQU(((*vte)->state), (STATE_ACTIVE));
	kp_flush_range((void *)&((*vte)->state), sizeof(ds_state), use_nvm);

	kp_debug("finished allocating new kp_vte: value=%p, size=%zu, "
			"lvn=%ju, ttl=%ju, snapshot=%ju, cr=%p\n", (*vte)->value,
			(*vte)->size, (*vte)->lvn, (*vte)->ttl, (*vte)->snapshot,
			(*vte)->cr);
	return 0;
}

char *kp_vte_to_string(kp_vte *vte)
{
	kp_die("not implemented yet! vte=%p\n", vte);
}

void kp_vte_recover(kp_vte *vte)
{
	kp_die("not implemented yet! vte=%p\n", vte);
}

/* Destroys a version table entry. If *vte->value is non-null, then this
 * function will always free it; so, if the caller doesn't want to free
 * the value that this vte points to yet, it should just set *vte->value
 * to NULL before calling this function. This is IMPORTANT!!
 *
 * Version table entries may contain a pointer to a commit record (in
 * the master store), but this is never touched / freed by this function.
 *
 * This function sets *vte to NULL before returning. If use_nvm is true,
 * then *vte will be flushed before returning.
 */
void kp_vte_destroy(kp_vte **vte, bool use_nvm)
{
	if (!vte) {
		kp_error("got NULL vte argument, returning\n");
		return;
	}

	if (*vte) {
		(*vte)->state = STATE_DEAD;
		kp_flush_range(&((*vte)->state), sizeof(ds_state), use_nvm);

		if ((*vte)->value) {
			kp_debug2("(*vte)->value is non-NULL, so freeing vte's value\n");
			kp_free((void **)&((*vte)->value), use_nvm);  //sets (*vte)->value to NULL
			  //de-const!
		} else {
			kp_debug2("(*vte)->value is NULL; means that this vte was either "
					"for a tombstone, or the caller didn't want us to free "
					"the value and so set this pointer to NULL.\n");
		}

		kp_free((void **)vte, use_nvm);  //sets *vte to NULL after free
	}
	kp_debug("destroyed *vte\n");
}

/* Allocates and initializes a version table. This function just copies
 * the pointer to the key, it does not create a new copy! The initialized
 * version table does not contain any versions yet (so its ver_count and
 * len are 0).
 * If the use_nvm argument is true, then this function will use the proper
 * allocation functions for nvm and will flush the new vt and the pointer
 * to it before returning.
 * Returns: 0 on success, -1 on error. On success, *new_vt is set to point
 * to the new version table. */
int kp_vt_create(kp_kvstore *kv, const char *key, kp_vt **new_vt)
{
	int ret;

	if (kv == NULL || key == NULL || new_vt == NULL) {
		kp_error("got a NULL argument: kv=%p, key=%p, new_vt=%p\n",
				kv, key, new_vt);
		return -1;
	}

	kp_kpalloc((void **)new_vt, sizeof(kp_vt), kv->use_nvm);
	if (!(*new_vt)) {
		kp_error("kp_kpalloc(kp_vt) failed\n");
		return -1;
	}

	/* Must set this immediately if we call kp_vt_destroy() from this
	 * function - that function checks the parent for its use_nvm
	 * value!
	 */
	PM_EQU(((*new_vt)->parent), (kv)); // persistent

	ret = vector_create(&((*new_vt)->vtes), 0, kv->use_nvm);
	  /* The vector of VTEs will be made CDDS for kvstores on non-volatile
	   * memory. */
	if (ret != 0 || (*new_vt)->vtes == NULL) {
		kp_error("vector_create(vtes) failed\n");
		/* Pass true for "vtes_already_freed" - no vtes allocated yet: */
		kp_vt_destroy(new_vt, true);
		  /* BUG: this is unsafe for use_nvm = false, because malloc()
		   *  may not have initialized the memory to 0s! */
		return -1;
	}

	ret = kp_mutex_create("(*new_vt)->lock", &((*new_vt)->lock));
	if (ret != 0) {
		kp_error("kp_mutex_create() returned error %d\n", ret);
		kp_vt_destroy(new_vt, true);
		return -1;
	}

	PM_EQU(((*new_vt)->key), (key));  //just copy the pointer; the ht entry owns the key // persistent
	PM_EQU(((*new_vt)->ver_count), (0));	// persistent
	PM_EQU(((*new_vt)->len), (0));		// persistent

	/* "CDDS": flush, set state, and flush again. */
	kp_flush_range((void *)*new_vt, sizeof(kp_vt) - sizeof(ds_state),
			kv->use_nvm);
	PM_EQU(((*new_vt)->state), (STATE_ACTIVE)); // persistent
	kp_flush_range((void *)&((*new_vt)->state), sizeof(ds_state), kv->use_nvm);

	kp_debug("allocated and initialized kp_vt for key %s\n", (*new_vt)->key);
	return 0;
}

//typedef bool (*vector_validator)(const void *e1);
bool kp_vte_validator(const void *vteptr)
{
	kp_commit_record *cr = ((kp_vte *)vteptr)->cr;
	//kp_print("got cr [%ju, %ju] with state %s; returning %s for vector "
	kp_debug2("got cr [%ju, %ju] with state %s; returning %s for vector "
			"validator\n", cr->begin_snapshot, cr->end_snapshot,
			commit_state_to_string(cr->state),
			cr->state == COMMITTED ? "true" : "false");
	return (cr->state == COMMITTED);
}

/* NOTE: this function does not perform any flushing and does not concern
 * itself with non-volatile memory things; it just sets *value and *size
 * to the internal locations in the found VTE before returning. The caller
 * of this function should be better-equipped to handle nvm issues, if
 * necessary.
 *
 * Pulls a value from the version table. To get the "current" version,
 * all of the lvn, gvn and snapshot args should be set to UINT64_MAX.
 * Otherwise, ONE of the lvn/gvn/snapshot should be set to a value that
 * is not UINT64_MAX, otherwise this function will return an error.
 * This function does NOT make a copy of the value; the returned pointer
 * points to memory "internal" to the KV store!
 * Note that the value pointer that is returned may be NULL, indicating
 * a DELETED value!
 * The vt's lock MUST be held before calling this function! This function
 * will not modify the vt's lock state.
 * Returns: On success 0 is returned, the value argument is set to point
 * to the value, and the size argument is set to the size of the value.
 * If the specified version was not found in this version table, 2 is
 * returned. If an error occurred, -1 is returned.
 */
int kp_vt_get_value(const kp_vt *vt, uint64_t lvn, uint64_t snapshot,
		const void **value, size_t *size, uint64_t max_snapshot)
{
	int count;
	uint64_t index;
	kp_vte *get_vte;

	if (!vt || !value || !size) {
		kp_error("one or more NULL pointer arguments\n");
		return -1;
	}

	kp_debug("entered: vt->key=%s, lvn=%ju, snapshot=%ju, max_snapshot=%ju\n",
			vt->key, lvn, snapshot, max_snapshot);

	/* Check arg validity (there must be a smarter way to do this...) */
	count = 0;
	if (lvn != UINT64_MAX)    {count++;}
	if (snapshot != UINT64_MAX) {count++;}
	if (count > 1) {
		kp_error("invalid arguments: for key [%s] more than one of lvn "
				"(%ju), and snapshot (%ju) is not UINT64_MAX\n",
				vt->key, lvn, snapshot);
		return -1;
	}

#ifdef KP_ASSERT
	if (vt->len == 0) {  //sanity check
		/* If a version table has been added to the kvstore, then it
		 * should immediately have at least one version added to it. If
		 * it happens that all of the versions are later garbage-collected
		 * from the version table, then the vt should probably be freed
		 * as well, and we should return "key not found" long before we
		 * reach this code.
		 */
		kp_die("version table is empty, len=0; this should never happen, "
				"I think.\n");
		return -1;
	}
#endif

	/* If versioning is disabled, then we ignore the lvn/snapshot args
	 * to this function and reset them to UINT64_MAX, which will cause
	 * us to hit the else case below and just get the "current" (only)
	 * value stored in the VT.
	 */
#ifdef DISABLE_VERSIONING
	kp_die("dead code\n");
	kp_debug("DISABLE_VERSIONING: ignoring lvn (%ju) and "
			"snapshot (%ju), and just getting the (only) value from the "
			"version table\n", lvn, snapshot);
	lvn = UINT64_MAX;
	snapshot = UINT64_MAX;
#endif

	if (lvn != UINT64_MAX) {  //get local version
		kp_warn("is this code (get-by-lvn) dead? If not, should probably "
				"only be alive for local, not for master!\n");
		if (lvn > vt->ver_count - 1) {  //don't forget -1!
			kp_debug("invalid lvn (%ju) for key %s: ver_count is %ju\n",
					lvn, vt->key, vt->ver_count);
			return 2;
		}
		index = vector_search_binary(vt->vtes, offsetof(kp_vte, lvn), lvn,
				true, NULL);
		  /* true: want exact match, not range match! 
		   * (CHECK: is this right for lvn??) */
		if (index == UINT64_MAX) {  //not found
			kp_debug_gc("lvn %ju not found for key %s, must have been "
					"garbage-collected (if this is master store; if this "
					"is a local store, then why is an lvn missing?)\n",
					lvn, vt->key);  //lvn passed from user could just be bogus...
			return 2;
		}
		kp_debug("getting lvn %ju for key %s from index=%ju\n",
				lvn, vt->key, index);
		vector_get(vt->vtes, index, (void **)(&get_vte));
		*value = get_vte->value;
		*size = get_vte->size;
	} else if (snapshot != UINT64_MAX) {  //get by snapshot number
#ifdef KP_ASSERT
		if (!(vt->parent->is_master)) {
			kp_die("unexpected code path for local store: get-by-snapshot "
					"(%ju)\n", snapshot);
		}
#endif

		/* Need to check search snapshot's validity here, because range search
		 * we're about to perform assumes that last element in table is
		 * valid for all future snapshot numbers: */
		if (snapshot > max_snapshot) {
			/* Should be an application error, but warn in case it's our error... */
			kp_warn("snapshot %ju greater than current max_snapshot %ju\n",
					snapshot, max_snapshot);
			kp_debug("snapshot %ju greater than current max_snapshot %ju\n",
					snapshot, max_snapshot);
			return 2;  //return version not-found, not error...
		}
		index = vector_search_binary(vt->vtes, offsetof(kp_vte, snapshot),
				snapshot, false, kp_vte_validator);
		  /* false: want RANGE match! */
		if (index == UINT64_MAX) {  //not found
			kp_debug("snapshot %ju not found for key %s\n", snapshot, vt->key);
			return 2;
		}
		kp_debug("getting snapshot %ju for key %s from index=%ju\n",
				snapshot, vt->key, index);
		vector_get(vt->vtes, index, (void **)(&get_vte));
		*value = get_vte->value;
		*size = get_vte->size;
	} else {  //get current version
		/* Current/latest version is found at the end of the VT; use
		 * len to index to it (ver_count doesn't account for versions that
		 * have been collected). We checked above that len is greater than 0.
		 */
#ifdef KP_ASSERT
		if (vt->parent->is_master) {
			kp_die("unexpected code path for master store: get current "
					"version (neither lvn nor snapshot specified\n");
		}
		if (vt->len > vt->ver_count) {  //sanity check
			kp_die("internal error: vt->len has been set to invalid "
					"value %ju, vt->ver_count is %ju\n",
					vt->len, vt->ver_count);
		}
#endif
		kp_debug_gc("getting version at index=%ju (vt->len-1); ver_count=%ju "
				"(if ver_count > index+1, means that versions have been "
				"garbage-collected)\n", vt->len-1, vt->ver_count);
		vector_get(vt->vtes, vt->len-1, (void **)(&get_vte));
		*value = get_vte->value;
		*size = get_vte->size;
	}
#ifdef KP_ASSERT
	if ((!*value && *size != 0) || (*value && *size == 0)) {
		kp_die("*value is %p, but *size is %zu\n", *value, *size);
	}
#endif
	if (!*value) {
		kp_debug_gc("got a tombstone vte from version table; will return "
				"0 now\n");
	}

	return 0;
}

/* IMPORTANT: this function must be called with exclusive access to the
 * version table! Either by locking the vt after it has been looked up,
 * or by calling this function on a vt that has just been created.
 *
 * This function will accept normal put values (non-NULL value pointer and
 * non-zero size), or tombstone values for deletions (NULL value pointer AND
 * size == 0).
 *
 * (September 2012, while adding garbage collection: I'm pretty sure all
 * of the comments following this one are out of date; this function is
 * only called by local stores now, and should never be used by the master
 * store, I think. The master store has to call kp_vt_insert_value() to
 * ensure that the version table stays sorted by ascending end-snapshot
 * number.)
 *
 * This function is unified to work with both local and master kvstores.
 * For a local store, the new_snapshot argument must be valid; this should
 * be the local worker's current snapshot (the "start-timestamp" of the
 * current transaction. For a local store, the cr argument (pointer to
 * a commit_record) must be NULL. Then, for a local store, this 
 * function always makes a COPY of the value, because it is coming from
 * the user and should always be stored permanently inside of the store.
 *
 * For a master store, the new_snapshot argument must also be valid,
 * however it should be set to the end_snapshot of the current commit.
 * Additionally, the cr argument must point to the commit_record of the
 * key-value pair that is being inserted. Because the key-value pair is
 * already present in the commit_record, this function does not copy the
 * key for master stores!
 *
 * For both local and master stores, this function currently copies a "gvn"
 * into the new vte, but I think this is now unused and should be removed.
 * The use_nvm argument should be set appropriately for volatile vs.
 * non-volatile memory.
 *
 * This function does not perform any conflict detection. For the local
 * store, conflict detection is unnecessary; for the master store, conflict
 * detection should be performed just before calling this function (while
 * the thread has exclusive access to the locked vt!).
 *
 * For local stores, currently this function actually appends to the vector
 * of vtes, and does not overwrite any previous puts to the local store.
 * This allows local workers to view the history of their current transaction,
 * if they wish; if this is not a necessary feature, then this function could
 * probably just overwrite existing values to optimize a bit (we could
 * optimize even further by completely getting rid of the vector for local
 * stores!).
 *
 * Returns: 0 on success, -1 on error.
 */
int kp_vt_append_value(kp_vt *vt, const void *value, size_t size,
		uint64_t new_snapshot, kp_commit_record *cr, bool use_nvm)
{
	int ret;
	uint64_t new_lvn, new_ttl;
	void *copy;
	kp_vte *new_vte;
#ifdef KP_DEBUG
	kp_vte *get_vte_debug;
#endif
	bool hack_to_skip_versioning = false;
	void *old_e = NULL;  //only really used when DISABLE_VERSIONING is defined
	bool is_master = false;

#ifdef DISABLE_VERSIONING
	hack_to_skip_versioning = true;
#endif

	if (!vt || new_snapshot == UINT64_MAX) {
		kp_error("bad argument: vt=%p, new_snapshot=%ju\n", vt, new_snapshot);
		return -1;
	}

	if (!cr) {
		is_master = false;
		if (use_nvm) {
			kp_warn("use_nvm is true for local store - expected?\n");
		}
	} else {
		is_master = true;  //master store sets cr
		kp_die("master should never call this function now, right?\n");
		if (!use_nvm) {
			kp_warn("use_nvm is false for local store - expected?\n");
		}
	}

	if (value != NULL) {  //insertion
		if (!is_master) {
			kp_debug("for local store, allocating %zu bytes for copy of "
					"value\n", size);
			kp_kpalloc(&copy, size, use_nvm);
			if (!copy) {
				kp_error("kp_kpalloc(copy) failed\n");
				return -1;
			}
			kp_memcpy(copy, value, size, use_nvm);  //flushes if use_nvm
		} else {
			kp_debug("for master, just copying pointer to value for new vte; "
					"the commit record is already in place with the value.\n");
			copy = (void *)value;  //de-const!
		}
	} else {  //deletion; same for local and for master 
		kp_debug_gc("got NULL value pointer, so this is a deletion "
				"(size=%zu)\n", size);
#ifdef KP_ASSERT
		if (size != 0) {
			kp_die("deletion: value pointer is NULL, but size is non-zero!\n");
		}
#endif
		copy = NULL;
	}

	/* LVNs start from 0, so the most-recent LVN is ver_count-1. This
	 * means that for a new version, LVN should be ver_count (which has
	 * not been incremented yet; see below).
	 * Use 0 for the initial ttl; ttls will be dealt with later. */
	new_lvn = vt->ver_count;
	new_ttl = 0;

	/* Finally, create the new vte. In the normal case (versioning enabled),
	 * this vte will then be appended to the vector of vtes. In the testing
	 * case where versioning is disabled, then this vte will overwrite the
	 * existing vte in the vector.
	 *
	 * We pass the new_snapshot and cr (commit record) arguments directly
	 * to kp_vte_create(), which will copy them in without looking at
	 * them. So, in a local store the commit_record pointers of all of
	 * the vtes will be NULL; oh well. */
	ret = kp_vte_create(&new_vte, copy, size, new_lvn, new_ttl, new_snapshot,
			cr, use_nvm);
	if (ret != 0 || !new_vte) {
		kp_error("kp_vte_create failed; ret=%d, new_vte=%p\n", ret, new_vte);
		if (!is_master && copy) {  //local store and not a deletion:
			kp_free(&copy, use_nvm);
		}
		return -1;
	}

	/* We want to run this code in the normal case (DISABLE_VERSIONING is not
	 * defined), or in the DISABLE_VERSIONING case when this is the _first_
	 * time we're putting to this key. */
	if (!hack_to_skip_versioning || vt->len == 0) {
		/* Now, append the new vte to the vector of vtes.
		 * IMPORTANT: in the future, if we try to allow simultaneous
		 * merges, then we won't be able to just append here; we'll have
		 * to "insert" in the proper order (log n operation), because
		 * we can't be sure that somebody with a snapshot number that's
		 * after ours didn't insert before us. */
		ret = vector_append(vt->vtes, (void *)new_vte, NULL);
		if (ret != 0) {
			/* Destroy the vte; pass false for use_nvm. kp_vte_destroy()
			 * will only free its value if the pointer is non-NULL, so
			 * if we just allocated a tombstone vte, no problem. However,
			 * for the master store we didn't copy the value, so set it
			 * to NULL first. */
			kp_error("vector_append(vtes) returned error=%d\n", ret);
			if (is_master) {
				new_vte->value = NULL;
			}
			kp_vte_destroy(&new_vte, use_nvm);
			return -1;
		}

		/* Increment counts only after value has been successfully added.
		 * Deletions count as a version. */
		vt->ver_count++;
		vt->len++;
		//kp_vt_sanity_check_lengths(vt);
#ifdef KP_DEBUG
		vector_get(vt->vtes, vt->len-1, (void **)(&get_vte_debug));
		kp_debug("Added new version to VT for key %s: size=%zu, value=%p, "
				"lvn=%ju, snapshot=%ju, ttl=%ju; vt len=%ju, "
				"ver_count=%ju\n", vt->key, get_vte_debug->size,
				get_vte_debug->value, get_vte_debug->lvn,
				get_vte_debug->snapshot, (int64_t)(get_vte_debug->ttl),
				vt->len, vt->ver_count);
#endif
	} else {
		if (!hack_to_skip_versioning) {  //sanity check
			kp_die("shouldn't have reached here\n");
		}

		kp_die("(temporarily?) disabled the code path for skipping "
				"versioning... old_e=%p\n", old_e);
	}

	if (is_master) {
		kp_debug("TODO: need to set TTL value of previous version here for "
				"master store??\n");
	}

	return 0;
}

/* Takes two pointers to version table entries (which come from a version
 * table vector) and returns -1 if vteptr1's end_snapshot is less than
 * vteptr2's, returns 1 if vteptr1's end_snapshot is greater than vteptr2's.
 * If vteptr1 and vteptr2 have the same end_snapshot, this function will
 * return 0, but it may abort before returning because this had better
 * not happen in our current code. */
int kp_end_snapshot_comparator(const void *vteptr1, const void *vteptr2)
{
	int retval;
	const kp_vte *vte1;
	const kp_vte *vte2;
	kp_commit_record *cr1;
	kp_commit_record *cr2;

	/* Each vte points to the commit record it was created from: */
	vte1 = (kp_vte *)vteptr1;
	vte2 = (kp_vte *)vteptr2;
#ifdef KP_ASSERT
	if (!vte1 || !vte2) {
		kp_die("got a null vte1=%p or vte2=%p\n", vte1, vte2);
	}
#endif
	cr1 = vte1->cr; 
	cr2 = vte2->cr;
#ifdef KP_ASSERT
	if (!cr1 || !cr2) {
		kp_die("got a null cr1=%p or cr2=%p\n", cr1, cr2);
	}
#endif
	if (cr1->end_snapshot < cr2->end_snapshot) {
		retval = -1;
	} else if (cr1->end_snapshot == cr2->end_snapshot) {
		retval = 0;
	} else {
		retval = 1;
	}
	kp_debug2("cr1->end_snapshot=%ju, cr2->end_snapshot=%ju; returning"
			" %d\n", cr1->end_snapshot, cr2->end_snapshot, retval);
	return retval;
}

#if 0
/* NOTE: it looks like this function will only be useful when new_state
 * is ABORTED; in other cases, we need to read-modify-write the state,
 * not just modify-write it.
 *
 * Performs the following steps:
 *   LOCKS the commit record's state_lock
 *   Sets the commit record's state to new_state
 *   If conflict_key is non-NULL, sets the cr's conflict_key to it (the
 *     cr's conflict_key is untouched if the argument is NULL)
 *   Flushes the commit record's state
 *   Broadcasts to waiters on the commit record's state condition variable
 *   UNLOCKS the commit record's state_lock
 * Returns: 0 on success, -1 on error (only on NULL arguments).
 */
int kp_set_cr_state(kp_commit_record *cr, commit_state new_state,
		const char *conflict_key, bool use_nvm)
{
	int ret;
	if (!cr) {
		kp_error("got null argument: cr=%p\n", cr);
		return -1;
	}

	kp_mutex_lock("cr->state_lock", cr->state_lock);
	cr->state = new_state;
	if (conflict_key) {
		/* BUG: isn't it dangerous to just copy the pointer to the key
		 * here? The vt could be removed, and then somebody could try
		 * to read the dangling conflict_key pointer... */
		cr->conflict_key = conflict_key;
	}
	kp_flush_range(&(cr->state), sizeof(commit_state), use_nvm);
	  /* CHECK: Do we have to flush here? Not sure... */
	ret = pthread_cond_broadcast(cr->state_cond);
	  /* guess we could have multiple threads waiting on a
	   * commit record's state, especially when we allow for
	   * asynchronous commits (and workers can request to wait
	   * on the state of a particular commit). So, we broadcast
	   * rather than signal here, which seems safer... */
	if (ret != 0) {
		kp_error("pthread_cond_broadcast() returned error=%d; ignoring it\n",
				ret);
	}
	kp_debug2("Got commit_record with range [%ju, %ju], set its state "
			"to %s and broadcasted to all waiters\n", cr->begin_snapshot,
			cr->end_snapshot, commit_state_to_string(cr->state));
	kp_mutex_unlock("cr->state_lock", cr->state_lock);

	return 0;
}
#endif

/* IMPORTANT: this function must be called with exclusive access to the
 * version table! Either by locking the vt after it has been looked up,
 * or by calling this function on a vt that has just been created.
 *
 * This function should only be called by the master store - it is intended
 * to insert a value into a version table that is kept sorted by ascending
 * end-snapshot number. Local stores call kp_vt_append_value(), which is
 * much simpler than this function.
 *
 * For the master store, the new_snapshot argument should be set to
 * the end_snapshot of the current commit. The cr argument must point
 * to the commit_record of the key-value pair that is being inserted.
 * Because the key-value pair is already present in the commit_record,
 * this function does not copy the key!
 *
 * Note that this function's return code is not impacted when it causes
 * conflicts for OTHER commit records; we'll update those commit records
 * to ABORTED and signal them, but our caller doesn't care unless there
 * was an abort for our own commit.
 *
 * Returns: 0 on success; -1 on error; 1 if we encountered a conflict
 * for the current commit. */
int kp_vt_insert_value(kp_vt *vt, const void *value, size_t size,
		uint64_t new_snapshot, kp_commit_record *cr, bool detect_conflicts,
		bool use_nvm)
{
	int ret, retval;
	uint64_t new_lvn, new_ttl, idx, count, i;
	void *copy;
	kp_vte *new_vte;
	kp_vte *get_vte;
#ifdef KP_DEBUG2
	kp_vte *get_vte_debug;
#endif
	kp_commit_record *earlier_cr;
	kp_commit_record *later_cr;
	commit_state new_state;

	if (!vt || !cr || new_snapshot == UINT64_MAX) {
		kp_error("bad argument: vt=%p, cr=%p, new_snapshot=%ju\n", vt, cr,
				new_snapshot);
		return -1;
	}
	if (!use_nvm) {
		kp_warn("use_nvm is false for master store - expected?\n");
	}
#ifdef KP_ASSERT
	if (new_snapshot != cr->end_snapshot) {
		kp_die("unexpected: new_snapshot=%ju, but cr->end_snapshot=%ju\n",
				new_snapshot, cr->end_snapshot);
	}
	if ((!value && size != 0) || (value && size == 0)) {
		kp_die("value is %p, but size is %zu\n", value, size);
	}
#endif

	kp_debug2("entered: value=%p, size=%zu, new_snapshot=%ju\n",
			value, size, new_snapshot);
	if (value) {  //insertion
		kp_debug("for master, just copying pointer to value for new vte; "
				"the commit record is already in place with the value.\n");
		copy = (void *)value;  //de-const!
	} else {              //deletion
		kp_debug_gc("got NULL value pointer, so this is a tombstone value "
				"for deleting this key/vt\n");
		copy = NULL;
	}

	/* LVNs start from 0, so the most-recent LVN is ver_count-1. This
	 * means that for a new version, LVN should be ver_count (which has
	 * not been incremented yet; see below).
	 * Use 0 for the initial ttl; ttls will be dealt with later... */
	new_lvn = vt->ver_count;
	new_ttl = 0;

	/* Create the new vte. We pass the new_snapshot and cr (commit record)
	 * arguments directly to kp_vte_create(), which will copy them in
	 * without looking at them. */
	ret = kp_vte_create(&new_vte, copy, size, new_lvn, new_ttl, new_snapshot,
			cr, use_nvm);
	if (ret != 0 || !new_vte) {
		kp_error("kp_vte_create failed; ret=%d, new_vte=%p\n", ret, new_vte);
		return -1;
	}

	/* Now insert the new vte into the vector of vtes. We hold the lock
	 * on the vt, so we know that the vector can't be changed by anybody
	 * else right now. We pass a comparator function that inserts the
	 * vte into the vector so that is kept sorted by ascending
	 * end_snapshot number.
	 *
	 * The insert function returns to us the index that it inserted the
	 * vte into. We compare this against the new length of the vt to know
	 * if we were appended to the end or not; if we were not, then this
	 * means that we are DEFINITELY causing a conflict for somebody else
	 * (one OR MORE other commits)!!
	 * ("Proof": the other commit's end_snapshot is definitely greater
	 *  than our end_snapshot. Previously we enforced (or should have
	 *  enforced) that no other transaction can get a begin_snapshot greater
	 *  than the greatest committed snapshot number, so we know that the
	 *  other commit's begin_snapshot is less than our end_snapshot (because
	 *  we haven't committed yet, obviously). Therefore, we are writing
	 *  inside of their snapshot range, and they have a conflict!)
	 * When we cause a conflict for somebody else, we perform a failed
	 * transition on their commit record, which will cause its state to
	 * be set to ABORTED. The transition function also signals the other
	 * commit record's condition variable, and so on. This is all that we
	 * have to do; this transition doesn't impact our own commit (we'll
	 * check for conflicts of our own below).
	 *
	 * Even if detect_conflicts is false, we still do a vector_insert()
	 * rather than a vector_append() - we may be not detecting conflicts,
	 * but we're still allowing concurrent merges, so we need to make
	 * sure that the vector remains sorted.
	 *
	 * The vector_insert() flushes the new element pointer and its new count
	 * internally, so no flushing here.
	 */
	idx = vector_insert(vt->vtes, (void *)new_vte,
			kp_end_snapshot_comparator);
	if (idx == UINT64_MAX) {
		/* Destroy the vte. kp_vte_destroy() will only free its value
		 * if the pointer is non-NULL; since we didn't copy the value
		 * earlier in this function, we must set it to NULL here. */
		kp_error("vector_insert(vt->vtes) returned error\n");
		new_vte->value = NULL;
		kp_vte_destroy(&new_vte, use_nvm);
		return -1;
	}

	count = vector_count(vt->vtes);
	kp_debug2("vector_insert() returned our idx=%ju; vector count=%ju\n",
			idx, count);
#ifdef KP_ASSERT
	if (count == UINT64_MAX || count < idx) {
		kp_die("invalid count=%ju and/or idx=%ju\n", count, idx);
	}
#endif

	/* Default to success! (some code below assumes this). */
	retval = 0;

	/* Skip all of this if detect_conflicts is false!! */
	if (detect_conflicts && count > 1) {
		for (i = idx+1; i < count; i++) {  //scan forward for conflicts:
			kp_debug2("insert caused a conflict for vte at index=%ju in "
					"vt with key=%s\n", i, vt->key);
			vector_get(vt->vtes, i, (void **)&get_vte);
			  /* get_vte is local, no flush. */
#ifdef KP_ASSERT
			if (!get_vte) {
				kp_die("got NULL get_vte!\n");
			}
#endif
			later_cr = get_vte->cr;
#ifdef KP_ASSERT
			if (!later_cr) {
				kp_die("null later_cr\n");
			}
#endif
			/* Skip this work if somebody else already aborted this commit.
			 * ABORTED is a final state, and it's ok to perform the state
			 * transition to ABORTED twice if somebody else sneaks in here
			 * in-between when we check the state and when we lock it in
			 * the transition function. */
			kp_debug2("in addition to checking later_cr->state != ABORTED, "
					"I think this could also check && != WAIT_TO_ABORT. "
					"This would probably have a very tiny impact...\n");
			if (later_cr->state != ABORTED) {
				/* Pass false for prev_is_finalized and false for success -
				 * we're failing later_cr and causing it to abort. Pass
				 * false for is_owner too. */
				new_state = kp_commit_state_transition(later_cr, false,
						false, false, detect_conflicts, use_nvm);
				if (new_state != ABORTED && new_state != WAIT_TO_ABORT) {
					//kp_error("kp_commit_state_transition() returned "
					kp_die("kp_commit_state_transition() returned "
							"unexpected state=%s. We'll ignore this "
							"error in the other later_cr and try to "
							"continue\n",
							commit_state_to_string(new_state));
				}
			}
			/* Loop again for the next commit in the vector that we're
			 * conflicting. */
		}
		kp_debug2("done looping _forward_ through vte and aborting other "
				"commit records due to conflicts (%ju times)\n",
				count - 1 - idx);
	
		/* Now, we check for conflicts in our own commit. We know that our
		 * vte was inserted into the version table in sorted end_snapshot order.
		 * We have a conflict if there is some other entry whose end_snapshot
		 * is less than ours but greater than our begin_snapshot (it's ok if
		 * their end_snapshot _equals_ our begin_snapshot - this means that we
		 * were able to read their snapshot during our own). So, we start from
		 * the index that the vector_insert() returned to us and scan backwards
		 * until we find a conflict, or until we find a vte whose end_snapshot
		 * is less than or equal to our begin_snapshot (or until we hit the
		 * beginning of the vector). If we don't find a conflict, then we can
		 * just return success. If we do hit a conflict, then we'll update our
		 * own commit record's state before returning (seems like we could
		 * also make the caller do it; not sure that it makes a difference).
		 */
		if (idx > 0) {
			/* i is unsigned!!! So, this for loop assumes that UINT64_MAX ==
			 * i-1; is this a safe assumption??? CHECK.... */
			for (i = idx-1; i != UINT64_MAX; i--) {  //don't go off the beginning of the vector!
				kp_debug2("checking if vte at index=%ju conflicts with our "
						"commit into vt at index=%ju (key=%s)\n", i, idx,
						vt->key);
				vector_get(vt->vtes, i, (void **)&get_vte);
#ifdef KP_ASSERT
				if (!get_vte) {
					kp_die("got NULL get_vte!\n");
				}
				if (!get_vte->cr) {
					kp_die("null earlier_cr\n");
				}
#endif

				earlier_cr = get_vte->cr;
				if (earlier_cr->end_snapshot <= cr->begin_snapshot) {
					kp_debug2("found first non-conflicting commit record: "
							"earlier cr's end_snapshot=%ju, but our cr's "
							"begin_snapshot=%ju, so nobody put into this "
							"key during our transaction\n",
							earlier_cr->end_snapshot, cr->begin_snapshot);
					break;
				} else {  //conflict!!!
					/* Version 3.1 enhancement: if we find a vte with an
					 * end_snapshot that's within our snapshot range, we
					 * also check that this vte is not ABORTED or 
					 * WAIT_TO_ABORT - if it is, then it shouldn't conflict
					 * with us. We'll still keep looping backwards.
					 * 
					 * Before making this enhancement, we may have had some
					 * "false aborts," but we were still meeting the definition
					 * of snapshot isolation; I think this is ok, we were just
					 * "less optimistic." However, after making this change,
					 * I saw that in a test_local_master run with just 4
					 * threads and 100 commits each, the number of successful
					 * commits per run jumped up from ~10 to >100!! Apparently
					 * we had a LOT of false aborts, yikes.
					 *
					 * We don't need to lock when checking the state for
					 * ABORTED or WAIT_TO_ABORT, because if the commit is in
					 * either of those states already, then we definitely
					 * know that the commit is not going to succeed. */
					if (earlier_cr->state == ABORTED ||
						earlier_cr->state == WAIT_TO_ABORT) {
						kp_debug2("earlier_cr is already in state %s, so "
								"it doesn't cause a conflict for us; looping "
								"again to check back further\n",
								commit_state_to_string(earlier_cr->state));
						/* This is the only code path that actually causes
						 * the loop to happen again... confusing, oops. */
					} else {
						/* Don't do anything yet - in kp_merge_commit_record() we're
						 * always going to update our state, so we just return
						 * "conflict" here, break out of the conflict-checking loop
						 * and update the commit record's state a little later.
						 */
						kp_debug2("got a conflict with previous commit in this vt! "
								"earlier cr's end_snapshot=%ju, which is within "
								"our current cr's snapshot range (%ju,%ju]; some "
								"other commit wrote this key after we started our "
								"commit's transaction but before we committed!\n",
								earlier_cr->end_snapshot, cr->begin_snapshot,
								cr->end_snapshot);
						retval = 1;
						break;
					}
				}
				if (i == 0) {  //for safety, because i is unsigned...
					kp_debug2("reached beginning of vector without finding "
							"any conflicts, so we'll insert here!\n");
					retval = 0;  //for safety...
					break;
				}
			}
		} else {
			kp_debug2("idx is 0 already, so we inserted into the very "
					"beginning of the vector (even though we weren't the "
					"first insertion); skipping scan back through the "
					"vector to check for already-put values that conflict "
					"with ours\n");
		}
	} else {
		if (!detect_conflicts) {
			kp_debug2("detect_conflicts is false, so we skipped the checking "
					"forwards and backwards in the VT for conflicts!\n");
		} else {
			kp_debug2("count is 1, so skipping conflict detection because "
					"we're the first put into this VT.\n");
		}
		retval = 0;  //for safety...
	}

	/* Increment counts only after value has been successfully added.
	 * Deletions count as a version. We increment these counts here even
	 * if there was a conflict on our insert, because the vte is still
	 * left in the table to be GC'd later. */
	PM_EQU((vt->ver_count), (vt->ver_count+1));
	PM_EQU((vt->len), (vt->len+1));
	kp_flush_range((void *)vt, sizeof(kp_vt), use_nvm);
	kp_todo("we could try to just flush ver_count and len here, and not "
			"the whole version table struct, but this is somewhat fragile; "
			"just leave it like this for now\n");

	//kp_vt_sanity_check_lengths(vt);
#ifdef KP_DEBUG2
	vector_get(vt->vtes, idx, (void **)(&get_vte_debug));
	kp_debug2("Inserted new version to VT for key %s: size=%zu, value=%p, "
			"lvn=%ju, snapshot=%ju, ttl=%ju; vt len=%ju, "
			"ver_count=%ju\n", vt->key, get_vte_debug->size,
			get_vte_debug->value, get_vte_debug->lvn,
			get_vte_debug->snapshot, (int64_t)(get_vte_debug->ttl),
			vt->len, vt->ver_count);
#endif

	kp_debug2("returning %d (0 for no conflicts, 1 for conflict occurred)\n",
			retval);
	return retval;
}

/* Finds the item in the version table corresponding to the indicated gvn,
 * and decrements its time-to-live counter. If the ttl hits 0, this function
 * will free that version of the value. If this function frees the last
 * version in the version table, it DOES NOT free the entire version table!
 * Instead, the caller should check the version table's length after this
 * function returns (even if it returns 0 (could have been a tombstone value)),
 * and free the version table if it is now empty.
 * IMPORTANT: the version table's lock should already be held when this
 * function is called!!!!!!!!
 * Returns: the number of DATA bytes collected (may be 0), or UINT64_MAX
 * on error.
 */
uint64_t kp_vt_decrement_ttl(kp_vt *vt, uint64_t gvn)
{
	uint64_t idx, ret64;
	uint64_t bytes_collected;
	int64_t ttl;
	kp_vte *get_vte;

	if (!vt) {
		kp_error("vt is null\n");
		return UINT64_MAX;
	}
	//kp_vt_sanity_check_lengths(vt);

	/* Search the version table to find the entry with the gvn that matches
	 * the gvn from the changeset log EXACTLY (last arg to search function
	 * is true). Because the changeset log contains an exact record of the
	 * gvns that were changed, we should never fail to find an entry with
	 * that matches the gvn.
	 */
	kp_die("this code is dead - gvn is removed from kp_vtes!");
	//idx = vector_search_binary(vt->vtes, offsetof(kp_vte, gvn), gvn, true);
	idx = UINT64_MAX;
	if (idx == UINT64_MAX) {  //not found: should never happen
		kp_error("failed to find entry in vt that matches gvn=%ju\n", gvn);
		return UINT64_MAX;
	}
	kp_debug("gvn=%ju for vt->key=%s is at idx=%ju\n", gvn, vt->key, idx);

	/* Decrement the entry's ttl. See kp_vt_add_value() for commentary about
	 * how the ttl is actually set/used...
	 */
	vector_get(vt->vtes, idx, (void **)(&get_vte));
	if (!get_vte) {
		kp_error("vector_get(vtes, %ju) failed, got NULL vte\n", idx);
		return UINT64_MAX;
	}
	ttl = (int64_t)(get_vte->ttl);
	ttl--;  //important: decrement in-place, ttl is used later in this function
	get_vte->ttl = (uint64_t)ttl;
	  //perform SIGNED arithmetic, but store as unsigned
#ifdef KP_DEBUG
	kp_debug("decremented ttl for gvn %ju (idx %ju) of vt %s to %jd\n",
			gvn, idx, vt->key, ttl);
	if (ttl < 0) {
		kp_debug("ttl is negative, so this version had better be still "
				"referenced by the current/active checkpoint! (its snapshot "
				"is %ju)\n", get_vte->snapshot);
	}
#endif

	/* If the ttl value hits 0 when we decrement it, it means that there are
	 * no more uncollected checkpoints that refer to this version, and thus
	 * we can delete it:
	 */
	if (ttl == 0) {
		kp_debug("ttl hit 0, collecting this version!\n");
		ret64 = kp_vt_delete_version(vt, idx);  //decrements vt->len
		if (ret64 == UINT64_MAX) {
			kp_error("kp_vt_delete_version returned error\n");
			return UINT64_MAX;
		}
		bytes_collected = ret64;
		kp_debug("kp_vt_delete_version returned %ju bytes freed; "
				"vt->len=%ju\n", bytes_collected, vt->len);

		if (vt->len == 0) {
			//todo: change this warning to a debug...
			kp_warn("vt->len hit 0; not freeing vt here, caller should "
					"also check this condition and free vt!\n");
		}
	} else {  //could be ttl > 0 OR ttl < 0; see kp_vt_add_value()
		kp_debug("not collecting this version because some uncollected "
				"checkpoints still refer to it (ttl=%jd)!\n", ttl);
		bytes_collected = 0;
	}

	return bytes_collected;
}

///* Frees a version table. There are two arguments that control which data
// * structures are freed. If vtes_already_freed is set to true, then this
// * function will ignore the contents of the vtes vector that the version
// * table contains, and will just destroy the vector (vtes_already_freed
// * should be set to true for a local store after a commit, for example).
// * If vtes_already_freed is set to false, then this function will iterate
// * over the vector of vtes and call kp_vte_destroy() on each one. If the
// * free_values argument is true, then kp_vte_destroy() will free the value
// * that the vte points to...
// */
/* Frees a version table. If the second argument is true, then this function
 * will assume that the vtes for the version table have already been freed
 * (i.e. during a local commit iteration), or no vtes have ever been added.
 * If vtes_already_freed is false, then this function will iterate over
 * the entire vector and free all of the vtes; however, this function will
 * NEVER free the actual VALUES that the vtes point to!
 *   Why not? Because in the master store, the values are "owned" by the
 *   commit record, not by the version table. In local stores, the version
 *   tables DO own the values, but in most cases the values are freed when
 *   the local store iterates over all of its vtes during a commit operation.
 *
 *   *** I think this logic works in every case EXCEPT for when a LOCAL
 *   *** store calls kp_vt_destroy() with vtes_already_freed set to false;
 *   *** in this case, we might be leaking memory...
 * 
 * This function does not check that the vt is not in use by somebody else,
 * so think carefully about where it is called. */
void kp_vt_destroy(kp_vt **vt, bool vtes_already_freed)
{
	uint64_t i, count;
	kp_vte *get_vte;
	bool use_nvm;

	if (!vt) {
		kp_error("got NULL vt pointer\n");
		return;
	}

	if ((*vt)->parent) {
		/* Follow several back-pointers to get to use_nvm... not hacky
		 * at all...
		 */
		use_nvm = (*vt)->parent->use_nvm;
	} else {
		/* Assume true to be on the safe side. This case is probably
		 * unexpected anyway.
		 */
		use_nvm = true;
	}

	kp_debug("need to update this for use_nvm=true; any state changing or "
			"explicit flushing that needs to happen??\n");

	if (*vt) {
		kp_debug("destroying version table for key %s\n", (*vt)->key);
		(*vt)->state = STATE_DEAD;
		kp_flush_range(&((*vt)->state), sizeof(ds_state), use_nvm);

		if ((*vt)->vtes) {
			if (!vtes_already_freed) {
				/* Loop through the vector of vtes and free each one. If we
				 * fail during this loop and re-start this function, we may
				 * get NULL vte pointers back (because kp_vte_destroy() sets
				 * the vte pointer to NULL after freeing), so we skip freeing
				 * those. Freeing the vte will free the value that it points at!
				 *
				 * This step could potentially be optimized (don't have to call
				 * vector_get() every iteration) and beautified by creating a
				 * vector_map() function, or adding a parameter on vector_free()
				 * that takes a function to call on each value pointed at.
				 */
				count = vector_count((*vt)->vtes);
				kp_debug2("length of vtes vector is %ju, looping through "
						"these and freeing them\n", count);
				for (i = 0; i < count; i++) {
					vector_get((*vt)->vtes, i, (void **)(&get_vte));
					if (!get_vte) {
						kp_warn("got NULL vte from vt->vtes; this should only "
								"happen if we got interrupted and re-started "
								"this function. Not freeing anything and looping "
								"again...\n");
						continue;  //loop again...
					}
					/* Set value to NULL to ensure we don't free it - see
					 * notes at top of this function!
					 */
					get_vte->value = NULL;
					kp_vte_destroy(&get_vte, use_nvm);
#ifdef KP_ASSERT
					if (get_vte) {
						kp_die("kp_free() in kp_vte_destroy() did not set get_vte "
								"to NULL as expected!\n");
					}
#endif
				}
				kp_debug("destroyed all of the vtes, now destroying the vector\n");
			} else {
				kp_debug2("vtes_already_freed is true, so skipped iterating "
						"over vtes vector. Now destroying the vector\n");
			}
			kp_debug("TODO: add use_nvm parameter, etc. to vector_destroy()!\n");
			vector_destroy(&((*vt)->vtes));  //sets vt->vtes to NULL
#ifdef KP_ASSERT
			if ((*vt)->vtes) {
				kp_die("vector_destroy did not set (*vt)->vtes to NULL as "
						"expected!\n");
			}
#endif
		}
		kp_debug("destroying the vt's lock\n");
		if ((*vt)->lock) {
			kp_mutex_destroy("(*vt)->lock", &((*vt)->lock));			
		}

		/* We don't free the vt's key here - in version 3.0, the key is now
		 * "owned" by the kp_ht_entry. */
		kp_free((void **)vt, use_nvm);
	}
}

/* Does a hash table lookup to get the version table for the specified
 * key. IMPORTANT: when this function returns successfully, the mutex
 * on the version table has been locked, and the caller must later
 * call kp_vt_unlock() to release it.
 *   TODO: this is kind of dumb; probably better coding form to NOT
 *     lock in this function and require the matching unlock to happen
 *     in some other function.
 *
 * The caller of this function must provide it with a pointer to a kp_ht_entry
 * that will be used for lookup in the hash table. This lookup entry should
 * have its key member set to point to the null-terminated key string used
 * for lookup. The lookup entry can be set by using either kp_ht_entry_set()
 * or kp_ht_entry_create(), or it can be set directly. We do not allocate the
 * lookup entry in this function itself because this would cause a non-trivial
 * performance overhead on every lookup operation; whenever possible, the
 * caller of this function should keep a pre-allocated lookup entry available
 * and  kp_ht_entry_set() it (or directly set it) before passing it to us.
 *
 * This function is called by both local and master stores! We don't flush
 * the pointer to the looked-up vt or anything though, because it's just a
 * lookup; we don't care if we lose the pointer on a failure.
 *
 * Returns: 0 on success, 1 if not found, and -1 on error. On success,
 * *lookup_vt is set to point to the found version table; otherwise,
 * *lookup_vt is untouched. If 0 is returned, the versiontable is guaranteed
 * to be locked; if anything else is returned, it is guaranteed that this
 * function did not lock any version tables.
 */
int kp_lookup_versiontable(kp_kvstore *kv, const kp_ht_entry *lookup_entry,
		kp_vt **lookup_vt)
{
	kp_vt *vt;
	kp_ht_entry *found_entry;
	const void *retptr;

	if (!kv || !lookup_entry || !lookup_vt) {
		kp_error("Got a NULL pointer argument! (kv=%p, lookup_entry=%p, "
				"lookup_vt=%p)\n", kv, lookup_entry, lookup_vt);
		return -1;
	}

	kp_debug("using lookup_entry passed by caller: key=%s, vt=%p (should "
			"be null)\n", lookup_entry->key, lookup_entry->vt);

	/* We pass the kp_ht_lock_vt function to hash_lookup as a callback
	 * function to be called on the looked-up version table, WHILE THE
	 * READ-LOCK IS STILL HELD! This ensures that in-between the lookup
	 * and the vt lock, no other threads can remove the vt / entry from
	 * the hash table. So, after this function returns correctly, the
	 * vt that is returned will be locked. */
	retptr = hash_lookup(kv->ht, (const void *)lookup_entry, kp_ht_lock_vt);
	found_entry = (kp_ht_entry *)retptr;  //just a pointer copy
	if (found_entry == NULL) {  //vt not found:
		kp_debug("hash_lookup() returned not found for key=%s; returning 1\n",
				lookup_entry->key);
		return 1;  //not-found
	} else {
		/* If we reach this code, then found_entry points to the found entry.
		 * Additionally, because of the kp_ht_lock_vt() callback function,
		 * the vt that the entry points to is already locked! */
		vt = found_entry->vt;
#ifdef KP_ASSERT
		if (vt == NULL) {
			kp_die("got NULL vt from looked-up kp_ht_entry (key=%s)\n",
					found_entry->key);
		}
		if (strcmp(found_entry->key, lookup_entry->key)) {
			kp_die("found_entry->key (%s) doesn't match lookup_entry->key "
					"(%s); found_entry=%p, lookup_entry=%p\n",
					found_entry->key, lookup_entry->key,
					found_entry, lookup_entry);
		}
#endif
		kp_debug("hash_lookup(key=%s) returned a kp_ht_entry: key=%s, "
				"vt->key=%s (these keys should all match, but their "
				"pointers may not: %p %p %p)\n",
				lookup_entry->key, found_entry->key, vt->key,
				lookup_entry->key, found_entry->key, vt->key);

		/* FAIL up to here: nothing to do, we haven't allocated anything
		 * and have only modified local variables. */

		/* Set the pointer to the looked-up version table. We don't need
		 * to flush it because even for the master, we don't really need
		 * this to be durable / reachable (in fact it's a local variable
		 * in kp_put_master()); it's just a lookup. */
		*lookup_vt = vt;

		/* FAIL here: we've set the caller's *lookup_vt pointer, but the
		 * caller should ignore the value we put there if this function
		 * never actually returned. */

		/* Are there any other possible race conditions here that don't
		 * involve deletions? Don't really think so...
		 */
		kp_debug("version table for lookup key %s was found, internal key=%s, "
				"now locked\n", lookup_entry->key, (*lookup_vt)->key);

		return 0;  //found
	}

	/* FAIL here: nothing to do; we've only set the caller's *lookup_vt
	 * pointer, which doesn't have any impact on recovery. */
	kp_die("unreachable code\n");
	return -1;
}

void kp_kvstore_destroy_internal(kp_kvstore **kv, bool free_values)
{
	if (!kv) {
		kp_error("kv is null\n");
	}

	kp_debug2("skipping kvstore destroy...\n");
	kp_todo("don't skip kvstore destroy!!\n");
	return;

	if (*kv) {
		(*kv)->state = STATE_DEAD;
		kp_flush_range(&((*kv)->state), sizeof(ds_state), (*kv)->use_nvm);

		kp_die("need to update kp_ht_entry_free_callback(), which is called "
				"when we call hash_free() here!\n");
		if ((*kv)->ht) {
			hash_free(&((*kv)->ht));  //calls kp_ht_entry_free_callback()
			kp_debug("hash_free((*kv)->ht) completed\n");
		}
		if ((*kv)->gc) {  //will be NULL for local store
			kp_gc_destroy(&((*kv)->gc));
		}
		if ((*kv)->commit_log) {
			vector_destroy(&((*kv)->commit_log));
		}
		if ((*kv)->rwlock) {
			kp_rwlock_destroy("(*kv)->rwlock", &((*kv)->rwlock));
		}
		if ((*kv)->snapshot_lock) {
			kp_mutex_destroy("(*kv)->snapshot_lock", &((*kv)->snapshot_lock));
		}

		kp_free((void **)kv, (*kv)->use_nvm);
		kp_debug("freed kv\n");
	}
}

/* NOTE that because this function is "internal" and is only called by
 * other kp_kvstore functions, it does not concern itself with nvm issues;
 * the caller of this function should do any flushing, etc. if necessary.
 * This function just finds and sets pointers to internal memory.
 *
 * Looks up the version table for the key in the store and gets the
 * right version of the value. To get the "current" version, all of the
 * lvn, gvn and snapshot args should be set to UINT64_MAX. Otherwise, ONE
 * of the lvn/gvn/snapshot should be set to a value that is not UINT64_MAX,
 * otherwise this function will return an error.
 * The caller must pass an already-allocated kp_ht_entry to this function
 * for the hash table lookup: lookup_entry's key should be set to the key,
 * and its vt should be set to NULL.
 * This function does NOT make a copy of the value that it looks up; the
 * returned pointer points to memory _internal_ to the KV store!
 * Note that the value pointer that is returned may be NULL, indicating
 * a DELETED value!
 * Returns: If the key is found, 0 is returned, the value argument
 * is set to point to the new copy of the value, and the size argument
 * is set to the size of the value. If the key is not found in the
 * store, 1 is returned. If the key was found but the specified version
 * was not, 2 is returned. If an error occurred, -1 is returned.
 */
int kp_get_internal(kp_kvstore *kv, const kp_ht_entry *lookup_entry,
		uint64_t lvn, uint64_t snapshot, const void **value,
		size_t *size)
{
	kp_vt *vt;
	int ret;

	if (!kv || !lookup_entry || !lookup_entry->key) {
		kp_error("one or more NULL pointer arguments (kv=%p, lookup_entry=%p)\n",
				kv, lookup_entry);
		return -1;
	}
	if (strlen(lookup_entry->key) < 1) {
		kp_error("key has no length!\n");
		return -1;
	}

	/* For version 3.0, we don't need to take any locks on the entire kvstore
	 * (hash table) while performing gets! See kp_put_local() and
	 * kp_put_master() for explanations of how/why. */

	/* Lookup the VT, using the lookup_entry passed to us by the caller.
	 * IMPORTANT: if the lookup succeeds, then the version table will be
	 * returned to us with the lock taken! */
	vt = NULL;  //not necessary, but sanity-checking
	ret = kp_lookup_versiontable(kv, lookup_entry, &vt);
	if (ret < 0) {
		kp_error("kp_lookup_versiontable() returned error: %d\n", ret);
		return -1;
	} else if (ret == 1) {
		kp_debug("version table for key %s not found\n", lookup_entry->key);
		return 1;  //key not found
	} else if (ret != 0) {
		kp_die("unexpected return value from kp_lookup_versiontable(): %d\n",
				ret);
	}
#ifdef KP_ASSERT
	if (!vt) {
		kp_die("kp_lookup_versiontable() returned success, but NULL vt!\n");
	}
#endif

	/* Now that we've found the right version table (and locked it), search
	 * for the right version. The current snapshot number (kv->global_snapshot)
	 * is the max that can be searched for. After the search is complete, we
	 * can unlock the version table and allow other put-ers and get-ers to
	 * access it.
	 */
	kp_debug("calling kp_vt_get_value() with lvn=%ju, "
			"snapshot=%ju on vt with key=%s\n", lvn, snapshot, vt->key);
	ret = kp_vt_get_value(vt, lvn, snapshot, value, size, kv->global_snapshot);
	  /* Note that kp_vt_get_value() doesn't do anything related to nvm.
	   * kv->global_snapshot is only modified in kp_increment_snapshot_mark_in_use();
	   * doesn't really matter if it's one or the other here, just need to
	   * make sure that snapshot passed by user is not out of range. */

//	/* See:
//	 *   http://en.wikipedia.org/wiki/Memory_barrier
//	 *   http://en.wikipedia.org/wiki/Memory_ordering#Compiler_memory_barrier
//	 *   http://lxr.linux.no/linux+v2.6.31/include/linux/compiler-gcc.h#L12
//	 *   http://answers.google.com/answers/threadview/id/408843.html
//	 */
//	asm volatile ("mfence");
//	asm volatile("" ::: "memory");
	kp_debug_lock("%s: unlocking version table\n", vt->key);
	kp_mutex_unlock("vt->lock", vt->lock);
//	asm volatile("" ::: "memory");
//	asm volatile ("mfence");
	//TODO: is releasing the lock before making the copy (in the function
	//  that calls this one) a very bad idea??? Seems like it.

	if (ret < 0) {
		kp_error("kp_vt_get_value() returned error: %d\n", ret);
		return -1;
	} else if (ret == 2) {
		kp_debug("specified version (lvn=%ju, snapshot=%ju) not "
				"found for key %s\n", lvn, snapshot, vt->key);
		return 2;  //version not found
	} else if (ret != 0) {
		kp_die("internal error: unexpected return value from "
				"kp_vt_get_value: %d\n", ret);
	}

	/* value and size have been set; don't make a copy of value, we're
	 * still "internal." */
	return 0;
}

/* Looks up the specified version of the specified key and returns a COPY
 * of the value in the provided pointer argument. To get the "current"
 * version, all of the lvn, gvn and snapshot args should be set to UINT64_MAX.
 * Otherwise, ONE of the lvn/gvn/snapshot should be set to a value that
 * is not UINT64_MAX, otherwise this function will return an error.
 *
 * The caller must pass an already-allocated kp_ht_entry to this function
 * for the hash table lookup: lookup_entry's key should be set to the key,
 * and its vt should be set to NULL. See kp_ht_entry_set().
 *
 * Returns: If the key is found, 0 is returned, the value argument
 * is set to point to the new copy of the value, and the size argument
 * is set to the size of the value. If the key is not found in the
 * store, 1 is returned. If the key was found but the specified version
 * was not, 2 is returned. If the key is found but the version we got
 * is a deletion tombstone (value is NULL), 3 is returned (and *value
 * will be set to NULL and *size will be set to 0). If an error
 * occurred, -1 is returned.
 */
int kp_get_copy(kp_kvstore *kv, const kp_ht_entry *lookup_entry, uint64_t lvn,
		uint64_t snapshot, void **value, size_t *size)
{
	int ret;
	const void *value_internal = NULL;
	void *value_copy;
	bool copy_to_nvm;

	ret = kp_get_internal(kv, lookup_entry, lvn, snapshot,
			&value_internal, size);  //doesn't do anything nvm-related
	if (ret != 0) {
		/* Error (negative), key not found (1), or value not found(2).
		 * (if a tombstone is gotten, 0 will have been returned)
		 */
		if (ret > 2) {  //sanity check
			kp_die("unexpected return value %d from kp_get_internal()\n", ret);
		}
		kp_debug("directly returning retval from kp_get_internal (%d)\n", ret);
		return ret;
	}

#ifdef KP_ASSERT
	if (!size) {
		kp_die("internal error: size is NULL after kp_get_internal()\n");
	}
#endif
	if (value_internal == NULL) {
		kp_debug_gc("got back a NULL value pointer; means that the version "
				"that we got is a tombstone (size=%zu)\n", *size);
		*value = NULL;
		return 3; // We have gotten a deleted key-value pair
	}

	/* kp_get_internal() returns a pointer to memory "internal" to the
	 * KV store; for this external-facing function, we want to make a
	 * copy of the value before returning it! User is responsible for
	 * freeing it.
	 */
	kp_debug2("kp_get_internal() succeeded, returned size=%zu, "
			"value_internal=%s\n", *size, (char *)value_internal);

	/* For now, we always store the result of a get in volatile memory;
	 * this is consistent with only writing things to PCM that can't be
	 * reconstructed by other means (after a failure, the client can just
	 * perform another get to get the value again, which is stored durably
	 * internal to the kvstore.
	 * If needed, we could add a flag "store_in_nvm" that is passed in to
	 * the get function by the application, but I'm not sure this will be
	 * necessary. */
	copy_to_nvm = false;
	if (copy_to_nvm) {
		kp_die("need to make sure that failure in-between this point and "
				"return to caller (i.e. kp_get() and all the functions in-"
				"between) is handled correctly!\n");
		kp_debug("kp_get_internal() succeeded, returned size=%zu, value=%p (%s); "
				"now calling kp_kpalloc() and kp_memcpy() to copy the value to "
				"NON-VOLATILE memory!\n",
				*size, value_internal, (char *)value_internal);
	} else {
		kp_debug2("kp_get_internal() succeeded, returned size=%zu, value=%p (%s); "
				"now calling kp_kpalloc() and kp_memcpy() to copy the value to "
				"VOLATILE memory!\n",
				*size, value_internal, (char *)value_internal);
	}

	/* Leak window begin (if copy_to_nvm true): value_copy is not "reachable" */
	kp_debug2("malloc-ing value_copy, *size=%zu\n", *size);
	kp_kpalloc(&value_copy, *size, copy_to_nvm);
	if (!value_copy) {
		kp_error("kp_kpalloc() failed\n");
		return -1;
	}
	kp_debug2("memcpy: value_copy=%p, value_internal=%p, *size=%zu\n",
			value_copy, value_internal, *size);
	kp_memcpy(value_copy, value_internal, *size, copy_to_nvm);
	*value = value_copy;
	kp_flush_range((void *)value, sizeof(void *), copy_to_nvm);
	/* Leak window end. */

	kp_debug2("copied *value_internal into *value_copy and set *value = "
			"value_copy (%s)\n", (char *)(*value));
	return 0;
}

/* Returns: 0 on success, -1 on error. On success, *lvn is set to the local
 * version number of the value that was put (on failure, *lvn may be set
 * to an arbitrary value). */
int kp_put_local(kp_kvstore *kv, const kp_ht_entry *lookup_entry,
		const void *value, size_t size, uint64_t *lvn, uint64_t local_snapshot)
{
	int ret, lookup_ret;
	kp_vt *lookup_vt;
	kp_vt *new_vt;
	kp_vt *append_vt;
	bool found, put_succeeded;
	kp_ht_entry *new_entry;
	bool copy_key = false;
	kp_vte *get_vte = NULL;

	if (!kv || !lookup_entry || !lvn) {
		kp_error("one or more arguments are null (kv=%p, lookup_entry=%p, "
				"lvn=%p)\n", kv, lookup_entry, lvn);
		return -1;
	}
	if (!lookup_entry->key) {
		kp_error("lookup_entry->key is NULL!\n");
		return -1;
	}
	if (kv->is_master) {
		kp_error("can't call kp_put_local() for the master store!\n");
		return -1;
	}
	if (strlen(lookup_entry->key) < 1) {
		kp_error("key has no length!\n");
		return -1;
	}
	if ((value && size == 0) || (!value && size != 0)) {
		/* Deletions should be indicated by both setting value to NULL
		 * and size to 0, not one or the other alone.
		 */
		kp_error("value is %p, but size is %zu\n", value, size);
		return -1;
	}

	kp_debug("entered: lookup_entry->key=%s, lookup_entry->vt=%p, "
			"value=%p, size=%zu, local_snapshot=%ju\n", lookup_entry->key,
			lookup_entry->vt, value, size, local_snapshot);

	/* In version 3.0, we've taken steps so that no exclusive lock on the
	 * entire store is required for puts - cool! This is pretty easy for
	 * local stores, for which we assume (right now) that only a single
	 * thread is accessing them at a time anyway.
	 */

	/* FAIL during this function: doesn't matter, this is a local store.
	 * Anything that we allocate will be on volatile memory and will
	 * disappear on failure.
	 */
	
	/* These are the steps in establishing the complete state of a put() to
	 * a NEW KEY:
	 *     Allocate a new hash table entry
	 *     Insert the hash table entry
	 *     Allocate a new version table, attached to the new hash entry
	 *     Lock the version table
	 *     Append the new value to the version table
	 *     Unlock the version table
	 */

	/* Search for the version table for the key. The caller (a local worker)
	 * has already passed us a kp_ht_entry that points to the lookup key
	 * (this should be local->lookup_entry), so we pass it directly to the
	 * lookup function. If this is an insertion, we add the new value
	 * to the version table if the table already exists, or create a new
	 * version table if it does not exist. If this is a deletion, we do
	 * the same thing - the key may exist in the master store, or it may
	 * not exist at all (we don't bother to check here, although we
	 * could if we anted to...), but we just always put the key into the
	 * local store so that it will make it into the next commit record.
	 */

	/* The code below has a possible case where a concurrent put-er inserts
	 * a hash table entry for a NEW KEY before we get a chance to; when this
	 * happens, we just loop again and re-try every step. */
	put_succeeded = false;
	while (!put_succeeded) {
#ifdef CONSERVATIVE_LOCKS
		kp_rdlock("kv->rwlock for local put", kv->rwlock);
#endif

		/* lookup_vt is a local variable: only used in this function, and we
		 * don't care if it's durable / "reachable" on PCM.
		 */
		lookup_vt = NULL;
		lookup_ret = kp_lookup_versiontable(kv, lookup_entry, &lookup_vt);
		if (lookup_ret < 0) {
			kp_error("kp_lookup_versiontable() returned error=%d\n", lookup_ret);
#ifdef CONSERVATIVE_LOCKS
			kp_rdunlock("kv->rwlock for local put", kv->rwlock);
#endif
			return -1;
		} else if (lookup_ret == 1) {
			kp_debug("kp_lookup_versiontable returned not-found\n");
			found = false;
#ifdef KP_ASSERT
			if (lookup_vt != NULL) {
				kp_die("lookup_vt != NULL as expected!\n");
			}
#endif
			if (value == NULL) {
				kp_debug_gc("version table for key to be deleted (%s) "
						"not found; we'll still insert this key (with the "
						"NULL tombstone value) into the local store\n",
						lookup_entry->key);
			}
			kp_debug("version table for key %s not found; creating ht_entry "
					"first, then vt\n", lookup_entry->key);

			/* Allocate a new hash table entry to insert. THIS IS WHERE WE
			 * MAKE OUR COPY OF THE KEY! - lots of other internal structures
			 * will point to this copy, but this is where we originally copy
			 * it internally. In other words, the hash table entry "OWNS"
			 * the key. This seems to make sense.
			 * Other arguments: we pass NULL as the vt pointer for now; we'll
			 * set this pointer just below. Pass the use_nvm of the local
			 * kvstore. */
			copy_key = true;
			ret = kp_ht_entry_create_internal(&new_entry, lookup_entry->key,
					copy_key, NULL, false);
			  /* Will copy the key, set new_entry->key, and will set
			   * new_entry->vt to NULL. To later set new_entry->vt, we
			   * could call kp_ht_entry_set() again, but instead we'll
			   * just directly set it below.
			   */
			if (ret != 0) {
				kp_error("kp_ht_entry_create_internal() returned error=%d\n",
						ret);
#ifdef CONSERVATIVE_LOCKS
				kp_rdunlock("kv->rwlock for local put", kv->rwlock);
#endif
				return -1;
			}
			kp_debug("allocated new kp_ht_entry, copied key=%s\n",
					new_entry->key);

			/* Now create the new vt, and on success link it to the new
			 * ht entry's vt pointer. Why don't we allocate the new vt
			 * before the new ht entry? It doesn't really matter, except
			 * that we think it makes more sense for the key to be owned
			 * by the ht entry rather than the vt, so we create the ht
			 * entry first. */
			ret = kp_vt_create(kv, new_entry->key, &(new_vt));
			if (ret != 0) {
				/* So far we've just created a new kp_ht_entry, so delete it
				 * here. IMPORTANT: DON'T set new_entry->key to NULL, so that
				 * kp_ht_entry_destroy() will free it! (we copied it above) */
				kp_error("kp_vt_create() returned error=%d, so freeing just-"
						"allocated hash table entry and returning.\n", ret);
				if (!copy_key) {
					new_entry->key = NULL;
				}
				kp_ht_entry_destroy(&new_entry, false);  //not nvram
#ifdef CONSERVATIVE_LOCKS
				kp_rdunlock("kv->rwlock for local put", kv->rwlock);
#endif
				return -1;
			}
			new_entry->vt = new_vt;
			kp_debug("Created new vt (%p) with key=%s, hooked it up to new "
					"ht entry (key=%s, vt=%p)\n", new_vt, new_vt->key,
					new_entry->key, new_entry->vt);

			/* Ok, we've now allocated a new hash table entry and a new vt
			 * and linked them together. Follow the common code below to add
			 * a new value to the vt; our last step will be inserting the new
			 * entry into the ht.
			 */
			append_vt = new_vt;
		} else {  //lookup_ret == 0
			found = true;
			append_vt = lookup_vt;
			kp_debug("kp_lookup_versiontable returned found (key=%s); "
					"vt is now locked!\n", append_vt->key);
		}

		/* At this point, append_vt points to the version table that we should
		 * be working with. If the vt was already found in the hash table, then
		 * we now hold the exclusive lock on it. If the vt was not found in the
		 * hash table, then we'll need to insert it after we append the value
		 * here; no other threads can access the new vt yet.
		 *
		 * Add the value to the version table. The version table stores the
		 * local store's current snapshot number ("start timestamp") in case
		 * we want to do things like caching of versions in the future. NOTE:
		 * we do not use the kvstore's global_snapshot itself here!
		 * We pass NULL for the commit_record pointer, which doesn't exist for
		 * a local store (kp_vt_append_value() is unified for locals + masters).
		 */
		ret = kp_vt_append_value(append_vt, value, size, local_snapshot, NULL,
				kv->use_nvm);
		  /* we'll check the return value momentarily */

		/* Return the most-recent LVN (ver_count minus one; this is only safe
		 * because we still hold the lock on the vt (or this is a new vt)).
		 * If kp_vt_add_value_local() failed, then this value may not make
		 * sense; the caller will ignore it though. */
		*lvn = append_vt->ver_count - 1;

		/* Unlock the vt right away! */
		if (found) {
			kp_debug_lock("%s: unlocking version table\n", 
					append_vt->key);
			kp_mutex_unlock("vt->lock", append_vt->lock);		
		}

		/* Now check the return value: */
		if (ret != 0) {
			kp_error("kp_vt_append_value() returned error=%d\n", ret);
			if (!found) {
				/* If kp_vt_append_value() got so far as to append a new
				 * vte to the new vt before it returned error, then calling
				 * kp_vt_destroy() here with the second argument set to
				 * false will cause that vte to be destroyed. However,
				 * kp_vt_destroy() never frees the actual values, which
				 * could be a problem if kp_vt_append_value() copied the
				 * value before returning an error.
				 *   BUG here, potentially: memory leak!
				 *   TODO: check kp_vt_append_value() to make sure that
				 *     if it returns an error, it frees the value if it
				 *     made a copy of it, because we don't do it here!
				 *     (I didn't want to do this right now and put it off
				 *     because this is just an error case....)
				 */
				kp_vt_destroy(&new_vt, false);
				if (!copy_key) {  //don't free the entry's key if we didn't copy it!!
					new_entry->key = NULL;
				}
				kp_ht_entry_destroy(&new_entry, false);  //not nvram
				  /* new_entry->key should be non-null, so it will be freed. */
				//CHECK - TODO - BUG: why are we passing false explicitly here
				//  for use_nvm????????????????????????????
			}
#ifdef CONSERVATIVE_LOCKS
			kp_rdunlock("kv->rwlock for local put", kv->rwlock);
#endif
			return -1;
		}
		kp_debug("kp_vt_add_value_local() succeeded\n");

		/* Ok, if this key was not already found in the hash table, then we
		 * finally insert the hash table entry that points to the version
		 * table that now contains the new (first) vte. The hash table
		 * insertion is ATOMIC with respect to other concurrent lookups:
		 * other threads performing gets will either see that our new hash
		 * table entry has not been inserted yet or it has, but will not
		 * see a partial state. This means that hash table insertions do
		 * not have to block gets, and hash table lookups don't have to take
		 * a rdlock on the hash table.
		 *   BUG: this is not quite true: hash table insertions are NOT
		 *     ATOMIC when _rehashing_ happens!! So, a lookup that happens
		 *     during a rehash could fail. Need to add a rwlock internally
		 *     in hash table!!
		 *
		 * IMPORTANT: see implementation of hash_insert_if_absent() and
		 * notes in there before changing any of this code!!!
		 *
		 * However, concurrent puts (insertions into the hash table) could
		 * interfere with each other, so we take the wrlock on the kvstore
		 * before proceeding. This means that puts will block other puts
		 * during this code segment. Additionally, it is possible that some
		 * other put could have inserted the new vt and hash table entry that
		 * we're trying to insert in-between this point and the point where
		 * we looked up the version table above! The hash table insert function
		 * will tell us if this is the case; if so, we'll clean up and
		 * retry. */
#ifdef CONSERVATIVE_LOCKS
		kp_rdunlock("kv->rwlock for local put", kv->rwlock);
#endif
		if (found) {
			put_succeeded = true;  //we're done
		} else {
#ifdef CONSERVATIVE_LOCKS
			kp_wrlock("kv->rwlock for insertion into hash table", kv->rwlock);
#endif

			/* Insert the new entry into the hash table. If
			 * hash_insert_if_absent()
			 * returns 0, then a concurrent put-er created an entry and
			 * a vt and inserted them into the hash table after we did our
			 * lookup above. If this happens, we don't try to use the
			 * concurrent entry here, we just clean up and retry from the
			 * beginning of this function.
			 */
			ret = hash_insert_if_absent(kv->ht, (void *)new_entry, NULL);
			if (ret == 0) {
				/* This case should be extremely rare... how can we test
				 * it??
				 *   This code is (hopefully) exactly the same as the code
				 *   in kp_put_master(), and we test that case anyway -
				 *   see the notes in that function.
				 */
				//kp_debug("hash_insert_if_absent() returned 0: this entry is "
				//		"already in the hash table!\n");
				kp_warn("hash_insert_if_absent() returned 0: this entry is "
						"already in the hash table!\n");

				/* Rather than trying to append to the concurrent entry
				 * here, just clean up everything that we've done in this
				 * function and re-loop; this will avoid creating another
				 * race condition at this point, where after
				 * hash_insert_if_absent() but before we have a chance to
				 * access and lock the vt, somebody else modifies the vt...
				 *
				 * First, we explicitly get the vte that kp_vt_append_value()
				 * just appended, then free it; we can't rely on
				 * kp_vt_destroy() to do this for us, because that function
				 * NEVER frees the actual values in the VTEs, but
				 * kp_vt_append_value() just made a copy of the value that
				 * we have to free!
				 *   NOTE: this code was written while fixing a memory-
				 *   ownership bug discovered while adding garbage
				 *   collection, but it has never been explicitly tested.
				 */
				get_vte = NULL;
				vector_get(new_vt->vtes, 0, (void **)(&get_vte));
				if (get_vte) {
					kp_vte_destroy(&get_vte, kv->use_nvm);
				} else {
					kp_warn("Unexpectedly failed to get the just-appended "
							"vte from new_vt!\n");
				}

				/* Because we just freed the one-and-only vte that was
				 * added to the new vt, call kp_vt_destroy with the
				 * vtes_already_freed argument set to true.
				 */
				*lvn = UINT64_MAX;
				kp_vt_destroy(&new_vt, true);
				kp_ht_entry_destroy(&new_entry, false);  //not nvram
				  /* new_entry->key should be non-null, so it will be freed. */
				put_succeeded = false;  //loop again and retry!
				kp_debug("cleaned up just-created vt and ht entry and set "
						"put_succeeded to false; will loop again now!\n");
				kp_warn("cleaned up just-created vt and ht entry and set "
						"put_succeeded to false; will loop again now!\n");
			} else if (ret != 1) {  //error
				kp_error("hash_insert_if_absent() returned error %d\n", ret);
				/* Same as above, we first get the one-and-only vte in
				 * the new vt and explicitly free it, then destroy the
				 * vt itself:
				 */
				get_vte = NULL;
				vector_get(new_vt->vtes, 0, (void **)(&get_vte));
				if (get_vte) {
					kp_vte_destroy(&get_vte, kv->use_nvm);
				} else {
					kp_warn("Unexpectedly failed to get the just-appended "
							"vte from new_vt!\n");
				}
				*lvn = UINT64_MAX;
				kp_vt_destroy(&new_vt, true);
				kp_ht_entry_destroy(&new_entry, false);  //not nvram
				  /* new_entry->key should be non-null, so it will be freed. */
#ifdef CONSERVATIVE_LOCKS
				kp_wrunlock("kv->rwlock for insertion into hash table", kv->rwlock);
#endif
				return -1;
			} else {  //success
				put_succeeded = true;  //don't re-loop
				kv->pairs_count++;  //we're still locked, so no race conditions
				  //CHECK: is this the right place to increment this??
				  //  I think pairs_count can probably just be eliminated...
				kp_debug("hash_insert_if_absent(key=%s) succeeded, incremented "
						"kv->pairs_count to %ju\n", new_entry->key,
						kv->pairs_count);
			}

#ifdef CONSERVATIVE_LOCKS
			kp_wrunlock("kv->rwlock for insertion into hash table", kv->rwlock);
#endif
		}  //if (!found)
	}  //while (!put_succeeded)

	kp_debug("local put complete, returning 0\n");
	return 0;
}

/* Puts the key (inside of lookup_entry) + value pair into the master
 * store. kp_vt_insert_value() is called to perform the actual insertion
 * of the new vte into the vt, and also performs conflict detection.
 * This function needs the commit_record because each vte now needs to
 * point to its commit_record, because gets from the master store might
 * read a version whose commit state is INPROGRESS or ABORTED!
 *
 * Returns: 0 on success, 1 if the put failed due to a conflict, 2 if this
 * is a deletion but the key was not found in the store, -1 on error. */
int kp_put_master(kp_kvstore *kv, const kp_ht_entry *lookup_entry,
		const void *value, const size_t size, kp_commit_record *cr)
{
	int ret, lookup_ret, retval;
	kp_vt *lookup_vt;
	kp_vt *new_vt;
	kp_vt *append_vt;
	bool found, put_succeeded;
	kp_ht_entry *new_entry;
	bool copy_key = false;
#ifdef KP_ASSERT
	unsigned int num_retries = 0;

	if (!kv || !lookup_entry) {
		kp_error("one or more arguments are null (kv=%p, lookup_entry=%p)\n",
				kv, lookup_entry);
		return -1;
	}
	if (!lookup_entry->key) {
		kp_error("lookup_entry->key is NULL!\n");
		return -1;
	}
	if (!kv->is_master) {
		kp_error("can't call this function for a local store!\n");
		return -1;
	}
	if (strlen(lookup_entry->key) < 1) {
		kp_error("key has no length!\n");
		return -1;
	}
	if ((!value && size != 0) || (value && size == 0)) {
		kp_die("value is %p, but size is %zu!\n", value, size);
	}
#endif
	retval = 0;

	kp_debug("entered: lookup_entry->key=%s, lookup_entry->vt=%p, "
			"value=%p, size=%zu\n", lookup_entry->key, lookup_entry->vt,
			value, size);

	/* In version 3.0, we've taken steps so that no exclusive lock on the
	 * entire store is required for puts - cool! However, puts to new keys
	 * require locking the hash table to block all other puts and gets while
	 * the new key is inserted. Puts to existing keys only block other puts
	 * to keys in the same hash bucket; puts to existing keys don't block
	 * gets.
	 */

	/* These are the steps in establishing the complete state of a put() to
	 * a NEW KEY:
	 *     Allocate a new hash table entry
	 *     Insert the hash table entry
	 *     Allocate a new version table, attached to the new hash entry
	 *     Lock the version table
	 *     Append the new value to the version table
	 *     Unlock the version table
	 */

	/* Search for the version table for the key. The caller has already
	 * passed us a kp_ht_entry that points to the lookup key, so we pass
	 * it directly to the lookup function.
	 * If this is an insertion, we add the new value
	 * to the version table if the table already exists, or create a new
	 * version table if it does not exist. If this is a deletion, return
	 * if the version table is not found. */

	/* The code below has a possible case where a concurrent put-er inserts
	 * a hash table entry for a NEW KEY before we get a chance to; when this
	 * happens, we just loop again and re-try every step.
	 * At first this seemed to me that it would be a rare case, but it turns
	 * out that this happens rather frequently in both our evaluation code
	 * and our test_local_master code. */
	put_succeeded = false;
	while (!put_succeeded) {
#ifdef KP_ASSERT
		if (num_retries > 0) {
			//kp_warn("num_retries is non-zero!: %u\n", num_retries);
			kp_debug2("num_retries is non-zero!: %u\n", num_retries);
			if (num_retries >= 5) {  //arbitrary...
				kp_die("too many retries?\n");
			}
		}
#endif
#ifdef CONSERVATIVE_LOCKS
		kp_rdlock("kv->rwlock for master put", kv->rwlock);
#endif

		/* FAIL during kp_lookup_versiontable()? Doesn't really matter; it's
		 * just a lookup, doesn't affect any state.
		 * lookup_vt is a local variable: only used in this function, and we
		 * don't care if it's durable / "reachable" on PCM. */
		lookup_vt = NULL;
		lookup_ret = kp_lookup_versiontable(kv, lookup_entry, &lookup_vt);
		if (lookup_ret < 0) {
			kp_error("kp_lookup_versiontable() returned error=%d\n", lookup_ret);
#ifdef CONSERVATIVE_LOCKS
			kp_rdunlock("kv->rwlock for master put", kv->rwlock);
#endif
			return -1;
		} else if (lookup_ret == 1) {
			kp_debug2("kp_lookup_versiontable returned not-found\n");
			found = false;
#ifdef KP_ASSERT
			if (lookup_vt != NULL) {
				kp_die("lookup_vt != NULL as expected!\n");
			}
#endif
			if (value == NULL) {  /* tombstone value for deletion */
				kp_debug_gc("version table for key to be deleted (%s) "
						"not found; we'll ignore this kvpair in the commit "
						"record then, and just return from here now.\n",
						lookup_entry->key);
#ifdef CONSERVATIVE_LOCKS
				kp_rdunlock("kv->rwlock for master put", kv->rwlock);
#endif
				return 2;  //(differs from kp_put_local)
			}
			kp_debug("version table for key %s not found; creating ht_entry "
					"first, then vt\n", lookup_entry->key);

			/* Allocate a new hash table entry to insert. Unlike a put
			 * to a local store, we don't make a copy of the key here; the
			 * key is already durable on nvm and is pointed to by the commit
			 * record. So, we just copy the pointer to the key here. However,
			 * let's still declare that the hash table entry now "OWNS" the
			 * key; after a commit record has been entirely merged in and
			 * its state set to COMMITTED, it no longer owns the keys that
			 * it points to. Cool.
			 * Other arguments: we pass NULL as the vt pointer for now;
			 * we'll set this pointer just below. We pass the use_nvm of
			 * the kvstore. */
			copy_key = false;
			ret = kp_ht_entry_create_internal(&new_entry, lookup_entry->key,
					copy_key, NULL, kv->use_nvm); /* kv is master store */
			if (ret != 0) {
				kp_error("kp_ht_entry_create_internal() returned error=%d\n", ret);
#ifdef CONSERVATIVE_LOCKS
				kp_rdunlock("kv->rwlock for master put", kv->rwlock);
#endif
				return -1;
			}
			kp_debug("allocated new kp_ht_entry, copied pointer to key=%s\n",
					new_entry->key);

			/* Now create the new vt, and on success link it to the new
			 * ht entry's vt pointer. Why don't we allocate the new vt
			 * before the new ht entry? It doesn't really matter, except
			 * that we think it makes more sense for the key to be owned
			 * by the ht entry rather than the vt, so we create the ht
			 * entry first. */
			ret = kp_vt_create(kv, new_entry->key, &(new_vt));
			if (ret != 0) {
				/* So far we've just created a new kp_ht_entry, so delete it
				 * here. IMPORTANT: we set new_entry->key to NULL first so
				 * that the destroy function can't free the key (we only
				 * copied the pointer to it earlier in this function). */
				kp_error("kp_vt_create() returned error=%d, so freeing just-"
						"allocated hash table entry and returning.\n", ret);
				if (!copy_key) {
					new_entry->key = NULL;
				}
				kp_ht_entry_destroy(&new_entry, kv->use_nvm);
#ifdef CONSERVATIVE_LOCKS
				kp_rdunlock("kv->rwlock for master put", kv->rwlock);
#endif
				return -1;
			}
			PM_EQU((new_entry->vt), (new_vt));
			kp_debug("Created new vt (%p) with key=%s, hooked it up to new "
					"ht entry (key=%s, vt=%p)\n", new_vt, new_vt->key,
					new_entry->key, new_entry->vt);

			/* Ok, we've now allocated a new hash table entry and a new vt
			 * and linked them together. Follow the common code below to add
			 * a new value to the vt; our last step will be inserting the new
			 * entry into the ht.
			 */
			append_vt = new_vt;
		} else {  //lookup_ret == 0
			found = true;
			append_vt = lookup_vt;
			kp_debug2("kp_lookup_versiontable returned found (key=%s); "
					"vt is now locked!\n", append_vt->key);
		}

		/* At this point, append_vt points to the version table that we should
		 * be working with. If the vt was already found in the hash table, then
		 * we now hold the exclusive lock on it. If the vt was not found in the
		 * hash table, then we'll need to insert it after we append the value
		 * here; no other threads can access the new vt yet. */

		/* Instead of kp_vt_append_value(), with conflict detection we use
		 * a new function, kp_vt_insert_value(). This function will insert
		 * the new value into the version table in sorted order, then will
		 * check backwards for conflicts that will cause this commit to
		 * abort and forwards for conflicts that will be aborted by this
		 * commit.
		 *
		 * At this point, we need to decide who "owns" the value, because
		 * multiple data structures point to it. Up until now, the commit
		 * record owns the value, because it has the original and only
		 * pointer to it. Let's keep it that way - even after we call
		 * kp_vt_insert_value() here to copy the pointer to the value into
		 * a new VTE, and even after kp_put_master() returns successfully,
		 * let's say that the commit record maintains ownership of the
		 * values. Why? Because otherwise, it's really difficult to clean
		 * up partial commits that fail due to an error or a conflict,
		 * because half of the values belong to version tables now and
		 * half of the values still belong to the commit records (I
		 * initially thought it would make more sense for version tables
		 * to take ownership of the values, but then I started going through
		 * kp_merge_commit_record() and it looked like trying to sanely
		 * free the values after an error/conflict would be a disaster).
		 *   Will this decision mean that values will stick around and
		 *   take up memory longer than they should? I'm not sure; we're
		 *   going to perform GC at snapshot granularity anyway, but
		 *   does this mean that a commit RECORD can't actually be freed
		 *   until all of the key-value pairs that it contains have already
		 *   been collected? I guess so... is that not expected?
		 *     Anyway, if we want to free certain values (or keys, for
		 *     that matter) before the actual commit record is freed, we
		 *     can do so, as long as we set the pointers in the commit
		 *     record to NULL so they won't be freed again.
		 *
		 * //However, let's declare that after kp_put_master()
		 * //returns SUCCESSFULLY, then the version table "owns" the value;
		 * //the function we're calling here, kp_vt_insert_value(), is going
		 * //to copy the pointer to the value into a new version table entry.
		 * //I think this makes it easiest to sort out the value freeing
		 * //when these various data structures are destroyed.
		 */
		kp_debug2("calling kp_vt_insert_value() for key=%s, value=%p, "
				"size=%zu, end_snapshot=%ju\n", append_vt->key, value,
				size, cr->end_snapshot);
		ret = kp_vt_insert_value(append_vt, value, size, cr->end_snapshot,
				cr, kv->detect_conflicts, kv->use_nvm);
		  /* we'll check the return value momentarily */

		/* Unlock the vt right away! */
		if (found) {
			kp_debug_lock("%s: unlocking version table\n", append_vt->key);
			kp_mutex_unlock("vt->lock", append_vt->lock);		
		}

		/* Now check the return value: */
		if (ret == 1) {  //conflict!
			kp_debug2("kp_vt_insert_value() returned that there was a "
					"conflict! Don't do anything about it here, but we'll "
					"tell our caller\n");
			retval = 1;
#ifdef KP_ASSERT
			if (!found) {
				kp_die("huh? vt was not found, but we got a conflict???\n");
			}
#endif
		} else if (ret != 0) {  //error
			kp_error("kp_vt_insert_value() returned error=%d\n", ret);
			if (!found) {
				/* If kp_vt_insert_value() got so far as to add a new vte
				 * to the new vt before it returned error, then calling
				 * kp_vt_destroy() here with the second argument set to
				 * false will cause that vte to be destroyed. kp_vt_destroy()
				 * never frees values that the vtes point to, but that is
				 * ok for us here because we're a master store, and
				 * kp_vt_insert_value() only made a copy of the pointer
				 * to the value (the commit record still "owns" the value).
				 */
				kp_vt_destroy(&new_vt, false);
				if (!copy_key) {
					/* don't free the entry's key if we didn't copy it!! */
					new_entry->key = NULL;
				}
				kp_ht_entry_destroy(&new_entry, kv->use_nvm);
			}
#ifdef CONSERVATIVE_LOCKS
			kp_rdunlock("kv->rwlock for master put", kv->rwlock);
#endif
			return -1;
		} else {
			kp_debug2("kp_vt_insert_value() succeeded, no conflict\n");
		}

		/* Ok, if this key was not already found in the hash table, then we
		 * finally insert the hash table entry that points to the version
		 * table that now contains the new (first) vte. Hash table inserts
		 * that do not require REHASHING are ATOMIC with respect to other
		 * concurrent lookups: other threads performing gets will either see
		 * that our new hash table entry has not been inserted yet or it has,
		 * but will not see a partial state. (How do I know this? Through
		 * careful, and hopefully correct, examination of the hash table code
		 * ...) This means that hash table insertions (without rehashing)
		 * do not have to block gets.
		 *
		 * HOWEVER, if rehashing is required on an insert (or delete), then
		 * we need to block all concurrent insertions and lookups in the
		 * hash table. Therefore, we've added a rwlock internally in the
		 * hash table; see the notes in hash.c for much more detail about
		 * this.
		 *
		 * IMPORTANT: see implementation of hash_insert_if_absent() and
		 * notes in there before changing any of this code!!!
		 *
		 * However, concurrent puts (insertions into the hash table) could
		 * interfere with each other, so we take the wrlock on the kvstore
		 * before proceeding. This means that puts will block other puts
		 * during this code segment. UPDATE: we got rid of this big lock
		 * and replaced it with a lock per-bucket in the hash table. Woohoo!
		 *
		 * Additionally, it is possible that some
		 * other put could have inserted the new vt and hash table entry that
		 * we're trying to insert in-between this point and the point where
		 * we looked up the version table above! The hash table insert function
		 * will tell us if this is the case; if so, we'll clean up and
		 * retry. */
#ifdef CONSERVATIVE_LOCKS
		kp_rdunlock("kv->rwlock for master put", kv->rwlock);
#endif
		if (found) {
			put_succeeded = true;  //we're done
		} else {
#ifdef CONSERVATIVE_LOCKS
			kp_wrlock("kv->rwlock for insertion into hash table", kv->rwlock);
#endif
			/* Don't have to do anything with conflict detection in this
			 * code segment - not applicable for first put to a version
			 * table. */

			/* Insert the new entry into the hash table. If
			 * hash_insert_if_absent() returns 0, then a concurrent put-er
			 * created an entry and a vt and inserted them into the hash
			 * table after we did our lookup above. If this happens, then
			 * we just clean up and retry from the beginning of this
			 * function.
			 */
			ret = hash_insert_if_absent(kv->ht, (void *)new_entry, NULL);
			if (ret == 0) {
				/* This case should be rare... how can we test it??
				 *   Actually, it may not be so hard - in test_local_master.c,
				 *   this case is hit when just running with 4 threads, because
				 *   there is a series of commits that are all committing just
				 *   one put to the same key. Not hit every time, but did hit
				 *   it the first time at least (may help to turn off debug
				 *   messages to try to hit it...).
				 *
				 *   And, a few months after writing this code, I still hit
				 *   this case when running test_local_master with just 4
				 *   threads. However, as noted above, with many debug2
				 *   messages enabled, I only hit it like 1 in 20 times.
				 */
				kp_debug2("hash_insert_if_absent() returned 0: this entry is "
						"already in the hash table!\n");

				/* Rather than trying to append to the concurrent entry
				 * here, just clean up everything that we've done in this
				 * function and re-loop; this will avoid creating another
				 * race condition at this point, where after
				 * hash_insert_if_absent() but before we have a chance to
				 * access and lock the vt, somebody else modifies the vt...
				 *
				 * Pass false for "vtes_already_freed" argument to
				 * kp_vt_destroy() here, so that the one-and-only vte that
				 * kp_vt_insert_value() just added will be freed.
				 */
				kp_vt_destroy(&new_vt, false);
				if (!copy_key) {
					/* don't free the entry's key if we didn't copy it!! */
					new_entry->key = NULL;
				}
				kp_ht_entry_destroy(&new_entry, kv->use_nvm);
				put_succeeded = false;  //loop again and retry!
#ifdef KP_ASSERT
				num_retries++;
#endif
				kp_debug2("cleaned up just-created vt and ht entry and set "
						"put_succeeded to false; will loop again now!\n");
				//kp_warn("cleaned up just-created vt and ht entry and set "
				//		"put_succeeded to false; will loop again now!\n");
			} else if (ret != 1) {  //error
				kp_error("hash_insert_if_absent() returned error %d\n", ret);
				kp_vt_destroy(&new_vt, false);
				if (!copy_key) {
					/* don't free the entry's key if we didn't copy it!! */
					new_entry->key = NULL;
				}
				kp_ht_entry_destroy(&new_entry, kv->use_nvm);
#ifdef CONSERVATIVE_LOCKS
				kp_wrunlock("kv->rwlock for insertion into master hash table",
						kv->rwlock);
#endif
				return -1;
			} else {  //success
				put_succeeded = true;  //don't re-loop
				/* I assume that this increment is performed using an atomic
				 * instruction (there is atomic 64-bit increment in x86_64,
				 * right?), so we don't need to be locked here. Even if
				 * something goes wrong, pairs_count is pretty much unused
				 * anyway. Eventually we could perform this in a compare-
				 * and-swap macro.
				 *
				 * If pairs_count is going to actually be used for anything,
				 * then we should flush it here, but whatever - it's not
				 * actually used at the moment, and it's also technically
				 * reconstructable, with an fsck over the entire store... */
				PM_EQU((kv->pairs_count), (kv->pairs_count+1));
				kp_debug("hash_insert_if_absent(key=%s) succeeded, incremented "
						"kv->pairs_count to %ju\n", new_entry->key,
						kv->pairs_count);
			}

#ifdef CONSERVATIVE_LOCKS
			kp_wrunlock("kv->rwlock for insertion into hash table", kv->rwlock);
#endif
		}  //if (!found)
	}  //while (!put_succeeded)

	kp_debug2("master put complete, returning retval=%d (0 for success, "
			"1 for a conflict, 2 if this is a deletion but key not found "
			"in the store, -1 on er-ror)\n", retval);
	return retval;
}

/* Removes the version entry at the specified index from the version table.
 * If no more entries remain in the version table, this function DOES NOT
 * free the version table itself; the caller should check for this condition
 * and free the version table if this happens.
 * The version table's lock should already be held when this function is
 * called!!
 * Returns: the number of data bytes freed, or UINT64_MAX on error.
 */
uint64_t kp_vt_delete_version(kp_vt *vt, uint64_t idx)
{
	int ret;
	kp_vte *old_vte = NULL;
	uint64_t bytes_collected;

	if (!vt) {
		kp_error("vt is null\n");
		return UINT64_MAX;
	}
	//kp_vt_sanity_check_lengths(vt);
	if (idx > vt->len - 1) {
		kp_error("invalid idx=%ju, vt len=%ju\n", idx, vt->len);
	}

	kp_die("this code hasn't been used / examined in a while - check "
			"it!\n");
	kp_die("IMPORTANT: this function calls kp_vte_destroy(), which "
			"will free the value pointed to by the vte if the pointer "
			"to it is not set to NULL. Check whether or not we want "
			"the value to be freed in master stores with garbage "
			"collection!\n");

	/* The vector_delete() functions remove an element, then shift all of
	 * the remaining elements down one position. This is an O(n) operation,
	 * so it could become fairly expensive if the number of versions for a
	 * key-value pair grows large. With a telescoping policy, the number of
	 * kept checkpoints is kept to a logarithmic factor of the number of
	 * checkpoints taken, so hopefully the number of versions in a given
	 * version table remains at a reasonable number. With other garbage
	 * collection policies, who knows...
	 *
	 * vector_delete() doesn't actually free the element that was stored
	 * in the vector; we store the pointer to this element in old_vte, then
	 * call kp_vte_destroy()...  Also, before
	 * freeing, we keep track of the number of bytes that we're freeing.
	 */
	kp_debug("calling vector_delete() on elements at idx=%ju\n", idx);
	ret = vector_delete(vt->vtes, idx, (void *)(&old_vte));
	if (ret != 0) {
		kp_error("vector_delete(vtes %ju) returned error=%d\n", idx, ret);
		return UINT64_MAX;
	}
	if (!old_vte) {
		kp_error("got null vte from vt->vtes index %ju!\n", idx);
		return UINT64_MAX;
	}

	/* We only care about the bytes from the value, not the bytes that the
	 * actualy kp_vte uses.
	 */
	bytes_collected = old_vte->size;
	kp_debug("size of deleted entry is bytes_collected=%ju\n",
			bytes_collected);
	if (old_vte->value == NULL) {
		kp_warn("for tombstone value, got bytes_collected=%ju; check that "
				"this makes sense!\n", bytes_collected);
	}
	if (bytes_collected == 0) {
		kp_warn("bytes_collected = 0; is this a bug?? Tombstone values "
				"should have size=1, right?\n");
	}
	if (old_vte->ttl != 0) {
		kp_warn("deleting an entry with non-zero ttl=%ju; is this "
				"intentional???\n", old_vte->ttl);
	}

	/* Finally, destroy the deleted vte. */
	kp_vte_destroy(&old_vte, true);  //why always true??
#ifdef KP_ASSERT
	if (old_vte) {
		kp_die("kp_vte_destroy() did not set old_vte to NULL, as expected\n");
	}
#endif

	vt->len--;
	kp_debug("decremented vt->len, now = %ju\n", vt->len);
	//kp_vt_sanity_check_lengths(vt);

	return bytes_collected;
}

/* Adds a checkpoint to the garbage collector's list of checkpoints
 * that are in-use and should not be collected. This function will
 * check that the checkpoint has not already been collected or marked
 * for collection, but it will not check the validity of the checkpoint
 * number (e.g. it is possible to add a checkpoint that no workers are
 * even using yet).
 * IMPORTANT: this function attempts to take the lock on the gc struct,
 * so it must not be called while the lock is already held, or deadlock
 * could result!
 * Returns: 0 on success, 1 if the specified checkpoint has already been
 * collected or marked collectable, -1 on other errors.
 */
int kp_add_to_cps_to_keep(kp_gc *gc, uint64_t cp_num)
{
	int ret, retval;
	uint64_t idx;

	/* TODO: add support for external calls to this function (which will
	 *   require some kind of counter)!
	 *   Do this work only after the vectors are replaced with search
	 *   trees...
	 *   Then make kp_remove_from_cps_to_keep() externally-callable too,
	 *   of course.
	 */

	/* Lock the GC struct: we don't want the cps_* vectors to change
	 * underneath us!
	 */
	retval = 0;
	kp_mutex_lock("gc->lock", gc->lock);

	/* Check that the cp_num isn't in any of the other vectors already.
	 * If it's in cps_to_collect or cps_collected, then it's too late
	 * for the checkpoint to be kept, so return 1. If it's in
	 * cps_collectable, then telescoping has marked it as collectable
	 * but hasn't put it in cps_to_collect yet, which means it should
	 * already be in cps_to_keep!
	 */
	idx = vector64_search_linear(gc->cps_to_collect, cp_num);
	if (idx != VECTOR64_MAX) {
		kp_debug("cp_num=%ju is already present in gc->cps_to_collect, "
				"can no longer be kept! Returning 1.\n", cp_num);
		retval = 1;
		goto unlock_and_return;
	}

	idx = vector64_search_linear(gc->cps_collected, cp_num);
	if (idx != VECTOR64_MAX) {
		kp_debug("cp_num=%ju is already present in gc->cps_collected, "
				"can no longer be kept! Returning 1.\n", cp_num);
		retval = 1;
		goto unlock_and_return;
	}

	idx = vector64_search_linear(gc->cps_collectable, cp_num);
	if (idx != VECTOR64_MAX) {
		kp_debug("cp_num=%ju is already present in gc->cps_collectable!\n",
				cp_num);
		//TODO:
		kp_die("figure out what to do in this situation: once this function "
				"is made externally-callable, this path should probably "
				"increment a reference count for the cp_num\n");
	}

	idx = vector64_search_linear(gc->cps_to_keep, cp_num);
	if (idx != VECTOR64_MAX) {
		kp_debug("cp_num=%ju is already present in gc->cps_to_keep!\n",
				cp_num);
		//TODO:
		kp_die("figure out what to do in this situation: once this function "
				"is made externally-callable, this path should probably "
				"increment a reference count for the cp_num\n");
	}

	/* Ok, append the cp_num to cps_to_keep: */
	ret = vector64_append(gc->cps_to_keep, cp_num);
	if (ret != 0) {
		kp_error("vector64_append() returned error=%d\n", ret);
		retval = -1;
		goto unlock_and_return;
	}
	kp_debug("appended cp_num=%ju to gc->cps_to_keep\n", cp_num);

	/* Unlock the kvstore and return success: */
	retval = 0;

unlock_and_return:
	kp_mutex_unlock("gc->lock", gc->lock);
	return retval;
}

/* Removes a checkpoint from the garbage collector's list of checkpoints
 * that are in-use or should be kept around.
 * IMPORTANT: this function attempts to take the lock on the gc struct,
 * so it must not be called while the lock is already held, or deadlock
 * could result!
 * Returns: 0 on success, 1 if the specified checkpoint is not-found in
 * the list of kept checkpoints, -1 on error.
 */
int kp_remove_from_cps_to_keep(kp_gc *gc, uint64_t cp_num)
{
	int ret, retval;
	uint64_t idx, ret64;

	/* Lock the GC struct: we don't want the cps_* vectors to change
	 * underneath us!
	 */
	retval = 0;
	kp_mutex_lock("gc->lock", gc->lock);

	/* Check that cp_num is actually present in cps_to_keep, then remove
	 * it. If the cp_num is also present in cps_collectable, then this
	 * means that it is "eligible" to be garbage-collected because it was
	 * chosen by telescoping, so remove it from cps_collectable and add
	 * it to cps_to_collect. If it's not present in cps_collectable, then
	 * do nothing; eventually telescoping will find it and make it collectable,
	 * or somebody else will mark it kept.
	 */
	idx = vector64_search_linear(gc->cps_to_collect, cp_num);
	if (idx != VECTOR64_MAX) {
		kp_error("cp_num=%ju is already present in gc->cps_to_collect, "
				"shouldn't be marking it unused here! Returning -1.\n",
				cp_num);
		retval = -1;
		goto unlock_and_return;
	}

	idx = vector64_search_linear(gc->cps_collected, cp_num);
	if (idx != VECTOR64_MAX) {
		kp_error("cp_num=%ju is already present in gc->cps_collected, "
				"shouldn't be marking it unused here! Returning -1.\n",
				cp_num);
		retval = -1;
		goto unlock_and_return;
	}

	idx = vector64_search_linear(gc->cps_to_keep, cp_num);
	if (idx == VECTOR64_MAX) {  //not-found
		if (cp_num == 0) {
			/* Special-case for kp_kv_local's initial cp_num.
			 * TODO: fix kp_kv_local to add its initial cp_num to
			 * cps_to_keep (after making add_to_cps_to_keep() externally-
			 * callable), then this special-case shouldn't be needed.
			 */
			kp_debug("special-case for worker's initial cp_num=0, returning "
					"success\n");
			retval = 0;
			goto unlock_and_return;
		}
		kp_error("cp_num=%ju not-found in cps_to_keep! Returning 1.\n",
				cp_num);
		retval = 1;
		goto unlock_and_return;
	}
	kp_debug("cp_num=%ju is present in gc->cps_to_keep (as expected) "
			"at idx=%ju\n", cp_num, idx);

	/* Ok, remove cp_num from cps_to_keep: */
	ret64 = vector64_delete(gc->cps_to_keep, idx);
	if (ret64 == UINT64_MAX) {
		kp_error("vector64_delete(cps_to_keep) returned error\n");
		retval = -1;
		goto unlock_and_return;
	}
	kp_debug("deleted cp_num=%ju from cps_to_keep\n", cp_num);
#ifdef KP_ASSERT
	if (ret64 != cp_num) {
		kp_die("ret64=%ju doesn't match cp_num=%ju\n", ret64, cp_num);
	}
#endif

	/* Finally, move the cp_num from cps_collectable to cps_to_collect, if
	 * applicable:
	 */
	idx = vector64_search_linear(gc->cps_collectable, cp_num);
	if (idx != VECTOR64_MAX) {  //found in cps_collectable too
		kp_debug("cp_num=%ju also found in cps_collectable (idx=%ju), "
				"so moving it to cps_to_collect\n", cp_num, idx);
		ret64 = vector64_delete(gc->cps_collectable, idx);
		if (ret64 == UINT64_MAX) {
			kp_error("vector64_delete(cps_collectable) returned error\n");
			retval = -1;
			goto unlock_and_return;
		}
		ret = vector64_append(gc->cps_to_collect, cp_num);
		if (ret != 0) {
			kp_error("vector64_append(cps_to_collect) returned error=%d\n",
					ret);
			retval = -1;
			goto unlock_and_return;
		}
		kp_debug("successfully moved cp_num=%ju from cps_collectable "
				"to cps_to_collect\n", cp_num);
	} else {  //not found in cps_collectable
		kp_debug("cp_num=%ju not found in cps_collectable, so doing "
				"nothing else.\n", cp_num);
	}

	/* Unlock the kvstore and return success: */
	retval = 0;

unlock_and_return:
	kp_mutex_unlock("gc->lock", gc->lock);
	return retval;
}

/* Marks a checkpoint as "collectable" by appending it to one of two vectors:
 * either cps_collectable (if the checkpoint is present in cps_to_keep), or
 * cps_to_collect (if the checkpoint is not being held in cps_to_keep).
 * IMPORTANT: this function assumes for now that the gc struct is locked, so
 * that these vectors are not changed while it is running!
 * Returns: 0 on success, -1 on error.
 */
int kp_gc_mark_cp_collectable(kp_gc *gc, uint64_t cp_num)
{
	int ret;
	uint64_t idx;

	if (!gc) {
		kp_error("gc is null\n");
		return -1;
	}

	/* If the checkpoint is present in cps_to_keep, then it is in use by
	 * some worker, so we add it to the cps_collectable vector; when the
	 * checkpoint is later removed from cps_to_keep, it will be moved
	 * into cps_to_collect. If the checkpoint is not found in cps_to_keep
	 * and is not in use by anybody, then we add it to cps_to_collect,
	 * so it will be collected on the next GC run.
	 * Invariants for checkpoint vectors:
	 *   - Checkpoints will be appended to all of the vectors in UNSORTED
	 *   order.
	 *   - A checkpoint may be present in either cps_collectable or
	 *   cps_to_collect, but never both.
	 *   - A checkpoint, once added to cps_to_collect, may never be added
	 *   to cps_to_keep or cps_collectable, and is only removed from
	 *   cps_to_collect after the checkpoint has been permanently garbage-
	 *   collected.
	 *   - A checkpoint passed into this function may not already be present
	 *   in cps_collectable, cps_to_collect, or cps_collected.
	 *   - Checkpoints added to cps_collected may not be present in any
	 *   other vectors, and are never removed (?).
	 */

#ifdef KP_ASSERT
	idx = vector64_search_linear(gc->cps_collectable, cp_num);
	if (idx != VECTOR64_MAX) {
		kp_die("cp_num=%ju is already present in gc->cps_collectable!\n",
				cp_num);
	}
	idx = vector64_search_linear(gc->cps_to_collect, cp_num);
	if (idx != VECTOR64_MAX) {
		kp_die("cp_num=%ju is already present in gc->cps_to_collect!\n",
				cp_num);
	}
	idx = vector64_search_linear(gc->cps_collected, cp_num);
	if (idx != VECTOR64_MAX) {
		kp_die("cp_num=%ju is already present in gc->cps_collected!\n",
				cp_num);
	}
#endif

	/* Remember, this code relies on the gc struct's lock already being
	 * held! If it's not, then things could change between the time that
	 * we check cps_to_keep and we append cp_num to the right vector.
	 */
	idx = vector64_search_linear(gc->cps_to_keep, cp_num);
	if (idx != VECTOR64_MAX) {  //found in cps_to_keep
		kp_debug("cp_num=%ju found in gc->cps_to_keep (idx=%ju)\n",
				cp_num, idx);
		ret = vector64_append(gc->cps_collectable, cp_num);
		if (ret != 0) {
			kp_error("vector64_append() returned error=%d\n", ret);
			return -1;
		}
		kp_debug("appended cp_num=%ju to gc->cps_collectable\n",
				cp_num);
	} else {  //not-found in cps_to_keep
		kp_debug("cp_num=%ju not found in gc->cps_to_keep\n", cp_num);
		ret = vector64_append(gc->cps_to_collect, cp_num);
		if (ret != 0) {
			kp_error("vector64_append() returned error=%d\n", ret);
			return -1;
		}
		kp_debug("appended cp_num=%ju to gc->cps_to_collect\n",
				cp_num);
	}

	return 0;
}
/* Runs the telescoping algorithm for a single checkpoint number. Checkpoints
 * that are chosen for collection by the telescoping algorithm are added to
 * one of two vectors, either cps_collectable or cps_to_collect (see 
 * kp_gc_mark_cp_collectable()).
 * IMPORTANT: this function assumes for now that the gc struct is locked (so
 * these vectors are not changed while it is running).
 * Returns: 0 on success, -1 on error.
 */
int kp_gc_perform_telescoping(kp_gc *gc, uint64_t cp_num)
{
	int ret;
	uint64_t i;
	uint64_t num_bits;
	uint64_t position;
	float factor;
	float cp_to_collect_offset;
	uint64_t cp_to_collect_offset_int;
	uint64_t cp_to_collect;
	uint64_t counter;
	uint64_t counter_inc;
	uint64_t counter_xor;

	if (!gc) {
		kp_error("gc is null\n");
		return -1;
	}

	/* We let the checkpoint number represent a binary counter, and use
	 * the bit-flips of the counter to tell us when to collect prior
	 * checkpoints. First, increment the counter; cp_num+1, rather than just
	 * cp_num, lines up in Pete's spreadsheet. Then, take the xor of the
	 * incremented counter with the non-incremented counter to get the bits
	 * that flipped when the counter was incremented.
	 */
	counter = cp_num;
	counter_inc = counter + 1;
	counter_xor = counter ^ counter_inc;

	factor = 2.0;  //2 is the only factor known to work properly, so far...
	cp_to_collect_offset = factor;
	i = 1;  //ignore least-significant bit
	position = (int)(exp2(i));
	num_bits = 8*sizeof(uint64_t);
	position = 2;
	kp_debug("cp_num=%ju, num_bits is %ju, position is %ju\n",
			cp_num, num_bits, position);
	for (; i < num_bits; i++) {
		cp_to_collect_offset_int = (uint64_t)cp_to_collect_offset;
		//kp_debug("i=%ju, position=%ju, counter_xor=%X, "
		//		"position & counter_xor=%X, "
		//		"cp_to_collect_offset_int=%ju\n",
		//		i, position, (unsigned int)counter_xor,
		//		(unsigned int)(position & counter_xor),
		//		cp_to_collect_offset_int);
		/* If bit in this position flipped, collect the checkpoint at the
		 * "corresponding" position:
		 */
		if (position & counter_xor) {
			/* The first time a particular bit flips, cp_to_collect_offset_int
			 * will be less than the number of checkpoints we have so far
			 * (cp_num), so just ignore the first flip:
			 */
			if (cp_num > cp_to_collect_offset_int) {
				/* The checkpoint that we actually want to collect is
				 * cp_to_collect_offset_int checkpoints prior to the current
				 * cp_num. Mark this checkpoint as collectable by adding it
				 * to the garbage collector's vector:
				 */
				cp_to_collect = cp_num - cp_to_collect_offset_int;
				kp_debug("marking as collectable (by calling kp_gc_mark_"
						"cp_collectable()): checkpoint %ju back from "
						"cp_num=%ju: cp_to_collect=%ju\n",
						cp_to_collect_offset_int, cp_num, cp_to_collect);
				ret = kp_gc_mark_cp_collectable(gc, cp_to_collect);
				if (ret != 0) {
					kp_error("kp_gc_mark_cp_collectable returned error=%d\n",
							ret);
					return -1;
				}
			}
		}
		
		/* cp_to_collect_offset: advances by doubling and adding 1 every
		 * iteration.
		 *   NOTE: becomes negative in last iteration (i=63)! We'll probably
		 *   never hit this many checkpoints though... (TODO?)
		 * position: advances by doubling (invariant: position = (int)(exp2(i))).
		 */
		cp_to_collect_offset = (cp_to_collect_offset * factor) + 1;
		position *= 2;
	}
	kp_debug("done looping through all %ju bits of cp_num counter\n",
			num_bits);

	return 0;
}

/* Updates the set of checkpoint numbers to be collected by running a
 * "telescoping" algorithm. The set of checkpoint numbers to be collected
 * is stored inside of the kp_gc struct.
 * Returns: 0 on success, -1 on error.
 */
int kp_update_checkpoints_to_collect(kp_gc *gc, uint64_t cp_just_taken)
{
	int ret;
	uint64_t cp_num;
#ifdef KP_DEBUG
	uint64_t first_cp_num;
#endif

	/* To perform telescoping, we consider every checkpoint number (every
	 * value of the "counter") one at a time. We perform telescoping for
	 * every checkpoint in the range from the last time we did telescoping
	 * up to and including the checkpoint that was just taken:
	 */
	cp_num = gc->last_gc_snapshot + 1;
#ifdef KP_DEBUG
	first_cp_num = cp_num;
	kp_debug("performing telescoping for checkpoints in range [%ju,%ju]\n",
			first_cp_num, cp_just_taken);
#endif
	for ( ; cp_num <= cp_just_taken; cp_num++) {
		ret = kp_gc_perform_telescoping(gc, cp_num);
		if (ret != 0) {
			kp_error("telescoping failed (%d) for cp_num=%ju, stopping loop "
					"and returning error\n", ret, cp_num);
			return -1;
		}
	}
#ifdef KP_DEBUG
	kp_debug("finished performing telescoping for checkpoints in range "
			"[%ju,%ju]\n", first_cp_num, cp_just_taken);
#endif

	return 0;
}

/* Scans through the changeset for the indicated checkpoint and decrements
 * the ttl counter for each versioned value in the changeset. If a ttl
 * counter hits 0, that version of the value is then freed. If the last
 * version from a version table is freed, then the version table itself
 * will also be freed.
 * Returns: the number of DATA bytes that were freed (which may be 0), or
 * UINT64_MAX on error.
 */
uint64_t kp_gc_decrement_ttls(kp_kvstore *kv, uint64_t cp_to_collect)
{
#if 0
	int ret;
	uint64_t i, len, ret64, bytes_collected;
	kp_cs *cs_to_collect;
	kp_vt *vt;
	uint64_t gvn;
	void *element;
#ifdef KP_DEBUG
	char *cs_string;
#endif
#endif
	kp_die("old code - need to update / remove! kv=%p; cp_to_collect=%ju\n",
			kv, cp_to_collect);

#if 0
	if (!kv) {
		kp_error("kv is null\n");
		return UINT64_MAX;
	}

	/* Check that we're not trying to collect the active changeset! */
#ifdef KP_ASSERT
	if (cp_to_collect >= kv->global_snapshot) {
		kp_die("invalid cp_to_collect %ju, kv->global_snapshot is %ju\n",
				cp_to_collect, kv->global_snapshot);
	} else if (cp_to_collect == kv->global_snapshot - 1) {
		kp_warn("cp_to_collect (%ju) is just before active kv->global_snapshot "
				"(%ju); does this make sense??\n", cp_to_collect,
				kv->global_snapshot);
	}
#endif

	/* Get the changeset: */
	ret = kp_cs_log_get_cs_for_cp(kv->cs_log, cp_to_collect,
			&cs_to_collect);
	if (ret == 1) {
		kp_error("kp_cs_log_get_cs_for_cp(%ju) returned not-found; this is "
				"an error, should never happen\n", cp_to_collect);
		return UINT64_MAX;
	} else if (ret != 0 || cs_to_collect == NULL) {
		kp_error("kp_cs_log_get_cs_for_cp(%ju) failed, ret=%d\n",
				cp_to_collect, ret);
		return UINT64_MAX;
	}
//#ifdef KP_DEBUG
//	kp_debug("fetched changeset for cp_to_collect=%ju\n", cp_to_collect);
//	cs_string = kp_cs_to_string(cs_to_collect);
//	if (cs_string) {
//		kp_debug("cs_to_collect: %s\n", cs_string);
//		free(cs_string);
//	}
//#endif

	/* Loop through the changeset (it will not change while we're doing
	 * this):
	 */
	bytes_collected = 0;
	len = cs_to_collect->len;
	for (i = 0; i < len; i++) {
		gvn = vector64_get(cs_to_collect->gvns, i);
		vector_get(cs_to_collect->vts, i, &element);
		vt = (kp_vt *)element;
		if (!vt) {
			kp_die("got null vt pointer at index=%ju, gvn=%ju\n", i, gvn);
		}
		kp_debug("got vt pointer for key=%s, was modified in checkpoint "
				"%ju's changeset to create gvn=%ju\n", vt->key,
				cp_to_collect, gvn);
#ifdef KP_ASSERT
		if (vt->len == 0) {
			kp_die("vt has zero length!\n");
		}
#endif

		/* Lock the version table, then decrement the ttl value for this
		 * item in the changeset.
		 */
		kp_debug_lock("%s: locking version table\n", 
				vt->key);
		kp_mutex_lock("vt->lock", vt->lock);
		ret64 = kp_vt_decrement_ttl(vt, gvn);
		kp_debug_lock("%s: unlocking version table\n", 
				vt->key);
		kp_mutex_unlock("vt->lock", vt->lock);
		if (ret64 == UINT64_MAX) {
			kp_error("kp_vt_decrement_ttl() returned error\n");
			return UINT64_MAX;
		}
		bytes_collected += ret64;
		kp_debug("kp_vt_decrement_ttl returned %ju bytes collected for "
				"key=%s, gvn=%ju; total bytes_collected=%ju (for this "
				"changeset so far)\n", ret64,
				vt->key, gvn, bytes_collected);

		/* Free the version table if it is now empty: */
		kp_debug("vt->len (key=%s) is now %ju\n", vt->key, vt->len);
		if (vt->len == 0) {
			/* NOTE: currently (1/3/12), we should never hit this code,
			 * because if a VT exists, then there is at least one uncollected
			 * checkpoint that refers to the VT: the active checkpoint
			 * ALWAYS includes the last value in the VT (even if it's a
			 * tombstone). So, the ttl of the last value in the VT is
			 * never increased (although garbage-collection of previous
			 * checkpoints may decrease it), so the last value will never
			 * be collected and the VT's length will never reach 0. We may
			 * decide to adjust this when tombstone values are put into
			 * the VT for deletions, so that it is possible for VTs to
			 * eventually die.
			 */
			kp_debug("freeing version table because it has length 0\n");
			//TODO: possible race condition here where another writer has
			//  tried to put() to this vt since the time that we checked
			//  its length! Need to lock the vt and re-check? ...
			//TODO: call function that removes it from hash table, right?
			kp_die("not implemented yet: freeing empty version tables!\n");
		}
	}
#ifdef KP_ASSERT
	if (len != cs_to_collect->len) {
		kp_die("len of cs_to_collect changed from %ju to %ju\n", len,
				cs_to_collect->len);
	}
#endif

	kp_debug("finished decrementing ttl counters for checkpoint %ju's "
			"changeset, collected %ju total bytes\n", cp_to_collect,
			bytes_collected);

	/* Remove the changeset: we won't need it anymore.
	 * todo: we already hold a pointer to the changeset, so we could make
	 *   this slightly more efficient by directly freeing the changeset,
	 *   and then deleting it from the vectors, rather than passing the
	 *   cp_to_collect and having to perform a lookup again.
	 */
#if 0
	ret = kp_cs_log_delete_entry(kv->cs_log, cp_to_collect);
	if (ret != 0) {
		kp_error("kp_cs_log_delete_entry() returned error=%d\n", ret);
		return UINT64_MAX;
	}
#endif
	return bytes_collected;
#endif
}

/* Performs garbage collection steps that should happen every time a
 * checkpoint is taken. For now, this function only returns once the
 * steps are entirely completed; nothing happens asynchronously, yet.
 * This function does not return anything because it is called/performed
 * without the user's knowledge, so we don't want to return an error
 * code in some (somewhat-unrelated) function just because this one failed.
 * Therefore, if there are errors in this code, we either just print
 * warning/error messages (which will hopefully be read by somebody...),
 * or die completely (which probably only needs to happen if things get
 * so bad that we can't allocate any more memory).
 */
void kp_gc_on_checkpoint(kp_kvstore *kv, uint64_t cp_just_taken)
{
	int ret;
	uint64_t ret64;

	/* NOTE: this function (currently) may be performed by multiple
	 * workers concurrently (after they have each atomically incremented
	 * the checkpoint number in kp_take_checkpoint(), the function that
	 * calls this one).
	 * For safety, we currently lock the kp_gc struct (not the entire kvstore);
	 * this means that garbage collection will not happen concurrently for
	 * multiple workers. This seems reasonable: eventually GC will probably
	 * be performed by a dedicated thread, so we won't have to worry about it
	 * being performed concurrently. We shouldn't need to lock the entire
	 * kvstore to perform GC, as long as we lock each individual version table
	 * when we're deleting entries from it.
	 */
	kp_error("garbage collection not implemented yet!\n");
	return;

	kp_debug("calling kp_update_checkpoints_to_collect()\n");
	kp_mutex_lock("kv->gc->lock", kv->gc->lock);
	ret = kp_update_checkpoints_to_collect(kv->gc, cp_just_taken);
	if (ret != 0) {
		kp_error("kp_update_checkpoints_to_collect returned error=%d\n", ret);
		goto error;
	}

	kp_debug("running synchronous garbage collection: calling kp_run_gc() "
			"for cp_just_taken=%ju\n", cp_just_taken);
	ret64 = kp_run_gc(kv, cp_just_taken);
	if (ret64 == UINT64_MAX) {
		kp_error("kp_run_gc() returned error\n");
		goto error;
	}
	kp_mutex_unlock("kv->gc->lock", kv->gc->lock);
	kp_debug("kp_run_gc returned %ju bytes garbage-collected\n", ret64);

	kp_debug("per-checkpoint garbage collection steps complete, returning\n");
	return;

error:
	/* Print "FAIL" so that we'll be sure to catch this error message in test
	 * output:
	 */
	kp_error("FAIL: error occurred during garbage collection, undefined "
			"behavior may follow!\n");
	return;
}

int kp_iter_item_create_internal(kp_iter_item **item, bool use_nvm)
{
	if (!item) {
		kp_error("got null argument: item=%p\n", item);
		return -1;
	}
	if (use_nvm) {
		kp_die("not implemented yet: use_nvm!\n");
	}

	kp_kpalloc((void **)item, sizeof(kp_iter_item), use_nvm);
	if (!*item) {
		kp_error("kp_kpalloc(kp_iter_item) failed\n");
		return -1;
	}

	/* Set initial / default values; these will be filled in for realz
	 * by kp_version_iter_next(). */
	(*item)->use_nvm = use_nvm;
	(*item)->is_internal = true;  //always true, for now....
	(*item)->key = NULL;
	(*item)->value = NULL;
	(*item)->size = UINT_MAX;
	(*item)->lvn = UINT64_MAX;
	(*item)->snapshot = UINT64_MAX;
	(*item)->is_last_version = true;
	kp_flush_range((void *)(*item), sizeof(kp_iter_item), (*item)->use_nvm);

	kp_debug("created new kp_iter_item: use_nvm=%s, is_internal=%s, "
			"key=%p, value=%p, size=%zu, lvn=%ju, snapshot=%ju, "
			"is_last_version=%s\n", (*item)->use_nvm ? "true" : "false",
			(*item)->is_internal ? "true" : "false", (*item)->key,
			(*item)->value, (*item)->size, (*item)->lvn, (*item)->snapshot,
			(*item)->is_last_version ? "true" : "false");
	return 0;
}

void kp_iter_item_destroy(kp_iter_item **item)
{
#ifdef KP_DEBUG
	const char *key = NULL;
#endif

	if (!item) {
		kp_error("got a null argument: item=%p\n", item);
		return;
	}

	if (*item) {
#ifdef KP_DEBUG
		if ((*item)->is_internal) {
			key = (*item)->key;
		} else {
			key = NULL;
		}
#endif
		if (! (*item)->is_internal) {
			kp_warn("destroying a non-internal kp_iter_item - should we "
					"free the key, value, etc.?\n");
		}
		kp_free((void **)item, (*item)->use_nvm);  //sets *item = NULL
	}
	kp_debug("successfully freed the kp_iter_item (but not its key %s)\n",
			key);
}

#if 0
//For now: 0 = no operation (invalid?), 1 = put, 2 = get, 3 = merge.
int kp_kvstore_recover(kp_kvstore *kv, int operation)
{
	/* How should this function work??
	 * Need to figure out what operation was actually happening - hopefully
	 * the caller tells us.
	 * IDEA: maybe the caller doesn't have to tell us, but we try to cover
	 * from _every_ operation that could be in progress.
	 *   i.e. we call get(), then we call put(), then we call merge();
	 *   these functions know how to undo themselves (if not past the "commit
	 *   point" yet), or else know how to resume themselves. We can probably
	 *   indicate to these functions that we're in the middle of a recovery
	 *   by just simply passing NULL / invalid values (e.g. NULL key and NULL
	 *   value) into them - no need for a "bool is_recovery" flag.
	 * Operation: put:
	 *   Check kv->lookup_vt: if it's not set, then we don't need to do
	 *   anything (right?). If it's set, then get the last value from it;
	 *   if the last value has the expected gvn / snapshot (how do we know
	 *   what's expected??), then we know that the put completed, and we
	 *   should RESUME the put. Otherwise, we should UNDO the in-progress
	 *   put.
	 *   UNDO: check the size of the VT; if it's zero, then it means that
	 *   we just allocated it, so destroy it and then we're done. If the
	 *   VT already has other items in it, then we don't have to do
	 *   anything.
	 *   RESUME: do a hash table get() on the VT's key; if it's present,
	 *   then we know that the hash table is all set. Otherwise, we need
	 *   to insert again...
	 *     Idea: this is code that kp_put_internal() is already doing.
	 *     So, can we just call it again directly, and slightly modify
	 *     its logic (possibly with a "bool is_recovery" argument) to
	 *     do this automatically?
	 *       YES!
	 */
	//CHECK...
	kp_die("not implemented yet!\n");
}
#endif

/***********************/
/* External functions: */
/***********************/
int kp_kvstore_create(kp_kvstore **kv, bool is_master,
		size_t expected_max_keys, bool do_conflict_detection, bool use_nvm)
{
	int ret;
	void (*hash_freer)(void *) = NULL;    //function pointer

	/* Allocate: */
	kp_kpalloc((void **)kv, sizeof(kp_kvstore), use_nvm);
	if (*kv == NULL) {
		kp_error("malloc failed\n");
		return -1;
	}

	/* Initialize. ... */
	kp_kvstore_count++;
	PM_EQU(((*kv)->id), (kp_kvstore_count)); // persistent
	PM_EQU(((*kv)->pairs_count), (0));		// persistent
	PM_EQU(((*kv)->detect_conflicts), (do_conflict_detection)); 	// persistent
	PM_EQU(((*kv)->global_snapshot), (INITIAL_GLOBAL_SNAPSHOT));	// persistent
	if (is_master) {
		ret = kp_mutex_create("(*kv)->snapshot_lock", &((*kv)->snapshot_lock));
		if (ret != 0) {
			kp_error("kp_mutex_create() returned error: %d\n", ret);
			return -1;
		}
	} else {
		PM_EQU(((*kv)->snapshot_lock), (NULL)); // persistent
		kp_debug("skipping allocation of snapshot_lock and master_lock "
				"for local store\n");
	}

	if (is_master) {
#ifndef UNSAFE_COMMIT
		ret = vector_create(&((*kv)->commit_log), 0, use_nvm);
#else
		ret = vector_create(&((*kv)->commit_log),
				(unsigned long long)UNSAFE_COMMIT_LOG_SIZE, use_nvm);
#endif
		if (ret != 0) {
			kp_error("vector_create((*kv)->commit_log) returned error=%d\n", ret);
			return -1;
		}
	} else {
		PM_EQU(((*kv)->commit_log), (NULL)); // persistent
		kp_debug("skipping allocation of (*kv)->commit_log for local store\n");
	}

	if (is_master) {
		ret = kp_gc_create(&((*kv)->gc), use_nvm);
		if (ret != 0) {
			kp_error("kp_gc_create() failed %d\n", ret);
			return -1;
		}
	} else {  //local stores don't need a garbage collector (right?)
		kp_debug_gc("skipping (*kv)->gc allocation for local store!\n");
		PM_EQU(((*kv)->gc), (NULL));
	}
	PM_EQU(((*kv)->is_master), (is_master)); // persistent
	PM_EQU(((*kv)->use_nvm), (use_nvm));	// persistent 
	kp_debug("new kp_kvstore: setting detect_conflicts=%s, is_master=%s, and "
			"use_nvm=%s (all of these are args from caller)\n",
			(*kv)->detect_conflicts ? "true" : "false",
			(*kv)->is_master ? "true" : "false", (*kv)->use_nvm ? "true" : "false");
#ifdef KP_ASSERT
	if ((*kv)->is_master && ! (*kv)->use_nvm) {
		kp_warn("use_nvm is false: double-check this implementation!\n");
	}
#endif

	/* Create the hash table. See hash.c:85 for a general description
	 * of the hash table (basic hash table with linear chaining to handle
	 * collisions and automatic re-hashing).
	 * The hash table stores pointers to arbitrary objects; we store
	 * pointers to "kp_ht_entry" structs, which store the key (a string)
	 * and a pointer to the key's version table.
	 * The first argument is the "candidate" size of the hash table. When we
	 * use the default tuning struct (see below), the hash table states that
	 * "The initial number of buckets is automatically selected so as to
	 * _guarantee_ that you may insert at least CANDIDATE different user
	 * entries before any growth of the hash table size occurs."
	 * The second argument is a "hash tuning" struct, which contains parameters
	 * for the growth threshold (% of buckets that must be filled before
	 * table will be resized), growth factor, shrink threshold, and shrink
	 * factor; see hash.c:110 for a detailed description of these parameters.
	 * We pass NULL to select the default hash tuning, which doubles the hash
	 * table size when 80% of the buckets/slots are in use, and which never
	 * shrinks the table.
	 * The third argument is a hash function, which hashes an entry to
	 * an unsigned integer (up to the size of the hash table) that represents
	 * the entry's index into the table. We specify our own hash function
	 * that hashes the string key in the kp_ht_entry.
	 * The fourth argument is a comparison function. We specify our own
	 * comparison function that performs string comparison on the keys in
	 * the kp_ht_entry structs.
	 * The fifth argument is a "freer" function that will be called on an
	 * entry when it is freed from the hash table (either hash_clear(), which
	 * frees all of the inserted entries but leaves the hash table itself
	 * intact, or hash_free(), which destroys the entire hash table). We
	 * specify our own freer function that frees our kp_ht_entry structs
	 * AND the version tables that they point to.
	 */
	if ((*kv)->is_master) {
		hash_freer = kp_ht_entry_free_callback;
	} else {
		hash_freer = kp_ht_entry_free_local;
	}
	ret = hash_initialize(&((*kv)->ht), expected_max_keys, NULL, kp_ht_hasher,
			kp_ht_comparator, hash_freer, use_nvm);
	if (ret != 0 || (*kv)->ht == NULL) {
		kp_error("hash_initialize failed\n");
		//todo: should free(kv) here (and elsewhere in this function) before returning
		return -1;
	}

	ret = kp_rwlock_create("(*kv)->rwlock", &((*kv)->rwlock));
	if (ret != 0) {
		kp_error("kp_rwlock_create returned error: %d\n", ret);
		return -1;
	}

	/* CDDS: flush, set state, and flush again. Flushes will be skipped if
	 * FLUSH_IT is not defined. */
	kp_flush_range((void *)*kv, sizeof(kp_kvstore) - sizeof(ds_state), (*kv)->use_nvm);
	PM_EQU(((*kv)->state), (STATE_ACTIVE)); // persist
	kp_flush_range((void *)&((*kv)->state), sizeof(ds_state), (*kv)->use_nvm);

	return 0;
}

void kp_kvstore_destroy(kp_kvstore **kv)
{
	/* External-facing function: ALWAYS free the values. */
	kp_kvstore_destroy_internal(kv, true);
}

/* Called by local stores only! */
uint64_t kp_put(kp_kvstore *kv, const kp_ht_entry *lookup_entry,
		const void *value, size_t size, uint64_t local_snapshot)
{
	int ret;
	uint64_t lvn;

	if (value == NULL) {
		kp_error("value pointer is NULL\n");
		return UINT64_MAX;
	}

	lvn = UINT64_MAX;
	if (kv->is_master) {  //this is a merge!
		kp_die("DEAD_CODE: should never hit this code path, kp_put_master() "
				"now called directly from merge code\n");
	} else {  //this is a put into a local store.
		ret = kp_put_local(kv, lookup_entry, value, size, &lvn, local_snapshot);
	}
	if (ret != 0) {
		if (ret == 1) {  /* should only be returned for deletions */
			kp_die("unexpected return value %d from kp_put_internal()\n",
					ret);
		}
		kp_error("kp_put_internal() returned error %d\n", ret);
		return UINT64_MAX;
	}

	/* Success: return current LVN for the key. */
#ifdef KP_ASSERT
	if (lvn == UINT64_MAX) {  /* sanity check */
		kp_die("internal error: lvn is UINT64_MAX (%ju)\n", lvn);
	}
#endif
	return lvn;
}

/* Called by local worker on its local kvstore, to get most-recent version
 * from the local store. */
int kp_get(kp_kvstore *kv, const kp_ht_entry *lookup_entry, void **value,
		size_t *size)
{
	int ret;
	uint64_t lvn, snapshot;

	/* kp_get() and kp_get_version_snapshot() should be duals. */
	lvn = UINT64_MAX;
	snapshot = UINT64_MAX;
	ret = kp_get_copy(kv, lookup_entry, lvn, snapshot, value, size);
	if (ret > 0 && (ret != 1 && ret!= 3)) {  //don't expect version-not-found
		kp_die("internal error: unexpected return value from "
				"kp_get_copy (%d)\n", ret);
	}
	return ret;
}

int kp_get_version_snapshot(kp_kvstore *kv, const kp_ht_entry *lookup_entry,
		uint64_t snapshot, void **value, size_t *size)
{
	int ret;
	uint64_t lvn;

	/* kp_get() and kp_get_version_snapshot() should be duals. */
	lvn = UINT64_MAX;
	kp_debug("calling kp_get_copy() with snapshot=%ju\n", snapshot);
	ret = kp_get_copy(kv, lookup_entry, lvn, snapshot, value, size);
	if (ret > 0 && (ret != 1 && ret != 2 && ret!= 3)) {
		kp_die("internal error: unexpected return value from "
				"kp_get_copy (%d)\n", ret);
	}
	return ret;
}

uint64_t kp_is_present(kp_kvstore *kv, const char *key)
{
	kp_die("Not implemented yet; kv=%p, key=%s\n", kv, key);
	return -1;
}

int kp_delete_key(kp_kvstore *kv, const kp_ht_entry *lookup_entry,
		uint64_t local_snapshot)
{
	int ret;
	void *value;
	size_t size;
	uint64_t lvn;

	if (!kv || !lookup_entry) {
		kp_error("got a null argument: kv=%p, lookup_entry=%p\n",
				kv, lookup_entry);
		return -1;
	}
#ifdef KP_ASSERT
	if (kv->is_master) {  //this is a merge!
		kp_die("this function should only be called for local stores, "
				"not master store!\n");
	}
#endif
	kp_debug_gc("entered - lookup_entry->key=%s, local_snapshot=%ju\n",
			lookup_entry->key, local_snapshot);

	/* Deletions are treated as a put of a "tombstone" value, which is
	 * represented by the combination of a NULL value pointer with size 0.
	 */
	value = NULL;
	size = 0;
	lvn = UINT64_MAX;

	kp_debug_gc("calling kp_put_local() with value=%p and size=%zu; this is "
			"where the (local) code path for deletes joins with the code "
			"path for local puts\n", value, size);
	ret = kp_put_local(kv, lookup_entry, value, size, &lvn, local_snapshot);
	if (ret != 0) {
		if (ret == 1) {
			kp_die("we used to return 1 from kp_put_local if the key for "
					"a deletion was not-found, but no longer expect this "
					"here (as of version 3.2)!\n");
		}
		kp_error("kp_put_local() returned error=%d\n", ret);
		return -1;
	}

	/* Ignore lvn that's returned, just return success. */
	kp_debug_gc("kp_put_local() returned lvn=%ju, but ignoring it and just "
			"returning 0 now.\n", lvn);
	return 0;
}

uint64_t kp_get_latest_snapshot(kp_kvstore *kv)
{
	if (!kv) {
		kp_error("kv is null\n");
		return UINT64_MAX;
	}
#ifdef KP_ASSERT
	if (kv->global_snapshot == 0 || kv->global_snapshot == UINT64_MAX) {  //initial snapshot is 1
		kp_die("internal error: snapshot number hit UINT64_MAX!\n");
	}
#endif
	return kv->global_snapshot;
}

bool kp_uses_nvm(kp_kvstore *kv)
{
	if (kv) {
		return kv->use_nvm;
	} else {
		kp_error("got NULL kv, returning false!\n");
	}
	return false;
}

/* IMPORTANT: currently, this function expects to be called with the gc
 * struct already locked!
 * NOTE: this function does not call kp_update_checkpoints_to_collect();
 * it will only perform garbage collection for checkpoints that have
 * already been marked as collectable.
 */
uint64_t kp_run_gc(kp_kvstore *kv, uint64_t cp_just_taken)
{
	kp_gc *gc;
	int ret;
	uint64_t i, ret64;
	uint64_t count, count_already_collected;
	uint64_t prev_gc_cp_num, cp_to_collect;
	uint64_t bytes_collected;

	kp_die("old code: need to update / remove!\n");

	if (!kv || !kv->gc) {
		kp_error("kv or kv->gc is null!\n");
		return UINT64_MAX;
	}

	/* Note that kv->global_snapshot tracks the "active" checkpoint number which is
	 * still being modified, so we use the cp_just_taken argument throughout
	 * this function to tell us the LAST checkpoint number that we want
	 * to perform GC on. Without any concurrent checkpointing strangeness,
	 * we expect cp_just_taken to equal kv->global_snapshot - 1...
	 */
	gc = kv->gc;
	prev_gc_cp_num = gc->last_gc_snapshot;
	kp_debug("prev_gc_cp_num = gc->last_gc_snapshot = %ju; cp_just_taken = "
			"%ju, kv->global_snapshot = %ju (probably == cp_just_taken+1)\n",
			prev_gc_cp_num, cp_just_taken, kv->global_snapshot);
	if (cp_just_taken == prev_gc_cp_num) {
		/* We might expect to hit this case if we perform garbage-collection
		 * when each checkpoint is taken while allowing checkpoints to be
		 * taken concurrently by multiple workers (right now, only the
		 * checkpoint number increment is locked/atomic; the GC stuff that
		 * happens afterwards may be concurrent (?)).
		 */
		kp_warn("checkpoint number has not advanced since the last time "
				"we garbage-collected; this is probably ok, so just returning "
				"from this function\n");
		return 0;
	}
#ifdef KP_ASSERT
	else if (cp_just_taken < gc->last_gc_snapshot) {
		kp_die("internal error: cp_just_taken=%ju < last_gc_snapshot=%ju\n",
				cp_just_taken, gc->last_gc_snapshot);
	}
#endif

	/* Sweep through the list (vector) of checkpoints to collect. For each
	 * of these checkpoints, decrement the counters of all of the versioned
	 * values in the checkpoint's changeset. Keep track of the total number
	 * of bytes collected. Because the gc struct is locked when this function
	 * is called, we don't have to worry about the cps_to_collect struct
	 * changing underneath us.
	 */
	bytes_collected = 0;
	count = vector64_count(gc->cps_to_collect);
	count_already_collected = vector64_count(gc->cps_collected);
	kp_debug("sweeping through gc->cps_to_collect: count=%ju, "
			"count_already_collected=%ju\n", count, count_already_collected);
	for (i = 0; i < count; i++) {
		cp_to_collect = vector64_get(gc->cps_to_collect, i);
		kp_debug("got cp_to_collect=%ju from vector64\n", cp_to_collect);
		ret64 = kp_gc_decrement_ttls(kv, cp_to_collect);
		if (ret64 == UINT64_MAX) {
			kp_error("kp_gc_decrement_ttls() returned error\n");
			return UINT64_MAX;
		}
		bytes_collected += ret64;
		kp_debug("kp_gc_decrement_ttls() freed %ju data bytes for cp %ju, "
				"now bytes_collected=%ju (total for this GC pass so far)\n",
				ret64, cp_to_collect, bytes_collected);

		/* Store the checkpoint numbers that we've collected: */
		ret = vector64_append(gc->cps_collected, cp_to_collect);
		if (ret != 0) {
			kp_error("vector64_append(cps_collected) failed\n");
			return UINT64_MAX;
		}
	}

#ifdef KP_ASSERT
	if (vector64_count(gc->cps_collected) != count + count_already_collected) {
		kp_die("new cps_collected count %ju doesn't match sum of count %ju "
				"and count_already_collected %ju\n",
				vector64_count(gc->cps_collected), count,
				count_already_collected);
	}
#endif
	kp_debug("finished looping through cps_to_collect vector, total "
			"bytes_collected=%ju for this GC pass\n", bytes_collected);

	/* Reset the cps_to_collect vector. This assumes that the gc struct is
	 * already locked (currently, it should be locked before this function
	 * is called):
	 */
	if (count > 0) {  //only necessary if cps_to_collect had any elements
		vector64_destroy(&(gc->cps_to_collect));
		//Commented out: vector64_create() doesn't exist yet.
		//ret = vector64_create(&(gc->cps_to_collect));
		ret = 0;
		if (ret != 0) {
			kp_error("could not allocate new cps_to_collect vector64, ret=%d\n",
					ret);
			return UINT64_MAX;
		}
		kp_debug("freed previous cps_to_collect vector and allocated an empty "
				"one\n");
	}

	gc->last_gc_snapshot = cp_just_taken;
	kp_debug("updated gc->last_gc_snapshot from %ju to %ju (cp_just_taken)\n",
			prev_gc_cp_num, gc->last_gc_snapshot);
#ifdef KP_ASSERT
	if (gc->last_gc_snapshot <= prev_gc_cp_num) {
		kp_error("last_gc_snapshot=%ju <= prev_gc_cp_num=%ju\n",
				gc->last_gc_snapshot, prev_gc_cp_num);
	}
#endif

	return bytes_collected;
}

int kp_write_stream(FILE *stream, char *msg){
	int len, ret;
	/* Write to stream. For details about streams, see "man stdio.h". Also:
	 * http://www.chemie.fu-berlin.de/chemnet/use/info/libc/libc_7.html
	 */
	len = strlen(msg) + 1;
	ret = fwrite((void *)msg, sizeof(char), len, stream);
	if (ret != len) {
		kp_error("fwrite() only wrote %d of %d characters\n",ret, len);
		//don't fail
	}
	ret = fflush(stream);
	if (ret != 0) {
		kp_error("fflush() returned error %d\n", ret);
		// don't fail
	}

	return ret;
}

#if 0
void kp_print_sweep_vt(void *ht_key, void *ht_value, void *user_data)
{
	//kp_vte *get_vte;
	kp_vte *get_vte1;
	kp_vt *vt = (kp_vt *)ht_value;
	stat_accumulator_t *stats = (stat_accumulator_t *)user_data;
	FILE *fp = stats->stream;
	char msg[KP_MSG_SIZE];

	kp_die("re-do this function!\n");
	kp_warn("this function probably needs updating after the addition of "
			"cp_nums / snapshots!\n");

	kp_debug_lock("%s: locking version table\n", 
			vt->key);
	kp_mutex_lock("vt->lock", vt->lock);

	if(stats->detailed){
		snprintf(msg, KP_MSG_SIZE,"This is key %s\n", vt->key);
		kp_write_stream(fp, msg);
		snprintf(msg, KP_MSG_SIZE,"\t...sanity check: we have added %ju "
				"versions, %ju of them are still stored in version table\n",
				vt->ver_count, vt->len);
		kp_write_stream(fp, msg);
	}

	// PJH: commented out this code to eliminate signed vs. unsigned
	// comparison warnings; this code is mostly obsolete anyway
	//if(vt->ver_count > stats->max_vt_versions)
	//	stats->max_vt_versions = vt->ver_count;
	//if(vt->ver_count < stats->min_vt_versions)
	//	stats->min_vt_versions = vt->ver_count;

	stats->vt_touched++;
	stats->total_versions_alive += vt->ver_count;
	stats->avg_vt_versions =  stats->total_versions_alive/
		(float)stats->vt_touched;

	//int small_index = (int)vector64_get(vt->gvns, 0);
	//int big_index = (int)vector64_get(vt->gvns, vt->len-1);
	//if(small_index < stats->min_gvn_alive)
	//	stats->min_gvn_alive = small_index;
	//if(big_index > stats->max_gvn_alive)
	//	stats->max_gvn_alive = big_index;

	vector_get(vt->vtes, vt->len-1, (void **)(&get_vte1));
	int current_size = (int)(get_vte1->size);
	stats->bytes_in_current_versions += current_size;
	stats->bytes_in_metadata += sizeof(size_t) + //size
		sizeof(uint64_t) + // gvn 
		sizeof(uint64_t); //lvn

	// PJH: commented out this code to eliminate signed vs. unsigned
	// comparison warnings; this code is mostly obsolete anyway
	//int i;
	//for(i = 0; i < vt->len-1; i++){
	//	vector_get(vt->vtes, i, (void **)(&get_vte));
	//	current_size = (int)(get_vte->size);
	//	stats->bytes_in_old_versions += current_size;
	//	stats->bytes_in_metadata += sizeof(size_t) + //size
	//		sizeof(uint64_t) + // gvn 
	//		sizeof(uint64_t); //lvn
	//}

	stats->bytes_in_metadata += strlen(vt->key) +1 +
		sizeof(pthread_mutex_t) +     //lock
		vector_struct_size() +        //values
		(4*vector64_struct_size()) +  //sizes, gvns, lvns, cp_nums
		sizeof(kp_vt);

	kp_debug_lock("%s: unlocking version table\n", vt->key);
	kp_mutex_unlock("vt->lock", vt->lock);
}
#endif

int kp_print_stats(kp_kvstore *kv, FILE *stream, bool detailed)
{
	time_t the_time;
	char msg[KP_MSG_SIZE];
	stat_accumulator_t stats;

	kp_die("re-do this function!\n");

	stats.stream = stream;
	stats.detailed = detailed;

	stats.avg_vt_versions = 0;

	stats.min_vt_versions = 
		stats.min_gvn_alive = INT_MAX;

	stats.vt_touched = 
		stats.max_vt_versions = 
		stats.max_gvn_alive = 
		stats.total_versions_alive =0;

	stats.bytes_in_current_versions = 
		stats.bytes_in_old_versions = 
		stats.bytes_in_metadata = 
		stats.bytes_wasted = 0;


	/* Construct message: */
	the_time = time(NULL);
	snprintf(msg, KP_MSG_SIZE, "%s", 
			ctime(&the_time));
	//TODO: make this more robust: not a fixed-size buffer!?
	kp_debug("msg=[%s]\n", msg);
	kp_write_stream(stream, msg);

	kp_rdlock("kv->rwlock", kv->rwlock);
	/* Introduction */
	snprintf(msg, KP_MSG_SIZE, "Printing memory usage for KP_KV #%d\n",
			kv->id);
	kp_write_stream(stream, msg);

	snprintf(msg, KP_MSG_SIZE, "\t The kp_kvstore contains %ju keys\n",
			kv->pairs_count);
	kp_write_stream(stream, msg);

	/* Collect data */
	stats.stream = stream;
	stats.detailed = detailed;
	void *data = (void *)&stats;
	hash_do_for_each(kv->ht, kp_ht_process_stats, data);

	/* Global break down */
	snprintf(msg, KP_MSG_SIZE, 
			"I see %d version tables and %d versions\n",
			stats.vt_touched,
			stats.total_versions_alive);
	kp_write_stream(stream, msg);
	snprintf(msg, KP_MSG_SIZE, 
			"That means the GVNs breakdown to:\n");
	kp_write_stream(stream, msg);
	snprintf(msg, KP_MSG_SIZE, 
			"\t%d put-inserts\n",
			stats.vt_touched);
	kp_write_stream(stream, msg);
	snprintf(msg, KP_MSG_SIZE, 
			"\t%d put-updates or key_deletes\n",
			stats.total_versions_alive - stats.vt_touched);
	kp_write_stream(stream, msg);
	//snprintf(msg, KP_MSG_SIZE, 
	//		"\tand %d local versions were garbage collected\n", 
	//		(int)kv->gvn - stats.total_versions_alive);
	//kp_write_stream(stream, msg);

	/* Version Tables */
	snprintf(msg, KP_MSG_SIZE,
			"The version tables have the following characteristics:\n");
	kp_write_stream(stream, msg);
	snprintf(msg, KP_MSG_SIZE,
			"\tThe largest version table has %d entries\n",
			stats.max_vt_versions);
	kp_write_stream(stream, msg);
	snprintf(msg, KP_MSG_SIZE,
			"\tThe smallest version table has %d entries\n",
			stats.min_vt_versions);
	kp_write_stream(stream, msg);
	snprintf(msg, KP_MSG_SIZE,
			"\tThe average version table has %f entries\n",
			stats.avg_vt_versions);
	kp_write_stream(stream, msg);

	/* Garbage Collection */
	snprintf(msg, KP_MSG_SIZE,
			"The garbage collection has the following properties:\n");
	kp_write_stream(stream, msg);
	snprintf(msg, KP_MSG_SIZE,
			"\tThe oldest gvn waiting for collection is %d\n",
			stats.min_gvn_alive);
	kp_write_stream(stream, msg);
	snprintf(msg, KP_MSG_SIZE,
			"\tThe youngest gvn waiting for collection is %d\n",
			stats.max_gvn_alive);
	kp_write_stream(stream, msg);

	/* Memory Footprint */
	snprintf(msg, KP_MSG_SIZE,
			"The overall memory footprint looks something like this:\n");
	kp_write_stream(stream, msg);
	snprintf(msg, KP_MSG_SIZE,
			"\t%ld bytes in current versions\n", 
			stats.bytes_in_current_versions);
	kp_write_stream(stream, msg);
	snprintf(msg, KP_MSG_SIZE,
			"\t%ld bytes in older versions\n", 
			stats.bytes_in_old_versions);
	kp_write_stream(stream, msg);
	snprintf(msg, KP_MSG_SIZE,
			"\t%ld bytes in version metadata\n", 
			stats.bytes_in_metadata);
	kp_write_stream(stream, msg);

	/* Get garbage collection overhead */
	long gc_data = 0;   //todo: repair this!

	stats.bytes_in_metadata+= gc_data;
	snprintf(msg, KP_MSG_SIZE,
			"\t%ld bytes in garbage collection metadata\n", 
			gc_data);
	kp_write_stream(stream, msg);

	/* Get total overhead */
	long global_data = 
		sizeof(kp_kvstore) +
		hash_get_hash_table_size() +                      //hash table itself
		hash_get_n_entries(kv->ht)*sizeof(kp_ht_entry) +  //hash table entries
		sizeof(pthread_mutex_t) +
		sizeof(pthread_rwlock_t);
		  //todo: hash table entries include just a char * and a void *; make
		  //  sure that the sizes of the data these point to are added
		  //  somewhere else (kp_print_sweep_vt(), right??)
	stats.bytes_in_metadata+= global_data;
	snprintf(msg, KP_MSG_SIZE,
			"\t%ld bytes in global metadata\n", 
			global_data);
	kp_write_stream(stream, msg);

	snprintf(msg, KP_MSG_SIZE,
			"\t%ld bytes in total metadata\n", 
			stats.bytes_in_metadata);
	kp_write_stream(stream, msg);

	/* TODO wasted space */
	snprintf(msg, KP_MSG_SIZE,
			"\tI haven't figured out how to count the %ld bytes wasted\n", 
			stats.bytes_wasted);
	kp_write_stream(stream, msg);

	kp_rdunlock("kv->rwlock", kv->rwlock);

	return 0;
}

#if OLD_CODE
int kp_iterator_get(kp_kvstore *kv, kp_iterator **iter, bool is_internal,
		bool use_nvm)
{
	kp_die("dead code?\n");
	if (!kv || !iter) {
		kp_error("got a null argument: kv=%p, iter=%p\n", kv, iter);
		return -1;
	}

	if (!is_internal) {
		kp_die("not implemented yet: external iterators!\n");
	}

	/* For iterators, we ALWAYS store them on volatile memory - they are
	 * certainly reconstructable state, so we don't want to pay the performance
	 * penalty for durability.
	 *   (Is this really true? If we fail and have to recover while something
	 *    is being iterated over, can we easily get back to where we were? As
	 *    long as the state that we're iterating over (and potentially
	 *    changing) is durable, then should be able to.)
	 */
	kp_debug("entered; got is_internal=%s, using use_nvm=%s\n",
			is_internal ? "true" : "false", use_nvm ? "true" : "false");
	if (use_nvm) {
		kp_die("not supported (yet?): use_nvm for iterators!\n");
	}

	if (!*iter) {
		kp_die("*iter is NULL, not implemented yet: allocating a new iter "
				"in this function?\n");
		kp_kpalloc((void **)iter, sizeof(kp_iterator), use_nvm);
		if (*iter == NULL) {
			kp_error("kp_kpalloc(kp_iterator) returned error\n");
			return -1;
		}
	} else {
		kp_debug("using the *iter that was passed to us (%p) and just "
				"setting its members\n");
	}

	/* Our hash table supports "hash_get_first()", which gets the first
	 * entry for walking / iterating over the hash table, and "hash_get_next()",
	 * which is called repeatedly after that. So, to keep track of the
	 * current iterator state, we only have to store a kp_ht_entry struct.
	 */
	kp_warn("this function doesn't appear to do much - can we skip it?\n");
	(*iter)->kv = kv;
	(*iter)->cur_entry = NULL;       //will be set on first kp_iterator_next()
	(*iter)->is_internal = is_internal;
	(*iter)->is_empty = false;
	(*iter)->is_invalidated = false;
	(*iter)->vt = NULL;
	(*iter)->vt_len = UINT64_MAX;    //"-1" for uint64_t type
	(*iter)->vt_index = UINT64_MAX;  //"-1" for uint64_t type
	(*iter)->use_nvm = use_nvm;

	//CHECK - TODO: if use_nvm is true, then do we need to add a ds_state
	//  member to this struct?

	kp_flush_range((void *)*iter, sizeof(kp_iterator), use_nvm);

	return 0;
}
#endif

int kp_iterator_set(kp_kvstore *kv, kp_iterator *iter, bool is_internal)
{
	int ret;

	if (!kv || !iter) {
		kp_error("got a null argument: kv=%p, iter=%p\n", kv, iter);
		return -1;
	}
	if (!is_internal) {
		kp_die("not implemented yet: external iterators!\n");
	}
	if (iter->use_nvm) {
		/* For iterators, we ALWAYS store them on volatile memory - they are
		 * certainly reconstructable state, so we don't want to pay the performance
		 * penalty for durability.
		 *   (Is this really true? If we fail and have to recover while something
		 *    is being iterated over, can we easily get back to where we were? As
		 *    long as the state that we're iterating over (and potentially
		 *    changing) is durable, then should be able to.)
		 */
		kp_die("use_nvm is true: is this expected?\n");
	}

	kp_debug("entered, got a kv pointer and is_internal=%s; preserving "
			"iter->use_nvm=%s, resetting version iterator's data, and "
			"resetting all other values to defaults\n",
			is_internal ? "true" : "false", iter->use_nvm ? "true" : "false");

	/* Our hash table supports "hash_get_first()", which gets the first
	 * entry for walking / iterating over the hash table, and "hash_get_next()",
	 * which is called repeatedly after that. So, to keep track of the
	 * current iterator state, we only have to store a kp_ht_entry struct.
	 *
	 * See kp_iterator_next2() for what is expected of a "reset" iterator.
	 * IMPORTANT: we shouldn't set the v_iter pointer to NULL: internal
	 * iterators pre-allocate the version_iter, and so we just want to
	 * reset its values by calling kp_version_iter_reset()!
	 *   **This may change if/when we support non-is_internal iterators**
	 */
	/* (Preserve iter->use_nvm.) */
	iter->kv = kv;
	iter->cur_entry = NULL;       //MUST be null; set on first kp_iterator_next2()
	iter->is_internal = is_internal;
	iter->is_empty = false;
	iter->is_invalidated = false;
	ret = kp_version_iter_reset(iter->v_iter);
	kp_flush_range((void *)iter, sizeof(kp_iterator), iter->use_nvm);
	if (ret != 0) {
		kp_error("kp_version_iter_rest() returned error=%d\n", ret);
		return -1;
	}

	//CHECK: if use_nvm is ever true, then do we need to add a ds_state
	//  member to this struct?

	return 0;
}

//int kp_iterator_get(kp_kvstore *kv, kp_iterator **iter)
//{
//	kp_die("not implemented yet!\n");
//
//	return kp_iterator_get_internal(kv, iter, false);
//}

#if 0
bool kp_iterator_has_next(kp_iterator *iter)
{
	kp_die("not implemented yet!\n");

	if (iter == NULL) {
		kp_error("iter is NULL\n");
		return false;
	}

	return true;
}
#endif

#ifdef OLD_CODE
int kp_iterator_next(kp_iterator *iter, kp_iter_item **item)
{
	/* Public version: make copies of key/value, don't reveal pointers
	 * into our internal memory.
	 */
	return _kp_iterator_next(iter, item, true);
}

int kp_iterator_next_internal(kp_iterator *iter, kp_iter_item **item)
{
	/* Private/internal version: don't make copies of the key/value,
	 * just copy the pointers to them.
	 */
	return _kp_iterator_next(iter, item, false);
}
#endif

#define KP_MAX_ITEM_LEN 256
char *kp_iter_item_to_string(kp_iter_item *item)
{
	char *str = malloc(KP_MAX_ITEM_LEN);
	if (str == NULL) {
		return NULL;
	}

	snprintf(str, KP_MAX_ITEM_LEN, "iter_item: is_internal=%s, key=%s, "
			"value=%s, size=%zu, lvn=%ju, snapshot=%ju, is_last_version=%s",
			item->is_internal ? "true" : "false", item->key,
			(char *)(item->value), item->size, item->lvn, item->snapshot,
			item->is_last_version ? "true" : "false");

	return str;
}

/* See comments in kp_kvstore_internal.h. */
int kp_iterator_next2(kp_iterator *iter, kp_iter_item **item)
{
	int ret, retval;

	if (!iter || !item) {
		kp_error("got a null argument: iter=%p, item=%p\n", iter, item);
		return -1;
	}
#ifdef KP_ASSERT
	if (!iter->kv) {
		kp_error("got a null argument: iter->kv=%p\n", iter->kv);
		return -1;
	}
	if (!iter->v_iter) {
		kp_die("this function should already have a pre-allocated version "
				"iterator (v_iter)\n");
	}
	if (!*item) {
		kp_die("this function currently expects the kp_iter_item to be "
				"already pre-allocated by the caller!\n");
	}
	if (iter->kv->is_master) {
		kp_die("this function only implemented/checked for local stores "
				"so far! For master stores, we need to figure out if/how "
				"to check for invalidation.\n");
	}
	if (!iter->v_iter) {
		kp_die("iter->v_iter should be pre-allocated before this function "
				"is called! (when is_internal is true anyway).\n");
	}
#endif

#ifdef DOESNT_WORK_ANYMORE
	if (iter->vt) {
		kp_debug("kp_iterator at beginning of function: kv->id=%d, "
				"vt->key=%s, vt_len=%ju, vt_index=%ju\n",
				iter->kv->id, iter->vt->key, iter->vt_len, iter->vt_index);
	} else {
		kp_debug("kp_iterator at beginning of function: kv->id=%d, "
				"vt=NULL, vt_len=%ju, vt_index=%ju\n",
				iter->kv->id, iter->vt_len, iter->vt_index);
	}
#endif

	/* Check if the iterator is already empty or invalidated: */
	if (iter->is_empty) {
		kp_debug("iterator is already empty, returning 1\n");
		return 1;
	}
	if (iter->is_invalidated) {
		kp_debug("iterator has already been invalidated, returning -2\n");
		return -2;
	}

	/* Currently, this function doesn't check if the iterator has been
	 * invalidated by a put to the underlying kvstore because it is
	 * only being used for local stores right now, where we are assuming
	 * that there is only a single worker thread, and so the store is
	 * implicitly unchanged while iteration is happening during a commit /
	 * merge. */
	kp_debug("skipping checks for invalidation...\n");

	/* Here we advance the hash table iterator (iter->cur_entry) to the next
	 * key/entry. We know that the iterator needs advancing in the initial
	 * case when the version iterator has not been set, or when the version
	 * iterator is empty. */
	if (!(iter->v_iter->is_set) || iter->v_iter->is_empty) {
		kp_debug("reached end of a VT (or initial case), getting next key\n");
		if (iter->cur_entry == NULL) {  //very first call to this function
			/* This function may return NULL if the hash table is empty. */
			iter->cur_entry = hash_get_first(iter->kv->ht);
		} else {
			/* This function returns NULL when we have iterated through all
			 * of the entries in the hash table.
			 * NOTE: hash_get_next() is less efficient than performing a
			 * hash_do_for_each(), because it has to call the hash function
			 * every time. I don't think this can be easily avoided for
			 * our current iterator interface though.
			 * Also note, it's ok to assign the result to the same cur_entry
			 * pointer that is passed in as an argument.
			 */
			iter->cur_entry = hash_get_next(iter->kv->ht, iter->cur_entry);
		}

		if (iter->cur_entry == NULL) {
			/* We've reached the end of the hash table iterator, meaning
			 * there are no more items to return. Before returning, set
			 * a flag to remember that this iterator has reached its end,
			 * so that we don't call hash_get_next() with a NULL pointer
			 * the next time that this function is called (I haven't tried
			 * to see what this does, but briefly looking at the implementation
			 * of hash_get_next() shows that it's probably not a good idea:
			 * it would lead to a hash on a NULL entry).
			 */
			kp_debug("hash_get_next() returned NULL: no more entries, "
					"setting is_empty and returning 1\n");
			iter->is_empty = true;
			retval = 1;  //no more items
			goto flush_and_return;
		}

		/* *cur_entry (a kp_ht_entry struct) contains the entry's key string
		 * and a pointer to its version table. We initialize our version
		 * table iterator to this vt: */
		ret = kp_version_iter_set(iter->v_iter, iter->cur_entry->vt);
		if (ret != 0) {
			kp_error("kp_version_iter_set() returned error=%d\n", ret);
			iter->is_invalidated = true;
			retval = -1;
			goto flush_and_return;
		}
		kp_debug("set iter->v_iter to iterate over next vt (key=%s)\n",
				iter->v_iter->vt->key);
	}  //hash table advance

	/* At this point, we know that our version table iterator is set
	 * (it was either just set in the code above, or it is still set from
	 * previous calls to this function AND its is_empty is false! So,
	 * we call its get-next function here: we directly pass the item
	 * that our caller passed to us down to kp_version_iter_next(), which
	 * will just set the members of the items (no memory allocation is
	 * done or anything). */
#ifdef KP_ASSERT
	if (iter->v_iter->is_empty) {
		kp_die("sanity check failed: iter->v_iter->is_empty is true!\n");
	}
#endif
	ret = kp_version_iter_next(iter->v_iter, item);
	if (ret == 1) {  //empty
		kp_error("unexpectedly got no-values-remaining (ret==1) from "
				"kp_version_iter_next(); returning error\n");
		iter->is_empty = true;
		iter->is_invalidated = true;
		retval = -1;
		goto flush_and_return;
	} else if (ret == -2) {  //invalidated
		kp_error("iter->v_iter was invalidated, so invalidating our hash "
				"table iterator too!\n");
		iter->is_invalidated = true;
		retval = -2;
		goto flush_and_return;
	} else if (ret != 0) {  //error
		kp_error("kp_version_iter_next() returned error=%d\n", ret);
		iter->is_invalidated = true;
		retval = -1;
		goto flush_and_return;
	}

	/* Ok, success: kp_version_iter_next() has set the caller's kp_iter_item
	 * successfully, so we can return. */
	kp_debug("kp_iterator_next2() succeeded and *item should be set, "
			"returning 0\n");
	retval = 0;  /* fall-through into flush_and_return: */

flush_and_return:
	/* We changed at least one member of the iter, so just flush the
	 * whole thing (if iter->use_nvm): */
	kp_flush_range((void *)iter, sizeof(kp_iterator), iter->use_nvm);
	return retval;
}

/* Creates a version table iterator. After creation, the version iterator
 * must be initialized by passing it to kp_version_iter_set() before it
 * can be used!
 * Returns: 0 on success, -1 on error.
 */
int kp_version_iter_create_internal(kp_version_iter **v_iter, bool use_nvm)
{
	if (use_nvm) {
		kp_die("not implemented yet: use_nvm!\n");  //CHECK: make this CDDS?
	}

	if (!v_iter) {
		kp_error("got a null argument: v_iter=%p\n", v_iter);
		return -1;
	}

	kp_kpalloc((void **)v_iter, sizeof(kp_version_iter), use_nvm);
	if (!*v_iter) {
		kp_error("kp_kpalloc(v_iter) failed\n");
		return -1;
	}
	(*v_iter)->use_nvm = use_nvm;
	(*v_iter)->vt = NULL;
	(*v_iter)->vt_ver_count = UINT64_MAX;
	(*v_iter)->vt_len = UINT64_MAX;
	(*v_iter)->vt_idx = UINT64_MAX - 1;
	  //UINT64_MAX is "initial" value (see kp_version_iter_reset())
	(*v_iter)->is_empty = false;
	(*v_iter)->is_invalidated = true;
	(*v_iter)->is_set = false;
	kp_flush_range((void *)*v_iter, sizeof(kp_version_iter),
			(*v_iter)->use_nvm);

	kp_debug("created v_iter: use_nvm=%s, vt=%p, vt_ver_count=%ju, "
			"vt_len=%ju, vt_idx=%ju, is_empty=%s, is_invalidated=%s, "
			"is_set=%s\n", (*v_iter)->use_nvm ? "true" : "false", (*v_iter)->vt,
			(*v_iter)->vt_ver_count, (*v_iter)->vt_len, (*v_iter)->vt_idx,
			(*v_iter)->is_empty ? "true" : "false",
			(*v_iter)->is_invalidated ? "true" : "false",
			(*v_iter)->is_set ? "true" : "false");
	return 0;
}

void kp_version_iter_destroy(kp_version_iter **v_iter)
{
	if (!v_iter) {
		kp_error("got null argument: v_iter=%p\n", v_iter);
		return;
	}

	if (*v_iter) {
		/* Not much to do: we don't want to free the vt that we're iterating
		 * over, just free the struct itself. */
		kp_free((void **)v_iter, (*v_iter)->use_nvm);  //sets *v_iter = NULL
	}
	kp_debug("freed the version iterator and set *v_iter to null\n");
}

/* See comments in kp_kvstore_internal.h. This function is called by local
 * kvstore during commit. */
int kp_iterator_create(kp_iterator **iter, bool use_nvm)
{
	int ret;

	if (use_nvm) {
		kp_die("not implemented yet: use_nvm!\n");  //CHECK: make this CDDS?
	}

	if (!iter) {
		kp_error("got a null argument: iter=%p\n", iter);
		return -1;
	}

	kp_kpalloc((void **)iter, sizeof(kp_iterator), use_nvm);
	if (!*iter) {
		kp_error("kp_kpalloc(iter) failed\n");
		return -1;
	}
	(*iter)->use_nvm = use_nvm;

	/* Pre-allocate the version iterator!
	 * (Note: when we make iterators externally visible again, we may want
	 *  to change this? Don't pre-allocate here, but only allocate when
	 *  kp_iterator_set() or kp_version_iter_set() is called??) */
	ret = kp_version_iter_create_internal(&((*iter)->v_iter),
			use_nvm);
	if (ret != 0) {
		kp_error("kp_version_iter_create_internal() returned error=%d\n", ret);
		kp_free((void **)iter, use_nvm);
		return -1;
	}

	/* Initialize the rest of the iterator to null / default values; the
	 * iterator can't actually be used until it's passed to kp_iterator_set().
	 * is_internal is always set to true, for now; in the future, we may
	 * add it as an argument to this function, which also might mean that
	 * we don't want to pre-allocate the version iterator above... */
	(*iter)->kv = NULL;
	(*iter)->cur_entry = NULL;
	(*iter)->is_internal = true;
	(*iter)->is_empty = false;
	(*iter)->is_invalidated = true;
	kp_flush_range((void *)*iter, sizeof(kp_iterator), use_nvm);

	return 0;
}

/* Destroys a kvstore iterator. */
void kp_iterator_destroy(kp_iterator **iter)
{
	if (!iter) {
		kp_error("got a null argument: iter=%p\n", iter);
		return;
	}
	
	if (*iter) {
		kp_debug("freeing the version iterator contained in this iterator\n");
		if ((*iter)->v_iter) {
			kp_version_iter_destroy(&((*iter)->v_iter));
		}

		kp_free((void **)iter, (*iter)->use_nvm);  //sets *iter = NULL
	}
	kp_debug("freed the iterator and set *iter to null\n");
}

/* Initializes *v_iter so that it can be used to iterate over *vt. This
 * function assumes that v_iter->use_nvm is already set, and doesn't change
 * it.
 * IMPORTANT: this function is currently called with either the vt explicitly
 * locked (kp_get_version_iter_internal()), or for a non-concurrent local
 * store (i.e. from kp_iterator_next2()). If it is called from anywhere
 * else, need to think about what could happen if the vt changes while this
 * function is running!
 * Returns: 0 on success, -1 on failure. */
int kp_version_iter_set(kp_version_iter *v_iter, kp_vt *vt)
{
	if (!v_iter || !vt) {
		kp_error("got a null argument: v_iter=%p, vt=%p\n", v_iter, vt);
		return -1;
	}

	/* (Preserve v_iter->use_nvm.) */
	v_iter->vt = vt;
	v_iter->vt_ver_count = vt->ver_count;  //total number of versions ever added
	v_iter->vt_len = vt->len;              //number of entries stored in vt
	v_iter->vt_idx = UINT64_MAX;           //initial value must be -1!
	v_iter->is_empty = false;
	v_iter->is_invalidated = false;
	v_iter->is_set = true;
	kp_flush_range((void *)v_iter, sizeof(kp_version_iter), v_iter->use_nvm);

	kp_debug("set v_iter: use_nvm=%s, vt=%p (key=%s), "
			"vt_ver_count=%ju, vt_len=%ju, vt_idx=%ju, is_empty=%s, "
			"is_invalidated=%s\n", v_iter->use_nvm ? "true" : "false",
			v_iter->vt, v_iter->vt->key, v_iter->vt_ver_count, v_iter->vt_len,
			v_iter->vt_idx, v_iter->is_empty ? "true" : "false",
			v_iter->is_invalidated ? "true" : "false");
	return 0;
}

/* Resets a version_iter. Calls to this function should eventually be
 * followed by calls to kp_version_iter_set() - otherwise, the reset
 * version iterator is useless.
 * Returns: 0 on success, -1 on failure. */
int kp_version_iter_reset(kp_version_iter *v_iter)
{
	if (!v_iter) {
		kp_error("got a null argument: v_iter=%p\n", v_iter);
		return -1;
	}

	/* (Preserve v_iter->use_nvm.) */
	v_iter->vt = NULL;
	v_iter->vt_ver_count = UINT64_MAX;  //total number of versions ever added
	v_iter->vt_len = UINT64_MAX;        //number of entries stored in vt
	v_iter->vt_idx = UINT64_MAX;        //initial value must be -1!
	v_iter->is_empty = false;
	v_iter->is_invalidated = true;
	v_iter->is_set = false;
	kp_flush_range((void *)v_iter, sizeof(kp_version_iter), v_iter->use_nvm);

	kp_debug("reset v_iter: use_nvm=%s, vt=%p, vt_ver_count=%ju, "
			"vt_len=%ju, vt_idx=%ju, is_empty=%s, is_invalidated=%s\n",
			v_iter->use_nvm ? "true" : "false", v_iter->vt,
			v_iter->vt_ver_count, v_iter->vt_len, v_iter->vt_idx,
			v_iter->is_empty ? "true" : "false",
			v_iter->is_invalidated ? "true" : "false");
	return 0;
}

/* This function assumes that *v_iter has already been allocated, and just
 * fills it in!
 * Returns: 0 on success, 1 if the specified key was not found, -1 on error. */
int kp_get_version_iter_internal(kp_kvstore *kv, const kp_ht_entry *lookup_entry,
		kp_version_iter *v_iter, bool use_nvm)
{
	int ret;
	kp_vt *lookup_vt;

	kp_die("this code is untested and not examined in a little while...\n");

	if (kv->is_master) {
		kp_die("not implemented / checked for master store! When we do "
				"expose this to clients, we need to check for race "
				"conditions during the lookup, etc.\n");
	}

	if (!kv || !lookup_entry || !v_iter) {
		kp_error("got null argument: kv=%p, lookup_entry=%p, v_iter=%p\n",
				kv, lookup_entry, v_iter);
		return -1;
	}

	/* Perform a vt lookup just like is done in kp_get_internal().
	 * IMPORTANT: if the lookup succeeds, then the version table will be
	 * returned to us with the lock taken! */
	ret = kp_lookup_versiontable(kv, lookup_entry, &lookup_vt);
	if (ret < 0) {
		kp_error("kp_lookup_versiontable() returned error: %d\n", ret);
		return -1;
	} else if (ret == 1) {
		kp_debug("version table for key %s not found\n", lookup_entry->key);
		return 1;  //key not found
	}
#ifdef KP_ASSERT
	if (!lookup_vt) {
		kp_die("lookup returned success, but lookup_vt is NULL!\n");
	}
#endif

	/* Ok, the lookup succeeded, and we hold the lock on the version table.
	 * We copy the data that we need from the version table while it's still
	 * locked, so that nobody can modify it or delete it while we initialize
	 * the v_iter anyway. We then release the vt lock when we're done.
	 *   After we release the lock, what's to stop anybody from modifying /
	 *   deleting the vt while we're iterating over it? Well, we can detect
	 *   modification, so that's ok; however, we can't detect deletion without
	 *   performing a lookup again! This is ok for now because this function
	 *   is only called by local stores (which are non-concurrent), but could
	 *   become a BUG later. */
	kp_debug("successfully looked up vt=%p (key=%s); now calling "
			"kp_version_iter_set() to initialize our iterator to it\n",
			lookup_vt, lookup_vt->key);
	v_iter->use_nvm = use_nvm;
	ret = kp_version_iter_set(v_iter, lookup_vt);
	kp_debug_lock("%s: unlocking version table\n", lookup_vt->key);
	kp_mutex_unlock("lookup_vt->lock", lookup_vt->lock);
	if (ret != 0) {
		kp_error("kp_version_iter_set() returned error=%d\n", ret);
		return -1;
	}

	kp_debug("successfully set v_iter\n");
	return 0;
}

int kp_get_version_iter(kp_kvstore *kv, const kp_ht_entry *lookup_entry,
		kp_version_iter **v_iter, bool use_nvm)
{
	kp_die("to make this externally visible, need to allocate a new "
			"kp_version_iter here before calling "
			"kp_get_version_iter_internal()! kv=%p, lookup_entry=%p, "
			"v_iter=%p, use_nvm=%d\n", kv, lookup_entry, v_iter, use_nvm);
}

/* Before calling this function, *v_iter must have been initialized to
 * a particular version table by kp_version_iter_set(); this could happen
 * via the path from kp_get_version_iter(), or directly (i.e. by an entire
 * hash table iterator (kp_iterator_next2())).
 * 
 * IMPORTANT: when this function is called and it returns the last version
 * from its version table, it will also set v_iter->is_empty to true before
 * returning. We use this instead of a proper "kp_version_iter_has_next()"
 * function, because we're stupid. Anyway, kp_iterator_next2() relies on
 * this.
 *
 * Returns: 0 on success, 1 if no more items are available, -1 on error,
 * or -2 if the iterator has been invalidated. On success, *item is set...
 */
int kp_version_iter_next(kp_version_iter *v_iter, kp_iter_item **item)
{
	int retval = 0;
	kp_vte *get_vte;

	if (!v_iter || !item) {
		kp_error("got a null argument: v_iter=%p, item=%p\n", v_iter, item);
		return -1;
	}
	if (!(*item)) {
		kp_error("caller should allocate a kp_iter_item before calling "
				"this function, and set *item to point to it!\n");
		return -1;
	}
	if (!(v_iter->vt)) {
		kp_error("caller must initialize the v_iter to work on some vt "
				"before passing it to this function!\n");
		return -1;
	}
	if (v_iter->use_nvm) {
		kp_die("not implemented yet: calling this function with a v_iter "
				"with use_nvm = true\n");
	}

	/* The logic in this function is similar to what used to be in
	 * _kp_iterator_next(). */
	kp_debug("v_iter at beginning of function: vt=%p (key=%s), "
			"vt_ver_count=%ju, vt_len=%ju, vt_idx=%ju, is_empty=%s, "
			"is_invalidated=%s\n", v_iter->vt, v_iter->vt->key,
			v_iter->vt_ver_count, v_iter->vt_len,
			v_iter->vt_idx, v_iter->is_empty ? "true" : "false",
			v_iter->is_invalidated ? "true" : "false");

	/* Check if we've already "finalized" this iterator: */
	if (v_iter->is_empty) {
		kp_debug("v_iter is already empty! Returning 1.\n");
		return 1;
	}
	if (v_iter->is_invalidated) {
		kp_debug("v_iter is already invalidated! Returning -2.\n");
		return -2;
	}

	/* Move to the next index in the version table vector: */
	if (v_iter->vt_idx == UINT64_MAX - 1) {
		kp_error("about to hit maximum vt_idx!\n");
		v_iter->is_invalidated = true;
		retval = -1;
		goto flush_and_return;
	}
	v_iter->vt_idx += 1;  //was initialized to UINT64_MAX (-1 for uint64_t type)
	kp_debug("incremented v_iter->vt_idx++ to %ju\n", v_iter->vt_idx);

	/* Check if we've reached the end of the version table vector: if
	 * the current call to this function will return the last version
	 * in the vt, then we set is_empty NOW (see comments above), but we
	 * don't return yet - we still perform the get below. */
	if (v_iter->vt_idx == v_iter->vt_len - 1) {
		kp_debug("this call will return the last version from the vt "
				"(vt_idx=%ju, vt_len=%ju), so we're setting is_empty "
				"now, but will still perform the get and return 0!\n",
				v_iter->vt_idx, v_iter->vt_len);
		v_iter->is_empty = true;
	}
#ifdef KP_ASSERT
	else if (v_iter->vt_idx >= v_iter->vt_len) {
		kp_die("v_iter->vt_idx=%ju reached unexpected value! "
				"(v_iter->vt_len=%ju)\n", v_iter->vt_idx, v_iter->vt_len);
	}
#endif

	/* Before we get from the vector, check for invalidation: if the vt's
	 * current ver_count is greater than the ver_count when the v_iter was
	 * initialized, then somebody has put into the vt and the iterator is
	 * invalidated. We also check that the vt_len is exactly the same,
	 * because a removal from the vt's vector of vtes wouldn't change
	 * ver_count.
	 * BUG: there is currently a race condition here: the vector could
	 * change in-between when we check for invalidation and when we get
	 * from the vector! We won't ever hit this bug at the moment (because
	 * only local stores use iterators), but when we expose version
	 * iterators to clients, we need to fix this!
	 * Also, shouldn't we check for invalidation again before we return
	 * from this function (after getting from the vector? Meh, whatever... */
	if (v_iter->vt->ver_count != v_iter->vt_ver_count ||
	    v_iter->vt->len != v_iter->vt_len) {
		kp_debug("version table has changed! Now has ver_count=%ju, "
				"len=%ju. Returning -2 for invalidated.\n",
				v_iter->vt->ver_count, v_iter->vt->len);
		v_iter->is_invalidated = true;
		retval = -2;
		goto flush_and_return;
	}

	/* Get the next vte from the vector and copy its data into the iterator
	 * item: */
	vector_get(v_iter->vt->vtes, v_iter->vt_idx, (void **)(&get_vte));
#ifdef KP_ASSERT
	if (!get_vte) {
		kp_error("got NULL vte from vector!\n");
		v_iter->is_invalidated = true;
		retval = -1;
		goto flush_and_return;
	}
#endif
	/* (preserve (*item)->use_nvm.) */
	(*item)->is_internal = true;  //always true, for now....
	(*item)->key = v_iter->vt->key;
	(*item)->value = get_vte->value;
	(*item)->size = get_vte->size;
	(*item)->lvn = get_vte->lvn;
	(*item)->snapshot = get_vte->snapshot;
	(*item)->is_last_version =
		(v_iter->vt_idx == v_iter->vt_len - 1) ? true : false;
	kp_flush_range((void *)(*item), sizeof(kp_iter_item), (*item)->use_nvm);
	  /* CHECK: not entirely sure that this makes sense; if we change
	   * this, then change comment in kp_kvstore_internal.h as well. */

	if (get_vte->value == NULL) {
		kp_warn("got NULL (tombstone) value from version table (key=%s, "
				"index=%ju) (size=%zu)\n", v_iter->vt->key, v_iter->vt_idx,
				get_vte->size);  //todo: change this to a debug
	}
	kp_debug("set kp_iter_item: use_nvm=%s (unchanged), is_internal=%s, "
			"key=%s, value=%s, size=%zu, lvn=%ju, snapshot=%ju, "
			"is_last_version=%s\n", (*item)->use_nvm ? "true" : "false",
			(*item)->is_internal ? "true" : "false", (*item)->key,
			(char *)((*item)->value), (*item)->size, (*item)->lvn,
			(*item)->snapshot, (*item)->is_last_version ? "true" : "false");

	retval = 0;
	/* fall-through to flush_and_return: */

flush_and_return:
	/* We changed at least one member of the v_iter, so just flush the
	 * whole thing (if v_iter->use_nvm): */
	kp_flush_range((void *)v_iter, sizeof(kp_version_iter), v_iter->use_nvm);
	return retval;
}


bool kp_process_commit_entries(void *entry_ptr, void *cr_ptr)
{
	uint64_t i, len;
	int ret;
	kp_ht_entry *entry = (kp_ht_entry *)entry_ptr;
	kp_commit_record *cr = (kp_commit_record *)cr_ptr;
	kp_vt *vt;
	kp_vte *vte;
	kp_kvpair *pair;

	if (!entry || !cr) {
		kp_error("got a null argument: entry=%p, cr=%p\n", entry, cr);
		return false;
	}
	kp_debug("entered: got entry for key=%s, building commit record with "
			"begin_snapshot=%ju\n", entry->key, cr->begin_snapshot);
#ifdef KP_ASSERT
	if (cr->state != CREATED) {
		kp_die("cr state is not CREATED, as expected, but rather %s\n",
				commit_state_to_string(cr->state));
	}
#endif

	/* "iterates over the version table for each entry in the hash
	 *  table and appends the latest versions to the commit record (which is
	 *  passed as extra data to the processor function); all vtes in the
	 *  version table (latest or not) are freed along the way."
	 * See kp_version_iter_next() for similar code... [6408]
	 */
	
	/* A kp_ht_entry just contains the key and a pointer to the version
	 * table:
	 */
	vt = entry->vt;
	if (!vt) {
		kp_error("got null vt pointer!\n");
		return false;
	}
	//kp_vt_sanity_check_lengths(vt);

	len = vector_count(vt->vtes);
	if (len == 0) {
		kp_error("got a zero-length version table\n");
		return false;
	}
#ifdef KP_ASSERT
	if (len != vt->len) {
		kp_die("mismatched lengths: len=%ju, vt->len=%ju\n", len, vt->len);
	}
#endif

	for (i = 0; i < len; i++) {
		/* Rather than vector_get(), we use vector_set() here: we want
		 * to get the value out of the vector, but this will be the last
		 * time we ever want to do that, so put NULL in there instead
		 * for safety. */
		vector_set(vt->vtes, i, NULL, (void **)&vte);
		if (!vte) {
			kp_error("got a null vte pointer\n");
			return false;
		}
		kp_debug("at index=%ju of %ju for key=%s got value=%p (%s)\n",
				i, len-1, vt->key, vte->value, (char *)vte->value);
#ifdef KP_ASSERT
		if (vte->snapshot != cr->begin_snapshot) {
			kp_die("vte->snapshot=%ju doesn't match cr->begin_snapshot=%ju\n",
					vte->snapshot, cr->begin_snapshot);
		}
#endif

		/* We only append the LAST version in the vt to the commit record: */
		if (i == len - 1) {  //last version in table!
			/* Allocate a new kvpair on nvm, then append it to the commit
			 * record. We take use_nvm from the commit record (the entire
			 * commit record is either put on nvm or is not).
			 * This function ALWAYS COPIES the key and the value! */
			ret = kp_kvpair_create(&pair, vt->key, vte->value, vte->size,
					cr->use_nvm);
			if (ret != 0) {
				kp_error("kp_kvpair_create() returned error=%d\n", ret);
				return false;
			}

			/* FAIL here: pair isn't reachable, so we will leak its memory.
			 * The commit record isn't reachable either? Then we will leak
			 * that memory also.
			 *   CHECK this! */

			ret = kp_commit_record_append(cr, pair);
			if (ret != 0) {
				kp_error("kp_commit_record_append() returned error=%d\n", ret);
				kp_kvpair_destroy(&pair);
				return false;
			}
			kp_debug("appended new kvpair to commit record, which now has "
					"length=%llu\n", vector_count(cr->kvpairs));
			kp_debug_print_cr("after appending kvpair", cr);
		}
#ifdef KP_DEBUG
		else {
			kp_debug("this entry is not the LAST version, so just freeing "
					"it now.\n");
		}
#endif

		/* For all versions (last or not), we now free the vte that was
		 * stored in the vector - the local worker is done with this data
		 * (we think...). Note that kp_vte_destroy() always frees the value
		 * that the vte points to if the pointer is not NULL.
		 * The second argument that we pass here is use_nvm; this isn't
		 * stored in the vte or in the vt, so we go all the way back up
		 * to the vt's parent local kvstore. I think this makes sense. */
		kp_vte_destroy(&vte, vt->parent->use_nvm);
		kp_debug("destroyed vte from index %ju (of %ju) (used "
				"local kvstore's use_nvm setting = %s).\n",	i, len-1,
				vt->parent->use_nvm ? "true" : "false");
	}
#ifdef KP_ASSERT
	if (len != vector_count(vt->vtes)) {
		kp_die("vector's length changed, from %ju to %llu!\n", len,
				vector_count(vt->vtes));
	}
#endif

	/* That should be it - the kvpair allocation, commit record append,
	 * and vte destroy all should have flushed memory appropriately and
	 * such. */
	kp_debug("done processing entry for key %s, returning true\n",
			entry->key);
	return true;
}

size_t kp_kvstore_do_for_each(kp_kvstore *kv, Hash_processor fn, void *aux)
{
	/* In this function we iterate over the LOCAL hash table, not the
	 * master hash table, so this doesn't count towards the MASTER yet.
	 * Dig into the Hash_processor function (kp_process_commit_entries())
	 * for the switch to master work (NVRAM allocation and copy). */

	if (!kv || !fn) {
		kp_error("got a null argument: kv=%p, fn=%p\n", kv, fn);
		return SIZE_MAX;
	}
	kp_debug("entered; calling hash_do_for_each() now\n");

	/* Returns the number of entries processed, or SIZE_MAX on error.*/
	return hash_do_for_each(kv->ht, fn, aux);
}

int kp_commit_record_create(kp_commit_record **cr, uint64_t begin_snapshot,
		size_t expected_max_keys, bool use_nvm)
{
	/* Assume that all of this function counts towards the master: the
	 * master's use_nvm value (true, presumably) is passed in here by
	 * the local store, so this is where we switch to NVRAM. */

	int ret;

	if (!use_nvm) {
		kp_warn("added this message here to eliminate not-used warning: "
				"expected_max_keys=%zu\n", expected_max_keys);
		kp_die("not implemented yet: !use_nvm\n");  //CHECK: anything extra to do?
	}

	if (!cr) {
		kp_error("got a null argument: cr=%p\n", cr);
	}
#ifdef KP_ASSERT
	if (begin_snapshot == UINT64_MAX) {
		kp_die("invalid begin_snapshot=%ju\n", begin_snapshot);
	}
	if (begin_snapshot == 0) {
		kp_warn("probably incorrect begin_snapshot=%ju\n", begin_snapshot);
	}
#endif

	kp_kpalloc((void **)cr, sizeof(kp_commit_record), use_nvm);
	if (!*cr) {
		kp_error("kp_kpalloc(cr) failed\n");
		return -1;
	}
#ifndef UNSAFE_COMMIT
	ret = vector_create(&((*cr)->kvpairs), 0, use_nvm);
#else  //UNSAFE_COMMIT _is_ defined:
	ret = vector_create(&((*cr)->kvpairs), expected_max_keys, use_nvm);
#endif
	if (ret != 0) {
		kp_error("vector_create(cr->kvpairs) returned error=%d\n", ret);
		kp_free((void **)cr, use_nvm);
		  /* Should we just call kp_commit_record_destroy() here?
		   * I think not: that function will try to free pointers that
		   * may not have been initialized, in the !use_nvm case! */
		return -1;
	}
	ret = kp_mutex_create("commit record mutex", &((*cr)->state_lock));
	if (ret != 0) {
		kp_error("kp_mutex_create() returned error=%d\n", ret);
		vector_destroy(&((*cr)->kvpairs));
		kp_free((void **)cr, use_nvm);
		return -1;
	}
	ret = kp_cond_create("commit record condition", &((*cr)->state_cond));
	if (ret != 0) {
		kp_error("kp_cond_create() returned error=%d\n", ret);
		kp_mutex_destroy("commit record mutex", &((*cr)->state_lock));
		vector_destroy(&((*cr)->kvpairs));
		kp_free((void **)cr, use_nvm);
		return -1;
	}
#ifdef KP_ASSERT
	if (!(*cr)->state_lock || !(*cr)->state_cond) {
		kp_die("cr's state_lock=%p or state_cond=%p is NULL!\n",
				(*cr)->state_lock, (*cr)->state_cond);
	}
#endif
	PM_EQU(((*cr)->use_nvm), (use_nvm)); // persistent
	PM_EQU(((*cr)->begin_snapshot), (begin_snapshot)); // persistent
	PM_EQU(((*cr)->end_snapshot), (UINT64_MAX));         //invalid // persistent
	PM_EQU(((*cr)->conflict_key), (NULL));	// persistent
	PM_EQU(((*cr)->next_cr), (NULL));		// persistent
	PM_EQU(((*cr)->debug_signalled), (false));	// persistent

	/* "CDDS": flush, set state, and flush again. */
	kp_flush_range((void *)*cr, sizeof(kp_commit_record) - sizeof(commit_state),
			(*cr)->use_nvm);
	PM_EQU(((*cr)->state), (CREATED)); // persistent 
	kp_flush_range((void *)&((*cr)->state), sizeof(commit_state),
			(*cr)->use_nvm);

	kp_debug("successfully created new commit record: kvpairs=%p, "
			"use_nvm=%s, begin_snapshot=%ju, end_snapshot=%ju, state=%s\n",
			(*cr)->kvpairs, (*cr)->use_nvm ? "true" : "false",
			(*cr)->begin_snapshot, (*cr)->end_snapshot,
			commit_state_to_string((*cr)->state));
	return 0;
}

void kp_debug_print_cr(const char *description, const kp_commit_record *cr)
{
#ifdef KP_DEBUG2
	kp_print("printing cr (%p) info: %s\n", cr, description);
	if (cr) {
		kp_print("   snapshots [%ju,%ju]; use_nvm=%s, kvpairs=%p "
				"(len=%llu)\n", cr->begin_snapshot, cr->end_snapshot,
				cr->use_nvm ? "true" : "false", cr->kvpairs,
				vector_count(cr->kvpairs));
		kp_print("   state_lock=%p, state_cond=%p, conflict_key=%p, "
				"next_cr=%p\n", cr->state_lock, cr->state_cond,
				cr->conflict_key, cr->next_cr);
		kp_print("   state: %s\n", commit_state_to_string(cr->state));
	}
#endif
}

int kp_commit_record_append(kp_commit_record *cr, const kp_kvpair *pair)
{
	/* The commit record resides in NVRAM, so count this entire function
	 * towards the master. */

	int ret;

	if (!cr || !pair) {
		kp_error("got null argument: cr=%p, pair=%p\n", cr, pair);
		return -1;
	}

	/* Simply appends the new pair to the commit record's vector; the rest
	 * of the state in the commit record remains unchanged. Vector append
	 * has been made CDDS (at least mostly...), so we don't have to do any
	 * flushing right here in the calling function. */
	ret = vector_append(cr->kvpairs, pair, NULL);
	if (ret != 0) {
		kp_error("vector_append(cr->kvpairs, pair) returned error=%d\n", ret);
		return -1;
	}

	/* BUG: if we fail during the vector append, and the vector append
	 * required a resize, then we may need to _recover_ from the partial
	 * operation! */

	kp_debug("appended pair (%s, %p (%s)) to commit record with "
			"begin_snapshot=%ju\n", pair->key, pair->value,
			(char *)pair->value, cr->begin_snapshot);
	return 0;
}

/* IMPORTANT: this function will iterate over all of the key-value pairs
 * that the commit record points to, and free each key or each value if
 * the free_keys and free_values arguments are set to true.
 */
void kp_commit_record_destroy(kp_commit_record **cr, bool free_keys,
		bool free_values)
{
	uint64_t i, count;
	kp_kvpair *pair;

	/* Commit records are stored in NVRAM, so just count this entire
	 * function towards the MASTER. */
	if (!cr) {
		kp_error("got a null argument: cr=%p\n", cr);
		return;
	}

	if (!free_values) {
		kp_die("this is unexpected so far: currently, no matter if we're "
				"calling this function from local store or master store, "
				"we always expect to free values. Right?\n");
	}

	if (*cr) {
		kp_debug("freeing commit record with begin_snapshot=%ju, "
				"end_snapshot=%ju; was in state=%s\n",
				(*cr)->begin_snapshot, (*cr)->end_snapshot,
				commit_state_to_string((*cr)->state));
		(*cr)->state = DEAD;
		kp_flush_range((void *)&((*cr)->state), sizeof(commit_state),
				(*cr)->use_nvm);

		if ((*cr)->state_lock) {
			kp_mutex_destroy("commit record mutex", &((*cr)->state_lock));
		}
		if ((*cr)->state_cond) {
			kp_cond_destroy("commit record condition", &((*cr)->state_cond));
		}
		if ((*cr)->kvpairs) {
			/* It was decided that commit records "own" the values in
			 * the master store (see kp_put_master()), so we have to
			 * free any values that the commit record points to here.
			 * To do this, we loop through the kvpairs vector and call
			 * kp_kvpair_destroy() on each pair. kp_kvpair_destroy()
			 * will free the key and the value if the pointers are
			 * non-NULL.
			 *   Do we also want to free keys here? I don't think so;
			 *   I think it was decided (see kp_put_master() again,
			 *   I think) that keys are "owned" by the hash table
			 *   entries after the put into the version table. However,
			 *   this function may be called from kp_kv_local.c in
			 *   an error condition, in which case we DO want to free
			 *   the keys in the commit record. Therefore, I added
			 *   an argument to this function to indicate whether or
			 *   not keys should be freed... ugh.
			 *   And then I added a free_values argument too, because
			 *   might as well...
			 */
			count = vector_count((*cr)->kvpairs);
			for (i = 0; i < count; i++) {
				vector_get((*cr)->kvpairs, i, (void **)(&pair));
				if (pair) {
					if (!free_keys) {
						pair->key = NULL;
					}
					if (!free_values) {
						/* Note: value might already be NULL if this pair
						 * was a deletion tombstone...
						 */
						pair->value = NULL;
					}
					kp_kvpair_destroy(&pair);
				} else {
					kp_warn("got NULL pair from kvpairs at index %ju; "
							"unexpected!\n", i);
				}
			}
			vector_destroy(&((*cr)->kvpairs));
		}
		kp_free((void **)cr, (*cr)->use_nvm);  //flushes and sets *cr = NULL
	}
}

int kp_kvpair_create(kp_kvpair **pair, const char *key, const void *value,
		size_t size, bool use_nvm)
{
	/* Count this entire function towards the MASTER - this is where the
	 * local store creates a kvpair for each value that will be stored in
	 * NVRAM and merged in. The use_nvm argument here comes from cr->use_nvm;
	 * it is presumably true. */

	int len;
	bool is_deletion = false;
	char *new_key;
	void *new_value;

	if (!use_nvm) {
		kp_die("not implemented yet: !use_nvm\n");
	}

	if (!pair || !key) {
		kp_error("got a null argument: pair=%p, key=%p, value=%p\n",
				pair, key, value);
		return -1;
	}
	if ((!value && size != 0) || (value && size == 0)) {
		kp_die("got value=%p, but size=%zu!\n", value, size);
	}
	if (size == SIZE_MAX) {
		kp_error("got invalid size=%zu\n", size);
		return -1;
	}
	kp_debug2("entered: key=%s, value=%p, size=%zu\n",
			key, value, size);

	if (!value) {  //tombstone for a deletion!
		is_deletion = true;
		kp_debug_gc("creating a new kvpair for tombstone value\n");
	}
	
	len = strlen(key);
	if (len < 1) {
		kp_error("got invalid strlen(key)=%d\n", len);
		return -1;
	}

	/* Allocate the kvpair struct itself: */
	kp_kpalloc((void **)pair, sizeof(kp_kvpair), use_nvm);
	if (!*pair) {
		kp_error("kp_kpalloc(kp_kvpair) failed\n");
		return -1;
	}

	/* Allocate space for the key and value. kp_kpalloc() will flush these
	 * pointers if use_nvm is true: */
	kp_kpalloc((void **)&new_key, len+1, use_nvm);
	if (!new_key) {
		kp_error("kp_kpalloc(new_key) failed\n");
		kp_free((void **)pair, use_nvm);
		return -1;
	}
	if (!is_deletion) {
		kp_kpalloc((void **)&new_value, size, use_nvm);
		if (!new_value) {
			kp_error("kp_kpalloc(new_value) failed\n");
			kp_free((void **)&new_key, use_nvm);
			kp_free((void **)pair, use_nvm);
			return -1;
		}
	} else {
		/* For deletions, we don't allocate anything for the value; we
		 * just set the pair's value to NULL. No need for flushing (we're
		 * ignoring the reachability problem). */
		new_value = NULL;
	}

	/* Copy the key, copy the value, set use_nvm and size, change the state
	 * to active, and flush the whole struct. Additionally, if use_nvm is
	 * true, then kp_strncpy() and kp_memcpy() flush the memory that they
	 * just modified, which is exactly what we want - when we return from
	 * this function, the copied key and value should be durable on nvm.
	 */
	kp_strncpy(new_key, key, len+1, use_nvm);
	if (!is_deletion) {
		kp_memcpy(new_value, value, size, use_nvm);
	}
	PM_EQU(((*pair)->key), (new_key));
	PM_EQU(((*pair)->value), (new_value));
	PM_EQU(((*pair)->use_nvm), (use_nvm));
	PM_EQU(((*pair)->size), (size));
	kp_debug("creating keypair: arg value=%p, local new_value=%p, "
			"kvpair->value=%p\n", value, new_value, (*pair)->value);
	
	/* "CDDS": flush, set state, and flush again. */
	kp_flush_range((void *)*pair, sizeof(kp_kvpair) - sizeof(ds_state),
			(*pair)->use_nvm);
	PM_EQU(((*pair)->state), (STATE_ACTIVE));
	kp_flush_range((void *)&((*pair)->state), sizeof(ds_state),
		(*pair)->use_nvm);

	kp_debug2("created and flushed a new kvpair (%p): use_nvm=%s, key=%s, "
			"value=%p, size=%zu, state=%s\n", *pair,
			(*pair)->use_nvm ? "true" : "false", (*pair)->key,
			(*pair)->value, (*pair)->size,
			ds_state_to_string((*pair)->state));
	return 0;
}

/* IMPORTANT: this function will free the key and the value that the pair
 * points to if the pointers contained in the kvpair are non-NULL!
 */
void kp_kvpair_destroy(kp_kvpair **pair)
{
	if (!pair) {
		kp_error("got a null argument: pair=%p\n", pair);
		return;
	}

	if (*pair) {
		(*pair)->state = STATE_DEAD;
		kp_flush_range((void *)&((*pair)->state), sizeof(ds_state),
				(*pair)->use_nvm);
		kp_debug("destroying pair with key=%s and value=%p (%s); set its "
				"state to %s (should be DEAD) and flushed it\n", (*pair)->key,
				(*pair)->value, (char *)((*pair)->value), 
				ds_state_to_string((*pair)->state));

		if ((*pair)->key) {
			kp_free((void **)&((*pair)->key), (*pair)->use_nvm);
		}
		if ((*pair)->value) {
			kp_free((void **)&((*pair)->value), (*pair)->use_nvm);
		}
		kp_free((void **)pair, (*pair)->use_nvm);  //flushes and sets *pair = NULL
	}
}

/* RECOVERY: ...*/
uint64_t kp_increment_snapshot_mark_in_use(kp_kvstore *kv)
{
	int ret;
	uint64_t snapshot;

#ifdef KP_ASSERT
	if (! kv->is_master) {
		kp_die("shouldn't call this function for local stores!\n");
	}
#endif

	kp_debug("entered; is_master=%s\n", kv->is_master ? "true" : "false");
	kp_mutex_lock("kv->snapshot_lock", kv->snapshot_lock);

	snapshot = kv->global_snapshot;
	PM_EQU((kv->global_snapshot), (kv->global_snapshot + 1));
	if (kv->use_nvm) {  //I think this makes sense... CHECK
		flush_range(&(kv->global_snapshot), sizeof(uint64_t));
	}
#ifdef KP_ASSERT
	else {
		kp_die("unexpected: use_nvm not set for master (this function should "
				"never be called with local store!)\n");
	}
#endif

	/* FAIL here: shouldn't be a problem; we've incremented the snapshot
	 * number, but haven't exposed it to anybody. This function can be
	 * called again on recovery and the failed snapshot number will just
	 * go unused (right?).
	 */

	/* We add the snapshot to the garbage collector's in-use list while we're
	 * still locked here, because if we don't, the garbage-collector could
	 * potentially run in-between when we unlock and when the append
	 * completes.
	 * Don't bother with the complexity of kp_add_to_cps_to_keep(): we
	 * are certain that our snapshot number here is unique, so we don't
	 * have to check that it's not in any other vectors already, etc.
	 */
#ifndef DISABLE_GC_SNAPSHOT_TRACKING
	kp_mutex_lock("kv->gc->lock", kv->gc->lock);
	ret = vector64_append(kv->gc->snapshots_in_use, snapshot);
	  //CHECK: make sure this is CDDS!
	/* FAIL here: we've added a snapshot number to the list of snapshots
	 * that are in-use, but nobody knows that it's there yet. This is a
	 * more general problem: after a failure, the local workers' snapshot
	 * numbers are lost, but snapshots_in_use still has them.
	 * SOLUTION: make snapshots_in_use a VOLATILE vector! We want to
	 * reset it on a failure.
	 *   CHECK - TODO: make sure this is implemented correctly!
	 */
	kp_mutex_unlock("kv->gc->lock", kv->gc->lock);
#else
	/* Temporarily disabled snapshots_in_use for testing!!! */
	ret = 0;
#endif
	kp_mutex_unlock("kv->snapshot_lock", kv->snapshot_lock);
	kp_debug("appended new snapshot %ju to gc->snapshots_in_use\n",
			snapshot);

	if (ret != 0) {
		kp_error("vector64_append(snapshots_in_use) returned error=%d\n", ret);
		return UINT64_MAX;
	}
	if (snapshot + 1 == UINT64_MAX) {
		kp_error("hit snapshot ceiling: snapshot=%ju, kv->global_snapshot=%ju\n",
				snapshot, kv->global_snapshot);
		return UINT64_MAX;
	}

	kp_debug("returning new, unique snapshot=%ju\n", snapshot);
	return snapshot;
}

int kp_kvstore_recover_master(kp_kvstore *kv)
{
	/* Notes about things that we need to do in this function:
	 * - kv->gc->snapshots_in_use keeps track of the snapshots that
	 *   each local worker is using; it should be created in volatile
	 *   memory and reset on every failure.
	 *     This is not only easy/necessary for recovery, but it also makes
	 *     sense from a performance standpoint: any state that we can keep
	 *     in volatile memory and throw away or reconstruct on recovery is
	 *     going to buy us performance gains! (if we actually do have some
	 *     faster non-volatile memory).
	 * - RESET pointers and state that we've allocated on non-volatile memory!
	 *   May need to re-allocate these data structures:
	 *     ~ kv->lookup_entry
	 *     ~ gc->snapshots_in_use
	 *     ~ ...
	 */

	kp_die("not implemented yet! kv=%p\n", kv);
}

int kp_kvstore_reset(kp_kvstore *kv)
{
	kp_debug("locking kvstore (id=%d) for safety, but should be unnecessary; "
			"this function should only be called on a not-in-use local kvstore "
			"after a merge!\n", kv->id);
	kp_wrlock("kv->rwlock for kvstore reset", kv->rwlock);

	/* hash_clear() will cause kp_ht_entry_free_local() to be called on
	 * every kp_ht_entry, which will free the key and vt that the entry
	 * points to as well as freeing the entry itself. The vtes should
	 * have already been freed!
	 */
	kp_debug("calling hash_free() on the kv->ht\n");
	hash_clear(kv->ht);  //can't return error

	/* Is there any other internal state that needs to be reset? Just one
	 * thing, for now (look at kp_kvstore struct definition): */
	kv->pairs_count = 0;
	kp_debug("reset kvstore's pairs_count to %ju\n", kv->pairs_count);
	  /* TODO: just remove pairs_count - is it used anywhere??
	   *   ALSO, double-check: if the kvstore struct changes, we may have
	   *   to change this function too! */

	kp_wrunlock("kv->rwlock for kvstore reset", kv->rwlock);
	return 0;
}

/* Merges a commit record into the master kvstore. The commit_record that
 * is passed to this function should already be CREATED (or perhaps
 * CREATED_OK) and inserted into the commit_log, and its end_snapshot should
 * be set and should never ever change.
 *
 * This is a critical function - it has been designed for high scalability
 * and allows multiple threads to merge commits into the master kvstore
 * concurrently. The basic merge consists of iterating over the list of
 * kvpairs to put into the master store, but the functions called here also
 * perform conflict detection and handle the "inter-commit communication"
 * that ensures that concurrent merges still follow the definition of
 * snapshot isolation.
 *
 * It is CRITICAL that this function be called exactly once on each commit
 * record; there is never any reason to call it more than once on each commit
 * record, and if we don't call it on some commit record then no other commits
 * that come after it will ever be completed. See the description of our
 * "invariants" below for more information about this. When this function
 * returns, the commit is guaranteed to be either COMMITTED or ABORTED.
 *
 * Returns: 0 on success, 1 if a conflict occurred, -1 on error.
 */
int kp_merge_commit_record(kp_kvstore *kv, kp_commit_record *cr,
		void **conflict_list)
{
	int ret;
	uint64_t i, len;
	const kp_kvpair *pair;
	bool conflict, error;
	kp_ht_entry lookup_entry;
	commit_state new_state;
	kp_commit_record *next_cr;

	/* This function CANNOT return without both finalizing its own commit
	 * record's state and signaling the commit record that follows it
	 * (cr->next_cr). So, throughout this function if we encounter a
	 * conflict or an error, we'll set these bools to true, and then
	 * skip stuff until we get to these final two steps at the end. */
	conflict = false;
	error = false;

	if (!kv || !cr) {
		kp_error("got null argument: kv=%p, cr=%p\n", kv, cr);
		error = true;  //yikes
		kp_warn("added this message here to eliminate not-used warning: "
				"conflict_list=%p\n", conflict_list);
	}
#ifdef KP_ASSERT
	if (!kv->is_master) {
		kp_die("can't call merge on a local kvstore!\n");
	}
	if (!kv->use_nvm) {
		kp_warn("make sure this function's implementation is correct "
				"for use_nvm = false!\n");
	}
	if (kv->use_nvm != cr->use_nvm) {
		kp_die("kv->use_nvm=%s but cr->use_nvm=%s!\n",
				kv->use_nvm ? "true" : "false",
				cr->use_nvm ? "true" : "false");
	}
	if (cr->state != CREATED && cr->state != CREATED_OK) {
		kp_die("commit_record has unexpected state=%s\n",
				commit_state_to_string(cr->state));
	}
	if (cr->end_snapshot == UINT64_MAX || cr->end_snapshot == 0) {
		kp_die("commit_record has invalid end_snapshot=%ju\n",
				cr->end_snapshot);
	}
#endif

	lookup_entry.vt = NULL;  //unused
	kp_debug2("entered; cr has begin_snapshot=%ju, end_snapshot=%ju, "
			"state=%s\n", cr->begin_snapshot, cr->end_snapshot,
			commit_state_to_string(cr->state));

	/* Merging is easy to perform when done one commit at a time, but for
	 * acceptable performance we need to allow for multiple concurrent
	 * merges, while also performing conflict detection to ensure that our
	 * snapshot isolation guarantees are met. Review the definition of
	 * snapshot isolation ("A Critique of ANSI SQL Isolation Levels",
	 * Berenson et. al 1995) and note that transactions have to "complete"
	 * (either COMMIT or ABORT) _IN ORDER_! (if a txn finishes its writes
	 * but an "earlier" txn is still operating, then the unfinished txn could
	 * still cause the finished txn to have a conflict). Therefore, we've
	 * designed our merge algorithm in such a way that it is as optimistic
	 * as possible (the only time when merges block each other is when
	 * their keys hash to the same bucket or when they put to the same
	 * key), but forces every txn to wait for all earlier txns to complete
	 * before it can complete.
	 *   It is worth noting that, according to several sources, most
	 *   traditional DBMS that implement snapshot isolation actually
	 *   accomplish these concurrent transactions by locking each tuple
	 *   as it is written, a form of two-phase locking that leads to
	 *   "first-updater wins," rather than "first-committer wins" as
	 *   defined by the original snapshot isolation paper. See these
	 *   papers:
	 *     - Snapshot Isolation and Integrity Constraints in Replicated
	 *       Databases, Lin et. al. (2009). "Basically all commercial
	 *       systems we are aware of do not implement the first-committer-
	 *       wins strategy but detect write/write conflicts during execution
	 *       using locks (first-updater-wins strategy)."
	 *     - Postgres-R(SI): Combining Replica Control with Concurrency
	 *       Control based on Snapshot Isolation, Wu & Kemme (ICDE 2005).
	 *       Describes (kind of) how SI is implemented in PostgreSQL in
	 *       section 4.1.
	 *     - Design, Implementation, and Evaluation of a Repairable Database
	 *       Management System, Chiueh and Pilania (ICDE 2005). Also describes
	 *       how SI is implemented (kind of) in PostgreSQL in section 4.1.
	 *   We actually implement first-committer wins here. I'm not sure if
	 *   this means that we're smarter than the others, or if we're dumber
	 *   for even trying.
	 *
	 * Here is a general description of our merge algorithm. Assume that we
	 * are working with a commit record Cr:
	 *   - Set Cr's state to CREATED. Append it to the commit log, then
	 *     unlock its state_lock. (this is already done before now by a
	 *     previous function). (note: we unlock the state_lock because
	 *     in order to update the state in the next step, we need to call
	 *     the transition function, which acquires the lock as its first
	 *     step!)
	 *   - In that same previous function, examine the commit record Cr-1
	 *     that comes just prior to Cr in the commit log:
	 *     ~ If Cr-1 is _not_ in a final state (not yet COMMITTED or
	 *       ABORTED), then we know that it HAS to signal us (Cr) at
	 *       some point (see below). This means that Cr's state should
	 *       stay as CREATED. (note: see the kp_commit_state_transition()
	 *       function for a detailed description of the state machine for
	 *       commit records) (also note: we can only know for certain
	 *       that it has to signal us because we know that (later on in
	 *       this algorithm) commit records must update their state
	 *       _before_ signaling the commit that follows them; for this
	 *       reason, we must mfence after all of our state changes!)
	 *     ~ If Cr-1 _is_ in a final state, then we know that it didn't
	 *       cause us any conflicts while it was merging, because Cr
	 *       hasn't actually done any merging yet (if there is a conflict
	 *       between Cr-1 and Cr, then Cr will discover it itself). This
	 *       means that Cr-1 will probably never signal us, so we move
	 *       the state of Cr to CREATED_OK. (note: it's possible that
	 *       Cr-1 is in-between the steps of setting its final state and
	 *       signaling the subsequent commit record Cr; in this case,
	 *       we'll get an OK signal while Cr is already in the CREATED_OK
	 *       state, so we should expect this. This race condition also
	 *       runs in the other direction as well, so we just allow this
	 *       "double-OK".)
	 *   - Now that the initial state of Cr has been set, we perform the
	 *     merge as usual: loop through the list of kvpairs and put each
	 *     of them into the master kvstore (in this function and the
	 *     functions that it calls). As this happens, this thread may
	 *     discover conflicts for Cr, this thread may discover conflicts
	 *     for later commits that are being merged concurrently, or other
	 *     threads for commits earlier than Cr may discover conflicts for
	 *     Cr.
	 *   - Once the merging is complete (or is stopped because we find that
	 *     Cr has a conflict), then we finalize Cr's own state. This actually
	 *     happens within the state transition function; the final transition
	 *     to COMMITTED or ABORTED may be performed by this thread or by some
	 *     other thread (for an earlier commit) that finds a conflict.
	 *     The finalization is done in kp_merge_commit_record() when it calls
	 *     kp_commit_state_transition() for the final time.
	 *   - Finally, the last thing that this function does (and must always
	 *     do - it cannot be skipped!) is to get the next commit record Cr+1
	 *     (if it exists in the commit log) and signal/transition it (using
	 *     the same transition function as is used throughout).
	 *     ~ Are there race conditions while we get the next commit record
	 *       and signal it? Perhaps, but it's ok; if we miss the next commit
	 *       record just before it is appended to the commit log, then its
	 *       next step is to check OUR (Cr's) commit record to see if it is
	 *       finalized. Because we require that Cr's state is finalized before
	 *       it checks Cr+1, we know that Cr+1 will see our state as finalized
	 *       when it checks, and it will transition to an OK state.
	 */

#ifdef KP_ASSERT
	if (conflict) {
		kp_die("don't expect any conflicts so soon!\n");
	}
#endif
	if (!error) {
		/* First, we transition the commit record's state to either INPROGRESS
		 * or INPROGRESS_OK. The state transition function handles this for
		 * us: it locks the state, does the transition, flushes the new state,
		 * and signals any waiters. We just check that it returns a new state
		 * that we expect at this point. Note that we DON'T expect to be
		 * ABORTED yet, because we haven't started to merge anything in yet!
		 *
		 * We pass false for prev_is_finalized (we're the owner of the commit
		 * record, so we don't know about the previous commit) and true for
		 * success (because nothing has gone wrong yet). Pass true for
		 * is_owner! */
		kp_debug2("owner calling kp_commit_state_transition() to move from "
				"CREATED[_OK] to INPROGRESS[_OK]\n");
		new_state = kp_commit_state_transition(cr, false, true, true,
				kv->detect_conflicts, kv->use_nvm);
		if (new_state != INPROGRESS && new_state != INPROGRESS_OK) {
			kp_error("expected new_state = INPROGRESS[_OK], but state transition "
					"function returned new_state = %s\n",
					commit_state_to_string(new_state));
			error = true;
		}
	}

	if (!error) {
		len = vector_count(cr->kvpairs);
		kp_debug("iterating over commit record [%ju,%ju] with %ju kvpairs\n",
				cr->begin_snapshot, cr->end_snapshot, len);
		for (i = 0; i < len; i++) {
			/* Is it possible for the commit record to change while we're
			 * iterating over it? Shouldn't be possible. */
			vector_get(cr->kvpairs, i, (void **)(&pair));  //pair is a local variable
	
			/* A kvpair in a commit_record contains pointers to the key and
			 * value, a size, and a state (which may not really be useful).
			 * These pointers are presumably to non-volatile memory (assuming
			 * that the master was created with use_nvm = true); the local
			 * worker created the commit record on non-volatile memory already.
			 * So, we pass just these pointers to kp_put_master(), which knows
			 * that it only has to copy the pointers, and not the data itself.
			 * We put the key into a kp_ht_entry struct first for use by
			 * kp_put_master().
			 *
			 * When kp_put_master() returns successfully, the version
			 * table, rather than the commit record, now "owns" the value
			 * in memory; both of these data structures point to the value,
			 * but the value will be freed when its version table entry is
			 * destroyed, not when the commit record is destroyed.
			 *
			 * Additionally, note that the value pointer may be NULL, which
			 * indicates a tombstone value for a deletion! kp_put_master()
			 * should be able to handle this. The value pointer will be NULL
			 * if and only if the size is 0.
			 *
			 * We pass the commit_record to kp_put_master because each vte
			 * now has to have a pointer to its commit record (gets on each
			 * version need to check that the state of the commit is COMMITTED
			 * and not ABORTED or INPROGRESS!).
			 */
#ifdef KP_ASSERT
			if (pair->use_nvm != kv->use_nvm) {
//				kp_die("pair->use_nvm (%s) does not match master's use_nvm(%s)!\n",
//						pair->use_nvm ? "true" : "false", kv->use_nvm ? "true" : "false");
				kp_die("pair->use_nvm (%d) does not match master's use_nvm(%d)!\n",
						pair->use_nvm , kv->use_nvm );
			}
			if ((!pair->value && pair->size != 0) ||
			    ( pair->value && pair->size == 0)) {
				kp_die("pair->value is %p, but size is %zu!\n", pair->value,
						pair->size);
			}
#endif
			if (!pair->value) {
				kp_debug_gc("merging in a tombstone kvpair\n");
			}
			lookup_entry.key = pair->key;
			ret = kp_put_master(kv, &(lookup_entry), pair->value, pair->size, cr);
			  /* It should be fine to use a kp_ht_entry that's on our own
			   * local stack here; lookup_entry is only used for lookup, and
			   * if the key isn't already in the store, then kp_put_master()
			   * will create a new entry.
			   */
			if (ret == 1) {  //conflict
				/* If the put failed due to a conflict, then we don't need to
				 * put the rest of the kvpairs; set the conflict flag and then
				 * break out of the loop. Below we will update our own commit's
				 * state as well as signal the commit record that follows ours. */
				kp_debug2("kp_put_master() encountered a conflict, so breaking "
						"out of our loop through the commit record's kvpairs\n");
				conflict = true;
				kp_todo("add to conflict_list?!\n");
				kp_todo("put this commit_record onto the garbage collector's "
						"list of snapshots to collect?\n");
				break;
			} else if (ret == 2) {  //deletion, but key not-found
				kp_debug_gc("kp_put_master() returned 2: tried to delete a "
						"key that's not in the master store. Ignoring this "
						"kvpair and continuing to loop.\n");
				kp_todo("what if this is the one and only kvpair in this "
						"commit? Could this cause problems??\n");
			} else if (ret != 0) {  //error
				kp_error("kp_put_master() returned error=%d, so breaking out "
						"of our loop through the commit record's kvpairs\n",
						ret);
				error = true;
				break;
			} else {
				kp_debug2("successfully merged pair with key=%s, value=%p, "
					"size=%zu; iterating to next kvpair\n", pair->key,
					pair->value, pair->size);
			}

			/* As soon as we have transitioned to INPROGRESS[_OK] and put our
			 * first kvpair into the master store, it's possible for _somebody
			 * else_ to cause us to have a conflict (i.e. a commit that has
			 * started before us but is still in-progress ends up inserting
			 * into a version table before an entry that we have already
			 * inserted during our merge). If this has happened, and our
			 * previous commit record has already signaled us (we were in
			 * INPROGRESS_OK), then we should be in the ABORTED state already.
			 * If this has happened but out previous cr has not finished yet
			 * (we were in INPROGRESS), then we should now be in WAIT_TO_ABORT.
			 * 
			 * If we find ourself in either of these states here, what do we
			 * want to do? The same thing that we would normally do if we were
			 * completing our merge normally, but we just want to exit this
			 * loop early, and hit the "else" case below. So, we simply
			 * break out of the loop here; without setting "conflict" or
			 * "error" to true; then we'll signal ourself below with is_owner
			 * and success set (even though we know that we're going to abort).
			 *
			 * This seems fragile, but as an add-on to an already-working
			 * merge process and state machine, it seems safe. This is mostly
			 * an optimization that will allow us to skip meaningless work
			 * when somebody aborts us, which in turn may lead to fewer
			 * aborts caused by this merge. Looking at the state transition
			 * function now, it seems that we could also set conflict = true
			 * here and still be safe; we'll always just remain in the proper
			 * WAIT_TO_ABORT or ABORTED state.
			 * 
			 * Whew. */
			if (cr->state == WAIT_TO_ABORT || cr->state == ABORTED) {
				kp_debug2("while merging our commit [%ju,%ju], somebody "
						"else has found a conflict and aborted us (our "
						"state is now %s); breaking out of the merge "
						"loop, and the next transition below will handle "
						"the next steps. We had %ju kvpairs left to merge\n",
						cr->begin_snapshot, cr->end_snapshot,
						commit_state_to_string(cr->state), (len - i - 1));
				break;
			}
		}  //loop through kvpairs
	}

	/* At this point, either conflict or error is true and we broke out
	 * of the merge loop early (or never entered if we got an early
	 * error...), or neither is true and we have finished
	 * merging in all of the kvpairs in this commit record. In the former
	 * case, we will always set this commit record's state to ABORTED.
	 * In the success case, we will update this commit record's state
	 * based on what its current state is set to.
	 */
	if (conflict || error) {
		kp_debug2("conflict=%s, er-ror=%s; calling state transition "
				"function with fa-ilure to transition this commit record's "
				"state to ABORTED\n", conflict ? "true" : "false",
				error ? "true" : "false");
		/* false: we're not the previous thread finalizing our commit.
		 * false: this is not a success transition, we're aborting.
		 * true: we are the owner of this commit record. Therefore,
		 * NOTE that we may wait on the cr's condition variable here! (in
		 * the WAIT_TO_ABORT state, until our previous commit finalizes!)
		 * Because we're the owner and we will wait, we don't expect the
		 * new_state that is returned to be WAIT_TO_ABORT. */
		new_state = kp_commit_state_transition(cr, false, false, true,
				kv->detect_conflicts, kv->use_nvm);
		if (new_state != ABORTED) {
			/* Cleanup? So far in this function we've set this commit record's
			 * state to INPROGRESS and merged zero or more kvpairs into the
			 * master store; nothing to clean up here. However, we still
			 * need to signal the commit record that follows us, so we don't
			 * exit yet!! */
			error = true;
			kp_error("kp_commit_state_transition() returned unexpected "
					"new_state=%s, should be ABORTED; set error to true\n",
					commit_state_to_string(new_state));
		} else if (error) {
			kp_debug2("an error occurred during merge loop, so we've set "
					"error to true\n");
		} else {
			conflict = true;  //redundant...
			kp_debug2("a conflict occurred but no er-rors yet, so we've set "
					"conflict to true\n");
		}
	} else {
		/* There were no conflicts or errors during the merge, so transition
		 * to the next success state. In the transition function, we may
		 * wait on the commit record's condition variable (in the
		 * WAIT_TO_COMMIT state) if the commit just before ours has not
		 * reached in and updated this commit record's state yet.
		 *
		 * For arguments we pass false for prev_is_finalized (we're the owner,
		 * not the previous thread finalizing itself) true for success, and
		 * true for is_owner.
		 * And again, after the transition function returns, we
		 * set our retval variables appropriately but cannot exit yet,
		 * because we still have to signal the commit record that follows
		 * us! */
		kp_debug2("no conflicts or er-rors during merge, so transitioning "
				"to next success state (either COMMITTED or ABORTED, but "
				"could stop at WAIT_TO_COMMIT internally)\n");
		new_state = kp_commit_state_transition(cr, false, true, true,
				kv->detect_conflicts, kv->use_nvm);
		if (new_state == COMMITTED) {
			kp_debug2("nobody else conflicted us either, so our state is "
					"now COMMITTED we'll return success\n");
		} else if (new_state == ABORTED) {
			conflict = true;
			kp_debug2("somebody else conflicted this commit, so our state is "
					"now ABORTED and we've set conflict to true\n");
		} else {
			error = true;
			kp_error("got back unexpected new_state=%s (expect COMMITTED "
					"or ABORTED); set error to true\n",
					commit_state_to_string(new_state));
		}
	}

	/* Finally, the last thing that we ALWAYS do in this function, no
	 * matter what, is to signal to the commit record that follows ours
	 * to tell it that our commit has completed.
	 * Rationale: If we maintain the invariants that kp_merge_commit_record()
	 * is called EXACTLY ONCE on every commit record, and that we ALWAYS
	 * signal the next commit record before kp_merge_commit_record() exits,
	 * then this should guarantee (right?) that we always make progress in
	 * completing the commits in the log.
	 *
	 * Does it make sense to perform this step here or in kp-commit? At
	 * the moment it doesn't matter, but eventually kp-commit is going to
	 * return before the merge actually happens, and some other thread is
	 * going to call this function, so it belongs here.
	 *
	 * The actual implementation is very simple - back in
	 * kp_append_to_commit_log(), each just-appended commit record set
	 * a pointer in the commit record just-before it to the new commit
	 * record (there is thus a singly linked list within the vector).
	 * We assume that the commit log hasn't changed (or if it has, that
	 * these pointers have been kept consistent), so we just get the
	 * next commit record and perform a transition on it - the same
	 * transition function that we always call. We pass true for
	 * prev_is_finalized, which will transition the next commit record
	 * into an "OK" state, or out of a WAIT_TO_[COMMIT,ABORT] state; this
	 * should be the only place where prev_is_finalized is set to true
	 * for a transition!! (one exception in append_commit_to_log() function,
	 * see that code for details). We pass true for success, and false for
	 * is_owner.
	 *
	 * NOTE: early on when version 3.1 was first implemented, it wasn't
	 * uncommon for the next_cr to have already "finished" and be in a
	 * WAIT_TO_* state by the time this commit got to here, even with
	 * just two threads.
	 *   Actually, early on there was a bug where the next_cr had already
	 *   gone to ABORTED by the time we got here, but I fixed that when
	 *   I added the WAIT_TO_ABORT state.
	 */
	kp_debug_print_cr("cr at end of merge", cr);
	next_cr = cr->next_cr;
	if (kv->detect_conflicts && next_cr) {
		kp_debug2("calling state transition function (prev_is_finalized"
				"=true, success=true) on next_cr!\n");
		kp_debug_print_cr("next_cr at end of merge", next_cr);
#ifdef KP_ASSERT
		if (next_cr->state == ABORTED || next_cr->state == COMMITTED) {
			/* I hit this case once in a test run, and what actually happened
			 * was that T5 was in WAIT_TO_COMMIT, then T4 signaled T5 and
			 * transitioned its state to COMMITTED, then T6 was appended and
			 * saw that its prev_cr T5 was already COMMITTED so it set its
			 * initial state to CREATED_OK, then T6 performed its entire
			 * merge and COMMITTED, and only _then_ did T5 wake up and
			 * try to transition its next_cr T6 right here. Holy crap.
			 *   The race condition between finalizing a commit and
			 *   signaling its successor strikes... what can/should we
			 *   do about it?
			 *   If we just let this code pass through here, then the
			 *   transition function will attempt to move a COMMITTED
			 *   record to a subsequent success state (with prev_is_finalized
			 *   set);
			 * In the one instance where I observed this behavior, the
			 * next_cr was COMMITTED, but I think it could also have been
			 * ABORTED (if it found a conflict _itself_ while it was
			 * performing its merge - it definitely couldn't have been
			 * conflicted by somebody else, because no prior commits could
			 * have been still running).
			 */
			kp_debug2("next commit record has already been finalized "
					"before its previous commit record has been "
					"finalized; this is likely a result of the \"benign "
					"race condition\" between when a commit record (us) "
					"is finalized and when its next commit record is "
					"transitioned (by us). We'll proceed to the transition "
					"code below anyway, which will also notify us about "
					"this...\n");
		}
#endif
		new_state = kp_commit_state_transition(next_cr, true, true, false,
				kv->detect_conflicts, kv->use_nvm);
		  /* Don't even think about passing false for prev_is_finalized
		   * here - this could do something crazy like put next_cr into
		   * a WAIT_TO_* state, which would cause the next_cr commit to
		   * wait (forever!). */
		kp_debug2("transitioned next_cr, got its new_state=%s\n",
				commit_state_to_string(new_state));
#ifdef KP_ASSERT
		/* We don't expect any of the WAIT_TO_* or non-_OK states to be
		 * returned here; we are the previous commit that is signaling
		 * the next commit, so that next commit better not be waiting
		 * for anything else! */
		if (new_state != CREATED_OK && new_state != INPROGRESS_OK &&
			new_state != COMMITTED && new_state != ABORTED) {
			kp_die("got unexpected new_state=%s when transitioning next_cr!\n",
					commit_state_to_string(new_state));
		}
#endif
		cr->debug_signalled = true;
	} else {
		if (!kv->detect_conflicts) {
			kp_debug2("kv->detect_conflicts=false, so we skipped the part "
					"of the merge where we signal our next_cr after we've "
					"completed; when conflict detection is disabled, all "
					"commit records start in an OK state, so they don't "
					"need to wait for their previous commits to signal "
					"them.\n");
		} else {
			kp_debug2("this commit record's next_cr pointer is still NULL, so "
					"we have nothing to signal / transition here; we've already "
					"finalized our state to COMMITTED or ABORTED though, so when "
					"the next cr is appended it will check our state and make "
					"itself OK.\n");
		}
	}

	/* Ok, we're finally done with the merge - whew! At this point, the
	 * error and conflict bools may have been set to true - if they have,
	 * then return error or return conflict. Otherwise, return success!
	 */
	if (error) {
		kp_debug2("error has been set to true, so returning -1\n");
		return -1;
	} else if (conflict) {
		kp_debug2("conflict has been set to true, so returning 1\n");
#ifdef KP_ASSERT
		if (!kv->detect_conflicts) {
			kp_die("about to return \"conflict\", but kv->detect_conflicts "
					"is false!!\n");
		}
#endif
		return 1;
	}
	kp_debug2("neither er-ror nor conflict set to true, so returning "
			"success\n");
	return 0;
}

/* Performs the following steps:
 *   Append the commit record to the commit log
 *   Increment and flush the global snapshot/timestamp number
 *   Write the new snapshot number as the commit record's end_snapshot
 *   Set the commit record's state to WRITTEN
 *   Flush the commit record's new snapshot + state
 *   Add the new snapshot number to the list of snapshots in use
 * These are performed ATOMICALLY by locking the kvstore's "snapshot
 * lock" while all of the operations are performed. Additionally,
 * if the kvstore's use_nvm flag is true, then this function will
 * make sure that everything happens durably; this is the "point of
 * no return," and after this function returns the commit will only
 * fail due to a conflict.
 *
 * Returns: the new snapshot number that was assigned as the end_snapshot
 * for the commit record, or UINT64_MAX on error.
 */
uint64_t kp_append_to_commit_log(kp_kvstore *kv, kp_commit_record *cr)
{
	int ret;
	uint64_t end_snapshot;
	kp_commit_record *prev_cr = NULL;
	commit_state new_state;
	uint64_t orig_count = UINT64_MAX;

	if (!kv || !cr) {
		kp_error("got a null argument: kv=%p, cr=%p\n", kv, cr);
		return UINT64_MAX;
	}

#ifdef KP_ASSERT
	if (! kv->is_master) {
		kp_die("shouldn't call this function for local stores!\n");
	}
	if (! kv->use_nvm) {
		kp_die("check that this is implemented correctly: use_nvm = false!\n");
	}
	if (cr->state != CREATED) {
		kp_die("expect cr->state to be CREATED, but it is %s\n",
				commit_state_to_string(cr->state));
	}
	if (!kv->commit_log) {
		kp_die("kv->commit_log is NULL\n");
	}
#endif

	/* NOTE: when printing the cr's end->snapshot here, we expect it to
	 * still be UINT64_MAX (18446744073709551615) - we don't set it until
	 * later in this function!!
	 */
	kp_debug("entered; is_master=%s, cr=%p [%ju, %ju]\n",
			kv->is_master ? "true" : "false", cr,
			cr->begin_snapshot, cr->end_snapshot);
	kp_debug_print_cr("cr in kp_append_to_commit_log", cr);
	orig_count = vector_count(kv->commit_log);
	kp_debug2("length of kv->commit_log = %ju at start of function\n",
			orig_count);

	/* Before appending the commit record to the commit log, we take its
	 * state_lock, so that after it is committed we can flush its snapshot
	 * and its new state before anybody else tries to read it.
	 */
	kp_mutex_lock("cr->state_lock", cr->state_lock);

	/* This code is similar to kp_increment_snapshot_mark_in_use() -
	 * they are currently the only two users of the snapshot_lock,
	 * so they block each other (but this is not a problem). */
#ifndef UNSAFE_COMMIT
	kp_mutex_lock("kv->snapshot_lock", kv->snapshot_lock);
#endif

	/* The vector_append() flushes the appended element and the vector's
	 * new count internally. */
	ret = vector_append(kv->commit_log, (void *)cr, (void **)&prev_cr);
	if (ret != 0) {
		kp_error("vector_append(kv->commit_log) returned error=%d\n", ret);
#ifndef UNSAFE_COMMIT
		kp_mutex_unlock("kv->snapshot_lock", kv->snapshot_lock);
#endif
		kp_mutex_unlock("cr->state_lock", cr->state_lock);
		return UINT64_MAX;
	}
	kp_debug("appended commit record to kv's commit_log\n");
#if 0 //used to be #ifdef KP_ASSERT
	if (orig_count > 0) {
		if (!prev_cr) {
			kp_die("vector_append() failed to set prev_cr\n");
		}
	}
	// Race condition here, dummy! Somebody else could have appended
	// in-between.
	//else if (prev_cr) {
	//	kp_die("vector_append() failed to set prev_cr to NULL\n");
	//}
#endif

	if (prev_cr) {
		kp_debug2("got prev_cr with range=[%ju, %ju] and state=%s\n",
				prev_cr->begin_snapshot, prev_cr->end_snapshot,
				commit_state_to_string(prev_cr->state));
	} else {
		kp_debug2("this is the first append to the commit log, so prev_cr "
				"is NULL!\n");
#ifdef KP_ASSERT
		if (vector_count(kv->commit_log) != 1) {
			kp_die("unexpected length for kv->commit_log=%llu\n",
					vector_count(kv->commit_log));
		}
#endif
	}

	/* FAIL here: the commit record has been appended to the commit log,
	 * but its state is still just CREATED; on recovery we can just
	 * destroy the commit record.
	 *   Seems like we could also resume, rather than cleaning up...
	 *   everything is reachable and durable, just need to get next
	 *   snapshot number.
	 */

	/* See kp_increment_snapshot_mark_in_use(): we always copy/use the
	 * snapshot number first, THEN increment it to the next value.
	 *
	 * We don't flush the global snapshot number here because we already
	 * copy it into every commit record (and it increases sequentially
	 * in the commit log). However, we _do_ put a memory fence after
	 * the update to the global snapshot; I'm not sure if this is required,
	 * but it seems to be a good idea (and I don't want to mess things up
	 * that are already working: we used to have the flush (with an mfence
	 * inside) here...). */
	end_snapshot = kv->global_snapshot;
	PM_EQU((kv->global_snapshot), (kv->global_snapshot + 1));
	kp_mfence();
	//kp_flush_range(&(kv->global_snapshot), sizeof(uint64_t), kv->use_nvm);
	kp_debug2("incremented kv->global_snapshot to %ju; end_snapshot=%ju\n",
	        kv->global_snapshot, end_snapshot);

	/* FAIL here: the commit record has been append and the global_snapshot
	 * incremented.
	 * We used to change the commit record state while locked, but this is
	 * definitely not global and so I think it can be done outside of the
	 * lock (I don't think this will have an impact on recovery). We also
	 * used to append the garbage collector's list of snapshots-in-use
	 * while under the lock; not doing so is potentially unsafe (because the
	 * garbage collector could start running before we have a chance to add
	 * our snapshot to the list, but it seems unlikely to happen...)
	 *   BUG: need to figure out how to detect when this happens, if we're
	 *   not going to prevent it!
	 *     Can't just say "it would never happen".
	 */
#ifndef UNSAFE_COMMIT
	kp_mutex_unlock("kv->snapshot_lock", kv->snapshot_lock);
#endif

	/* At this point, we still hold our commit record's state_mutex, but
	 * we don't hold the commit log's lock. We still have to set the commit
	 * record's end_snapshot; we do this while the commit record is locked,
	 * although I'm not sure this is even necessary (who reads it?). We've
	 * got to do it at some point anyway. We also flush the end_snapshot,
	 * for consistency (not durability) purposes, I think. If we don't flush
	 * it, then on recovery we might not know how to undo / redo a partial
	 * merge that this commit may have been in the middle of when the
	 * failure occurred (although it seems like the commit_log should
	 * implicitly know the end_snapshot of every commit record...).
	 * We unlock when we're done. */
	PM_EQU((cr->end_snapshot), (end_snapshot));
	kp_flush_range(&(cr->end_snapshot), sizeof(uint64_t), kv->use_nvm);

	kp_mutex_unlock("cr->state_lock", cr->state_lock);

	/* Ok, here's another step for version 3.1 (concurrent merges
	 * with conflict detection): when each commit record has finished
	 * being merged in, we need to signal the NEXT commit record. We
	 * could do this by searching in the commit log for the next commit
	 * record when we reach this point, but that seems expensive. Instead,
	 * we link the prior commit record to the just-appended commit record
	 * now.
	 * This is a bit hacky for several reasons: it somewhat relies on
	 * the fact that the commit log is immutable right now, and it is
	 * effectively creating a linked-list within a vector. I think it
	 * makes sense for now though, because eventually I think we'll want
	 * to replace the commit log vector with a linked-list (or a skiplist
	 * or something with efficient indexing?) anyway, and also because
	 * we've already gotten the prev_cr (because we hacked vector_append...),
	 * so we might as well use it.
	 *
	 * IMPORTANT: there is a potential race condition with the steps in
	 * this function (set forward-pointer to new commit record, check if
	 * the state of the previous commit is finalized or not) and the steps
	 * in kp_merge_commit_record() (finalize the merged commit's state,
	 * signal the next commit record) - to avoid a race, we must set this
	 * forward pointer BEFORE we check the state of the previous commit!
	 * If we didn't then this interleaving would be possible:
	 *        Commit n                    Commit n+1
	 *                         1) Check n's state, see that it's unfinalized
	 * 2) Finalize state (committed or aborted)
	 * 3) Attempt to signal next, but null ptr
	 *                         4) Set n's next pointer
	 * By performing the next-pointer-set before the check-previous-state,
	 * we avoid this potential interleaving; if n gets a null next pointer,
	 * this means that n+1 has yet to check n's state, so it will eventually
	 * do so (just below) and n+1 will transition to a committable state.
	 *
	 * prev_cr was set by the vector_append(); we check its state now.
	 * Is there a race condition here? Probably, but at the moment the
	 * commit log is append-only (commits are _never_ removed), so let's
	 * ignore the fact that it might be garbage-collected, etc. for now...
	 */
	if (kv->detect_conflicts) {
		if (prev_cr) {
#ifdef KP_ASSERT
			if (!cr) {
				kp_die("cr (%p) is unexpectedly NULL\n", cr);
			}
			if (!(cr->state_lock)) {
				kp_die("cr->state_lock (%p) is unexpectedly NULL\n",
						cr->state_lock);
			}
#endif
			kp_debug_print_cr("cr later in kp_append_to_commit_log", cr);
			kp_debug_print_cr("prev_cr in kp_append_to_commit_log", prev_cr);
			PM_EQU((prev_cr->next_cr), (cr));
			  /* Should we flush this? Seems like it's recoverable, can
			   * be set again during recovery just using commit log. */
			kp_mfence();
			  /* Putting this mfence here actually seems to have fixed
			   * a deadlock bug: after the prev_cr had set its state to
			   * ABORTED / COMMITTED (in kp_merge_commit_record()), it
			   * was apparently not seeing that its next_cr had been set
			   * to a non-NULL value, so it was failing to signal the
			   * cr that was appended in this function. Apparently this
			   * function was re-ordering this setting of prev_cr->next_cr
			   * until _after_ the check of the prev_cr's ABORTED /
			   * COMMITTED state below? wtf??
			   *
			   * Apparently I recognized this at some point, but just
			   * never did it: I had this line in the code.
			   *   kp_todo("we don't necessarily need the flush here, "
			   *       "but we DO need an mfence!!! Replace with an "
			   *       "explicit mfence call??\n");
			   * Crap.
			   *
			   * I think I figured out what was going on here (thanks to this
			   * web page:
			   * http://bartoszmilewski.com/2008/11/05/who-ordered-memory-fences-on-an-x86/):
			   * on x86, loads (the check for prev_cr ABORTED / COMMITTED later
			   * in this function) can be re-ordered to come before stores to
			   * _different_ memory locations (the setting of prev_cr->next_cr
			   * right here). So, we need an mfence anywhere in-between the
			   * store here and the load that comes below. */
			//kp_todo("also, we can save a flush / fence here by skipping the "
			//		"next_cr setting for prev_cr that is already finalized!\n");
			  /* Save this for later - don't want to adjust this anymore, now
			   * that deadlock is gone... ugh. */
			kp_debug2("set previous commit record [%ju,%ju]'s next_cr to point "
					"to the just-appended commit record (%p)\n",
					prev_cr->begin_snapshot, prev_cr->end_snapshot, cr);
		} else {
#ifdef KP_ASSERT
			//check this for the fourth time...
			if (orig_count != 0) {
				kp_die("orig_count unexpectedly=%ju (should be 0)\n", orig_count);
			}
#endif
			kp_debug2("this is the first commit in the commit log: prev_cr "
					"is NULL, so skipping the forward-pointer step\n");
		}
	} else {
		kp_debug2("skipping prev_cr manipulation because detect_conflicts is "
				"false\n");
		PM_EQU((prev_cr), (NULL));  //to be safe, below.
	}

	/* The commit record's state is CREATED, but we need to check the
	 * state of the commit prior to this one and set the current commit
	 * record's initial state to CREATED_OK if the previous commit's
	 * state is finalized. See the lengthy description in
	 * kp_merge_commit_record() for more details; it's possible that
	 * the state may be set to "OK" twice (once by us and once by the prior
	 * commit), but that's ok. We perform this without the lock held,
	 * because when we do transition the state, the first thing that
	 * the transition function does is acquire the lock!
	 *
	 * Additionally, if prev_cr is NULL, this means that this is the first
	 * commit into the log, and so we should always transition the commit
	 * record's state to ok!
	 */
	if (!kv->detect_conflicts || !prev_cr || prev_cr->state == COMMITTED ||
			prev_cr->state == ABORTED) {
		if (!kv->detect_conflicts) {
			kp_debug2("detect_conflicts is false, so we always start in "
					"the CREATED_OK state! (our prev_cr is never going "
					"to transition us)\n");
		}

		/* Update our own state to CREATED_OK and flush. This is the only
		 * case where the owner of the commit record can pass true for
		 * prev_is_finalized to the state transition function. Pass true for
		 * success, or else we'll abort! And pass true for is_owner. */
		new_state = kp_commit_state_transition(cr, true, true, true,
				kv->detect_conflicts, kv->use_nvm);
		if (new_state != CREATED_OK) {
			kp_error("expected new_state=CREATED_OK, but got state %s\n",
					commit_state_to_string(new_state));
			return UINT64_MAX;
		}
		if (prev_cr) {
			kp_debug2("prev_cr [%ju,%ju] has finalized state=%s, so updated "
					"just-appended commit record's state to %s\n",
					prev_cr->begin_snapshot, prev_cr->end_snapshot,
					commit_state_to_string(prev_cr->state),
					commit_state_to_string(cr->state));
		} else {
			kp_debug2("this is the first commit in the log (or "
					"detect_conflicts is false), so updated just-"
					"appended commit record's state to %s\n",
					commit_state_to_string(cr->state));
		}
	} else {
		kp_debug2("prev_cr [%ju,%ju] has not-finalized state=%s, so updated "
				"just-appended commit record's state to %s\n",
				prev_cr->begin_snapshot, prev_cr->end_snapshot,
				commit_state_to_string(prev_cr->state),
				commit_state_to_string(cr->state));
	}

	kp_debug2("set commit record's snapshot range to [%ju,%ju] and left "
			"its state as %s; commit record is now permanent, woohoo!\n",
			cr->begin_snapshot, cr->end_snapshot,
			commit_state_to_string(cr->state));

	/* FAIL here: the recovery process will complete the merge of the commit
	 * (which is now WRITTEN into the commit log). We haven't added this
	 * commit's end_snapshot to the list of snapshots in use, which is fine
	 * because all of the local workers are going to get new snapshot numbers
	 * (greater than the one that we just wrote into the commit record)
	 * during recovery. */

	/* We add the snapshot to the garbage collector's in-use list while we're
	 * still locked here, because if we don't, the garbage-collector could
	 * potentially run in-between when we unlock and when the append
	 * completes.
	 *   Actually, we changed this code; there is now a race condition here...
	 *   BUG!
	 * Don't bother with the complexity of kp_add_to_cps_to_keep(): we
	 * are certain that our snapshot number here is unique, so we don't
	 * have to check that it's not in any other vectors already, etc.
	 * NOTE: if we ever take the gc lock elsewhere in our code, we must
	 * make sure that it doesn't ever try to take the snapshot_lock too,
	 * or else we could deadlock!
	 */
#ifndef DISABLE_GC_SNAPSHOT_TRACKING
	kp_mutex_lock("kv->gc->lock", kv->gc->lock);
	ret = vector64_append(kv->gc->snapshots_in_use, end_snapshot);
	  //CHECK: make sure this is CDDS!
	/* FAIL here: same as just above; the snapshots_in_use vector should
	 * be volatile, so don't need to do anything additional for it. */
	kp_mutex_unlock("kv->gc->lock", kv->gc->lock);
	kp_debug("appended end_snapshot %ju to gc->snapshots_in_use\n",
			end_snapshot);
#else
	/* Temporarily disabled snapshots_in_use for testing!!! */
	ret = 0;
#endif

	if (ret != 0) {
		kp_error("vector64_append(snapshots_in_use) returned error=%d\n", ret);
		kp_error("leaving all other state modified and returning error...\n");
		return UINT64_MAX;
	}
	if (end_snapshot + 1 == UINT64_MAX) {
		kp_error("hit snapshot ceiling: end_snapshot=%ju, kv->global_snapshot=%ju\n",
				end_snapshot, kv->global_snapshot);
		kp_error("leaving all other state modified and returning error...\n");
		return UINT64_MAX;
	}

	kp_debug("done committing this commit record; returning snapshot %ju "
			"to be used as local worker's new working snapshot\n",
			end_snapshot);
	return end_snapshot;
}

/* See kp_kvstore_internal.h for detailed notes. */
int kp_commit(kp_kvstore *kv, kp_commit_record *cr,
		        void **conflict_list, uint64_t *new_snapshot)
{
	int ret_merge, retval;
#ifndef DISABLE_GC_SNAPSHOT_TRACKING
	uint64_t idx;
#endif
	uint64_t ret64;
	uint64_t begin_snapshot, commit_snapshot, snapshot_to_remove;

	if (!kv || !cr) {
		kp_error("got a null argument: kv=%p, cr=%p\n", kv, cr);
		return -1;
	}
	kp_debug("entered; got conflict_list=%p (probably nil). Beginning "
			"the process of merging in this commit record\n", conflict_list);

    /* Eventually, we'd like this function to simply take the commit record
	 * from the local store, append it to a queue, and be done - some other
	 * master thread would come along, dequeue the commit record, and merge
	 * it in. However, for now, this function just merges in each commit
	 * synchronously.
	 */

	/* "Point of no return" - after kp_append_to_commit_log() returns,
	 * the commit can only fail due to a conflict. See the function
	 * definition for extensive notes. */
	ret64 = kp_append_to_commit_log(kv, cr);
	if (ret64 == UINT64_MAX) {
		kp_error("kp_append_to_commit_log() returned error\n");
		return -1;
	}
	begin_snapshot = cr->begin_snapshot;
	commit_snapshot = ret64;
	  /* This snapshot number is the end_snapshot for the commit record
	   * that we just appended, and will also become the begin_snapshot
	   * for the local worker's next commit record. */

	/* Now that the commit record is durable, we take that same commit
	 * record and perform the actual merge itself. This is the part that
	 * eventually we'd like to be performed by a separate thread.
	 *
	 * Note the requirement specified by kp_merge_commit_record(): it must
	 * be called EXACTLY ONCE (no more, no fewer) on every commit that
	 * is appended to the commit log. Only if this requirement is met
	 * are we guaranteed to make forward progress on our merging of commits
	 * in the commit log.
	 *
	 * Note that we just pass the same kp_commit_record pointer here that
	 * we received from the caller and that we just committed to the
	 * commit log; however, in the future, this commit record may be
	 * pulled out of the commit log itself. */
	ret_merge = kp_merge_commit_record(kv, cr, conflict_list);
#ifdef KP_ASSERT
	/* "When this function [kp_merge_commit_record()] returns, the commit
	 *  is guaranteed to be either COMMITTED or ABORTED." */
	if (cr->state != COMMITTED && cr->state != ABORTED) {
		kp_die("failed the guarantee for kp_merge_commit_record(): "
				"cr->state=%s, but supposed to be COMMITTED or ABORTED\n",
				commit_state_to_string(cr->state));
	}
#endif
	snapshot_to_remove = UINT64_MAX;
	retval = -1;
	if (ret_merge == 1) {
		*new_snapshot = commit_snapshot;
		kp_debug2("kp_merge_commit_record() returned 1, means that this "
				"merge had a conflict - we'll return 1 from this function. "
				"Regardless, we've still set *new_snapshot=%ju, to be used "
				"by the local worker. "
				"We'll also remove commit_snapshot=%ju from the list of "
				"snapshots_in_use and add it to the garbage collector's "
				"list of collectable snapshots.\n", *new_snapshot,
				commit_snapshot);
		retval = 1;
		snapshot_to_remove = commit_snapshot;
	} else if (ret_merge != 0) {
		kp_error("kp_merge_commit_record() returned error=%d; removing "
				"the commit_snapshot=%ju from the list of snapshots_in_use "
				"and adding it to the garbage collector's list of "
				"collectable snapshots, then returning error\n",
				ret_merge, commit_snapshot);
		retval = -1;
		snapshot_to_remove = commit_snapshot;
	} else {
		/* On success, we'll remove the local worker's previous snapshot
		 * number from the list of snapshots_in_use. */
		*new_snapshot = commit_snapshot;
		kp_debug2("commit record was successfully added to commit log and "
				"merged into the master kvstore; we'll return success with "
				"*new_snapshot=%ju\n", *new_snapshot);
		snapshot_to_remove = begin_snapshot;
		retval = 0;
	}
	kp_debug("set snapshot_to_remove=%ju\n", snapshot_to_remove);
	kp_todo("IMPORTANT: BUG: we return the snapshot number of the just-"
			"appended commit record to the local worker here, but what "
			"we need to return is the greatest COMMITTED snapshot number "
			"here. In order to track this without using a lock, we need "
			"to use a compare-and-swap thingy, which I don't want to "
			"implement right now. So, we just continue returning the "
			"just-created snapshot, and if this transaction was actually "
			"aborted and not committed, then we'll pay the price when the "
			"worker performs some gets.\n");

	/* After merging in the commit, we still have a few things left to
	 * do: remove the local
	 * worker's previous snapshot from the snapshots_in_use vector (or
	 * remove the new snapshot if the merge failed), and then kick off
	 * the garbage collector. It's ok to _remove_ things from the
	 * snapshots_in_use vector outside of the merge lock
	 * (kp_merge_commit_record()); because we're just removing a snapshot,
	 * it's ok if the garbage collector happens to run in-between the
	 * lock release and this code. We still take the gc's lock whenever
	 * we manipulate snapshots_in_use. It's also ok that we are doing
	 * this after the commit record is COMMITTED - snapshots_in_use is
	 * volatile state used only for gc purposes, so if a failure occurs
	 * the list of snapshots in use will just be reconstructed.
	 *
	 * snapshots_in_use isn't guaranteed to be in-order, because local
	 * workers may go back in time to older but still-existing snapshots.
	 * So, we first perform a LINEAR search for the snapshot that we
	 * want to remove, then delete it.
	 *   TODO: replace snapshots_in_use with a BST to optimize this?? */
#ifndef DISABLE_GC_SNAPSHOT_TRACKING
	kp_mutex_lock("kv->gc->lock", kv->gc->lock);
	idx = vector64_search_linear(kv->gc->snapshots_in_use,
			snapshot_to_remove);
	if (idx == VECTOR64_MAX) {
		kp_error("vector64_search_linear() returned not-found "
				"%d! Skipping removal of snapshot %ju from "
				"snapshots_in_use vector\n", ret, snapshot_to_remove);
	} else {
		ret64 = vector64_delete(kv->gc->snapshots_in_use, idx);
		  //CHECK - TODO: make sure this is CDDS!
		if (ret64 != begin_snapshot) {
			kp_error("vector64_delete() unexpectedly returned %ju\n",
					ret64);
		} else {
			kp_debug("successfully removed snapshot %ju from gc's "
					"snapshots_in_use vector (idx=%ju)\n",
					snapshot_to_remove, idx);
		}
	}
	kp_mutex_unlock("kv->gc->lock", kv->gc->lock);
#else
	/* Temporarily disabled snapshots_in_use for testing!!! */
	if (snapshot_to_remove == UINT64_MAX) {
		// mostly added this to eliminate "unused" warning...
		kp_die("snapshot_to_remove still == UINT64_MAX, unexpected!\n");
	}
#endif

	/* In error or conflict case, mark the busted snapshot for removal: */
	if (retval != 0) {
		kp_todo("add busted new_snapshot to garbage collector's list "
				"of collectable snapshots!\n");
	}

	/* FAIL here: ... */

	kp_todo("kick off garbage collector??\n");
	  //kp_gc_on_snapshot(kv, ...);
	  //  //see old code: kp_gc_on_checkpoint()
	
	return retval;
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
