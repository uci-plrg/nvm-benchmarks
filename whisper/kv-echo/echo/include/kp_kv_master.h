/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Katelin Bailey & Peter Hornyack
 * Created: 11/16/2011
 * University of Washington
 *
 * Master portion of the key-value store.
 * No puts should go directly to the master; gets from the master are still
 * required.
 */

#ifndef KP_KV_MASTER_H
#define KP_KV_MASTER_H

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>

/* Opaque handle to the master portion of the key-value store. */
struct kp_kv_master_struct;
typedef struct kp_kv_master_struct kp_kv_master;

/* These modes specify the consistency guarantees that the kvstore should
 * provide.
 *   WEAK: gets to keys that are not present in the local store always
 *     return the most-recent value from the master store.
 *   SNAPSHOT: gets to keys that are not present in the local store always
 *     return the value from the local store's current snapshot. See
 *     kp_kv_local.h for a description of how snapshot numbers work.
 *   SEQUENTIAL: puts to the local store are always immediately merged
 *     into the master, so gets will always return the most-recent value
 *     from any worker.
 *   ...
 */
typedef enum consistency_mode_enum {
	MODE_WEAK,
	MODE_SNAPSHOT,
	MODE_SEQUENTIAL,
} consistency_mode;
#define consistency_mode_to_string(m) \
	m == MODE_WEAK ? "WEAK" : \
	m == MODE_SNAPSHOT ? "SNAPSHOT" : \
	m == MODE_SEQUENTIAL ? "SEQUENTIAL" : "UNKNOWN"
	
/* Initializes the master portion of a KV store. This master portion must
 * be created first, and then at least one local portion must be created
 * (because puts cannot be done directly to the master). The consistency
 * mode must be specified at creation time, and cannot be changed after
 * creation. If the kvstore will be stored on non-volatile memory, then
 * use_nvm should be set to true so that the store will handle the memory
 * appropriately (allocating to 0, flushing, etc.).
 * Returns: 0 on success, -1 on error. On success, *master is set to
 * point to a newly-allocated kp_kv_master struct.
 */
int kp_kv_master_create(kp_kv_master **master, consistency_mode mode,
		size_t expected_max_keys, bool do_conflict_detection, bool use_nvm);

/* Deallocates a kp_kv_master struct and performs other cleanup tasks.
 * Returns: 0 on success, -1 on error.
 */
int kp_kv_master_destroy(kp_kv_master *master);

/* Returns: the master's id.
 */
uint32_t kp_master_get_id(kp_kv_master *master);

/* Returns: the consistency mode that was set for this master kvstore
 * at creation time, or -1 on error.
 */
consistency_mode kp_master_get_mode(kp_kv_master *master);

/* Returns: the latest/maximum snapshot/checkpoint number in the master's
 * kp_kvstore, or UINT64_MAX on error (the only possible error is a null
 * pointer, so it is probably ok to not check the return value).
 * NOTE that the latest snapshot number is the one that is still being
 * modified; depending on the use case, the caller of this function may
 * want to subtract 1 from it!
 */
uint64_t kp_master_get_latest_snapshot(kp_kv_master *master);

/* ...
 * Prints statistics about the current state of the master KV store to the
 * specified stream. The stream argument should be a valid, open file
 * pointer; "stdout" or "stderr" can be used for printing to standard
 * output/error. The stream is not closed by this function. This function
 * flushes the data that it writes to the stream (fflush(stream)), but
 * does not sync it to disk (fsync(stream)).
 * http://www.chemie.fu-berlin.de/chemnet/use/info/libc/libc_7.html
 * Returns: 0 on success, -1 on error.
 */
int kp_master_print_stats(kp_kv_master *master, FILE *stream, bool detailed);

/* Returns true if the master was created to run on non-volatile memory,
 * otherwise returns false. */
bool kp_master_uses_nvm(kp_kv_master *master);

#endif  // KP_KV_MASTER_H
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
