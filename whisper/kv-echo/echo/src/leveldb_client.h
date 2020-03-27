/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Peter Hornyack and Katelin Bailey
 * 11/10/11
 * University of Washington
 *
 * API for a basic C client for LevelDB.
 *
 * This client library should follow these tips, but probably doesn't:
 * https://git.kernel.org/?p=linux/kernel/git/kay/libabc.git;a=blob_plain;f=README
 */

#ifndef LEVELDB_CLIENT_H
#define LEVELDB_CLIENT_H

#include <stdbool.h>

/* Opaque identifier for the LevelDB database that this client interacts
 * with.
 */
struct leveldb_struct;
typedef struct leveldb_struct leveldb;

#define LEVELDB_DIR ""

/* Creates and initializes a new levelDB database. The database will be
 * created in the LEVELDB_DIR in a folder with the given name; the name
 * that is passed into this function can be freed after the function returns,
 * if appropriate.
 * NOTE: currently, if a database with the given name already exists, the
 * client will destroy it!
 * write_buffer_size is the amount of data to build up in-memory, before
 * converting the data to a sorted on-disk file; pass in 0 to use the
 * default value (4MB).
 * If use_compression is true, then "SnappyCompression" will be used.
 * If sync_writes is true, then all writes will be flushed from the OS's
 * buffer cache to disk before the write returns.
 * Returns: 0 on success, -1 on error. On success, *db is set to point
 * to a newly-allocated leveldb.
 */
int leveldb_client_start(leveldb **db, const char *name,
		size_t write_buffer_size, bool use_compression, bool sync_writes);

/* Stops a LevelDB database and performs necessary cleanup tasks. The
 * leveldb passed as an argument here is freed, so the pointer should no
 * longer be used after this function returns.
 * Returns: 0 on success, -1 on error.
 */
int leveldb_client_stop(leveldb *db);

void *leveldb_client_start_trans(leveldb *db);
int leveldb_client_stop_trans(leveldb *db, void* wb);


/* Puts a key-value pair into the database. Both the key and the value must
 * be null-terminated strings. The caller can free these strings after
 * the function has returned.
 * Returns: 0 on success, -1 on error.
 */
int leveldb_client_put(leveldb *db, const char *key, const char *value);
int leveldb_client_batch_put(leveldb *db, void *batch,const char *key, const char *value);

/* Gets a key-value pair from the database. The key should be a
 * null-terminated string. The caller is responsible for freeing key and
 * *value after this function returns.
 * The internal leveldb get() function does not return a null-zero-
 * terminated string; if the add_null_zero argument is set to true,
 * then our client function will allocate a new string that IS null-
 * zero-terminated. Note that doing this will add a performance cost
 * that should not be attributed to leveldb itself!
 * Returns: 0 on success, 1 if key not found, -1 on error.
 */
int leveldb_client_get(leveldb *db, const char *key, char **value,
		bool add_null_zero);


/* NOT IMPLEMENTED YET!
 * Deletes a key-value pair from the database. The key must be a
 * null-terminated string.
 * Returns: 0 on success, 1 if key not found, -1 on error.
 */
int leveldb_client_delete(leveldb *db, const char *key);
int leveldb_client_batch_delete(leveldb *db, void *batch, const char *key);

#endif  //LEVELDB_CLIENT_H

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
