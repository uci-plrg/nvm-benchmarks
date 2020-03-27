/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Peter Hornyack and Katelin Bailey
 * 11/10/11
 * University of Washington
 *
 * Implementation of a basic C client for LevelDB. Copies heavily from
 * leveldb/db/c_test.c in the leveldb source code. leveldb provides us
 * with C bindings (it is written in C++), so the client code in this
 * file is very straightforward; our functions here are just wrappers
 * that provide the interface that we desire.
 */

#include "kp_macros.h"
#include "leveldb_client.h"
#include "leveldb/c.h"
#include <stdlib.h>
#include <string.h>

#define WRITE_BUFFER_DEFAULT (4<<20)
#define WRITE_BUFFER_MIN 1024
  /* Minimum write buffer size of 1024 is chosen arbitrarily... */
//#define CACHE_CAPACITY (100 * 1048576)
#define CACHE_CAPACITY (1024)
  /* Not really sure how big an impact this has on performance, or how it
   * differs from the primary in-memory log/map...
   */
#define MAX_OPEN_FILES 1
  /* Number of open files that can be used by the DB.  You may need to
   * increase this if your database has a large working set (budget
   * one open file per 2MB of working set).
   * Default: 1000 (test file uses 10)
   */
#define BLOCK_SIZE (1024)
  /* Control over blocks (user data is stored in a set of blocks, and
   * a block is the unit of reading from disk):
   * Approximate size of user data packed per block.  Note that the
   * block size specified here corresponds to uncompressed data.  The
   * actual size of the unit read from disk may be smaller if
   * compression is enabled.  This parameter can be changed dynamically.
   * Default: 4K (test file uses 1K)
   */
#define BLOCK_RESTART_INTERVAL 16
  /* Number of keys between restart points for delta encoding of keys.
   * This parameter can be changed dynamically.  Most clients should
   * leave this parameter alone.
   * Default: 16 (test file uses 8)
   */
#define DELETE_DB_FROM_DISK 1
  /* Set this to 0 if you do not want the database files (usually stored
   * in /tmp) to be deleted on leveldb_stop().
   */

/* External structures: */
struct leveldb_struct {
	char *name;
	leveldb_t *db;
	leveldb_comparator_t *cmp;
	leveldb_cache_t *cache;
	leveldb_env_t *env;
	leveldb_options_t *options;
	leveldb_readoptions_t *roptions;
	leveldb_writeoptions_t *woptions;
};

/* Frees an error if non-NULL. */
static void free_err(char **err)
{
	if (*err) {
		free(*err);
		*err = NULL;
	}
}

/* Destructor for a comparator. */
static void cmp_destroy(void* arg)
{
	return;
}

/* Comparison function used by leveldb_comparator_t. */
static int cmp_compare(void* arg, const char* a, size_t alen,
		const char* b, size_t blen)
{
	int n = (alen < blen) ? alen : blen;
	int r = memcmp(a, b, n);
	if (r == 0) {
		if (alen < blen) {
			r = -1;
		} else if (alen > blen) {
			r = +1;
		}
	}
	return r;
}

/* Returns the name of the comparator. */
static const char* cmp_name(void* arg)
{
	return "comparator-name";
}

//TODO: add "bool create_if_missing" to this API!
int leveldb_client_start(leveldb **db, const char *name,
		size_t write_buffer_size, bool use_compression, bool sync_writes)
{
	int namelen;
	char *db_name;
	char *err = NULL;
	leveldb_t* db_t;
	leveldb_comparator_t *cmp;
	leveldb_cache_t *cache;
	leveldb_env_t *env;
	leveldb_options_t *options;
	leveldb_readoptions_t *roptions;
	leveldb_writeoptions_t *woptions;
	
	*db = (leveldb *)malloc(sizeof(leveldb));
	if (*db == NULL) {
		kp_error("malloc(leveldb) failed!\n");
		return -1;
	}

	/* Check arguments: */
	if (strlen(name) < 1) {
		kp_error("name has no length!\n");
		return -1;
	}
	if (write_buffer_size == 0) {
		write_buffer_size = WRITE_BUFFER_DEFAULT;
	}
	if (write_buffer_size < WRITE_BUFFER_MIN) {
		kp_error("write_buffer_size=%zu too small; min=%u\n",
				write_buffer_size, WRITE_BUFFER_MIN);
		return -1;
	}
	kp_debug("got args: name=%s, write_buffer_size=%zu, use_compression=%s, "
			"sync_writes=%s\n",
			name, write_buffer_size, use_compression ? "true" : "false",
			sync_writes ? "true" : "false");

	/* Format of db name: LEVELDB_DIR + / + name */
	namelen = strlen(name);
	db_name = malloc(namelen + 1);
	if (!db_name) {
		kp_error("malloc(db_name) failed!\n");
		return -1;
	}
	strncpy(db_name, name, namelen+1);          //copy null-zero
	(*db)->name = db_name;
	kp_debug("constructed db->name=[%s]\n", (*db)->name);

	/* Set up the database's comparator, environment, cache, etc. According
	 * to leveldb/include/leveldb/c.h, all functions that can raise an error
	 * must be passed a "char **errptr" (set to NULL!) as the last argument;
	 * I guess functions that don't take this pointer can't return errors.
	 */
	cmp = leveldb_comparator_create(NULL, cmp_destroy, cmp_compare,
			cmp_name);  //first arg is "state"
	env = leveldb_create_default_env();
	cache = leveldb_cache_create_lru(CACHE_CAPACITY);
	//	cache = NULL;
	(*db)->cmp = cmp;
	(*db)->env = env;
	(*db)->cache = cache;

	/* Set up the database's various options. Many of these will affect the
	 * database's performance! (see leveldb/include/leveldb/options.h).
	 */
	options = leveldb_options_create();
	leveldb_options_set_create_if_missing(options, 1);
	leveldb_options_set_comparator(options, cmp);
	leveldb_options_set_error_if_exists(options, 1);  //raise error if db already exists
	leveldb_options_set_cache(options, cache);
	//	leveldb_options_set_cache(options, NULL); //disable cache??
	leveldb_options_set_env(options, env);
	leveldb_options_set_info_log(options, NULL);  //NULL: write info to file in db's dir
	leveldb_options_set_write_buffer_size(options, write_buffer_size);
	  /* Amount of data to build up in memory (backed by an unsorted log
	   * on disk) before converting to a sorted on-disk file.
	   * Larger values increase performance, especially during bulk loads.
	   * Up to two write buffers may be held in memory at the same time,
	   * so you may wish to adjust this parameter to control memory usage.
	   * Also, a larger write buffer will result in a longer recovery time
	   * the next time the database is opened.
	   * Default: 4MB  (test file uses 100000 bytes)
	   */
	leveldb_options_set_paranoid_checks(options, 0);  //default false; test file uses true
	leveldb_options_set_max_open_files(options, MAX_OPEN_FILES);
	leveldb_options_set_block_size(options, BLOCK_SIZE);
	leveldb_options_set_block_restart_interval(options, BLOCK_RESTART_INTERVAL);
	leveldb_options_set_compression(options,
			use_compression ? leveldb_snappy_compression : leveldb_no_compression);
	(*db)->options = options;

	roptions = leveldb_readoptions_create();
	leveldb_readoptions_set_verify_checksums(roptions, 0);
	  /* If true, all data read from underlying storage will be
	   * verified against corresponding checksums.
	   * Default false; test file uses true.
	   */
	leveldb_readoptions_set_fill_cache(roptions, true);
	  /* Should the data read for this iteration be cached in memory?
	   * Callers may wish to set this field to false for bulk scans.
	   * Default true; test file uses false.
	   */
	(*db)->roptions = roptions;

	woptions = leveldb_writeoptions_create();
	leveldb_writeoptions_set_sync(woptions, sync_writes ? 1 : 0);
	  /* If true, the write will be flushed from the operating system
	   * buffer cache (by calling WritableFile::Sync()) before the write
	   * is considered complete.  If this flag is true, writes will be
	   * slower.
	   * If this flag is false, and the machine crashes, some recent
	   * writes may be lost.  Note that if it is just the process that
	   * crashes (i.e., the machine does not reboot), no writes will be
	   * lost even if sync==false.
	   * In other words, a DB write with sync==false has similar
	   * crash semantics as the "write()" system call.  A DB write
	   * with sync==true has similar crash semantics to a "write()"
	   * system call followed by "fsync()".
	   */
	(*db)->woptions = woptions;

	kp_debug("destroying previous copy of database, if it exists\n");
	leveldb_destroy_db((*db)->options, (*db)->name, &err);
	free_err(&err);

	kp_debug("opening/creating database [%s]\n", (*db)->name);
	db_t = leveldb_open((*db)->options, (*db)->name, &err);
	if (err) {
		kp_error("opening db returned error: %s\n", err);
		return -1;
	}
	free_err(&err);
	(*db)->db = db_t;

	kp_debug("successfully started leveldb [%s]\n", (*db)->name);
	return 0;
}

int leveldb_client_stop(leveldb *db)
{
	int retval = 0;
#if DELETE_DB_FROM_DISK
	char *err = NULL;
#endif

	kp_debug("stopping db [%s]\n", db->name);

	leveldb_close(db->db);
#if DELETE_DB_FROM_DISK
	leveldb_destroy_db(db->options, db->name, &err);  //destroy db on disk
	if (err) {
		kp_error("leveldb_destroy_db() returned error: %s\n", err);
		retval = -1;
	}
	free_err(&err);
#endif
	leveldb_options_destroy(db->options);
	leveldb_readoptions_destroy(db->roptions);
	leveldb_writeoptions_destroy(db->woptions);
	leveldb_cache_destroy(db->cache);
	leveldb_comparator_destroy(db->cmp);
	leveldb_env_destroy(db->env);
	free(db->name);
	free(db);

	kp_debug("freed the leveldb, returning %d\n", retval);
	return retval;
}

void * leveldb_client_start_trans(leveldb *db){
	if (!db){
		kp_error("got a NULL argument\n");
		return NULL;
	}
	leveldb_writebatch_t* trans = NULL;
	trans = leveldb_writebatch_create();

	return (void*) trans;
}

int leveldb_client_stop_trans(leveldb *db, void *trans){
	if (!db || !trans ){
		kp_error("got a NULL argument\n");
		return -1;
	}
	
	char *err = NULL;
	leveldb_write(db->db, db->woptions, (leveldb_writebatch_t*)trans,  &err);
	leveldb_writebatch_destroy(trans);

	return 0;
}

int leveldb_client_batch_put(leveldb *db, void *trans, 
														 const char *key, const char *value){
	char *err = NULL;
	size_t keylen, vallen;
	leveldb_writebatch_t*batch = (leveldb_writebatch_t*)trans;
	if (!db || !batch || !key || !value) {
		kp_error("got a NULL argument\n");
		return -1;
	}

	/* The lengths that we pass into leveldb_put() should NOT include
	 * the null-zero!
	 */
	keylen = strlen(key);
	vallen = strlen(value);
	if (keylen < 1 || vallen < 1) {
		kp_error("got empty string for key (%zu) or val (%zu)\n",
				keylen, vallen);
	}
	kp_debug("putting [%s:%s] into batch for [%s]; keylen=%zu, vallen=%zu\n",
			key, value, db->name, keylen, vallen);

	/* leveldb_put() goes to internal leveldb code that will copy the key
	 * and the value; we do not have to do this here in our leveldb client.
	 * After our client function returns, the caller will be able to free
	 * the key and the value. The leveldb code will call Slice() to slice
	 * off the null-zero from the key and value strings we are passing it,
	 * and just the character bytes will be stored in the db.
	 */
	leveldb_writebatch_put(batch, key, keylen, value, vallen);
	if (err) {
		kp_error("leveldb_writebatch_put() returned error: %s\n", err);
		free_err(&err);
		return -1;
	}

	kp_debug("leveldb_writebatch_put() succeeded\n");
	return 0;
}
int leveldb_client_put(leveldb *db, const char *key, const char *value)
{
	char *err = NULL;
	size_t keylen, vallen;

	if (!db || !key || !value) {
		kp_error("got a NULL argument\n");
		return -1;
	}

	/* The lengths that we pass into leveldb_put() should NOT include
	 * the null-zero!
	 */
	keylen = strlen(key);
	vallen = strlen(value);
	if (keylen < 1 || vallen < 1) {
		kp_error("got empty string for key (%zu) or val (%zu)\n",
				keylen, vallen);
	}
	kp_debug("putting [%s:%s] into db [%s]; keylen=%zu, vallen=%zu\n",
			key, value, db->name, keylen, vallen);

	/* leveldb_put() goes to internal leveldb code that will copy the key
	 * and the value; we do not have to do this here in our leveldb client.
	 * After our client function returns, the caller will be able to free
	 * the key and the value. The leveldb code will call Slice() to slice
	 * off the null-zero from the key and value strings we are passing it,
	 * and just the character bytes will be stored in the db.
	 */
	leveldb_put(db->db, db->woptions, key, keylen, value, vallen, &err);
	if (err) {
		kp_error("leveldb_put() returned error: %s\n", err);
		free_err(&err);
		return -1;
	}

	kp_debug("leveldb_put() succeeded\n");
	return 0;
}

int leveldb_client_get(leveldb *db, const char *key, char **value,
		bool add_null_zero)
{
	char *err = NULL;
	size_t keylen, vallen;
	char *re_value;

	if (!db || !key) {
		kp_error("got a NULL argument\n");
		return -1;
	}

	keylen = strlen(key);
	if (keylen < 1) {
		kp_error("got empty string for key (%zu)\n", keylen);
	}
	
	kp_debug("calling leveldb_get() with key=%s, keylen=%zu\n", key, keylen);
	*value = leveldb_get(db->db, db->roptions, key, keylen, &vallen, &err);
	if (err) {
		kp_error("leveldb_get() returned error: %s\n", err);
		free_err(&err);
		return -1;
	}

	/* leveldb_get() will return NULL if the key is not found. */
	if (*value == NULL) {
		kp_debug("did not find key=%s in db; returning 1\n", key);
		return 1;
	}

	/* If we got a value back from the db, then it is not null-zero-
	 * terminated. If the caller asks us to, we can allocate another
	 * string that is null-terminated.
	 */
	kp_debug("get(%s) returned vallen=%zu, not-null-terminated-value=[%s]\n",
			key, vallen, *value);
	if (add_null_zero) {
		re_value = (char *)malloc(vallen + 1);
		strncpy(re_value, *value, vallen);
		re_value[vallen] = '\0';
		free(*value);
		*value = re_value;
		kp_debug("created null-terminated value=%s; vallen=%zu, strlen=%zu\n",
				*value, vallen, strlen(*value));
	} else {
		kp_debug("add_null_zero is false, so value returned will not end "
				"in a null-zero\n");
	}

	kp_debug("got a value, returning 0\n");
	return 0;
}

/* NOT IMPLEMENTED YET! */
int leveldb_client_batch_delete(leveldb *db, void *trans, const char *key){
	size_t keylen;
	leveldb_writebatch_t *batch = (leveldb_writebatch_t*)trans;
	if (!db || !key || !batch){
		kp_error("got a NULL arguement\n");
		return -1;
	}

	keylen = strlen(key);
	if (keylen < 1 )
		kp_error("got empty string for key (%zu)\n", keylen);
	kp_debug("deleting{%s} via batch from  db [%s]; keylen=%zu\n", 
					 key, db->name, keylen);

	leveldb_writebatch_delete(batch, key, keylen);
	
	kp_debug("leveledb_writebatch_delete() succeeded\n");
	return 0;

}
int leveldb_client_delete(leveldb *db, const char *key)
{
	char *err = NULL;
	size_t keylen;

	if (!db || !key){
		kp_error("got a NULL arguement\n");
		return -1;
	}

	keylen = strlen(key);
	if (keylen < 1 )
		kp_error("got empty string for key (%zu)\n", keylen);
	kp_debug("deleting{%s} from db [%s]; keylen=%zu\n", key, db->name, keylen);
	leveldb_delete(db->db, db->woptions, key, keylen, &err);
	if (err){
		kp_error("leveldb_delete() returned error: %s\n", err);
		free_err(&err);
		return -1;
	}
	
	kp_debug("leveledb_delete() succeeded\n");
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
