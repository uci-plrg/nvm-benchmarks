/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Peter Hornyack and Katelin Bailey
 * 1/10/12
 * University of Washington
 *
 * This file was copied directly from leveldb/db/c_test.c, then edited.
 */

#include "../kp_macros.h"
#include "../leveldb_client.h"
#include "../leveldb/c.h"
#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

const char* phase = "";
static char dbname[200];

static void StartPhase(const char* name) {
	kp_print("=== Test %s\n", name);
	phase = name;
}

#define CheckNoError(err)                                               \
	if ((err) != NULL) {                                                \
		kp_error("%s:%d: %s: %s\n", __FILE__, __LINE__, phase, (err));  \
		abort();                                                        \
	}

#define CheckCondition(cond)                                            \
	if (!(cond)) {                                                      \
		kp_error("%s:%d: %s: %s\n", __FILE__, __LINE__, phase, #cond);  \
		abort();                                                        \
	}

/* Note that the size (n) specified here probably does not include the
 * null-zero. The char*s passed in here probably are not null-terminated
 * anyway; see how leveldb_put() and CheckGet() work.
 */
static void CheckEqual(const char* expected, const char* v, size_t n) {
	if (expected == NULL && v == NULL) {
		// ok
	} else if (expected != NULL && v != NULL && n == strlen(expected) &&
			memcmp(expected, v, n) == 0) {
		// ok
		kp_debug("%s: got '%s', as expected (size=%zu)\n", phase, expected,
				n);
		  //print expected, not v, because v comes from the db, and the
		  //size of v when it was put did not include the null-zero.
		return;
	} else {
		kp_error("%s: expected '%s', got '%s'\n",
				phase,
				(expected ? expected : "(null)"),
				(v ? v : "(null"));
		abort();
	}
}

static void Free(char** ptr) {
	if (*ptr) {
		free(*ptr);
		*ptr = NULL;
	}
}

static void CheckGet(
		leveldb_t* db,
		const leveldb_readoptions_t* options,
		const char* key,
		const char* expected) {
	char* err = NULL;
	size_t val_len;
	char* val;
	val = leveldb_get(db, options, key, strlen(key), &val_len, &err);
	CheckNoError(err);
	CheckEqual(expected, val, val_len);
	Free(&val);
}

static void CheckIter(leveldb_iterator_t* iter,
		const char* key, const char* val) {
	size_t len;
	const char* str;
	str = leveldb_iter_key(iter, &len);
	CheckEqual(key, str, len);
	str = leveldb_iter_value(iter, &len);
	CheckEqual(val, str, len);
}

// Callback from leveldb_writebatch_iterate()
static void CheckPut(void* ptr,
		const char* k, size_t klen,
		const char* v, size_t vlen) {
	int* state = (int*) ptr;
	CheckCondition(*state < 2);
	switch (*state) {
		case 0:
			CheckEqual("bar", k, klen);
			CheckEqual("b", v, vlen);
			break;
		case 1:
			CheckEqual("box", k, klen);
			CheckEqual("c", v, vlen);
			break;
	}
	(*state)++;
}

// Callback from leveldb_writebatch_iterate()
static void CheckDel(void* ptr, const char* k, size_t klen) {
	int* state = (int*) ptr;
	CheckCondition(*state == 2);
	CheckEqual("bar", k, klen);
	(*state)++;
}

/* Destructor for a comparator. */
static void CmpDestroy(void* arg) { }

/* Comparison function used by leveldb_comparator_t. */
static int CmpCompare(void* arg, const char* a, size_t alen,
		const char* b, size_t blen) {
	int n = (alen < blen) ? alen : blen;
	int r = memcmp(a, b, n);
	if (r == 0) {
		if (alen < blen) r = -1;
		else if (alen > blen) r = +1;
	}
	return r;
}

/* Returns the name of the comparator. */
static const char* CmpName(void* arg) {
	return "comparator-name";
}

#define check_ret(ret)  do                         \
	if (ret != 0) {                                \
			kp_error("got error: ret=%d\n", ret);  \
			return;                                \
	}                                              \
	} while(0)

void test_leveldb_client(void)
{
	int ret;
	char *retstring = NULL;
	leveldb *db = NULL;
	unsigned long tid = pthread_self();
	bool add_null_zero = true;
	  /* Note: setting this to false will cause kp_testcase_string() to fail
	   * after each leveldb_client_get().
	   */

	ret = leveldb_client_start(&db, "test_leveldb_client_name", 0, false,
			false);
	//ret = leveldb_client_start(&db, "test_leveldb_client_name", 1234, true,
	//		true);
	kp_testcase_int(tid, "leveldb_client_start", 0, ret);
	if (ret != 0) {
		return;
	}
	if (!db) {
		kp_error("db is NULL!\n");
		return;
	}

	ret = leveldb_client_get(db, "foo", &retstring, add_null_zero);
	kp_testcase_int(tid, "get empty", 1, ret);
	if (retstring) free(retstring); retstring = NULL;

	ret = leveldb_client_put(db, "foo", "hello");
	kp_testcase_int(tid, "put foo - hello", 0, ret);

	ret = leveldb_client_get(db, "foo", &retstring, add_null_zero);
	kp_testcase_int(tid, "get foo", 0, ret);
	kp_testcase_string(tid, "get foo", "hello", retstring);
	if (retstring) free(retstring); retstring = NULL;

	ret = leveldb_client_put(db, "bar", "barbarbar");
	kp_testcase_int(tid, "put bar - barbarbar", 0, ret);
	ret = leveldb_client_put(db, "foo", "xyz");
	kp_testcase_int(tid, "put foo - xyz", 0, ret);

	ret = leveldb_client_get(db, "bar", &retstring, add_null_zero);
	kp_testcase_int(tid, "get bar", 0, ret);
	kp_testcase_string(tid, "get bar", "barbarbar", retstring);
	if (retstring) free(retstring); retstring = NULL;
	ret = leveldb_client_get(db, "foo", &retstring, add_null_zero);
	kp_testcase_int(tid, "get foo", 0, ret);
	kp_testcase_string(tid, "get foo", "xyz", retstring);
	if (retstring) free(retstring); retstring = NULL;

#ifdef DELETES_IMPLEMENTED
	ret = leveldb_client_delete(db, "oops");
	kp_testcase_int(tid, "delete oops", 1, ret);
	ret = leveldb_client_delete(db, "bar");
	kp_testcase_int(tid, "delete bar", 0, ret);
	ret = leveldb_client_delete(db, "foo");
	kp_testcase_int(tid, "delete foo", 0, ret);

	ret = leveldb_client_get(db, "bar", &retstring);
	kp_testcase_int(tid, "get bar", 1, ret);
	if (retstring) free(retstring); retstring = NULL;
	ret = leveldb_client_get(db, "foo", &retstring);
	kp_testcase_int(tid, "get foo", 1, ret);
	if (retstring) free(retstring); retstring = NULL;
#endif

	ret = leveldb_client_stop(db);
	kp_testcase_int(tid, "leveldb_client_stop", 0, ret);

	return;
}

int main(int argc, char *argv[])
{
	leveldb_t* db;
	leveldb_comparator_t* cmp;
	leveldb_cache_t* cache;
	leveldb_env_t* env;
	leveldb_options_t* options;
	leveldb_readoptions_t* roptions;
	leveldb_writeoptions_t* woptions;
	char* err = NULL;

	test_leveldb_client();
	kp_testcase_int(pthread_self(), "exiting main after test_leveldb_client()",
			0, 0);
	return 0;

	/* The resulting directory ends up at around 1 MB in size. */
	snprintf(dbname, sizeof(dbname), "/tmp/leveldb_c_test-%d",
			((int) geteuid()));

	StartPhase("create_objects");
	cmp = leveldb_comparator_create(NULL, CmpDestroy, CmpCompare, CmpName);
	env = leveldb_create_default_env();
	  /* Sets up a new "environment," which provides a portable way to
	   * create different types of files, schedule threads, and perform
	   * other system functions. See leveldb/include/leveldb/env.h.
	   */
	cache = leveldb_cache_create_lru(100000);
	  /* From leveldb/include/leveldb/cache.h:
	   * A Cache is an interface that maps keys to values.  It has internal
	   * synchronization and may be safely accessed concurrently from
	   * multiple threads.  It may automatically evict entries to make room
	   * for new entries.  Values have a specified charge against the cache
	   * capacity.  For example, a cache where the values are variable
	   * length strings, may use the length of the string as the charge for
	   * the string.
	   * The argument to the constructor is the capacity.
	   * See also: leveldb/util/cache.cc
	   */

	/* "Options to control the behavior of a database (passed to DB::Open)".
	 * See leveldb/include/leveldb/options.h, particularly the section
	 * "Parameters that affect performance". See leveldb/util/options.cc
	 * for default values.
	 */
	options = leveldb_options_create();
	leveldb_options_set_create_if_missing(options, 1);
	leveldb_options_set_comparator(options, cmp);
	leveldb_options_set_error_if_exists(options, 1);
	leveldb_options_set_cache(options, cache);
	leveldb_options_set_env(options, env);
	leveldb_options_set_info_log(options, NULL);
	leveldb_options_set_write_buffer_size(options, 100000);
	  /* Amount of data to build up in memory (backed by an unsorted log
	   * on disk) before converting to a sorted on-disk file.
	   * Larger values increase performance, especially during bulk loads.
	   * Up to two write buffers may be held in memory at the same time,
	   * so you may wish to adjust this parameter to control memory usage.
	   * Also, a larger write buffer will result in a longer recovery time
	   * the next time the database is opened.
	   * Default: 4MB (4<<20)
	   */
	leveldb_options_set_paranoid_checks(options, 1);
	leveldb_options_set_max_open_files(options, 10);
	  /* Number of open files that can be used by the DB.  You may need to
	   * increase this if your database has a large working set (budget
	   * one open file per 2MB of working set).
	   * Default: 1000
	   */
	leveldb_options_set_block_size(options, 1024);
	  /* Control over blocks (user data is stored in a set of blocks, and
	   * a block is the unit of reading from disk):
	   * Approximate size of user data packed per block.  Note that the
	   * block size specified here corresponds to uncompressed data.  The
	   * actual size of the unit read from disk may be smaller if
	   * compression is enabled.  This parameter can be changed dynamically.
	   * Default: 4096
	   */
	leveldb_options_set_block_restart_interval(options, 8);
	  /* Number of keys between restart points for delta encoding of keys.
	   * This parameter can be changed dynamically.  Most clients should
	   * leave this parameter alone.
	   * Default: 16
	   */
	leveldb_options_set_compression(options, leveldb_no_compression);
	  /* Compress blocks. Default: kSnappyCompression ("lightweight but
	   * fast").
	   */

	roptions = leveldb_readoptions_create();
	leveldb_readoptions_set_verify_checksums(roptions, 1);
	  /* If true, all data read from underlying storage will be
	   * verified against corresponding checksums.
	   */
	leveldb_readoptions_set_fill_cache(roptions, 0);
	  /* Should the data read for this iteration be cached in memory?
	   * Callers may wish to set this field to false for bulk scans.
	   */

	woptions = leveldb_writeoptions_create();
	leveldb_writeoptions_set_sync(woptions, 1);
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

	/* Destroy previous copy of database, if it exists: */
	StartPhase("destroy (previous)");
	leveldb_destroy_db(options, dbname, &err);
	Free(&err);

//	StartPhase("open_error");
//	db = leveldb_open(options, dbname, &err);
//	CheckCondition(err != NULL);
//	Free(&err);

	StartPhase("open");
	db = leveldb_open(options, dbname, &err);
	CheckNoError(err);
	CheckGet(db, roptions, "foo", NULL);
	  /* Checks that getting key "foo" from the db (with the specified
	   * read options) returns NULL.
	   */

	StartPhase("put");
	leveldb_put(db, woptions, "foo", 3, "hello", 5, &err);
	  /* Put key=foo (length 3) and value=hello (length 5) to db (with the
	   * specified write options). This function will call Slice() to slice
	   * off the null-zero and just put the character bytes.
	   */
	CheckNoError(err);
	CheckGet(db, roptions, "foo", "hello");

	StartPhase("writebatch");
	{
		/* Used to batch writes, I guess (not much comments in
		 * leveldb/db/write_batch.cc); queues them up, then iterates
		 * over all of them at once.
		 * If sync-after-writes is enabled, then I'm guessing that a
		 * writebatch delays the sync until after all of the writes
		 * have been written.
		 */
		leveldb_writebatch_t* wb = leveldb_writebatch_create();
		leveldb_writebatch_put(wb, "foo", 3, "a", 1);
		leveldb_writebatch_clear(wb); //"Change this slice to refer to an empty array"
		leveldb_writebatch_put(wb, "bar", 3, "b", 1);
		//leveldb_writebatch_put(wb, "box", 3, "c", 1);
		//leveldb_writebatch_put(wb, "box", 3, "d", 1);
		leveldb_writebatch_put(wb, "box", 3, "c", 1);
		leveldb_writebatch_delete(wb, "bar", 3);
		leveldb_write(db, woptions, wb, &err);
		CheckNoError(err);
		CheckGet(db, roptions, "foo", "hello");
		  //Expect back "hello", not "a", because of the _clear()
		CheckGet(db, roptions, "bar", NULL);
		CheckGet(db, roptions, "box", "c");
		int pos = 0;
		leveldb_writebatch_iterate(wb, &pos, CheckPut, CheckDel);
		CheckCondition(pos == 3);
		leveldb_writebatch_destroy(wb);
	}

	StartPhase("iter");
	{
		/* Iterates over the sequence of key/value pairs from the db.
		 * See leveldb/include/leveldb/iterator.h
		 */
		leveldb_iterator_t* iter = leveldb_create_iterator(db, roptions);
		CheckCondition(!leveldb_iter_valid(iter));
		leveldb_iter_seek_to_first(iter);
		CheckCondition(leveldb_iter_valid(iter));
		CheckIter(iter, "box", "c");
		leveldb_iter_next(iter);
		CheckIter(iter, "foo", "hello");
		leveldb_iter_prev(iter);
		CheckIter(iter, "box", "c");
		leveldb_iter_prev(iter);
		CheckCondition(!leveldb_iter_valid(iter));
		leveldb_iter_seek_to_last(iter);
		CheckIter(iter, "foo", "hello");
		leveldb_iter_seek(iter, "b", 1);
		CheckIter(iter, "box", "c");
		leveldb_iter_get_error(iter, &err);
		CheckNoError(err);
		leveldb_iter_destroy(iter);
	}

	StartPhase("approximate_sizes");
	{
		int i;
		int n = 20000;
		char keybuf[100];
		char valbuf[100];
		uint64_t sizes[2];
		const char* start[2] = { "a", "k00000000000000010000" };
		size_t start_len[2] = { 1, 21 };
		const char* limit[2] = { "k00000000000000010000", "z" };
		size_t limit_len[2] = { 21, 1 };
		leveldb_writeoptions_set_sync(woptions, 0);  //disable syncing on each write
		for (i = 0; i < n; i++) {
			snprintf(keybuf, sizeof(keybuf), "k%020d", i);
			snprintf(valbuf, sizeof(valbuf), "v%020d", i);
			leveldb_put(db, woptions, keybuf, strlen(keybuf), valbuf, strlen(valbuf),
					&err);
			CheckNoError(err);
		}
		leveldb_approximate_sizes(db, 2, start, start_len, limit, limit_len, sizes);
		CheckCondition(sizes[0] > 0);
		CheckCondition(sizes[1] > 0);
	}

	StartPhase("property");
	{
		char* prop = leveldb_property_value(db, "nosuchprop");
		CheckCondition(prop == NULL);
		prop = leveldb_property_value(db, "leveldb.stats");
		CheckCondition(prop != NULL);
		kp_debug("leveldb.stats:\n%s\n", prop);
		Free(&prop);
	}

	StartPhase("snapshot");
	{
		/* Whoa! */
		const leveldb_snapshot_t* snap;
		snap = leveldb_create_snapshot(db);
		leveldb_delete(db, woptions, "foo", 3, &err);
		CheckNoError(err);
		leveldb_readoptions_set_snapshot(roptions, snap);
		CheckGet(db, roptions, "foo", "hello");
		leveldb_readoptions_set_snapshot(roptions, NULL);
		CheckGet(db, roptions, "foo", NULL);
		leveldb_release_snapshot(db, snap);
	}

	StartPhase("repair");
	{
		/* Not exactly sure what this does... */
		leveldb_close(db);
		leveldb_options_set_create_if_missing(options, 0);
		leveldb_options_set_error_if_exists(options, 0);
		leveldb_repair_db(options, dbname, &err);
		CheckNoError(err);
		db = leveldb_open(options, dbname, &err);
		CheckNoError(err);
		CheckGet(db, roptions, "foo", NULL);
		CheckGet(db, roptions, "bar", NULL);
		CheckGet(db, roptions, "box", "c");
	}

	StartPhase("cleanup");
	leveldb_close(db);
	leveldb_options_destroy(options);
	leveldb_readoptions_destroy(roptions);
	leveldb_writeoptions_destroy(woptions);
	leveldb_cache_destroy(cache);
	leveldb_comparator_destroy(cmp);
	leveldb_env_destroy(env);

	kp_testcase_int(pthread_self(), "reached end of file", 1, 1);
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
