/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Peter Hornyack and Katelin Bailey
 * 11/20/11
 * University of Washington
 *
 * Test file for local and master functionality.
 */

#include "../kp_macros.h"
#include "../../include/kp_kv_local.h"
#include "../../include/kp_kv_master.h"
#include <errno.h>
#include <sys/types.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

bool do_conflict_detection = true;

/* These two constants define the range of CPUs that the worker threads
 * will be placed on: [CPU_OFFSET, CPU_OFFSET + NUM_CPUS - 1]. If
 * the high end of that range is greater than the number of CPUs on
 * the system, who knows what will happen (I think the set-affinity
 * call will just fail and the test code will error).
 */
#define NUM_CPUS 8
#define CPU_OFFSET 0

#define COMMIT_SIZE 10
  // Number of puts to the thread's private keys before it merges.
#define NUM_COMMITS 100
  // Number of commits each worker will make (each of COMMIT_SIZE).
#define KEY1_PUTS 4
  // Number of times each worker will put to key 1 before merging the first
  // time.
#define MASTER_EXPECTED_MAX_KEYS (16 * (KEY1_PUTS + (COMMIT_SIZE * \
			NUM_COMMITS)) * 3)
  /* If this is set high enough, then no rehashing will happen. Should
   * approximately be the number of threads times (KEY1_PUTS +
   * (COMMIT_SIZE * NUM_COMMITS)).
   */

#define MASTER_CONSISTENCY_MODE MODE_SNAPSHOT

#define MAX_THREADS 256
//#define YES_SLEEP
#define SLEEP_TIME 4
#define SMALL_BUF_SIZE 31

/* Global keys and values: quick and dirty. */
char *key1 = "key1";
char *key2 = "key2";
char *key3 = "key3";
char *key4 = "key4";
char *key_del = "key_delete";
char *val10 = "val10";
char *val20 = "val20";
char *val30 = "val30";
char *val40 = "val40";
char *val_del = "val_delete";
char *val11 = "value11";
char *val21 = "value21";
char *val31 = "value31";
char *val41 = "value41";
char *val22 = "value22___";
#define size10 (strlen(val10)+1)
#define size20 (strlen(val20)+1)
#define size30 (strlen(val30)+1)
#define size40 (strlen(val40)+1)
#define size_del (strlen(val_del)+1)
#define size11 (strlen(val11)+1)
#define size21 (strlen(val21)+1)
#define size31 (strlen(val31)+1)
#define size41 (strlen(val41)+1)
#define size22 (strlen(val22)+1)

typedef struct worker_thread_args_struct {
	cpu_set_t cpu_set;
	kp_kv_master *master;
	int num_threads;
} worker_thread_args;

#define RET_STRING_LEN 64
void *worker_thread_start(void *arg)
{
	int i, j, ret;
	bool commit_conflict;
	//char *ret_string;
	void *ret_val;
	size_t ret_size;
	unsigned long tid;
	uint64_t lvn;
	worker_thread_args *thread_args;
	kp_kv_master *master;
	kp_kv_local *local;
	char *str, *descr, *key;
	int descr_len, num_threads;
	char *key_gc, *val_gc;
	
	/* Initialize worker: */
	tid = pthread_self() % 10000;
	thread_args = (worker_thread_args *)arg;
	master = thread_args->master;
	num_threads = thread_args->num_threads;
	kp_debug("thread_%lu: started, master id=%u\n", tid,
			kp_master_get_id(master));
	ret = kp_kv_local_create(master, &local, COMMIT_SIZE, false);
	  /* false: don't use nvm for local workers! */
	if (ret != 0) {
		kp_error("thread_%lu: kp_kv_local_create() returned error=%d\n",
				tid, ret);
		return NULL;
	}

	/* NOTE: these test cases are fragile in MODE_SEQUENTIAL because they
	 * assume (when they check the returned values and sizes) that other
	 * workers have not put values to the master store since this worker
	 * put its values. This is easily seen by bumping the number of
	 * workers up to 8 or more; it happens more rarely with 4 worker
	 * threads.
	 */

	///* Empty get: */
	/* NOTE: this testcase may fail because some workers may actually
	 * get a value here, if the first worker already committed it in.
	 * So, only run it when there's a single worker.
	 */
	if (num_threads == 1) {
		ret = kp_local_get(local, key1, &ret_val, &ret_size);
		kp_testcase_int(tid, "empty get", 1, ret);
	}

	/* Empty commit.
	 * To check that concurrent commits with multiple threads are working
	 * (or are being synchronized by locks), can grep debug output for
	 * "starting to iterate\|merge complete". Even in SNAPSHOT mode, the
	 * threads should be scheduled in/out often enough that a second commit
	 * will start before the first empty commit finishes.
	 */
	kp_debug("%lu: about to perform empty commit\n", tid);
	ret = kp_local_commit(local, NULL);
	kp_testcase_int(tid, "empty commit", 2, ret);
	//if (ret != 0) {
	//	kp_die("%lu: kp_local_commit() returned error=%d\n", tid, ret);
	//}
	//

	/* Commit with only deletes: only run when single-threaded, otherwise
	 * could delete keys that some other thread already committed.
	 */
	if (num_threads == 1) {
		ret = kp_local_delete_key(local, key3);
		kp_testcase_int(tid, "delete key3", 0, ret);
		ret = kp_local_delete_key(local, key4);
		kp_testcase_int(tid, "delete key4", 0, ret);
		ret = kp_local_commit(local, NULL);
		kp_testcase_int(tid, "commit with just deletes", 0, ret);
	}

	/* Put lvn 0: */
	lvn = kp_local_put(local, key1, (void *)val10, size10);
	kp_testcase_int(tid, "put key1", 0, (int)lvn);
	lvn = kp_local_put(local, key2, (void *)val20, size20);
	kp_testcase_int(tid, "put key2", 0, (int)lvn);
	lvn = kp_local_put(local, key3, (void *)val30, size30);
	kp_testcase_int(tid, "put key3", 0, (int)lvn);
	lvn = kp_local_put(local, key4, (void *)val40, size40);
	kp_testcase_int(tid, "put key4", 0, (int)lvn);
	lvn = kp_local_put(local, key_del, (void *)val_del, size_del);
	kp_testcase_int(tid, "put key_del", 0, (int)lvn);

//#if 0
	/* Get current: */
	ret_val = NULL;
	ret = kp_local_get(local, key1, &ret_val, &ret_size);
	kp_testcase_int(tid, "get key1 lvn0", 0, ret);
	kp_testcase_size_t(tid, "get key1 lvn0 size", size10, ret_size);
	kp_testcase_string(tid, "get key1 lvn0 str", val10, (char *)ret_val);
	if (ret_val) { free(ret_val); ret_val = NULL; }
	ret = kp_local_get(local, key2, &ret_val, &ret_size);
	kp_testcase_int(tid, "get key2 lvn0", 0, ret);
	kp_testcase_size_t(tid, "get key2 lvn0 size", size20, ret_size);
	kp_testcase_string(tid, "get key2 lvn0 str", val20, (char *)ret_val);
	if (ret_val) { free(ret_val); ret_val = NULL; }
	ret = kp_local_get(local, key3, &ret_val, &ret_size);
	kp_testcase_int(tid, "get key3 lvn0", 0, ret);
	kp_testcase_size_t(tid, "get key3 lvn0 size", size30, ret_size);
	kp_testcase_string(tid, "get key3 lvn0 str", val30, (char *)ret_val);
	if (ret_val) { free(ret_val); ret_val = NULL; }
	ret = kp_local_get(local, key4, &ret_val, &ret_size);
	kp_testcase_int(tid, "get key4 lvn0", 0, ret);
	kp_testcase_size_t(tid, "get key4 lvn0 size", size40, ret_size);
	kp_testcase_string(tid, "get key4 lvn0 str", val40, (char *)ret_val);
	if (ret_val) { free(ret_val); ret_val = NULL; }
//#endif

	/* Delete the key that we just put, then check that a get returns
	 * not-found: */
	ret = kp_local_delete_key(local, key_del);
	kp_testcase_int(tid, "delete key_del", 0, ret);
	ret = kp_local_get(local, key_del, &ret_val, &ret_size);
	kp_testcase_int(tid, "get key_del from local", 1, ret);
	if (ret_val) { free(ret_val); ret_val = NULL; }

	/* Put lvn 1 (key1, key2 x2): */
	lvn = kp_local_put(local, key1, (void *)val11, size11);
	if (MASTER_CONSISTENCY_MODE == MODE_SNAPSHOT) {			
		kp_testcase_int(tid, "put key1 val11", 1, (int)lvn);
	} else if (MASTER_CONSISTENCY_MODE == MODE_SEQUENTIAL) {			
		kp_testcase_int(tid, "put key1 val11", 0, (int)lvn);
	}
	lvn = kp_local_put(local, key2, (void *)val21, size21);
	if (MASTER_CONSISTENCY_MODE == MODE_SNAPSHOT) {			
		kp_testcase_int(tid, "put key2 val21", 1, (int)lvn);
	} else if (MASTER_CONSISTENCY_MODE == MODE_SEQUENTIAL) {			
		kp_testcase_int(tid, "put key2 val21", 0, (int)lvn);
	}
	lvn = kp_local_put(local, key2, (void *)val22, size22);
	if (MASTER_CONSISTENCY_MODE == MODE_SNAPSHOT) {			
		kp_testcase_int(tid, "put key2 val22", 2, (int)lvn);
	} else if (MASTER_CONSISTENCY_MODE == MODE_SEQUENTIAL) {			
		kp_testcase_int(tid, "put key2 val22", 0, (int)lvn);
	}

	/* Get current again: */
	ret_val = NULL;
	ret = kp_local_get(local, key1, &ret_val, &ret_size);
	kp_testcase_int(tid, "get key1 lvn1", 0, ret);
	kp_testcase_size_t(tid, "get key1 lvn1 size", size11, ret_size);
	kp_testcase_string(tid, "get key1 lvn1 str", val11, (char *)ret_val);
	if (ret_val) { free(ret_val); ret_val = NULL; }
	ret = kp_local_get(local, key2, &ret_val, &ret_size);
	kp_testcase_int(tid, "get key2 lvn2", 0, ret);
	kp_testcase_size_t(tid, "get key2 lvn2 size", size22, ret_size);
	kp_testcase_string(tid, "get key2 lvn2 str", val22, (char *)ret_val);
	if (ret_val) { free(ret_val); ret_val = NULL; }
	ret = kp_local_get(local, key3, &ret_val, &ret_size);
	kp_testcase_int(tid, "get key3 lvn0", 0, ret);
	kp_testcase_size_t(tid, "get key3 lvn0 size", size30, ret_size);
	kp_testcase_string(tid, "get key3 lvn0 str", val30, (char *)ret_val);
	if (ret_val) { free(ret_val); ret_val = NULL; }
	ret = kp_local_get(local, key4, &ret_val, &ret_size);
	kp_testcase_int(tid, "get key4 lvn0", 0, ret);
	kp_testcase_size_t(tid, "get key4 lvn0 size", size40, ret_size);
	kp_testcase_string(tid, "get key4 lvn0 str", val40, (char *)ret_val);
	if (ret_val) { free(ret_val); ret_val = NULL; }

#ifdef YES_SLEEP
	/* Sleep... */
	kp_debug("thread_%lu: sleeping for %d seconds\n", tid, SLEEP_TIME);
	sleep(SLEEP_TIME);
#endif

	/* Commit!! (1) */
	kp_debug("%lu: about to perform commit (1)\n", tid);
	ret = kp_local_commit(local, NULL);
	if (!do_conflict_detection) {
		kp_testcase_int(tid, "commit 1", 0, ret);
	}
	if (ret != 0 && ret != 1) {
		kp_die("%lu: kp_local_commit() returned error=%d\n", tid, ret);
	}
	commit_conflict = ret == 1 ? true : false;
	//kp_test("kp_local_get_current_snapshot: %ju\n",
	//		kp_local_get_current_snapshot(local));

#ifdef YES_SLEEP
	/* Sleep... */
	kp_debug("thread_%lu: sleeping for %d seconds\n", tid, SLEEP_TIME);
	sleep(SLEEP_TIME);
#endif

	/* Get current again (post-commit): */
	if (!commit_conflict) {
		ret_val = NULL;
		ret = kp_local_get(local, key1, &ret_val, &ret_size);
		kp_testcase_int(tid, "post-commit get key1", 0, ret);
		kp_testcase_size_t(tid, "post-commit get key1 size", size11, ret_size);
		kp_testcase_string(tid, "post-commit get key1 str", val11, (char *)ret_val);
		if (ret_val) { free(ret_val); ret_val = NULL; }
		ret = kp_local_get(local, key2, &ret_val, &ret_size);
		kp_testcase_int(tid, "post-commit get key2", 0, ret);
		kp_testcase_size_t(tid, "post-commit get key2 size", size22, ret_size);
		kp_testcase_string(tid, "post-commit get key2 str", val22, (char *)ret_val);
		if (ret_val) { free(ret_val); ret_val = NULL; }
		ret = kp_local_get(local, key3, &ret_val, &ret_size);
		kp_testcase_int(tid, "post-commit get key3", 0, ret);
		kp_testcase_size_t(tid, "post-commit get key3 size", size30, ret_size);
		kp_testcase_string(tid, "post-commit get key3 str", val30, (char *)ret_val);
		if (ret_val) { free(ret_val); ret_val = NULL; }
		ret = kp_local_get(local, key4, &ret_val, &ret_size);
		kp_testcase_int(tid, "post-commit get key4", 0, ret);
		kp_testcase_size_t(tid, "post-commit get key4 size", size40, ret_size);
		kp_testcase_string(tid, "post-commit get key4 str", val40, (char *)ret_val);
		if (ret_val) { free(ret_val); ret_val = NULL; }
	} else {
		kp_print("skipping gets on keys post-commit because this worker's "
				"commit had a conflict!\n");
	}

	/* Check for key that was deleted before commit - it should never
	 * be found, in either the local or the master store.
	 */
	ret = kp_local_get(local, key_del, &ret_val, &ret_size);
	kp_testcase_int(tid, "get key_del from master post-commit", 1, ret);
	if (ret_val) { free(ret_val); ret_val = NULL; }

	/* How can we test garbage collection? I think if we add a commit here
	 * that creates a key specific to this worker, then perform the next
	 * commit (which used to be commit 2), then perform one more commit that
	 * deletes the key that we create here, we'll have a key that is alive
	 * for at least two snapshot numbers (possibly more than two if there
	 * are multiple worker threads), with only one commit record referencing
	 * the key (besides the commit record that deletes it). Then, when we
	 * perform the big commit loop below, telescoping will eventually /
	 * hopefully collect all two (or more) snapshots that reference this
	 * key, and the garbage collector will be able to collect the data,
	 * the version table, the hash table entry, and the commit record!!
	 *
	 * Are there other garbage collection code paths that we need to add
	 * test code to exercise?
	 *   - A key referenced by more than one commit record (e.g. multiple
	 *   puts to the key before deleting it). This code path is half-tested
	 *   in the big commit loop below, in that multiple puts will be performed
	 *   to the same keys and the collector will traverse their VTs and
	 *   check their liveness ranges, but these keys will never be permanently
	 *   deleted (at the moment...).
	 */
	key_gc = (char *)malloc(SMALL_BUF_SIZE + 1);
	val_gc = (char *)malloc(SMALL_BUF_SIZE + 1);
	snprintf(key_gc, SMALL_BUF_SIZE + 1, "gckey_thread%lu", tid);
	snprintf(val_gc, SMALL_BUF_SIZE + 1, "gcval_thread%lu", tid);
	lvn = kp_local_put(local, key_gc, (void *)val_gc, strlen(val_gc) + 1);
	kp_testcase_int(tid, "put key_gc", 0, (int)lvn);
	if (val_gc) { free(val_gc); val_gc = NULL; }
	ret = kp_local_commit(local, NULL);  // should never conflict!
	kp_testcase_int(tid, "commit key_gc put", 0, ret);

	/* Put some more to key1: */
	descr_len = SMALL_BUF_SIZE;
	str = (char *)malloc(SMALL_BUF_SIZE + 1);
	descr = (char *)malloc(descr_len + 1);
	for (i = 0; i < KEY1_PUTS; i++) {
		snprintf(str, SMALL_BUF_SIZE+1, "valval%d", i);
		snprintf(descr, descr_len+1, "put key1 %d", i);
		lvn = kp_local_put(local, key1, (void *)str, strlen(str)+1);
		kp_testcase_int(tid, descr, i, (int)lvn);
	}
	//don't free(str) yet: used again in testcases below!
	free(descr);

	/* Get, pre-commit 2: */
	ret_val = NULL;
	ret = kp_local_get(local, key1, &ret_val, &ret_size);
	kp_testcase_int(tid, "pre-commit-2 get key1", 0, ret);
	kp_testcase_size_t(tid, "pre-commit-2 get key1 size", strlen(str)+1, ret_size);
	kp_testcase_string(tid, "pre-commit-2 get key1 str", str, (char *)ret_val);
	if (ret_val) { free(ret_val); ret_val = NULL; }

	/* Commit again (only key1 has changed): */
	kp_debug("%lu: about to perform commit (2)\n", tid);
	ret = kp_local_commit(local, NULL);
	if (!do_conflict_detection) {
		kp_testcase_int(tid, "commit 2", 0, ret);
	}
	if (ret != 0 && ret != 1) {
		kp_die("%lu: kp_local_commit() returned error=%d\n", tid, ret);
	}
	commit_conflict = ret == 1 ? true : false;

	/* Get, post-commit 2: */
	if (!commit_conflict) {
		ret_val = NULL;
		ret = kp_local_get(local, key1, &ret_val, &ret_size);
		kp_testcase_int(tid, "post-commit-2 get key1", 0, ret);
		kp_testcase_size_t(tid, "post-commit-2 get key1 size", strlen(str)+1, ret_size);
		kp_testcase_string(tid, "post-commit-2 get key1 str", str, (char *)ret_val);
		if (ret_val) { free(ret_val); ret_val = NULL; }
		free(str);
	} else {
		kp_print("skipping get on key1 post-commit-2 because this worker's "
				"commit had a conflict!\n");
	}

	/* See note above: to test garbage collection, delete the key_gc (which
	 * was committed two snapshots ago) and commit.
	 */
	ret = kp_local_delete_key(local, key_gc);
	kp_testcase_int(tid, "delete key_gc", 0, ret);
	ret = kp_local_commit(local, NULL);  // should never conflict!
	kp_testcase_int(tid, "commit key_gc delete", 0, ret);
	ret = kp_local_get(local, key_gc, &ret_val, &ret_size);
	kp_testcase_int(tid, "get key_gc after delete but before colection",
			1, ret);
	if (ret_val) { free(ret_val); ret_val = NULL; }

//TODO: these tests are broken, see kp_die() statement in kp_local_set_snapshot()
#if 0
	uint64_t snapshot, snapshot_max, ret64, j;
	/* Change snapshot number, then do some gets again: */
	snapshot = kp_local_get_current_snapshot(local);
	snapshot_max = kp_local_get_latest_snapshot(local);
	kp_testcase_uint64_not(tid, "kp_local_get_current_snapshot", UINT64_MAX,
			snapshot);
	kp_testcase_uint64_not(tid, "kp_local_get_latest_snapshot", UINT64_MAX,
			snapshot_max);
	kp_test("snapshot=%ju, snapshot_max=%ju\n", snapshot, snapshot_max);
	if (snapshot > snapshot_max) {
		kp_testcase_uint64(tid, "kp_local_get_*_snapshot", (uint64_t)0,
				UINT64_MAX);
	}
	for (j = snapshot_max-1; j >= 1; j--) {
		kp_test("setting local snapshot to %ju\n", j);
		ret64 = kp_local_set_snapshot(local, j);
		kp_testcase_uint64(tid, "kp_local_set_snapshot",
				j == snapshot_max-1 ? snapshot : j+1, ret64);
		snapshot = kp_local_get_current_snapshot(local);
		kp_testcase_uint64(tid, "kp_local_get_current_snapshot", j,
				snapshot);
		//todo: would be nice to turn these all into pass/fail testcases...
		ret_val = NULL;
		ret = kp_local_get(local, key1, &ret_val, &ret_size);
		kp_test("from snapshot %ju, for key1 got value=%s\n", snapshot,
				(char *)ret_val);
		if (snapshot >= num_threads+1) {
			kp_testcase_int(tid, "previous snapshot get key1", 0, ret);
		} else {
			/* First commits were empty, and (currently) the checkpoint
			 * number of the internal kp_kvstore is incremented even on
			 * empty commits, so gets from low snapshot numbers should
			 * return version-not-found for every key.
			 * HOWEVER, it's possible that one thread performs its
			 * empty commit and then its first real commit before some other
			 * thread performs its empty commit; for example, in one
			 * (bizarre?) execution of the test file, I got this order of
			 * operations:
			 *   TEST PASS: 3074083696: empty commit: expected=0, actual=0
			 *   TEST PASS: 3074083696: commit 1: expected=0, actual=0
			 *   TEST PASS: 3074083696: commit 2: expected=0, actual=0
			 *   TEST PASS: 3074083696: worker reached end of its test function
			 *   TEST PASS: 3065690992: empty commit: expected=0, actual=0
			 *   TEST PASS: 3057298288: empty commit: expected=0, actual=0
			 *   TEST PASS: 3048905584: empty commit: expected=0, actual=0
			 * Therefore, just skip this test case.
			 */
			//kp_testcase_int(tid, "previous snapshot get key1", 1, ret);
			;
		}
		if (snapshot == snapshot_max-1) {
			/* TODO: this test case may also fail, if some worker does
			 * its first non-empty commit after this worker reached this
			 * code.
			 */
			kp_test("snapshot=%ju, snapshot_max=%ju\n", snapshot,
					snapshot_max);
			kp_testcase_int(tid, "previous snapshot (snapshot_max-1) "
					"get key1 size", strlen(str)+1, ret_size);
			kp_testcase_string(tid, "previous snapshot (snapshot_max-1) "
					"get key1 str", str, (char *)ret_val);
		}
		/* Similar to above: this test case doesn't work, because some
		 * threads may perform multiple commits before other threads
		 * perform any.
		 */
		//else if (snapshot == num_threads+1) {  //first commits were empty
		//	kp_testcase_size_t(tid, "previous snapshot (2) get key1 size",
		//			size11, ret_size);
		//	kp_testcase_string(tid, "previous snapshot (2) get key1 str",
		//			val11, (char *)ret_val);
		//}
		if (ret_val) { free(ret_val); ret_val = NULL; }
	}
	ret64 = kp_local_set_snapshot(local, snapshot_max);
	kp_testcase_uint64(tid, "kp_local_set_snapshot reset", (uint64_t)1, ret64);
	snapshot_max = kp_local_get_latest_snapshot(local);
	kp_test("snapshot_max is now %ju\n", snapshot_max);
	free(str);

	/* TODO: what happens if we PUT with an old snapshot?? */
#endif

#if 0
	/* Take more checkpoints, so that garbage collection will kick in.
	 * With the telescoping algorithm, checkpoint 1 will be GC'd first;
	 * key1 is the only key that changed between cp 1 and cp 2, so the
	 * initial version of key1 ("value11") will be collected in the first
	 * GC pass, while all other keys will just be decremented.
	 * Then, checkpoint 3 will be GC'd second; key4 will be the only key
	 * that changed from cp 3 to cp 4, so one of its versions will be
	 * freed (the first one that we put in the loop below: offset (100)).
	 * Then, checkpoints 5 and 2 will be GC'd in the same pass.
	 */
	int bignumber, offset;
	descr_len = 31;
	bignumber = 0;  //disabled for now. This code is wtf'd anyway...
	offset = 100;
	str = (char *)malloc(bignumber + offset + 1);
	descr = (char *)malloc(descr_len + 1);
	for (i = offset; i < bignumber + offset; i++) {
		ret = snprintf(str, bignumber+offset+1, "v%d", i);
		if (ret >= bignumber+offset+1) {
			kp_die("error: snprintf returned ret=%d, bignumber+offset+1=%d\n",
					ret, bignumber+offset+1);
		}
		snprintf(descr, descr_len+1, "put key4 v%d", i);
		if (ret >= descr_len+1) {
			kp_die("error: snprintf returned ret=%d, descr_len+1=%d\n",
					ret, descr_len+1);
		}
		lvn = kp_local_put(local, key4, (void *)str, strlen(str)+1);
		kp_testcase_uint64_not(tid, descr, UINT64_MAX, lvn);

		ret = kp_local_commit(local, NULL);
		kp_testcase_int(tid, "commit for gc", 0, ret);
		if (ret != 0 && ret != 1) {
			kp_die("%lu: kp_local_commit() returned error=%d\n", tid, ret);
		}
	}
	free(str);
	free(descr);
#endif

	/* Perform a bunch of commits of fixed size... */
#define BUF_SIZE 128
	const char *bytes = "00000000001111111111222222222233333333334444444444"
		"5555555555666666666677777777778";
	key = (char *)malloc(BUF_SIZE);
	str = (char *)malloc(BUF_SIZE);
	for (i = 0; i < NUM_COMMITS; i++) {
		for (j = 0; j < COMMIT_SIZE; j++) {
#if 0
			/* Use keys that are unique to this thread, but the same
			 * across each of this thread's commits: */
			ret = snprintf(key, BUF_SIZE, "%d-key%d-%s",
					(int)(tid%10000), j, bytes);
#endif
#if 1
			/* Use COMMIT_SIZE different keys that are common across all
			 * workers, to generate conflicts: */
			ret = snprintf(key, BUF_SIZE, "key%d-%s", j, bytes);
#endif
			if (ret >= BUF_SIZE) {
				kp_die("error: snprintf returned ret=%d, BUF_SIZE=%d\n",
						ret, BUF_SIZE);
			}
			ret = snprintf(str, BUF_SIZE, "%d-value%d-%d-%s",
					(int)(tid%10000), j, i, bytes);
			if (ret >= BUF_SIZE) {
				kp_die("error: snprintf returned ret=%d, BUF_SIZE=%d\n",
						ret, BUF_SIZE);
			}
			if (i == NUM_COMMITS / 2) {
				/* Halfway through the series of commits, instead of putting
				 * to the keys, delete the keys! What will be the effects of
				 * this?
				 *   With a single thread, every key should be successfully
				 *   deleted and a tombstone value will be appended to the
				 *   VT during the commit. Then, the get that we perform
				 *   below should return not-found during the worker's next
				 *   transaction!
				 *   With multiple threads, the same thing should happen,
				 *   although it's possible / likely that the commit will
				 *   abort due to a conflict and then the deletes should
				 *   have no effect!
				 * Will any of this impact garbage collection? Sort of;
				 * these deletes won't cause these keys to ever be freed,
				 * because we're just going to put to them again during
				 * the next commit, but if the commits abort then the
				 * garbage collector's code paths for cleaning up aborted
				 * commits _that include tombstone values in their commit
				 * records_ will be exercised.....
				 */
				ret = kp_local_delete_key(local, key);
				kp_testcase_int(tid, "delete every key", 0, ret);
			} else {
				lvn = kp_local_put(local, key, (void *)str, strlen(str)+1);
			}
		}
		/* Commit after COMMIT_SIZE puts: */
		ret = kp_local_commit(local, NULL);
		if (ret != 0 && ret != 1) {
			kp_die("%lu: kp_local_commit() returned error=%d\n", tid, ret);
		}
/* These tests will produce a ton of output: this block will be executed
 * NUM_COMMITS times for each thread. */
#if 1
		if (ret == 0) {
			/* We expect to run these testcases when the following condition
			 * is true:
			 *   if (!do_conflict_detection || num_threads == 1 || ret == 0)
			 * However, when the number of threads is large, it's possible
			 * for somebody else's commit to finish in-between when ours
			 * finishes and when we perform the get() below, so these tests
			 * may not always pass.
			 * Actually, I think they should _always_ pass: because of the
			 * way that we currently return a new working snapshot number
			 * to the local workers, the returned snapshot number should
			 * always match the just-committed values if the commit did
			 * not abort. (I think this will change in the future though,
			 * once we have asynchronous commits.)
			 *   cat test.out | grep "get just-put-value" | grep -v "PASS"
			 */
			kp_test("commit returned success, so checking just-put-value "
					"for key=%s\n", key);
			kp_testcase_int(tid, "commit loop commit", 0, ret);
			ret_val = NULL;
			ret = kp_local_get(local, key, &ret_val, &ret_size);

			if (i == NUM_COMMITS / 2) {
				kp_testcase_int(tid, "get just-deleted-value after commit",
						1, ret);
			} else {
				kp_testcase_int(tid, "get just-put-value after commit", 0, ret);
				kp_testcase_size_t(tid, "get just-put-value after commit size",
						strlen(str)+1, ret_size);
				kp_testcase_string(tid, "get just-put-value after commit str",
						str, (char *)ret_val);
			}
			if (ret_val) { free(ret_val); ret_val = NULL; }
		} else {
			kp_test("commit returned conflict, so skipping just-put-value "
					"check\n");
		}
#endif
	}
	free(key);
	free(str);

	/* Double-check that getting deleted key from master returns not-found
	 * after garbage collection:
	 */
	ret = kp_local_get(local, key_gc, &ret_val, &ret_size);
	kp_testcase_int(tid, "get key_gc after collection, hopefully", 1, ret);
	if (ret_val) { free(ret_val); ret_val = NULL; }
	if (key_gc) { free(key_gc); key_gc = NULL; }
	
	//sleep(8);
	//kp_die("abort!\n");

	/* Cleanup and return: */
	kp_warn("skipping kp_kv_local_destroy()\n");
	//ret = kp_kv_local_destroy(local);
	//if (ret != 0) {
	//	kp_error("thread_%lu: kp_kv_local_destroy() returned error=%d\n",
	//			tid, ret);
	//	return NULL;
	//}
	//ret_string = malloc(RET_STRING_LEN);
	//snprintf(ret_string, RET_STRING_LEN, "success_%lu", tid);
	//kp_debug("thread %lu returning string=%s\n", tid, ret_string);

	kp_testcase_int(tid, "worker reached end of its test function", 0, 0);
	//return (void *)ret_string;
	
	return (void *)local;
}

void print_usage(char *argv0)
{
	printf("usage: %s <num_threads>\n", argv0);
}

int main(int argc, char *argv[])
{
	int i, ret;
	int num_threads;
	void *ret_thread;
	int cpu;
	pthread_attr_t attr;
	pthread_t threads[MAX_THREADS];
	kp_kv_local* ret_threads[MAX_THREADS];
	worker_thread_args thread_args[MAX_THREADS];
	kp_kv_master *master;

	if (argc != 2) {
		print_usage(argv[0]);
		exit(1);
	}
	num_threads = atoi(argv[1]);
	if (num_threads <= 0) {
		kp_die("invalid num_threads=%d\n", num_threads);
	}
	if (num_threads > MAX_THREADS) {
		num_threads = MAX_THREADS;
	}
	kp_debug("pid=%d, num_threads=%d\n", getpid(), num_threads);


	/* Create master: */
	ret = kp_kv_master_create(&master, MASTER_CONSISTENCY_MODE,
			MASTER_EXPECTED_MAX_KEYS, do_conflict_detection, true);  //true: use_nvm!
	if (ret != 0) {
		kp_die("kp_kv_master_create() failed\n");
	}
	kp_debug("kp_kv_master_create() succeeded, consistency mode=%s\n",
			consistency_mode_to_string(kp_master_get_mode(master)));

	/* Start threads: */
	for (i = 0; i < num_threads; i++) {
		cpu = CPU_OFFSET + (i % NUM_CPUS);
		CPU_ZERO(&(thread_args[i].cpu_set));
		CPU_SET(cpu, &(thread_args[i].cpu_set));
		thread_args[i].master = master;
		thread_args[i].num_threads = num_threads;
		kp_debug("creating thread %d, set cpu to %d (CPU_COUNT=%d)\n",
				i, cpu, CPU_COUNT(&(thread_args[i].cpu_set)));
		kp_print("pinning new thread to CPU=0x%x\n", cpu);

		ret = pthread_attr_init(&attr);  //init with default values
		if (ret != 0) {
			kp_die("pthread_attr_init() returned error=%d\n", ret);
		}
		ret = pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t),
				&(thread_args[i].cpu_set));
		if (ret != 0) {
			kp_die("pthread_attr_setaffinity_np() returned error=%d\n", ret);
		}
		ret = pthread_create(&(threads[i]), &attr, &worker_thread_start,
				(void *)(&(thread_args[i])));
		if (ret != 0) {
			if (ret == EAGAIN) {
				kp_error("pthread_create() returned error EAGAIN\n");
			} else if (ret == EINVAL) {
				kp_error("pthread_create() returned error EINVAL\n");
			} else if (ret == EPERM) {
				kp_error("pthread_create() returned error EPERM\n");
			} else {
				kp_error("pthread_create() returned unknown error\n");
			}
			kp_die("pthread_create(%d) returned error=%d\n", i, ret);
		}
		kp_debug("created thread %d\n", i);

		ret = pthread_attr_destroy(&attr);
		if (ret != 0) {
			kp_die("pthread_attr_destroy() returned error=%d\n", ret);
		}
	}
	
	/* Wait for threads to finish: */
	for (i = 0; i < num_threads; i++) {
		ret = pthread_join(threads[i], &ret_thread);
		if (ret != 0) {
			kp_die("pthread_join(%d) returned error=%d\n", i, ret);
		}
		//kp_debug("joined with thread %d, it returned %s\n", i,
		//		(char *)ret_thread);
		//if (ret_thread) {
		//	free(ret_thread);
		//}
		ret_threads[i] = (kp_kv_local *)ret_thread;
		kp_print("joined with thread %d, it returned %p\n", i,
				ret_threads[i]);
	}
	//kp_die("abort!\n");
	
	/* Cleanup */
	kp_warn("skipping kp_kv_master_destroy()\n");
	//ret = kp_kv_master_destroy(master);
	//if (ret != 0) {
	//	kp_die("kp_kv_master_destroy() failed\n");
	//}

	kp_testcase_int((long unsigned int)0, "master reached end of test file",
			0, 0);
	kp_print("finished.\n");

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
