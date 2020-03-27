/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 * 
 * Katelin Bailey & Peter Hornyack
 * 8/28/2011
 * 
 */

#include "../include/kp_kvstore.h"
#include "../include/kp_kv_local.h"
#include "../include/kp_kv_master.h"
#include "kp_macros.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Iterates over the entire key-value store and prints out the items.
 * Various failure cases are included in the code below and may be
 * commented out. Also, another test case is to call this function
 * twice in a row on the same KV store to make sure it returns the same
 * results.
 */
void test_iterate(kp_kvstore *kv)
{
	int ret;
	unsigned int iterations;
	kp_iterator *iter;
	kp_iter_item *item;


	ret = kp_iterator_get(kv, &iter);
	kp_test("kp_iterator_get: returned %d\n", ret);
	if (!iter) {
		kp_test("iter is NULL! returning\n");
		return;
	}

	iterations = 0;
	while (1) {
		ret = kp_iterator_next(iter, &item);
		if (ret == 1) {
			kp_test("iterator reached end, breaking loop\n");
			break;
		} else if (ret == -2) {
			kp_test("iterator invalidated!, breaking loop\n");
			break;
		} else if (ret != 0) {
			kp_test("iterator error!, breaking loop\n");
			break;
		}
		kp_test("iterator item: key=%s, value=%s, size=%d, lvn=%llu, "
				"gvn=%llu, is_last_version=%s\n", item->key,
				(char *)(item->value), item->size, item->lvn, item->gvn,
				item->is_last_version ? "true" : "false");
		kp_iter_item_destroy(item);
		iterations++;
//#define TEST_ITER_PUT
#ifdef TEST_ITER_PUT
		/* Failure case: test put() in the middle of iterator: */
		char *k4 = "4k";
		size_t s41;
		char v41[] = "XYZ";
		s41 = strlen(v41) + 1;
		/* Failure case: put() in middle of iterator! */
		ret = kp_put(kv, k4, (void *)v41, s41);
		kp_test("put (%s, %s): returned %d\n", k4, v41, ret);
#endif
	}

//#define TEST_ITER_REPEAT
#ifdef TEST_ITER_REPEAT
	int i;
	for (i = 0; i < 3; i++) {
		kp_test("trying repeated nexts of spent iterator (%d)\n", i+1);
		ret = kp_iterator_next(iter, &item);
		if (ret == 1) {
			kp_test("iterator reached end, looping again\n");
			continue;
		} else if (ret == -2) {
			kp_test("iterator invalidated!, looping again\n");
			continue;
		} else if (ret != 0) {
			kp_test("iterator error!, looping again\n");
			continue;
		}
		kp_test("iterator item: key=%s, value=%s, size=%d, lvn=%llu, "
				"gvn=%llu, is_last_version=%s\n", item->key,
				(char *)(item->value), item->size, item->lvn, item->gvn,
				item->is_last_version ? "true" : "false");
		kp_iter_item_destroy(item);
	}
#endif

	ret = kp_iterator_destroy(iter);
	if (ret != 0) {
		kp_test("kp_iterator_destroy() returned error! %d\n", ret);
	}
	kp_test("iterated %u times\n\n", iterations);
}

void testharness()
{
#ifdef BAH
	return;
#else
	kp_kvstore *kv;
	int i, ret;
	char *str;
	size_t size;
	size_t s10, s20, s30, s40;
	size_t s11, s21, s31, s41;
	uint64_t ver;
	int bignumber;
	FILE *stream;
	bool use_nvm;

	const char *statsfile = "./stats.out";

	char *k1dup;
	char *k1 = "key1";
	char *k2 = "k2";
	char *k3 = "3key";
	char *k4 = "4k";
	char *boguskey = "thisisbogus";

	k1dup = malloc(strlen(k1) + 1);
	strncpy(k1dup, k1, strlen(k1) + 1);

	/* Declaring these as char[] instead of char* allows them to be
	 * modified: http://stackoverflow.com/questions/1704407/what-is-the-difference-between-char-s-and-char-s-in-c
	 */
	char v10[] = "abcd";
	char v11[] = "ABCD";
	char v20[] = "efghijkl";
	char v21[] = "EFGHIJKL";
	char v30[] = "mnopqrstuvw";
	char v31[] = "MNOPQRSTUVW";
	char v40[] = "xyz";
	char v41[] = "XYZ";

	/* Add 1 to strlen so that \0 will be copied */
	s10 = strlen(v10) + 1;
	s11 = strlen(v11) + 1;
	s20 = strlen(v20) + 1;
	s21 = strlen(v21) + 1;
	s30 = strlen(v30) + 1;
	s31 = strlen(v31) + 1;
	s40 = strlen(v40) + 1;
	s41 = strlen(v41) + 1;

	/* Create kv store: indicate that this is a master kvstore, which
	 * makes sense because we're not using worker threads in this
	 * file...
	 */
	use_nvm = false;  //...
	kv = kp_kvstore_create(true, use_nvm);

	/* Stats: */
	stream = fopen(statsfile, "a");  //open for appending
	//ret = kp_print_stats(kv, stream);
	//kp_test("kp_print_stats returned %d\n", ret);
	//ret = kp_print_stats(kv, stdout);
	//kp_test("kp_print_stats returned %d\n", ret);

	kp_test("iterate over kvstore, size=0:\n");
	test_iterate(kv);

	/* Put initial versions: */
	ret = kp_put(kv, k1, (void *)v10, s10);
	kp_test("put (%s, %s): returned %d\n", k1, v10, ret);
	ret = kp_put(kv, k2, (void *)v20, s20);
	kp_test("put (%s, %s): returned %d\n", k2, v20, ret);
	ret = kp_put(kv, k3, (void *)v30, s30);
	kp_test("put (%s, %s): returned %d\n", k3, v30, ret);
	ret = kp_put(kv, k4, (void *)v40, s40);
	kp_test("put (%s, %s): returned %d\n", k4, v40, ret);
	kp_test("iterate over kvstore, size=4:\n");
	//test_iterate(kv);

	/* Put new versions: */
	ret = kp_put(kv, k1, (void *)v11, s11);
	kp_test("put (%s, %s): returned %d\n", k1, v11, ret);
	ret = kp_put(kv, k2, (void *)v21, s21);
	kp_test("put (%s, %s): returned %d\n", k2, v21, ret);
	kp_test("iterate over kvstore, size=6:\n");
	//test_iterate(kv);

	/* Get current versions. Remember to free the returned values! */
	ret = kp_get(kv, k1, (void *)(&str), &size);
	kp_test("get %s returned ret=%d, size=%d, value=[%s]\n", k1, ret, size, str);
	if (str) {free(str); str=NULL;}
	ret = kp_get(kv, k2, (void *)(&str), &size);
	kp_test("get %s returned ret=%d, size=%d, value=[%s]\n", k2, ret, size, str);
	if (str) {free(str); str=NULL;}
	ret = kp_get(kv, k3, (void *)(&str), &size);
	kp_test("get %s returned ret=%d, size=%d, value=[%s]\n", k3, ret, size, str);
	if (str) {free(str); str=NULL;}
	ret = kp_get(kv, k4, (void *)(&str), &size);
	kp_test("get %s returned ret=%d, size=%d, value=[%s]\n", k4, ret, size, str);
	if (str) {free(str); str=NULL;}

	/* Get previous versions: */
	ver = 0;
	ret = kp_get_version_local(kv, k1, ver, (void *)(&str), &size);
	kp_test("get local version %llu of %s returned ret=%d, size=%d, value=[%s]\n",
			ver, k1, ret, size, str);
	if (str) {free(str); str=NULL;}
	ret = kp_get_version_local(kv, k2, ver, (void *)(&str), &size);
	kp_test("get local version %llu of %s returned ret=%d, size=%d, value=[%s]\n",
			ver, k2, ret, size, str);
	if (str) {free(str); str=NULL;}
	ver = 1; size = 99999;
	ret = kp_get_version_local(kv, k3, ver, (void *)(&str), &size);
	kp_test("get local version %llu of %s returned ret=%d, size=%d, value=[%s]\n",
			ver, k3, ret, size, str);
	if (str) {free(str); str=NULL;}

	/* Get bogus key: */
	ret = kp_get(kv, boguskey, (void *)(&str), &size);
	kp_test("get %s (bogus) returned ret=%d, size=%d, value=[%s]\n", boguskey, ret, size, str);

	/* Test key comparison function: k1dup has same string as k1, but
	 * different address.
	 */
	//kp_test("k1 address=%p, k1dup address=%p\n", k1, k1dup);
	ret = kp_get(kv, k1dup, (void *)(&str), &size);
	kp_test("get %s (dup!) returned ret=%d, size=%d, value=[%s]\n", k1dup, ret, size, str);
	if (str) {free(str); str=NULL;}

	/* Put a whole bunch of versions: */
	bignumber = 12;
	str = (char *)malloc(bignumber+1);  //should be big enough :)
	for (i = 1; i <= bignumber; i++) {
		snprintf(str, bignumber, "%d", i);
		ret = kp_put(kv, k3, (void *)str, strlen(str)+1);
		kp_test("put (%s, %s): returned %d\n", k3, str, ret);
	}
	kp_test("iterate over kvstore, size=18:\n");
	test_iterate(kv);

	/* Get ALL of the gvns for k3 (and then some): use this to check that
	 * kp_garray_uint64_search() range search is working.
	 */
	kp_put(kv, k4, (void *)v41, s41);
	for (i = 0; i <= 20; i++) {
		if (str) free(str);
		str=NULL;
		ver = i; size = 99999;
		ret = kp_get_version_global(kv, k1, ver, (void *)(&str), &size);
		kp_test("get global version %llu of %s returned ret=%d, size=%d, value=[%s]\n",
				ver, k1, ret, size, str);
	}

	for (i = 0; i <= 20; i++) {
		if (str) free(str);
		str=NULL;
		ver = i; size = 99999;
		ret = kp_get_version_global(kv, k2, ver, (void *)(&str), &size);
		kp_test("get global version %llu of %s returned ret=%d, size=%d, value=[%s]\n",
				ver, k2, ret, size, str);
	}

	for (i = 0; i <= 20; i++) {
		if (str) free(str);
		str=NULL;
		ver = i; size = 99999;
		ret = kp_get_version_global(kv, k3, ver, (void *)(&str), &size);
		kp_test("get global version %llu of %s returned ret=%d, size=%d, value=[%s]\n",
				ver, k3, ret, size, str);
	}

	for (i = 0; i <= 20; i++) {
		if (str) free(str);
		str=NULL;
		ver = i; size = 99999;
		ret = kp_get_version_global(kv, k4, ver, (void *)(&str), &size);
		kp_test("get global version %llu of %s returned ret=%d, size=%d, value=[%s]\n",
				ver, k4, ret, size, str);
	}

	/* Get a whole bunch of versions */
	free(str); str=NULL;
	ver = 0; size = 99999;
	ret = kp_get_version_global(kv, k3, ver, (void *)(&str), &size);
	kp_test("get global version %llu of %s returned ret=%d, size=%d, value=[%s]\n",
			ver, k3, ret, size, str);
	if (str) {free(str); str=NULL;}
	ver = 3; size = 99999;
	ret = kp_get_version_local(kv, k3, ver, (void *)(&str), &size);
	kp_test("get local version %llu of %s returned ret=%d, size=%d, value=[%s]\n",
			ver, k3, ret, size, str);
	if (str) {free(str); str=NULL;}
	ret = kp_get_version_global(kv, k3, ver, (void *)(&str), &size);
	kp_test("get global version %llu of %s returned ret=%d, size=%d, value=[%s]\n",
			ver, k3, ret, size, str);
	if (str) {free(str); str=NULL;}
	ver = 11; size = 99999;
	ret = kp_get_version_local(kv, k3, ver, (void *)(&str), &size);
	kp_test("get local version %llu of %s returned ret=%d, size=%d, value=[%s]\n",
			ver, k3, ret, size, str);
	if (str) {free(str); str=NULL;}
	ret = kp_get_version_global(kv, k3, ver, (void *)(&str), &size);
	kp_test("get global version %llu of %s returned ret=%d, size=%d, value=[%s]\n",
			ver, k3, ret, size, str);
	if (str) {free(str); str=NULL;}
	ver = 111; size = 99999;
	ret = kp_get_version_local(kv, k3, ver, (void *)(&str), &size);
	kp_test("get local version %llu of %s returned ret=%d, size=%d, value=[%s]\n",
			ver, k3, ret, size, str);
	if (str) {free(str); str=NULL;}
	ret = kp_get_version_global(kv, k3, ver, (void *)(&str), &size);
	kp_test("get global version %llu of %s returned ret=%d, size=%d, value=[%s]\n",
			ver, k3, ret, size, str);
	if (str) {free(str); str=NULL;}
	//ver = 1111; size = 99999;
	//ret = kp_get_version_local(kv, k3, ver, (void *)(&str), &size);
	//kp_test("get local version %llu of %s returned ret=%d, size=%d, value=[%s]\n",
	//		ver, k3, ret, size, str);
	//if (str) {free(str); str=NULL;}
	ver = kp_get_gvn(kv); size = 99999;
	ret = kp_get_version_local(kv, k3, ver, (void *)(&str), &size);
	kp_test("get local version %llu of %s returned ret=%d, size=%d, value=[%s]\n",
			ver, k3, ret, size, str);
	if (str) {free(str); str=NULL;}
	ret = kp_get_version_global(kv, k3, ver, (void *)(&str), &size);
	kp_test("get global version %llu of %s returned ret=%d, size=%d, value=[%s]\n",
			ver, k3, ret, size, str);
	if (str) {free(str); str=NULL;}

	/* Test delete of versions */

	char *key = k1;
	ret = kp_delete_key(kv, key);
	kp_test("kp_delete_key() returned: %d\n", ret);
	ret = kp_get(kv, key, (void*)(&str), &size);
	kp_test("get current version of deleted key returns ret=%d, size=%d, value=[%s]\n",
		ret, size, str);
	if(str) {free(str); str=NULL;}
	kp_test("iterate over kvstore, size=17:\n");
	test_iterate(kv);

	key = k2;
	ret = kp_delete_key(kv, key);
	kp_test("kp_delete_key() returned: %d\n", ret);
	ret = kp_get(kv, key, (void*)(&str), &size);
	kp_test("get current version of deleted key returns ret=%d, size=%d, value=[%s]\n",
		ret, size, str);
	if(str) {free(str); str=NULL;}
	kp_test("iterate over kvstore, size=16:\n");
	test_iterate(kv);

	key = k3;
	ret = kp_delete_key(kv, key);
	kp_test("kp_delete_key() returned: %d\n", ret);
	ret = kp_get(kv, key, (void*)(&str), &size);
	kp_test("get current version of deleted key returns ret=%d, size=%d, value=[%s]\n",
		ret, size, str);
	if(str) {free(str); str=NULL;}
	kp_test("iterate over kvstore, size=15:\n");
	test_iterate(kv);

	key = k4;
	ret = kp_delete_key(kv, key);
	kp_test("kp_delete_key() returned: %d\n", ret);
	ret = kp_get(kv, key, (void*)(&str), &size);
	kp_test("get current version of deleted key returns ret=%d, size=%d, value=[%s]\n",
		ret, size, str);
	if(str) {free(str); str=NULL;}
	kp_test("iterate over kvstore, size=14:\n");
	test_iterate(kv);

	/* Test put after deletion:*/
	ret = kp_put(kv, k4, (void *)v41, s41);
	kp_test("put (%s, %s): returned %d\n", k4, v41, ret);
	kp_test("iterate over kvstore, size=15:\n");
	test_iterate(kv);

#ifdef TEST_GC
	uint64_t gvn;

	/* Garbage collection: */
	gvn = 0;
	ret = kp_add_gvn_to_gc(kv, gvn);
	kp_test("kp_add_gvn_to_gc(%llu) returned %d\n", gvn, ret);
	gvn = 3;
	ret = kp_add_gvn_to_gc(kv, gvn);
	kp_test("kp_add_gvn_to_gc(%llu) returned %d\n", gvn, ret);
	gvn = 5;
	ret = kp_add_gvn_to_gc(kv, gvn);
	kp_test("kp_add_gvn_to_gc(%llu) returned %d\n", gvn, ret);
	gvn = 25;
	ret = kp_add_gvn_to_gc(kv, gvn);
	kp_test("kp_add_gvn_to_gc(%llu) returned %d\n", gvn, ret);

	ret = kp_run_gc(kv);
	kp_test("kp_run_gc() returned %d\n", ret);

	/* Garbage collection policy */
	uint64_t num = 0;
	ret = kp_gc_set_global_kept(kv, num);
	kp_test("kp_gc_set_global_kept(%llu) returned %d\n", num, ret);
	num = 10;
	ret = kp_gc_set_global_kept(kv, num);
	kp_test("kp_gc_set_global_kept(%llu) returned %d\n", num, ret);
	num = 5;
	ret = kp_gc_set_global_kept(kv, num);
	kp_test("kp_gc_set_global_kept(%llu) returned %d\n", num, ret);
	num = 7;
	ret = kp_gc_set_global_kept(kv, num);
	kp_test("kp_gc_set_global_kept(%llu) returned %d\n", num, ret);
	num = UINT64_MAX;
	ret = kp_gc_set_global_kept(kv, num);
	kp_test("kp_gc_set_global_kept(%llu) returned %d\n", num, ret);
	num = 12;
	ret = kp_gc_set_global_kept(kv, num);
	kp_test("kp_gc_set_global_kept(%llu) returned %d\n", num, ret);

	num = 0;
	ret = kp_gc_set_local_kept(kv, num);
	kp_test("kp_gc_set_local_kept(%llu) returned %d\n", num, ret);
	num = 10;
	ret = kp_gc_set_local_kept(kv, num);
	kp_test("kp_gc_set_local_kept(%llu) returned %d\n", num, ret);
	num = 5;
	ret = kp_gc_set_local_kept(kv, num);
	kp_test("kp_gc_set_local_kept(%llu) returned %d\n", num, ret);
	num = 7;
	ret = kp_gc_set_local_kept(kv, num);
	kp_test("kp_gc_set_local_kept(%llu) returned %d\n", num, ret);
	num = UINT64_MAX;
	ret = kp_gc_set_local_kept(kv, num);
	kp_test("kp_gc_set_local_kept(%llu) returned %d\n", num, ret);
	num = 12;
	ret = kp_gc_set_local_kept(kv, num);
	kp_test("kp_gc_set_local_kept(%llu) returned %d\n", num, ret);
#endif  //#ifdef TEST_GC

	/* Free the kv store: */
	ret = kp_kvstore_destroy(kv);
	kp_test("kp_kvstore_destroy() returned: %d\n", ret);

	/* Other cleanup: */
	free(k1dup);
	ret = fclose(stream);
	
#endif  //#ifndef BAH
}

int main(int argc, char *argv[])
{


	testharness();
    //mtrace_enable_set(mtrace_record_enable,str);

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
