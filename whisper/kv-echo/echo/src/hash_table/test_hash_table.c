/* Peter Hornyack and Katelin Bailey
 * 1/13/12
 * University of Washington
 */

#include <stdlib.h>
#include <time.h>
#include "hash.h"
#include "ht_macros.h"

#define HT_EXPECTED_SIZE 1000000  //arbitrary...
  /* Set HT_EXPECTED_SIZE to 10 to test bucket chaining: "oops" will hash
   * to same value (5) as "yek3".
   */

typedef struct _ht_entry {
	char *key;  //MUST be null-terminated, or hash function will fail!
	void *val;
} ht_entry;

/* This hash function was found by googling for "c string hash function,"
 * and viewing this result: http://www.cse.yorku.ca/~oz/hash.html.
 * There are many, many other possibilities for a hash function; search
 * stackoverflow.com for much more information. MD5, SHA, etc. are other
 * potential good bets.
 */
static size_t ht_hasher(const void *entry_ptr, size_t table_size)
{
	int c;
	char *key;
	unsigned long hash;
	ht_entry *entry = (ht_entry *)entry_ptr;

	if (!entry) {
		ht_error("entry is NULL! Returning 0 (all NULL pointers will "
				"hash to this slot)\n");
		return 0;
	}

	ht_debug("hashing entry: key=%s, val=%p\n", entry->key, entry->val);
	key = entry->key;  //"unsigned char *key" on web page...
	hash = 0;

	/* "sdbm" algorithm: apparently this is used in gawk, BerkelyDB, etc. */
	c = *key;
	while (c != '\0') {
		hash = c + (hash << 6) + (hash << 16) - hash;
		key++;
		c = *key;
	}
	hash = hash % table_size;  //don't forget this!
	ht_debug("returning hash=%u for entry->key=%s\n", (size_t)hash, entry->key);
	return (size_t)hash;
}

/* Simple string-comparison comparator: */
bool ht_comparator(const void *entry1_ptr, const void *entry2_ptr)
{
	ht_entry *entry1 = (ht_entry *)entry1_ptr;
	ht_entry *entry2 = (ht_entry *)entry2_ptr;
	if (!entry1 || !entry2) {
		ht_error("got a null ptr: %p %p\n", entry1, entry2);
		return false;
	}
	ht_debug("comparing entry1 [%s] to entry2 [%s]\n",
			entry1->key, entry2->key);
	if (strcmp(entry1->key, entry2->key) == 0) {
		return true;
	}
	return false;
}

/* Function passed to hash_do_for_each(); for now, just prints each entry,
 * and ignores the "extra data" pointer.
 */
bool ht_process_entry(void *entry_ptr, void *extra_data)
{
	ht_entry *entry;

	entry = (ht_entry *)entry_ptr;
	if (!entry) {
		ht_debug("got NULL entry, halting processing\n");
		return false;
	}

	ht_print("processing entry: key=[%s], val=%p\n", entry->key, entry->val);
	return true;
}

/* Allocates a new ht_entry, then COPIES the specified key string and value
 * pointer into it. The key pointer must not be empty and the key must not
 * be an empty string.
 * Returns: 0 on success, -1 on error.
 */
int ht_entry_alloc(ht_entry **entry, const char *key, void *val)
{
	unsigned int len = strlen(key);
	if (len == 0) {
		ht_error("key len is 0\n");
		return -1;
	}

	*entry = (ht_entry *)malloc(sizeof(ht_entry));
	if (*entry == NULL) {
		ht_error("malloc(ht_entry) failed\n");
		return -1;
	}

	(*entry)->key = (char *)malloc(len+1);
	if ((*entry)->key == NULL) {
		ht_error("malloc(entry->key) failed\n");
		free(entry);
		return -1;
	}
	strncpy((*entry)->key, key, len);
	((*entry)->key)[len] = '\0';

	(*entry)->val = val;
	ht_debug("allocated new entry, key=%p [%s], val=%p\n",
			(*entry)->key, (*entry)->key, (*entry)->val);

	return 0;
}

void ht_entry_free(void *entry_ptr)
{
	ht_entry *entry = (ht_entry *)entry_ptr;
	if (!entry) {
		ht_error("entry is NULL! Returning.\n");
		return;
	}
	ht_debug("freeing ht_entry: key=[%s], val=[%s]\n", entry->key,
			(char *)entry->val);
	if (entry->key) {
		free(entry->key);
	} else {
		ht_warn("entry->key is NULL! This is unexpected.\n");
	}
	if (entry->val) {
		//TODO: make sure this makes sense!
		free(entry->val);
		//ht_debug("entry->val = %p; not freeing it though!\n");
	} else {
		ht_debug("entry->val is NULL! This may be expected, e.g. if this "
				"entry was only used for performing a lookup.\n");
	}
	free(entry);
}

int main(int argc, char *argv[])
{
	int ret;
	void const *retptr;
	Hash_table *ht = NULL;
	size_t expected_size;
	ht_entry *entry1, *entry2, *entry3, *lookup_entry, *process_entry;
	char *key1 = "key1";
	//char *key1 = "oops";
	char *key2 = "2key";
	char *key3 = "yek3";
	void *val1, *val2, *val3;
	//Hash_tuning ht_tuning;  //needs to be const??

	/* Set up entries: our hash table entries are structs that hold a
	 * string key and a void * value.
	 */
	val1 = malloc(sizeof(int));     //*val1 = 56789;
	val2 = malloc(sizeof(char)*2);  //*val2[0] = 'Z';  *val2[1] = '\0';
	val3 = malloc(sizeof(void *));  //*val3 = 0xA1A1A1A1;
	ret = ht_entry_alloc(&entry1, key1, val1);
	ht_testcase_int("alloc entry1", 0, ret);
	ret = ht_entry_alloc(&entry2, key2, val2);
	ht_testcase_int("alloc entry2", 0, ret);
	ret = ht_entry_alloc(&entry3, key3, val3);
	ht_testcase_int("alloc entry3", 0, ret);

	/* Create the hash table. See hash.c:85 for a general description
	 * of the hash table (basic hash table with linear chaining to handle
	 * collisions and automatic re-hashing).
	 * The first argument is the "expected" size of the hash table; we are
	 * guaranteed to be able to insert at least this many entries into the
	 * table before any resizing is needed. For now, we just use some
	 * arbitrary value. 10 is the minimum (hash.c:468). The hash table
	 * initialization function succeeds with up to 100,000,000 as the
	 * expected size, but 1,000,000,000 failed.
	 *   Expected size 10000: hash table will be created with 12503 slots
	 *   Expected size 1000: hash table will be created with 1259 slots
	 *   Expected size 100: hash table will be created with 127 slots
	 *   Expected size 10: hash table will be created with 13 slots
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
	 * that hashes string keys.
	 * The fourth argument is a comparison function. We specify our own
	 * comparison function that performs string comparison.
	 * The fifth argument is a "freer" function that will be called on an
	 * entry when it is freed from the hash table (either hash_clear(), which
	 * frees all of the inserted entries but leaves the hash table itself
	 * intact, or hash_free(), which destroys the entire hash table). We
	 * specify our own freer function that frees our ht_entry structs.
	 */
	expected_size = HT_EXPECTED_SIZE;
	//hash_reset_tuning(&ht_tuning);  //reset to default tuning

	/* Initialize hash table: */
	ht = hash_initialize(expected_size, NULL, ht_hasher, ht_comparator,
			ht_entry_free);
	if (!ht) {
		ht_error("hash_initialize() failed\n");
		return -1;
	}
	ht_testcase_int("hash_initialize()", 0, 0);

	/* Check stuff, print stuff: */
	ht_test("hash_get_n_buckets: %u\n", hash_get_n_buckets(ht));
	ht_testcase_int("hash_get_n_buckets_used", 0, hash_get_n_buckets_used(ht));
	ht_testcase_int("hash_get_n_entries", 0, hash_get_n_entries(ht));
	ht_testcase_int("hash_get_max_bucket_length", 0, hash_get_max_bucket_length(ht));
	ht_testcase_int("hash_table_ok", 1, hash_table_ok(ht) ? 1 : 0);
	hash_print_statistics(ht, stdout);

	/* Insert some. Use hash_insert_if_absent(), rather than hash_if_insert(),
	 * which is just a wrapper around hash_insert_if_absent() anyway.
	 */
	retptr = NULL;
	ret = hash_insert_if_absent(ht, (void *)entry1, &retptr);
	ht_testcase_int("insert entry1", 1, ret);
	ht_testcase_ptr("insert entry1 retptr", NULL, retptr);
	retptr = NULL;
	ret = hash_insert_if_absent(ht, (void *)entry3, &retptr);
	ht_testcase_int("insert entry3", 1, ret);
	ht_testcase_ptr("insert entry3 retptr", NULL, retptr);
	retptr = NULL;
	ret = hash_insert_if_absent(ht, (void *)entry2, &retptr);
	ht_testcase_int("insert entry2", 1, ret);
	ht_testcase_ptr("insert entry2 retptr", NULL, retptr);
	retptr = NULL;
	ret = hash_insert_if_absent(ht, (void *)entry3, &retptr);
	ht_testcase_int("insert entry3 again", 0, ret);
	ht_testcase_ptr("insert entry3 again retptr", entry3, retptr);

	/* Lookup entries: to perform lookup, we need to use another ht_entry
	 * struct, but we only need to fill in the key. On success, the
	 * pointer to the matching entry is returned; on failure, NULL is
	 * returned.
	 */
	ret = ht_entry_alloc(&lookup_entry, key3, NULL);
	ht_testcase_int("alloc lookup_entry key3", 0, ret);
	retptr = hash_lookup(ht, (void *)lookup_entry);
	ht_testcase_ptr("lookup key3", entry3, retptr);
	if (retptr) ht_testcase_string("lookup key3 string", key3,
			((ht_entry *)retptr)->key);
	ht_entry_free((void *)lookup_entry);

	ret = ht_entry_alloc(&lookup_entry, key2, NULL);
	ht_testcase_int("alloc lookup_entry key2", 0, ret);
	retptr = hash_lookup(ht, (void *)lookup_entry);
	ht_testcase_ptr("lookup key2", entry2, retptr);
	if (retptr) ht_testcase_string("lookup key2 string", key2,
			((ht_entry *)retptr)->key);
	ht_entry_free((void *)lookup_entry);

	ret = ht_entry_alloc(&lookup_entry, key1, NULL);
	ht_testcase_int("alloc lookup_entry key1", 0, ret);
	retptr = hash_lookup(ht, (void *)lookup_entry);
	ht_testcase_ptr("lookup key1", entry1, retptr);
	if (retptr) ht_testcase_string("lookup key1 string", key1,
			((ht_entry *)retptr)->key);
	ht_entry_free((void *)lookup_entry);

	ret = ht_entry_alloc(&lookup_entry, "oops", NULL);
	ht_testcase_int("alloc lookup_entry oops", 0, ret);
	retptr = hash_lookup(ht, (void *)lookup_entry);
	ht_testcase_ptr("lookup oops", NULL, retptr);
	ht_entry_free((void *)lookup_entry);

	ht_test("hash_get_n_buckets: %u\n", hash_get_n_buckets(ht));
	ht_testcase_int("hash_get_n_buckets_used", 3, hash_get_n_buckets_used(ht));
	ht_testcase_int("hash_get_n_entries", 3, hash_get_n_entries(ht));
	ht_testcase_int("hash_get_max_bucket_length", 1, hash_get_max_bucket_length(ht));
	ht_testcase_int("hash_table_ok", 1, hash_table_ok(ht) ? 1 : 0);

	/* Walk table, two different ways. Note that hash_do_for_each() should
	 * be more efficient than calling hash_get_next() repeatedly, because
	 * hash_get_next() has to call the hasher each time it is called, while
	 * hash_do_for_each() does not!
	 */
	process_entry = hash_get_first(ht);
	while (process_entry) {
		ht_process_entry((void *)process_entry, NULL);
		process_entry = hash_get_next(ht, process_entry);
	}

	ret = hash_do_for_each(ht, ht_process_entry, NULL);
	ht_testcase_int("hash_do_for_each counter", ret, 3);

	/* Delete entries and re-walk. hash_delete() returns the just-deleted entry
	 * (it is up to the caller to free it!), or NULL if the entry is not found
	 * in the table.
	 */
	process_entry = hash_delete(ht, (void *)entry3);  //delete key3
	ht_testcase_ptr("delete key3", entry3, process_entry);
	retptr = hash_lookup(ht, (void *)entry3);
	ht_testcase_ptr("lookup deleted key3", NULL, retptr);
	ht_entry_free((void *)entry3);  //actually free entry3's data
	ret = hash_do_for_each(ht, ht_process_entry, NULL);
	ht_testcase_int("hash_do_for_each counter", ret, 2);

	process_entry = hash_delete(ht, (void *)entry2);  //delete key2
	ht_testcase_ptr("delete key2", entry2, process_entry);
	retptr = hash_lookup(ht, (void *)entry2);
	ht_testcase_ptr("lookup deleted key2", NULL, retptr);
	ht_entry_free((void *)entry2);  //actually free entry2's data
	ret = hash_do_for_each(ht, ht_process_entry, NULL);
	ht_testcase_int("hash_do_for_each counter", ret, 1);

	retptr = hash_lookup(ht, (void *)entry1);
	ht_testcase_ptr("lookup key1 again", entry1, retptr);

	/* Clear table. The freer function (free) will be called on each entry: */
	ht_test("calling hash_clear()\n");
	hash_clear(ht);
	ret = ht_entry_alloc(&lookup_entry, key1, NULL);
	ht_testcase_int("alloc lookup_entry key1", 0, ret);
	retptr = hash_lookup(ht, (void *)lookup_entry);
	ht_testcase_ptr("lookup in cleared table", NULL, retptr);
	ht_entry_free((void *)lookup_entry);

	ret = hash_do_for_each(ht, ht_process_entry, NULL);
	ht_testcase_int("hash_do_for_each counter", ret, 0);
	ht_test("hash_get_n_buckets: %u\n", hash_get_n_buckets(ht));
	ht_testcase_int("hash_get_n_buckets_used", 0, hash_get_n_buckets_used(ht));
	ht_testcase_int("hash_get_n_entries", 0, hash_get_n_entries(ht));
	ht_testcase_int("hash_get_max_bucket_length", 0, hash_get_max_bucket_length(ht));
	ht_testcase_int("hash_table_ok", 1, hash_table_ok(ht) ? 1 : 0);

	/* Free table: */
	ht_test("calling hash_free()\n");
	hash_free(ht);
	ht_testcase_int("reached end of test file. Running valgrind on this "
			"test file should report 0 bytes in 0 blocks in-use at exit",
			0, 0);

	return 0;
}
