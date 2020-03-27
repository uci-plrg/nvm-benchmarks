/* Peter Hornyack and Katelin Bailey
 * 12/8/11
 * University of Washington
 */

#include <stdlib.h>
#include <time.h>
#include "vector.h"
#include "vector_macros.h"

#define ELT_SIZE_MIN 2     //at least 2!!
#define ELT_SIZE_MAX 256
#define ELT_SIZE_DIFF 8
#define APPENDS_MIN 2
#define APPENDS_MAX 8
#define DELETES_MIN 4
#define DELETES_MAX 6
#define LOOPS 1000
unsigned long long preallocate = 2;
bool use_nvm = true;

void stress_test()
{
	int i, j, ret;
	unsigned long long idx, count;
	vector *v;
	void *e;
	long int rand_int;
	ssize_t size;
	char *final;

	if (APPENDS_MAX <= APPENDS_MIN) {
		v_die("invalid APPENDS_MAX %u and APPENDS_MIN %u\n", APPENDS_MAX,
				APPENDS_MIN);
	}
	if (DELETES_MAX <= DELETES_MIN) {
		v_die("invalid DELETES_MAX %u and DELETES_MIN %u\n", DELETES_MAX,
				DELETES_MIN);
	}

	srandom((unsigned int)time(NULL));
	size = ELT_SIZE_MIN;
	ret = vector_create(&v, preallocate, use_nvm);

	for (i = 0; i < LOOPS; i++) {
		/* First, do some appends: */
		rand_int = random();
		rand_int = (rand_int % (APPENDS_MAX - APPENDS_MIN)) + APPENDS_MIN;
		v_debug("appending %ld elements of size %zu to vector:\n",
				rand_int, size);
		for (j = rand_int; j > 0; j--) {
			e = malloc(size);
			((char *)e)[0] = 'A' + ((i+rand_int-j)%26);
			((char *)e)[1] = '\0';
			ret = vector_append(v, e, NULL);
		}
		v_debug("appended %ld elements, now count=%llu\n", rand_int, vector_count(v));

		/* Then, do some deletes: */
		rand_int = random();
		rand_int = (rand_int % (DELETES_MAX - DELETES_MIN)) + DELETES_MIN;
		v_debug("deleting up to %ld values from vector\n", rand_int);
		for (j = rand_int; j > 0; j--) {
			count = vector_count(v);
			if (count == 0) {
				break;
			}
			idx = (i + j) % count;
			ret = vector_delete(v, idx, &e);
			v_debug("deleted element %s at idx=%llu, now freeing element\n",
					(char *)e, idx);
			free(e);
		}
		v_debug("deleted %ld elements, now count=%llu\n", rand_int - j,
				vector_count(v));

		/* Loop again: */
		size = size + ELT_SIZE_DIFF;
		if (size > ELT_SIZE_MAX) {
			size = ELT_SIZE_MIN;
		}
	}

	/* Print final contents of vector: */
	count = vector_count(v);
	v_debug("final vector count=%llu\n", count);
	final = malloc(count + 1);
	for (i = 0; i < count; i++) {
		ret = vector_get(v, i, &e);
		final[i] = ((char *)e)[0];
	}
	final[count] = '\0';
	v_debug("final contents of vector: %s\n", final);
	free(final);

	/* Free vector: */
	vector_free_contents(v);
	vector_destroy(&v);
}

int string_comparator(const void *e1, const void *e2)
{
	return strcmp((char *)e1, (char *)e2);
}

int main(int argc, char *argv[])
{
	int i, ret;
	uint64_t ret64;
	unsigned long long count;
	unsigned long tid;
	vector *v;
	void *e;
	char *prev_e;

#if 1
	stress_test();
	v_print("stress test complete\n");
#endif

	tid = pthread_self();
	v_print("sizeof(void *)=%zu, sizeof(unsigned int)=%zu, "
			"sizeof(unsigned long int)=%zu, sizeof(unsigned long)=%zu, "
			"sizeof(unsigned long long)=%zu\n",
			sizeof(void *), sizeof(unsigned int), sizeof(unsigned long int),
			sizeof(unsigned long), sizeof(unsigned long long));
	v_print("value of null-zero = %p\n", (void *)'\0');

	ret = vector_create(&v, preallocate, use_nvm);
	v_testcase_int(tid, "vector_create", 0, ret);

#if 0
	v_testcase_uint64(tid, "vector_count", (uint64_t)0, vector_count(v));
	ret64 = vector_insert(v, "erik", string_comparator);
	v_testcase_int(tid, "vector_insert", 0, ret);
	v_testcase_uint64(tid, "vector_insert idx", (uint64_t)0, ret64);
	ret64 = vector_insert(v, "lydia", string_comparator);
	v_testcase_int(tid, "vector_insert", 0, ret);
	v_testcase_uint64(tid, "vector_insert idx", (uint64_t)1, ret64);
	ret64 = vector_insert(v, "hannes", string_comparator);
	v_testcase_int(tid, "vector_insert", 0, ret);
	v_testcase_uint64(tid, "vector_insert idx", (uint64_t)1, ret64);
	ret64 = vector_insert(v, "olle", string_comparator);
	v_testcase_int(tid, "vector_insert", 0, ret);
	v_testcase_uint64(tid, "vector_insert idx", (uint64_t)3, ret64);
	ret64 = vector_insert(v, "emil", string_comparator);
	v_testcase_int(tid, "vector_insert", 0, ret);
	v_testcase_uint64(tid, "vector_insert idx", (uint64_t)0, ret64);
	ret64 = vector_insert(v, "erik", string_comparator);
	v_testcase_int(tid, "vector_insert", 0, ret);
	v_testcase_uint64(tid, "vector_insert idx", (uint64_t)2, ret64);
	v_die("exit early\n");
#endif

	/* TODO: how/where are these strings allocated? Only have local scope
	 * (this main() function), right?
	 */
	ret = vector_append(v, "emil", (void **)&prev_e);
	v_testcase_int(tid, "vector_append", 0, ret);
	ret = vector_append(v, "hannes", (void **)&prev_e);
	v_testcase_int(tid, "vector_append", 0, ret);
	v_testcase_string(tid, "vector_append prev", "emil", prev_e);
	ret = vector_append(v, "lydia", (void **)&prev_e);
	v_testcase_int(tid, "vector_append", 0, ret);
	v_testcase_string(tid, "vector_append prev", "hannes", prev_e);
	ret = vector_append(v, "olle", (void **)&prev_e);
	v_testcase_int(tid, "vector_append", 0, ret);
	v_testcase_string(tid, "vector_append prev", "lydia", prev_e);
	ret = vector_append(v, "erik", (void **)&prev_e);
	v_testcase_int(tid, "vector_append", 0, ret);
	v_testcase_string(tid, "vector_append prev", "olle", prev_e);

	v_test("first round:\n");
	count = vector_count(v);
	for (i = 0; i < count; i++) {
		ret = vector_get(v, i, &e);
		v_testcase_int(tid, "vector_get", 0, ret);
		v_test("got element: %s\n", (char *)e);
	}

	ret = vector_delete(v, 1, &e);  //don't free e, statically allocated
	v_testcase_int(tid, "vector_delete", 0, ret);
	v_testcase_string(tid, "vector_delete", "hannes", (char *)e);
	ret = vector_delete(v, 3, &e);  //don't free e, statically allocated
	v_testcase_int(tid, "vector_delete", 0, ret);
	v_testcase_string(tid, "vector_delete", "erik", (char *)e);

	v_test("second round:\n");
	count = vector_count(v);
	for (i = 0; i < count; i++) {
		ret = vector_get(v, i, &e);
		v_testcase_int(tid, "vector_get", 0, ret);
		v_test("got element: %s\n", (char *)e);
	}

#if 0
	ret = vector_delete(v, 3, &e);
	v_testcase_int(tid, "vector_delete", -1, ret);
#endif
	ret = vector_delete(v, 2, &e);  //don't free e, statically allocated
	v_testcase_int(tid, "vector_delete", 0, ret);
	v_testcase_string(tid, "vector_delete", "olle", (char *)e);
	ret = vector_delete(v, 0, &e);  //don't free e, statically allocated
	v_testcase_int(tid, "vector_delete", 0, ret);
	v_testcase_string(tid, "vector_delete", "emil", (char *)e);
	ret = vector_delete(v, 0, &e);  //don't free e, statically allocated
	v_testcase_int(tid, "vector_delete", 0, ret);
	v_testcase_string(tid, "vector_delete", "lydia", (char *)e);

	v_testcase_uint64(tid, "vector_count", (uint64_t)0, (uint64_t)vector_count(v));
	ret64 = vector_insert(v, "erik", string_comparator);
	v_testcase_int(tid, "vector_insert", 0, ret);
	v_testcase_uint64(tid, "vector_insert idx", (uint64_t)0, ret64);
	ret64 = vector_insert(v, "lydia", string_comparator);
	v_testcase_int(tid, "vector_insert", 0, ret);
	v_testcase_uint64(tid, "vector_insert idx", (uint64_t)1, ret64);
	ret64 = vector_insert(v, "hannes", string_comparator);
	v_testcase_int(tid, "vector_insert", 0, ret);
	v_testcase_uint64(tid, "vector_insert idx", (uint64_t)1, ret64);
	ret64 = vector_insert(v, "olle", string_comparator);
	v_testcase_int(tid, "vector_insert", 0, ret);
	v_testcase_uint64(tid, "vector_insert idx", (uint64_t)3, ret64);
	ret64 = vector_insert(v, "emil", string_comparator);
	v_testcase_int(tid, "vector_insert", 0, ret);
	v_testcase_uint64(tid, "vector_insert idx", (uint64_t)0, ret64);
	ret64 = vector_insert(v, "erik", string_comparator);
	v_testcase_int(tid, "vector_insert", 0, ret);
	v_testcase_uint64(tid, "vector_insert idx", (uint64_t)2, ret64);

	vector_destroy(&v);

	return 0;
}
