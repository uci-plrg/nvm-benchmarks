/* Peter Hornyack and Katelin Bailey
 * 12/8/11
 * University of Washington
 */

#include <stdlib.h>
#include <time.h>
#include "vector64.h"
#include "vector_macros.h"

#define RUN_STRESS_TESTS
#define RUN_BASIC_TESTS
//#define TEST_OOB
bool use_nvm = true;
bool keep_sorted = true;  //false -> appends, true -> inserts

#define APPENDS_MIN 2
#define APPENDS_MAX 8
#define DELETES_MIN 4
#define DELETES_MAX 5
#define ELT_MIN 0
#define ELT_MAX 1000000
#define LOOPS 1000

void stress_test64(bool keep_sorted, bool use_nvm)
{
	int ret;
	unsigned long ret_ul;
	unsigned long i, j;
	unsigned long idx, count;
	unsigned long tid;
	vector64 *v;
	unsigned long e;
	long int rand_int;
	char *v64_string;

	if (APPENDS_MAX <= APPENDS_MIN) {
		v_die("invalid APPENDS_MAX %u and APPENDS_MIN %u\n", APPENDS_MAX,
				APPENDS_MIN);
	}
	if (DELETES_MAX <= DELETES_MIN) {
		v_die("invalid DELETES_MAX %u and DELETES_MIN %u\n", DELETES_MAX,
				DELETES_MIN);
	}
	if (ELT_MAX <= ELT_MIN) {
		v_die("invalid ELT_MAX %u and ELT_MIN %u\n", ELT_MAX, ELT_MIN);
	}

	tid = pthread_self();
	ret = vector64_create(&v, 1, use_nvm);
	v_testcase_int(tid, "vector64_create", 0, ret);

	for (i = 0; i < LOOPS; i++) {
		/* First, do some appends or insertions: */
		rand_int = random();
		rand_int = (rand_int % (APPENDS_MAX - APPENDS_MIN)) + APPENDS_MIN;
		v_debug("adding %ld elements to vector64:\n", rand_int);
		for (j = rand_int; j > 0; j--) {
			e = (unsigned long)random();
			e = (e % (ELT_MAX - ELT_MIN)) + ELT_MIN;
			if (keep_sorted) {
				ret_ul = vector64_insert(v, e, false);  //no duplicates
			} else {
				ret_ul = vector64_append(v, e);
			}
		}
		v_debug("appended %ld elements, now count=%lu\n", rand_int,
				vector64_count(v));

		/* Then, do some deletes: */
		rand_int = random();
		rand_int = (rand_int % (DELETES_MAX - DELETES_MIN)) + DELETES_MIN;
		v_debug("deleting up to %ld values from vector64\n", rand_int);
		for (j = rand_int; j > 0; j--) {
			count = vector64_count(v);
			if (count == 0) {
				break;
			}
			idx = (unsigned long)random() % count;
			e = vector64_delete(v, idx);
			//v_debug("deleted element %lu at idx=%lu\n", e, idx);
		}
		v_debug("deleted %lu elements, now count=%lu\n", rand_int - j,
				vector64_count(v));
	}

	/* Print final contents of vector64: */
	count = vector64_count(v);
	v_print("final vector64 count=%lu\n", count);
	v_print("final vector64 contents: ");
	for (i = 0; i < count; i++) {
		e = vector64_get(v, i);
		printf("%lu ", e);
		if (keep_sorted && j > e && i > 0) {
			v_die("keep_sorted true, but got e=%lu at idx=%lu while "
					"previous element was j=%lu\n", e, i, j);
		}
		j = e;
	}
	printf("\n");
	v64_string = vector64_to_string(v);
	printf("to_string string: %s\n\n", v64_string);
	if (v64_string)
		free(v64_string);

	/* Free vector64: */
	vector64_destroy(&v);
}

int main(int argc, char *argv[])
{
	int ret;
	unsigned long i, count, ret64;
	unsigned long tid;
	vector64 *v;

#ifdef RUN_STRESS_TESTS
	srandom((unsigned int)time(NULL));
	stress_test64(keep_sorted, use_nvm);
	v_print("stress test (keep_sorted = %s, use_nvm=%s) complete\n",
			keep_sorted ? "true" : "false",
			use_nvm ? "true" : "false");
#endif

#ifdef RUN_BASIC_TESTS
	tid = pthread_self();
	//v_print("sizeof(void *)=%zu, sizeof(unsigned int)=%zu, "
	//		"sizeof(unsigned long)=%zu\n", sizeof(void *),
	//		sizeof(unsigned int), sizeof(unsigned long));

	ret = vector64_create(&v, 0, use_nvm);
	v_testcase_int(tid, "vector64_create", 0, ret);

#ifdef TEST_OOB
	ret64 = vector64_get(v, 0);  //error case
	v_testcase_ul(tid, "vector64_get empty", VECTOR64_MAX, ret64);
#endif

	/* keep_sorted: when keep_sorted is true, we use vector64_insert
	 * operations, which should keep the array sorted automatically.
	 * When keep_sorted is false, we use vector64_append operations,
	 * but we perform them IN-ORDER to keep the vector sorted manually.
	 * This allows us to use the same test cases and operations in
	 * the rest of the test file, and we only have to flip a compile-time
	 * switch to test both.
	 */
	if (keep_sorted) {
		ret64 = vector64_insert(v, 678, false);
		v_testcase_ul(tid, "vector64_insert", 0, ret64);
		ret64 = vector64_insert(v, 9012, false);
		v_testcase_ul(tid, "vector64_insert", 1, ret64);
		ret64 = vector64_insert(v, 0, false);
		v_testcase_ul(tid, "vector64_insert", 0, ret64);
		ret64 = vector64_insert(v, 12, false);
		v_testcase_ul(tid, "vector64_insert", 1, ret64);
		ret64 = vector64_insert(v, 345, false);
		v_testcase_ul(tid, "vector64_insert", 2, ret64);
		ret64 = vector64_insert(v, 678, false);
		v_testcase_ul(tid, "vector64_insert", 3, ret64);
		ret64 = vector64_count(v);
		v_testcase_ul(tid, "vector64_count", 5, ret64);
		ret64 = vector64_insert(v, 678, true);
		v_testcase_ul(tid, "vector64_insert", 4, ret64);
		ret64 = vector64_count(v);
		v_testcase_ul(tid, "vector64_count", 6, ret64);
		ret64 = vector64_get(v, 3);
		v_testcase_ul(tid, "vector64_get", 678, ret64);
		ret64 = vector64_get(v, 4);
		v_testcase_ul(tid, "vector64_get", 678, ret64);
		ret64 = vector64_get(v, 5);
		v_testcase_ul(tid, "vector64_get", 9012, ret64);
		ret64 = vector64_delete(v, 3);
		v_testcase_ul(tid, "vector64_delete", 678, ret64);
	} else {
		ret64 = vector64_append(v, 0);
		v_testcase_ul(tid, "vector64_append", 1, ret64);
		ret64 = vector64_append(v, 12);
		v_testcase_ul(tid, "vector64_append", 2, ret64);
		ret64 = vector64_append(v, 345);
		v_testcase_ul(tid, "vector64_append", 3, ret64);
		ret64 = vector64_append(v, 678);
		v_testcase_ul(tid, "vector64_append", 4, ret64);
		ret64 = vector64_append(v, 9012);
		v_testcase_ul(tid, "vector64_append", 5, ret64);
	}
	
	ret64 = vector64_search_linear(v, 0);
	v_testcase_ul(tid, "vector64_search_linear 0", 0, ret64);
	ret64 = vector64_search_linear(v, 1);
	v_testcase_ul(tid, "vector64_search_linear 1", VECTOR64_MAX, ret64);
	ret64 = vector64_search_linear(v, 9012);
	v_testcase_ul(tid, "vector64_search_linear 9012", 4, ret64);
	ret64 = vector64_search_linear(v, 345);
	v_testcase_ul(tid, "vector64_search_linear 345", 2, ret64);

	ret64 = vector64_search_binary(v, 0, true);
	v_testcase_ul(tid, "vector64_search_binary exact 0", 0, ret64);
	ret64 = vector64_search_binary(v, 1, true);
	v_testcase_ul(tid, "vector64_search_binary exact 1", VECTOR64_MAX, ret64);
	ret64 = vector64_search_binary(v, 1, false);
	v_testcase_ul(tid, "vector64_search_binary non-exact 1", 0, ret64);
	ret64 = vector64_search_binary(v, 11, false);
	v_testcase_ul(tid, "vector64_search_binary non-exact 11", 0, ret64);
	ret64 = vector64_search_binary(v, 12, false);
	v_testcase_ul(tid, "vector64_search_binary non-exact 12", 1, ret64);
	ret64 = vector64_search_binary(v, 12, true);
	v_testcase_ul(tid, "vector64_search_binary exact 12", 1, ret64);
	ret64 = vector64_search_binary(v, 200, false);
	v_testcase_ul(tid, "vector64_search_binary non-exact 200", 1, ret64);
	ret64 = vector64_search_binary(v, 9012, true);
	v_testcase_ul(tid, "vector64_search_binary exact 9012", 4, ret64);
	ret64 = vector64_search_binary(v, 345, true);
	v_testcase_ul(tid, "vector64_search_binary exact 345", 2, ret64);
	ret64 = vector64_search_binary(v, 600, false);
	v_testcase_ul(tid, "vector64_search_binary non-exact 600", 2, ret64);
	ret64 = vector64_search_binary(v, 700, false);
	v_testcase_ul(tid, "vector64_search_binary non-exact 700", 3, ret64);
	ret64 = vector64_search_binary(v, 9999, false);
	v_testcase_ul(tid, "vector64_search_binary non-exact 9999", 4, ret64);

	v_test("first round:\n");
	count = vector64_count(v);
	for (i = 0; i < count; i++) {
		ret64 = vector64_get(v, i);
		v_testcase_ul_not(tid, "vector64_get", VECTOR64_MAX, ret64);
		v_test("got element %lu from idx %lu\n", ret64, i);
	}
#ifdef TEST_OOB
	ret64 = vector64_get(v, 10);  //error case
	v_testcase_ul(tid, "vector64_get out-of-bounds", VECTOR64_MAX, ret64);
#endif

	ret64 = vector64_delete(v, 1);
	v_testcase_ul(tid, "vector64_delete", 12, ret64);
	ret64 = vector64_delete(v, 3);
	v_testcase_ul(tid, "vector64_delete", 9012, ret64);

	v_test("second round:\n");
	count = vector64_count(v);
	for (i = 0; i < count; i++) {
		ret64 = vector64_get(v, i);
		v_testcase_ul_not(tid, "vector64_get", VECTOR64_MAX, ret64);
		v_test("got element %lu from idx %lu\n", ret64, i);
	}

	ret64 = vector64_search_linear(v, 0);
	v_testcase_ul(tid, "vector64_search_linear 0", 0, ret64);
	ret64 = vector64_search_linear(v, 9012);
	v_testcase_ul(tid, "vector64_search_linear 9012", VECTOR64_MAX, ret64);
	ret64 = vector64_search_linear(v, 345);
	v_testcase_ul(tid, "vector64_search_linear 345", 1, ret64);

	ret64 = vector64_search_binary(v, 0, true);
	v_testcase_ul(tid, "vector64_search_binary exact 0", 0, ret64);
	ret64 = vector64_search_binary(v, 9012, true);
	v_testcase_ul(tid, "vector64_search_binary exact 9012", VECTOR64_MAX, ret64);
	ret64 = vector64_search_binary(v, 9012, false);
	v_testcase_ul(tid, "vector64_search_binary not-exact 9012", 2, ret64);
	ret64 = vector64_search_binary(v, 345, true);
	v_testcase_ul(tid, "vector64_search_binary exact 345", 1, ret64);

#ifdef TEST_OOB
	ret64 = vector64_delete(v, 3);  //error case
	v_testcase_ul(tid, "vector64_delete out-of-bounds", VECTOR64_MAX, ret64);
#endif
	ret64 = vector64_delete(v, 2);
	v_testcase_ul(tid, "vector64_delete", 678, ret64);
	ret64 = vector64_delete(v, 0);
	v_testcase_ul(tid, "vector64_delete", 0, ret64);

	/* Important: test binary search with vector length = 1 - this has
	 * caused problems in the past!
	 */
	ret64 = vector64_search_binary(v, 123, true);
	v_testcase_ul(tid, "vector64_search_binary exact 123", VECTOR64_MAX, ret64);
	ret64 = vector64_search_binary(v, 123, false);
	v_testcase_ul(tid, "vector64_search_binary non-exact 123", VECTOR64_MAX, ret64);
	ret64 = vector64_search_binary(v, 345, false);
	v_testcase_ul(tid, "vector64_search_binary non-exact 345", 0, ret64);
	ret64 = vector64_search_binary(v, 345, true);
	v_testcase_ul(tid, "vector64_search_binary exact 345", 0, ret64);
	ret64 = vector64_search_binary(v, 1234, true);
	v_testcase_ul(tid, "vector64_search_binary exact 1234", VECTOR64_MAX, ret64);
	ret64 = vector64_search_binary(v, 1234, false);
	v_testcase_ul(tid, "vector64_search_binary non-exact 1234", 0, ret64);
	ret64 = vector64_delete(v, 0);
	v_testcase_ul(tid, "vector64_delete", 345, ret64);
#ifdef TEST_OOB
	ret64 = vector64_delete(v, 0);  //error case
	v_testcase_ul(tid, "vector64_delete empty", VECTOR64_MAX, ret64);
#endif

	ret64 = vector64_search_linear(v, 345);
	v_testcase_ul(tid, "vector64_search_linear 345", VECTOR64_MAX, ret64);

	vector64_destroy(&v);
#endif

	return 0;
}
