/* Peter Hornyack and Katelin Bailey
 * 12/8/11
 * University of Washington
 */

#include <stdlib.h>
#include <time.h>
#include "vector64.h"
#include "vector_macros.h"

#define APPENDS_MIN 2
#define APPENDS_MAX 8
#define DELETES_MIN 4
#define DELETES_MAX 5
#define ELT_MIN 0
#define ELT_MAX 1000000
#define LOOPS 1000

void stress_test64()
{
	int ret;
	uint64_t i, j;
	uint64_t idx, count;
	unsigned long tid;
	vector64 *v;
	uint64_t e;
	long int rand_int;

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

	srandom((unsigned int)time(NULL));
	tid = pthread_self();
	ret = vector64_alloc(&v);
	v_testcase_int(tid, "vector64_alloc", 0, ret);

	for (i = 0; i < LOOPS; i++) {
		/* First, do some appends: */
		rand_int = random();
		rand_int = (rand_int % (APPENDS_MAX - APPENDS_MIN)) + APPENDS_MIN;
		v_debug("appending %ld elements to vector64:\n", rand_int);
		for (j = rand_int; j > 0; j--) {
			e = (uint64_t)random();
			e = (e % (ELT_MAX - ELT_MIN)) + ELT_MIN;
			ret = vector64_append(v, e);
			//v_testcase_int(tid, "vector64_append", 0, ret);
		}
		v_debug("appended %ld elements, now count=%llu\n", rand_int,
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
			idx = (uint64_t)random() % count;
			e = vector64_delete(v, idx);
			v_debug("deleted element %llu at idx=%llu\n", e, idx);
		}
		v_debug("deleted %llu elements, now count=%llu\n", rand_int - j,
				vector64_count(v));
	}

	/* Print final contents of vector64: */
	count = vector64_count(v);
	v_print("final vector64 count=%llu\n", count);
	v_print("final vector64 contents: ");
	for (i = 0; i < count; i++) {
		e = vector64_get(v, i);
		printf("%llu ", e);
	}
	printf("\n");

	/* Free vector64: */
	vector64_free(v);
}

int main(int argc, char *argv[])
{
	int ret;
	uint64_t i, count, ret64;
	unsigned long tid;
	vector64 *v;

	stress_test64();
	v_print("stress test complete\n");

	tid = pthread_self();
	v_print("sizeof(void *)=%u, sizeof(unsigned int)=%u, sizeof(uint64_t)=%u\n",
			sizeof(void *), sizeof(unsigned int), sizeof(uint64_t));

	ret = vector64_alloc(&v);
	v_testcase_int(tid, "vector64_alloc", 0, ret);

	ret64 = vector64_get(v, 0);  //error case
	v_testcase_uint64(tid, "vector64_get empty", UINT64_MAX, ret64);

	ret = vector64_append(v, 0);
	v_testcase_int(tid, "vector64_append", 0, ret);
	ret = vector64_append(v, 12);
	v_testcase_int(tid, "vector64_append", 0, ret);
	ret = vector64_append(v, 345);
	v_testcase_int(tid, "vector64_append", 0, ret);
	ret = vector64_append(v, 678);
	v_testcase_int(tid, "vector64_append", 0, ret);
	ret = vector64_append(v, 9012);
	v_testcase_int(tid, "vector64_append", 0, ret);
	
	ret = vector64_search_linear(v, 0, &ret64);
	v_testcase_int(tid, "vector64_search_linear 0", 0, ret);
	v_testcase_uint64(tid, "vector64_search_linear 0", (uint64_t)0, ret64);
	ret = vector64_search_linear(v, 1, &ret64);
	v_testcase_int(tid, "vector64_search_linear 1", 1, ret);
	ret = vector64_search_linear(v, 9012, &ret64);
	v_testcase_int(tid, "vector64_search_linear 9012", 0, ret);
	v_testcase_uint64(tid, "vector64_search_linear 9012", (uint64_t)4, ret64);
	ret = vector64_search_linear(v, 345, &ret64);
	v_testcase_int(tid, "vector64_search_linear 345", 0, ret);
	v_testcase_uint64(tid, "vector64_search_linear 345", (uint64_t)2, ret64);

	v_test("first round:\n");
	count = vector64_count(v);
	for (i = 0; i < count; i++) {
		ret64 = vector64_get(v, i);
		v_testcase_uint64_not(tid, "vector64_get", UINT64_MAX, ret64);
		v_test("got element %llu from idx %llu\n", ret64, i);
	}
	ret64 = vector64_get(v, 10);  //error case
	v_testcase_uint64(tid, "vector64_get out-of-bounds", UINT64_MAX, ret64);

	ret64 = vector64_delete(v, 1);
	v_testcase_uint64(tid, "vector64_delete", (uint64_t)12, ret64);
	ret64 = vector64_delete(v, 3);
	v_testcase_uint64(tid, "vector64_delete", (uint64_t)9012, ret64);

	v_test("second round:\n");
	count = vector64_count(v);
	for (i = 0; i < count; i++) {
		ret64 = vector64_get(v, i);
		v_testcase_uint64_not(tid, "vector64_get", UINT64_MAX, ret64);
		v_test("got element %llu from idx %llu\n", ret64, i);
	}

	ret = vector64_search_linear(v, 0, &ret64);
	v_testcase_int(tid, "vector64_search_linear 0", 0, ret);
	v_testcase_uint64(tid, "vector64_search_linear 0", (uint64_t)0, ret64);
	ret = vector64_search_linear(v, 9012, &ret64);
	v_testcase_int(tid, "vector64_search_linear 9012", 1, ret);
	ret = vector64_search_linear(v, 345, &ret64);
	v_testcase_int(tid, "vector64_search_linear 345", 0, ret);
	v_testcase_uint64(tid, "vector64_search_linear 345", (uint64_t)1, ret64);

	ret64 = vector64_delete(v, 3);  //error case
	v_testcase_uint64(tid, "vector64_delete out-of-bounds", UINT64_MAX, ret64);
	ret64 = vector64_delete(v, 2);
	v_testcase_uint64(tid, "vector64_delete", (uint64_t)678, ret64);
	ret64 = vector64_delete(v, 0);
	v_testcase_uint64(tid, "vector64_delete", (uint64_t)0, ret64);
	ret64 = vector64_delete(v, 0);
	v_testcase_uint64(tid, "vector64_delete", (uint64_t)345, ret64);
	ret64 = vector64_delete(v, 0);  //error case
	v_testcase_uint64(tid, "vector64_delete empty", UINT64_MAX, ret64);

	ret = vector64_search_linear(v, 345, &ret64);
	v_testcase_int(tid, "vector64_search_linear 345", 1, ret);

	vector64_free(v);

	return 0;
}
