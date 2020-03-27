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

void stress_test()
{
	int i, j, ret;
	unsigned long long idx, count;
	unsigned long tid;
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
	tid = pthread_self();
	size = ELT_SIZE_MIN;
	ret = vector_alloc(&v);

	for (i = 0; i < LOOPS; i++) {
		/* First, do some appends: */
		rand_int = random();
		rand_int = (rand_int % (APPENDS_MAX - APPENDS_MIN)) + APPENDS_MIN;
		v_debug("appending %ld elements of size %u to vector:\n",
				rand_int, size);
		for (j = rand_int; j > 0; j--) {
			e = malloc(size);
			((char *)e)[0] = 'A' + ((i+rand_int-j)%26);
			((char *)e)[1] = '\0';
			ret = vector_append(v, e);
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
	vector_free(v);
}

int main(int argc, char *argv[])
{
	int i, ret;
	unsigned long long count;
	unsigned long tid;
	vector *v;
	void *e;

	stress_test();
	v_print("stress test complete\n");

	tid = pthread_self();
	v_print("sizeof(void *)=%u, sizeof(unsigned int)=%u, "
			"sizeof(unsigned long int)=%u, sizeof(unsigned long)=%u, "
			"sizeof(unsigned long long)=%u\n",
			sizeof(void *), sizeof(unsigned int), sizeof(unsigned long int),
			sizeof(unsigned long), sizeof(unsigned long long));
	v_print("value of null-zero = %p\n", (void *)'\0');

	ret = vector_alloc(&v);
	v_testcase_int(tid, "vector_alloc", 0, ret);

	/* TODO: how/where are these strings allocated? Only have local scope
	 * (this main() function), right?
	 */
	ret = vector_append(v, "emil");
	v_testcase_int(tid, "vector_append", 0, ret);
	ret = vector_append(v, "hannes");
	v_testcase_int(tid, "vector_append", 0, ret);
	ret = vector_append(v, "lydia");
	v_testcase_int(tid, "vector_append", 0, ret);
	ret = vector_append(v, "olle");
	v_testcase_int(tid, "vector_append", 0, ret);
	ret = vector_append(v, "erik");
	v_testcase_int(tid, "vector_append", 0, ret);

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

	ret = vector_delete(v, 3, &e);
	v_testcase_int(tid, "vector_delete", -1, ret);
	ret = vector_delete(v, 2, &e);  //don't free e, statically allocated
	v_testcase_int(tid, "vector_delete", 0, ret);
	v_testcase_string(tid, "vector_delete", "olle", (char *)e);
	ret = vector_delete(v, 0, &e);  //don't free e, statically allocated
	v_testcase_int(tid, "vector_delete", 0, ret);
	v_testcase_string(tid, "vector_delete", "emil", (char *)e);
	ret = vector_delete(v, 0, &e);  //don't free e, statically allocated
	v_testcase_int(tid, "vector_delete", 0, ret);
	v_testcase_string(tid, "vector_delete", "lydia", (char *)e);

	vector_free(v);

	return 0;
}
