/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Forked from: https://gist.github.com/953968
 */

#include <stdlib.h>
#include "vector64.h"
#include "vector_macros.h"

/* Factors that define when and how the vector is resized. The initial
 * size must be an integer, but the resize factor may be non-integral
 * (although I've only tested integer factors, namely 2.0).
 */
#define VECTOR64_INIT_SIZE 4
#define VECTOR64_RESIZE_FACTOR 2.0

struct vector64_ {
	/* Tip: use "%llu" in printf strings to print uint64_t types. */
	uint64_t *array; //array of unsigned 64-bit integers
	uint64_t size;   //number of array elements allocated
	uint64_t count;  //number of elements in use
};

int vector64_alloc(vector64 **v)
{
	*v = malloc(sizeof(vector64));
	if (*v == NULL) {
		v_error("malloc(vector64) failed\n");
		return -1;
	}

	/* NOTE: we don't pre-allocate the array here, so there will be some
	 * overhead on the first put. For evaluation purposes, that could
	 * disrupt the measurement of the cost of an alloc versus a put/append,
	 * but then again other puts/appends may require re-allocation of the
	 * array anyway, so this is probably no big deal.
	 */
	(*v)->array = NULL;
	(*v)->size = 0;
	(*v)->count = 0;

	v_debug("successfully allocated new vector64\n");
	return 0;
}

uint64_t vector64_count(vector64 *v)
{
	if (v == NULL) {
		v_error("v is NULL\n");
		return UINT64_MAX;
	}
	v_debug("returning count=%llu (size=%llu)\n", v->count, v->size);
	return v->count;
}

int vector64_append(vector64 *v, uint64_t e)
{
	if (v == NULL) {
		v_error("v is NULL\n");
		return -1;
	}
	if (v->count == UINT64_MAX-1) {
		v_error("v has hit max size (%llu)\n", UINT64_MAX-1);
		return -1;
	}

	if (v->size == 0) {
		v->size = VECTOR64_INIT_SIZE;
		v->array = malloc(sizeof(uint64_t) * v->size);
		if (v->array == NULL) {
			v_error("malloc(array) failed\n");
			return -1;
		}
		v_debug("allocated new array of size %llu (%llu slots)\n",
				sizeof(uint64_t) * v->size, v->size);
	}

	/* When the last array slot is exhausted, increase the size of the
	 * array by multiplying it by the resize factor.
	 * Realloc leaves the contents at the beginning of the array unchanged;
	 * the newly-allocated memory will be uninitialized.
	 */
	if (v->size == v->count) {
		v->size = (uint64_t)(v->size * VECTOR64_RESIZE_FACTOR);
		v->array = realloc(v->array, sizeof(uint64_t) * v->size);
		if (v->array == NULL) {
			v_error("realloc(array) failed\n");
			return -1;
		}
		v_debug("re-allocated array, now has size %llu (%llu slots)\n",
				sizeof(uint64_t) * v->size, v->size);
	}

	v->array[v->count] = e;
	v->count++;
	v_debug("stored new element %llu in slot %llu (now count=%llu, "
			"size=%llu)\n", v->array[(v->count)-1], v->count-1, v->count,
			v->size);

	return 0;
}

uint64_t vector64_set(vector64 *v, uint64_t idx, uint64_t e)
{
	uint64_t old_e;

	if (v == NULL) {
		v_error("v is NULL\n");
		return UINT64_MAX;
	}
	if (idx >= v->count) {
		if (VECTOR64_DIE_ON_OOB) {
			v_die("index %llu out-of-bounds, v->count=%llu\n", idx, v->count);
		}
		v_error("index %llu out-of-bounds, v->count=%llu\n", idx, v->count);
		return UINT64_MAX;
	}

	old_e = v->array[idx];
	v->array[idx] = e;
	v_debug("stored element %llu in slot %llu (count=%llu, size=%llu)\n",
			v->array[idx], idx, v->count, v->size);

	return old_e;
}

uint64_t vector64_get(vector64 *v, uint64_t idx)
{
	uint64_t e;

	if (v == NULL) {
		v_error("v is NULL\n");
		return UINT64_MAX;
	}
	if (idx >= v->count) {
		if (VECTOR64_DIE_ON_OOB) {
			v_die("index %llu out-of-bounds, v->count=%llu\n", idx, v->count);
		}
		v_error("index %llu out-of-bounds, v->count=%llu\n", idx, v->count);
		return UINT64_MAX;
	}

	e = v->array[idx];
	//v_debug("got element %llu from slot %llu (count=%llu, size=%llu)\n",
	//		e, idx, v->count, v->size);

	return e;
}

uint64_t vector64_delete(vector64 *v, uint64_t idx)
{
	uint64_t old_e;
	uint64_t i;

	if (v == NULL) {
		v_error("v is NULL\n");
		return UINT64_MAX;
	}
	if (idx >= v->count) {
		if (VECTOR64_DIE_ON_OOB) {
			v_die("index %llu out-of-bounds, v->count=%llu\n", idx, v->count);
		}
		v_error("index %llu out-of-bounds, v->count=%llu\n", idx, v->count);
		return UINT64_MAX;
	}

	/* Remember the deleted element, then shift all of the other elements
	 * in the array down one slot:
	 */
	old_e = v->array[idx];
	v_debug("deleting element %llu from slot %llu\n", old_e, idx);
	for (i = idx; i < v->count - 1; i++) {
		v->array[i] = v->array[i+1];
		v_debug("shifted element %llu from slot %llu down to slot %llu\n",
				v->array[i], i+1, i);
	}
	v->count--;
	v_debug("now count=%llu, size=%llu (resize factor=%f)\n", v->count,
			v->size, VECTOR64_RESIZE_FACTOR);

	/* Shrink the array if the number of used slots falls below the number
	 * of allocated slots divided by the resize factor times 2. We double
	 * the resize factor when checking this condition, but only shrink the
	 * array by a single resize factor, to avoid "pathological" behavior
	 * where the vector reaches some size and then the client repeatedly
	 * adds one element and deletes one element, causing a resize on every
	 * operation (note: this analysis is not scientific nor empirical).
	 *
	 * In the condition below, <= causes resizing to happen a bit earlier
	 * and seems better than just <. With VECTOR64_RESIZE_FACTOR = 2, this
	 * logic causes the array to be cut in half when the number of elements
	 * is decreased to 1/4 of the number of allocated slots (so the array
	 * will be half-filled after it is shrunk). Also, we allow
	 * the resize factor to be non-integral, so we cast the computation
	 * results back to uint64_t; there could presumably be rounding errors
	 * here, but during debugging such errors did not occur, and even if
	 * they do, it shouldn't be a big deal.
	 */
	if ((v->size > VECTOR64_INIT_SIZE) &&
	    (v->count <= (uint64_t)(v->size / (VECTOR64_RESIZE_FACTOR * 2)))) {
		v_debug("count %llu is <= %llu, shrinking array\n", v->count,
				(uint64_t)(v->size / (VECTOR64_RESIZE_FACTOR * 2)));
		v->size = (uint64_t)(v->size / VECTOR64_RESIZE_FACTOR);  //inverse of vector64_append()
		v->array = realloc(v->array, sizeof(uint64_t) * v->size);
		if (v->array == NULL) {
			v_die("realloc(array) failed\n");
		}
		v_debug("shrunk array, now has size %llu (%llu slots)\n",
				sizeof(uint64_t) * v->size, v->size);
	}

	return old_e;
}

int vector64_search_linear(vector64 *v, uint64_t key, uint64_t *idx)
{
	uint64_t i;

	if (v == NULL) {
		v_error("v is NULL\n");
		return -1;
	}

	for (i = 0; i < v->count; i++) {
		if (v->array[i] == key) {
			*idx = i;
			v_debug("found search key %llu at idx=%llu\n", key, *idx);
			return 0;
		}
	}

	v_debug("searched through all %llu elements, did not find search key "
			"%llu\n", v->count, key);
	return 1;
}

void vector64_free(vector64 *v)
{
	if (v == NULL) {
		v_error("v is NULL\n");
		return;
	}

	free(v->array);
	free(v);
	v_debug("freed vector64's array and vector64 struct itself\n");
}

unsigned int vector64_struct_size()
{
	return sizeof(vector64);
}

#define VECTOR64_MAX_STRLEN 2048
char *vector64_to_string(vector64 *v)
{
	int len;
	uint64_t i;
	char *bigstring, *littlestring;
	
	bigstring = malloc(VECTOR64_MAX_STRLEN);
	if (bigstring == NULL) {
		return NULL;
	}
	littlestring = malloc(VECTOR64_MAX_STRLEN);
	if (littlestring == NULL) {
		return NULL;
	}

	bigstring[0]='\0';
	len = 0;
	for (i = 0; i < v->count && len < VECTOR64_MAX_STRLEN-1; i++) {
		/* hopefully vector doesn't change during loop... */
		snprintf(littlestring, VECTOR64_MAX_STRLEN, "[%llu:%llu]", i,
				v->array[i]);
		strncat(bigstring, littlestring, VECTOR64_MAX_STRLEN - len - 1);
		len = strlen(bigstring);
	}
	
	free(littlestring);
	return bigstring;
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
