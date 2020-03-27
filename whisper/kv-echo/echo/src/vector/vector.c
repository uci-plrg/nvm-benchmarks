/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Forked from: https://gist.github.com/953968
 */

#include <stdlib.h>
#include "vector.h"
#include "vector_macros.h"

#define VECTOR_INIT_SIZE 2
#define VECTOR_RESIZE_FACTOR 2

struct vector_ {
	void** data;         //array of void pointers
	unsigned long long size;   //number of array elements allocated
	unsigned long long count;  //number of elements in use
};

int vector_alloc(vector **v)
{
	*v = malloc(sizeof(vector));
	if (*v == NULL) {
		v_error("malloc(vector) failed\n");
		return -1;
	}

	(*v)->data = NULL;
	(*v)->size = 0;
	(*v)->count = 0;

	v_debug("successfully allocated new vector\n");
	return 0;
}

unsigned long long vector_count(vector *v)
{
	if (v == NULL) {
		v_error("v is NULL\n");
		return -1;  //vector_count isn't supposed to return an error, oh well
	}
	v_debug("returning count=%llu (size=%llu)\n", v->count, v->size);
	return v->count;
}

int vector_append(vector *v, void *e)
{
	if (v == NULL) {
		v_error("v is NULL\n");
		return -1;
	}
	//TODO: should check if vector has hit max size here!

	if (v->size == 0) {
		v->size = VECTOR_INIT_SIZE;
		v->data = malloc(sizeof(void*) * v->size);
		if (v->data == NULL) {
			v_error("malloc(array) failed\n");
			return -1;
		}
		v_debug("allocated new array of size %llu (%llu slots)\n",
				sizeof(void*) * v->size, v->size);
	}

	/* When the last array slot is exhausted, increase the size of the
	 * array by multiplying it by the resize factor.
	 * Realloc leaves the contents at the beginning of the array unchanged;
	 * the newly-allocated memory will be uninitialized.
	 */
	if (v->size == v->count) {
		v->size *= VECTOR_RESIZE_FACTOR;
		v->data = realloc(v->data, sizeof(void*) * v->size);
		if (v->data == NULL) {
			v_error("realloc(array) failed\n");
			return -1;
		}
		v_debug("re-allocated array, now has size %llu (%llu slots)\n",
				sizeof(void*) * v->size, v->size);
	}

	v->data[v->count] = e;
	v->count++;
	v_debug("stored new element %s in slot %llu (now count=%llu, size=%llu)\n",
			(char *)(v->data[(v->count)-1]), v->count-1, v->count, v->size);

	return 0;
}

int vector_set(vector *v, unsigned long long idx, void *e, void **old_e)
{
	if (v == NULL) {
		v_error("v is NULL\n");
		return -1;
	}
	if (idx >= v->count) {
		if (VECTOR_DIE_ON_OOB) {
			v_die("index %llu out-of-bounds, v->count=%llu\n", idx, v->count);
		}
		v_error("index %llu out-of-bounds, v->count=%llu\n", idx, v->count);
		return -1;
	}

	*old_e = v->data[idx];
	v->data[idx] = e;
	v_debug("stored element %s in slot %llu (count=%llu, size=%llu)\n",
			(char *)(v->data[idx]), idx, v->count, v->size);

	return 0;
}

int vector_get(vector *v, unsigned long long idx, void **e)
{
	if (v == NULL) {
		v_error("v is NULL\n");
		return -1;
	}
	if (idx >= v->count) {
		if (VECTOR_DIE_ON_OOB) {
			v_die("index %llu out-of-bounds, v->count=%llu\n", idx, v->count);
		}
		v_error("index %llu out-of-bounds, v->count=%llu\n", idx, v->count);
		return -1;
	}

	*e = v->data[idx];
	v_debug("got element %s from slot %llu (count=%llu, size=%llu)\n",
			(char *)(*e), idx, v->count, v->size);

	return 0;
}

int vector_delete(vector *v, unsigned long long idx, void **e)
{
	unsigned long long i;

	if (v == NULL) {
		v_error("v is NULL\n");
		return -1;
	}
	if (idx >= v->count) {
		if (VECTOR_DIE_ON_OOB) {
			v_die("index %llu out-of-bounds, v->count=%llu\n", idx, v->count);
		}
		v_error("index %llu out-of-bounds, v->count=%llu\n", idx, v->count);
		return -1;
	}

	/* Don't free the element to be deleted, but set *e to point to it
	 * so that the caller can free it. Then, shift all of the other
	 * elements in the array down one slot:
	 */
	v_debug("deleting element %s from slot %llu\n", (char *)v->data[idx], idx);
	if (e) {
		*e = v->data[idx];
	}
	for (i = idx; i < v->count - 1; i++) {
		v->data[i] = v->data[i+1];
		v_debug("shifted element %s from slot %llu down to slot %llu\n",
				(char *)(v->data[i]), i+1, i);
	}
	v->count--;
	v_debug("now count=%llu, size=%llu (resize factor=%u)\n", v->count, v->size,
			VECTOR_RESIZE_FACTOR);

	/* Shrink the array if the number of used slots falls below the number
	 * of allocated slots divided by the resize factor times 2. We double
	 * the resize factor when checking this condition, but only shrink the
	 * array by a single resize factor, to avoid "pathological" behavior
	 * where the vector reaches some size and then the client repeatedly
	 * adds one element and deletes one element, causing a resize on every
	 * operation (note: this analysis is not scientific nor empirical).
	 *
	 * In the condition below, <= causes resizing to happen a bit earlier
	 * and seems better than just <. With VECTOR_RESIZE_FACTOR = 2, this
	 * logic causes the array to be cut in half when the number of elements
	 * is decreased to 1/4 of the number of allocated slots.
	 */
	if ((v->size > VECTOR_INIT_SIZE) &&
	    (v->count <= v->size / (VECTOR_RESIZE_FACTOR * 2))) {
		v_debug("count %llu is <= %llu, shrinking array\n", v->count,
				v->size / (VECTOR_RESIZE_FACTOR * 2));
		v->size /= VECTOR_RESIZE_FACTOR;  //inverse of vector_append()
		v->data = realloc(v->data, sizeof(void*) * v->size);
		if (v->data == NULL) {
			v_die("realloc(array) failed\n");
		}
		v_debug("shrunk array, now has size %llu (%llu slots)\n",
				sizeof(void*) * v->size, v->size);
	}

	return 0;
}

void vector_free_contents(vector *v)
{
	unsigned long long i, count;

	if (v == NULL) {
		v_error("v is NULL\n");
		return;
	}

	count = v->count;
	for (i = 0; i < count; i++) {
		if (v->data[i]) {
			v_debug("freeing element %s from slot %llu\n",
					(char *)(v->data[i]), i);
			free(v->data[i]);
		} else {
			v_debug("NULL pointer in array, not freeing it\n");
		}
	}
	v_debug("successfully freed %llu elements from vector\n", count);
}

void vector_free(vector *v)
{
	if (v == NULL) {
		v_error("v is NULL\n");
		return;
	}

	free(v->data);
	free(v);
	v_debug("freed vector's array and vector struct itself\n");
}

unsigned int vector_struct_size()
{
	return sizeof(vector);
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
