/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Forked from: https://gist.github.com/953968
 */

#include <stdlib.h>
#include "vector.h"
#include "vector_macros.h"
#include "../kp_common.h"
#include "../kp_recovery.h"
#include "../kp_macros.h"
  //hacky...

#define VECTOR_INIT_SIZE 8
#define VECTOR_RESIZE_FACTOR 2
#define VECTOR_MAX_COUNT ((unsigned long long)(0-2))
//#define VECTOR_RESIZE_PRINT

/* On systems lab machines, unsigned long long is 8 bytes (64 bits).
 * This structure MUST keep its size member adjacent to the data
 * member: on a vector resize, after the realloc() we need to flush
 * BOTH of these values, and if they are kept adjacent then this
 * maximizes the likelihood that they are found in the same cache line
 * and hence only require one flush. The vector_append() code currently
 * depends on the following order of members in the struct:
 *   data
 *   size
 *   count
 * Changing this order without changing the vector_append() code will break
 * things.
 */
struct vector_ {
	void** data;               //array of void pointers
	unsigned long long size;   //number of array elements allocated
	unsigned long long count;  //number of elements in use
	bool use_nvm;
	ds_state state;
};

int vector_create(vector **v, unsigned long long size, bool use_nvm)
{
	unsigned long long init_size;

	v_debug("creating new vector with use_nvm=%s\n",
			use_nvm ? "true" : "false");

	/* Use calloc if use_nvm is true, otherwise just malloc: */
	kp_kpalloc((void **)v, sizeof(vector), use_nvm);
	if (*v == NULL) {
		v_error("kp_kpalloc(vector) failed\n");
		return -1;
	}

	init_size = size ? size : VECTOR_INIT_SIZE;
	v_debug("using init_size=%llu for vector\n", init_size);

	kp_kpalloc((void **)&((*v)->data), sizeof(void *) * init_size,
			use_nvm);
	if ((*v)->data == NULL) {
		v_error("kp_kpalloc(data) failed\n");
		vector_destroy(v);
#ifdef VECTOR_ASSERT
		if (*v != NULL) {
			v_die("vector_destroy() did not set *v to NULL as expected!\n");
		}
#endif
		return -1;
	}
	PM_EQU(((*v)->size), (init_size)); // persistent
//	printf("init_size=%d, sizeof(vector)=%d\n", init_size, sizeof(vector));
	PM_EQU(((*v)->count), (0));	// persistent
	PM_EQU(((*v)->use_nvm), (use_nvm));	// persistent
	
	/* "CDDS": flush, set state, and flush again. */
	kp_flush_range((void *)*v, sizeof(vector) - sizeof(ds_state), use_nvm);
	PM_EQU(((*v)->state), (STATE_ACTIVE));
	kp_flush_range((void *)&((*v)->state), sizeof(ds_state), use_nvm);

	v_debug("successfully created new vector\n");
	return 0;
}

unsigned long long vector_count(const vector *v)
{
	if (v == NULL) {
		v_error("v is NULL\n");
		return -1;  //vector_count isn't supposed to return an error, oh well
	}
	v_debug("returning count=%llu (size=%llu)\n", v->count, v->size);
	return v->count;
}

/* Ideally, vector_append would be consistent, durable, and _atomic_, which
 * would mean that it doesn't have to be _recovered_ after a failure. This
 * is probably possible by following the instructions in "A Lock-Free
 * Dynamically Resizable Array." However, this would require a significant
 * restructuring of the vector code that we already have, so we won't do
 * this for now. Instead, when the vector needs resizing, we focus on making
 * it _recoverable_, rather than atomic; when the vector doesn't need resizing,
 * then append is consistent and durable and _repeatable_, rather than atomic.
 *
 * Note that vector_append is NOT thread-safe or "lock-free" - high-level
 * synchronization is needed so that only one append occurs at a time!
 *
 * Relevant links:
 *   http://www2.research.att.com/~bs/lock-free-vector.pdf
 *   https://parasol.tamu.edu/~peterp/slides/opodis06.pdf
 *   Intel patent, 8006064: "Lock-free vector utilizing a resource allocator...":
 *     http://www.google.com/patents/US8006064?printsec=abstract#v=onepage&q&f=false
 *   http://www.ibm.com/developerworks/aix/library/au-intelthreadbuilding/index.html?ca=drs-
 *   http://software.intel.com/en-us/blogs/2009/04/09/delusion-of-tbbconcurrent_vectors-size-or-3-ways-to-traverse-in-parallel-correctly/
 *
 * This function allows NULL pointers for the element put into the array,
 * for now. 
 *
 * NOTE: for version 3.1 (simultaneous merges with conflict detection), I
 * hacked this interface to return the element that used to be the last
 * element in the vector, before we appended this one. Hmmm...
 */
int vector_append(vector *v, const void *e, void **previous_tail)
{
	unsigned long long orig_size, new_size;
	int flush_size = 0;
	/* freud : Everything here is persistent */
	/* HEADS UP: this function is mostly the same as vector_insert(), so
	 * if you update one, you should probably update the other! */

	if (v == NULL) {
		v_error("v is NULL\n");
		return -1;
	}
	if (v->count == VECTOR_MAX_COUNT) {
		v_error("hit maximum vector length: v->count=%llu\n", v->count);
		return -1;
	}
#ifndef UNSAFE_COMMIT
//#ifdef VECTOR_ASSERT
#if 0
	/* This assert only makes sense if we're not garbage collecting or
	 * otherwise removing things from vectors. Note that the resizing
	 * code doesn't contain any explicit checks for this (it will
	 * happily reduce the size of the vector below the initial size
	 * if you remove enough things); maybe this behavior should change,
	 * but whatever. */
	if (v->size < VECTOR_INIT_SIZE) {
		v_die("vector size %llu is less than initial size %d!!\n",
				v->size, VECTOR_INIT_SIZE);
	}
#endif
#endif

	/* When the last array slot is exhausted, increase the size of the
	 * array by multiplying it by the resize factor.
	 * Resizing the array consists of two steps: A) re-allocing the memory
	 * region, and B) setting the new size of the vector. A) is repeatable,
	 * but unfortunately could result in a memory leak if we don't correctly
	 * remember the pointer AND remember the new size! To minimize the leak
	 * window here, we immediately flush both the pointer and the size (which
	 * should be adjacent to each other in the struct!) after the realloc.
	 * We calculate the size of the flush that we need to perform by using
	 * the offsetof operator, because the compiler may insert padding between
	 * members that we don't know about. This code assumes that the order of
	 * the members in the struct is [data, size, count].
	 *
	 * ...
	 */
	if (v->size == v->count) {
#ifdef VECTOR_RESIZE_PRINT
		v_print("v->size hit v->count = %llu; append resizing!!!\n",
				v->size);
#endif
		v_debug("v->size hit v->count = %llu; resizing!!!\n", v->size);
		/* Check if multiplying v->size by VECTOR_RESIZE_FACTOR will put
		 * it over the VECTOR_MAX_COUNT:
		 */
		if (v->size > (VECTOR_MAX_COUNT / VECTOR_RESIZE_FACTOR)) {
			v_debug("vector size (%llu) times resize factor (%d) would "
					"overflow max count (%llu), so setting size directly "
					"to max count\n", v->size, VECTOR_RESIZE_FACTOR,
					VECTOR_MAX_COUNT);
			new_size = VECTOR_MAX_COUNT;
		} else {
			new_size = v->size * VECTOR_RESIZE_FACTOR;
		}
#ifdef VECTOR_RESIZE_PRINT
		v_print("calculated new_size=%llu (append)\n", new_size);
#endif
		orig_size = v->size;
#ifdef UNSAFE_COMMIT
		if (new_size > UNSAFE_COMMIT_LOG_SIZE) {
			v_print("WARNING: new vector size is greater than %d, probably "
					"means that we're resizing the commit_log vector!!\n",
					UNSAFE_COMMIT_LOG_SIZE);
		}
#endif
#ifdef V_ASSERT
		if (new_size > 100) {
			v_debug("WARNING: resizing vector to new_size=%llu\n", new_size);
		}
#endif

		/* We expect the flush_size to be 12, but the compiler could possibly
		 * insert padding that changes this. On brief examination, no padding
		 * is inserted and both the data pointer and the size are flushed in
		 * a single flush.
		 * todo: could put the following code segment in its own "pcm_realloc()"
		 * function...
		 */
		if (v->use_nvm) {
			/* We only need to flush data and size; count comes _after_ size,
			 * so use it as the end of the range to flush. */
			flush_size = offsetof(vector, count) - offsetof(vector, data);
#ifdef VECTOR_ASSERT
			if (flush_size < (sizeof(void **) + sizeof(unsigned long long))) {
				v_die("got unexpected flush_size %d! offsetof(count)=%zu, "
						"offsetof(data)=%zu\n", flush_size,
						offsetof(vector, count), offsetof(vector, data));
			}
			//v_print("calculated flush_size %d from offsetof(count)=%u, "
			//		"offsetof(data)=%d\n", flush_size,
			//		offsetof(vector, count), offsetof(vector, data));
#endif
		}

		/* Leak window begin. Note that kp_realloc() doesn't flush internally,
		 * because we want to flush both the data and the size in the same
		 * cache line to get consistency
		 *   Also note that we don't currently GUARANTEE this, if the compiler
		 *   happens to allocate v->data and v->size in two different cache
		 *   lines. */
		kp_realloc((void **)&(v->data), sizeof(void*) * new_size, sizeof(void*) * v->size, v->use_nvm);
		PM_EQU((v->size), (new_size));
		kp_flush_range(&(v->data), flush_size, v->use_nvm);  //flush both data and size!
		/* Leak window end. If we fail after the flush() has returned,
		 * then the next call to vector_append() will skip the resizing
		 * step.
		 */
		if (v->data == NULL) {
			v_error("kp_realloc(array) failed\n");
			/* Best effort on failure: reset size to what it was before,
			 * and let caller handle the rest.
			 */
			PM_EQU((v->size), (orig_size));
			return -1;
		}

		v_debug("re-allocated array, now has size %llu (%llu slots)\n",
				sizeof(void*) * v->size, v->size);
	}

	/* The actual append part of vector_append() is repeatable: we first
	 * fill in the element in the data array, then increment the count.
	 * If we fail in-between these two steps, then vector_append() can
	 * just be called again and we'll overwrite the memory area with the
	 * same value. We do, however, have to flush the written element before
	 * incrementing the count: we don't want the incremented count to hit
	 * memory before the new element does.
	 * ACTUALLY, this makes the vector_append ATOMIC, not repeatable (right?).
	 * After a failure, if the caller didn't get a return value from this
	 * function, then it can't be certain whether or not the append succeeded,
	 * and so it should probably do a vector_get() to check if the append
	 * happened or not.
	 */
	if (previous_tail) {  //super hacky
		if (v->count > 0) {
			*previous_tail = v->data[v->count - 1];
			/* printf("3\n"); */
		} else {
			*previous_tail = NULL;
			/* printf("4\n"); */
		}
		/* Don't need to flush; caller will do it... */
	}

	/* Use two flushes here to make this "repeatable" - if we fail after
	 * the first set + flush, there are no real effects. */
	PM_EQU((v->data[v->count]), ((void *)e));
	kp_flush_range(&(v->data[v->count]), sizeof(void *), v->use_nvm);

	/* Do we need a memory fence right here? Only if we're flushing (so
	 * the fence is already internal in kp_flush_range()); otherwise,
	 * we're not concerned about anybody else seeing the count and the
	 * element out-of-order... (right?). */
	PM_EQU((v->count), (v->count+1));
	kp_flush_range((void *)&(v->count), sizeof(unsigned long long), v->use_nvm);
	v_debug("stored new element %s in slot %llu (now count=%llu, size=%llu)\n",
			(char *)(v->data[(v->count)-1]), v->count-1, v->count, v->size);

	return 0;
}

/* CHECK - TODO: how consistent / durable / atomic / recoverable is this
 * function???
 *
 * This function allows NULL pointers for the element put into the array,
 * for now. */
uint64_t vector_insert(vector *v, const void *e, vector_comparator cmp)
{
	unsigned long long orig_size, new_size;
	uint64_t insert_idx, shift_idx;
	int flush_size = 0;

	/* HEADS UP: this function is mostly the same as vector_append(), so
	 * if you update one, you should probably update the other! */

	if (v == NULL) {
		v_error("v is NULL\n");
		return -1;
	}
	if (v->count == VECTOR_MAX_COUNT) {
		v_error("hit maximum vector length: v->count=%llu\n", v->count);
		return -1;
	}
#ifndef UNSAFE_COMMIT
//#ifdef VECTOR_ASSERT
#if 0
	/* This assert only makes sense if we're not garbage collecting or
	 * otherwise removing things from vectors. Note that the resizing
	 * code doesn't contain any explicit checks for this (it will
	 * happily reduce the size of the vector below the initial size
	 * if you remove enough things); this bhavior should probably
	 * change, but whatever. */
	if (v->size < VECTOR_INIT_SIZE) {
		v_die("vector size %llu is less than initial size %d!!\n",
				v->size, VECTOR_INIT_SIZE);
	}
#endif
#endif

	kp_todo("factor out the resizing code that's common to both append "
			"and insert, dummy!\n");

	/* When the last array slot is exhausted, increase the size of the
	 * array by multiplying it by the resize factor.
	 * Resizing the array consists of two steps: A) re-allocing the memory
	 * region, and B) setting the new size of the vector. A) is repeatable,
	 * but unfortunately could result in a memory leak if we don't correctly
	 * remember the pointer AND remember the new size! To minimize the leak
	 * window here, we immediately flush both the pointer and the size (which
	 * should be adjacent to each other in the struct!) after the realloc.
	 * We calculate the size of the flush that we need to perform by using
	 * the offsetof operator, because the compiler may insert padding between
	 * members that we don't know about. This code assumes that the order of
	 * the members in the struct is [data, size, count].
	 *
	 * ...
	 */
	if (v->size == v->count) {
#ifdef VECTOR_RESIZE_PRINT
		v_print("v->size hit v->count = %llu; insert resizing!!!\n",
				v->size);
#endif
		v_debug("v->size hit v->count = %llu; resizing!!!\n", v->size);
		/* Check if multiplying v->size by VECTOR_RESIZE_FACTOR will put
		 * it over the VECTOR_MAX_COUNT:
		 */
		if (v->size > (VECTOR_MAX_COUNT / VECTOR_RESIZE_FACTOR)) {
			v_debug("vector size (%llu) times resize factor (%d) would "
					"overflow max count (%llu), so setting size directly "
					"to max count\n", v->size, VECTOR_RESIZE_FACTOR,
					VECTOR_MAX_COUNT);
			new_size = VECTOR_MAX_COUNT;
		} else {
			new_size = v->size * VECTOR_RESIZE_FACTOR;
		}
#ifdef VECTOR_RESIZE_PRINT
		v_print("calculated new_size=%llu (insert)\n", new_size);
#endif
		orig_size = v->size;
#ifdef UNSAFE_COMMIT
		if (new_size > UNSAFE_COMMIT_LOG_SIZE) {
			v_print("WARNING: new vector size is greater than %d, probably "
					"means that we're resizing the commit_log vector!!\n",
					UNSAFE_COMMIT_LOG_SIZE);
		}
#endif
#ifdef V_ASSERT
		if (new_size > 100) {
			v_debug("WARNING: resizing vector to new_size=%llu\n", new_size);
		}
#endif

		/* We expect the flush_size to be 12, but the compiler could possibly
		 * insert padding that changes this. On brief examination, no padding
		 * is inserted and both the data pointer and the size are flushed in
		 * a single flush.
		 * todo: could put the following code segment in its own "pcm_realloc()"
		 * function...
		 */
		if (v->use_nvm) {
			/* We only need to flush data and size; count comes _after_ size,
			 * so use it as the end of the range to flush. */
			flush_size = offsetof(vector, count) - offsetof(vector, data);
			//v_print("calculated flush_size=%d from offsetof(count)=%u, "
			//		"offsetof(data)=%u\n", flush_size, offsetof(vector, count),
			//		offsetof(vector, data));
#ifdef VECTOR_ASSERT
			if (flush_size < (sizeof(void **) + sizeof(unsigned long long))) {
				v_die("got unexpected flush_size %d! offsetof(count)=%zu, "
						"offsetof(data)=%zu\n", flush_size,
						offsetof(vector, count), offsetof(vector, data));
			}
			//v_print("calculated flush_size %d from offsetof(count)=%u, "
			//		"offsetof(data)=%d\n", flush_size,
			//		offsetof(vector, count), offsetof(vector, data));
#endif
		}

		/* Leak window begin. Note that kp_realloc() doesn't flush internally,
		 * because we want to flush both the data and the size in the same
		 * cache line to get consistency
		 *   Also note that we don't currently GUARANTEE this, if the compiler
		 *   happens to allocate v->data and v->size in two different cache
		 *   lines. */
		kp_realloc((void **)&(v->data), sizeof(void*) * new_size, sizeof(void*) * v->size, v->use_nvm);
		v->size = new_size;
		kp_flush_range(&(v->data), flush_size, v->use_nvm);  //flush both data and size!
		/* Leak window end. If we fail after the flush() has returned,
		 * then the next call to vector_append() will skip the resizing
		 * step.
		 */
		if (v->data == NULL) {
			v_error("kp_realloc(array) failed\n");
			/* Best effort on failure: reset size to what it was before,
			 * and let caller handle the rest.
			 */
			v->size = orig_size;
			return -1;
		}

		v_debug("re-allocated array, now has size %llu (%llu slots)\n",
				sizeof(void*) * v->size, v->size);
	}

	/* We expect that this function will often be an append anyway, so
	 * we start at the end of the array and then search backwards for the
	 * index to insert into. */
	insert_idx = v->count;
	/* The comparator function returns positive if e1 > e2. By using
	 * > and not >= here, we will stop as early as possible, so if
	 * elements happen to be equal to each other then the later elements
	 * will be further along in the array. */
	while (insert_idx > 0 && cmp(v->data[insert_idx-1], e) > 0) {
		insert_idx--;
	}
	//v_print("set insert_idx=%llu (count=%llu)\n", insert_idx, v->count);

	/* Start at the back of the array again and shift elements forward one
	 * at a time. This is somewhat "repeatable" - if a failure occurs while
	 * we're doing this, then we can either remember the shift index, or
	 * look through the array for duplicates, and then resume where we left
	 * off. We flush after every single shift (ouch!) so that no data can
	 * be lost. */
	shift_idx = v->count;
	while (shift_idx > insert_idx) {
		//v_print("shifting element from %llu to %llu\n", shift_idx-1, shift_idx);
		v->data[shift_idx] = v->data[shift_idx - 1];
		kp_flush_range(&(v->data[shift_idx]), sizeof(void *), v->use_nvm);
		shift_idx--;
	}
	//v_print("now inserting new element into idx=%llu\n", insert_idx);

	/* Use two flushes here to make this "repeatable" - if we fail after
	 * the first set + flush, there are no real effects (well, this was
	 * true for append... is it any different for insert??). */
	PM_EQU((v->data[insert_idx]), ((void *)e));
	kp_flush_range(&(v->data[insert_idx]), sizeof(void *), v->use_nvm);
	PM_EQU((v->count), (v->count+1));
	kp_flush_range(&(v->count), sizeof(unsigned long long), v->use_nvm);
	//v_print("stored new element %s in slot %llu (now count=%llu, size=%llu)\n",
	//		(char *)(v->data[insert_idx]), insert_idx, v->count, v->size);
	//v_print("stored new element %p in slot %llu (now count=%llu, size=%llu)\n",
	//		v->data[insert_idx], insert_idx, v->count, v->size);

	return insert_idx;
}

/* This function allows NULL pointers for the element put into the array,
 * for now. 
 */
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

	/* Set operation happens "atomically," as soon as the pointer that we
	 * set gets flushed to memory.
	 */
	*old_e = v->data[idx];
	v->data[idx] = e;
	kp_flush_range(&(v->data[idx]), sizeof(void *), v->use_nvm);
	v_debug("stored element %s in slot %llu (count=%llu, size=%llu)\n",
			(char *)(v->data[idx]), idx, v->count, v->size);

	return 0;
}

int vector_get(const vector *v, unsigned long long idx, void **e)
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

	/* No need for flushing / failure recovery here; not changing anything,
	 * so user can just call vector_get() again.
	 */
	*e = v->data[idx];
	//v_debug("got pointer %p (string %s) from slot %llu (count=%llu, size=%llu); "
	//		"string may not make sense because now we're storing vtes in vector\n",
	//		e, (char *)(*e), idx, v->count, v->size);
	v_debug("got pointer %p from slot %llu (count=%llu, size=%llu)\n",
			e, idx, v->count, v->size);

	return 0;
}

int vector_delete(vector *v, unsigned long long idx, void **e)
{
	unsigned long long i;
	int flush_size = 0;

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

	/* Start at the removed index and shift elements backwards one at a
	 * time. This is somewhat "repeatable" - if a failure occurs while
	 * we're doing this, then we can either remember the shift index, or
	 * look through the array for duplicates, and then resume where we left
	 * off. We flush after every single shift (ouch!) so that no data can
	 * be lost. This is similar (but opposite) to vector_insert() above. */
	for (i = idx; i < v->count - 1; i++) {
		v->data[i] = v->data[i+1];
		kp_flush_range(&(v->data[i]), sizeof(void *), v->use_nvm);
		v_debug("shifted element %s from slot %llu down to slot %llu\n",
				(char *)(v->data[i]), i+1, i);
	}
	v->count--;
	kp_flush_range((void *)&(v->count), sizeof(unsigned long long), v->use_nvm);
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

		/* See notes about flush_size in vector_append() above. */
		if (v->use_nvm) {
			flush_size = offsetof(vector, count) - offsetof(vector, data);
#ifdef VECTOR_ASSERT
			if (flush_size < (sizeof(void **) + sizeof(unsigned long long))) {
				v_die("got unexpected flush_size %d! offsetof(count)=%zu, "
						"offsetof(data)=%zu\n", flush_size,
						offsetof(vector, count), offsetof(vector, data));
			}
#endif
		}

		/* We set the vector's new size first, then set its data pointer, and
		 * then finally flush them both to memory (if use_nvm is true). See
		 * the notes in vector_append() for this. */
		/* Leak window begin: */
		size_t old_size = v->size;
		v->size /= VECTOR_RESIZE_FACTOR;  //inverse of vector_append()
		kp_realloc((void **)&(v->data), sizeof(void*) * v->size, sizeof(void*) * old_size, v->use_nvm);
		kp_flush_range(&(v->data), flush_size, v->use_nvm);
		/* Leak window end. If we fail after the flush has returned, then
		 * the next call to vector_delete() will skip the resizing step. */
		if (v->data == NULL) {
			v_die("kp_realloc(array) failed\n");
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
			kp_free(&(v->data[i]), v->use_nvm);  //sets v->data[i] to NULL
		} else {
			/* could be a deleted value? */
			v_debug("NULL pointer in array, not freeing it\n");
		}
	}
	v_debug("successfully freed %llu elements from vector\n", count);
}

void vector_destroy(vector **v)
{
	bool use_nvm;
	
	if (!v) {
		v_error("got NULL argument unexpectedly; returning\n");
		return;
	}

	if (*v) {
		use_nvm = (*v)->use_nvm;
		(*v)->state = STATE_DEAD;
		kp_flush_range((void *)&((*v)->state), sizeof(ds_state), use_nvm);

		if ((*v)->data) {
			v_debug("testing 0: (*v)->data = %p\n", (*v)->data);
			kp_free((void **)&((*v)->data), use_nvm);  //sets (*v)->data to NULL
#ifdef VECTOR_ASSERT
			v_debug("testing 1: (*v)->data = %p\n", (*v)->data);
			if ((*v)->data != NULL) {
				v_die("kp_free() did not set (*v)->data to NULL as expected!\n");
			}
#endif
		}
		v_debug("testing 2: *v = %p\n", *v);
		kp_free((void **)(v), use_nvm);  //sets *v to NULL after free
#ifdef VECTOR_ASSERT
		v_debug("testing 3: *v = %p\n", *v);
		if (*v != NULL) {
			v_die("kp_free() did not set *v to NULL as expected!\n");
		}
#endif
	}
	v_debug("freed vector's array and vector struct itself\n");
}

unsigned int vector_struct_size()
{
	return sizeof(vector);
}

/* Does the pointer arithmetic to pull a uint64_t from inside of the void*
 * data at the specified index.
 */
uint64_t vector_get_uint64_t(const vector *v, size_t offset, uint64_t idx)
{
	uint64_t val;
	uint64_t *val_ptr;

	/* To get the specified uint64_t, first get the right pointer from
	 * the data array, then add the offset to it. The pointer's value
	 * contains the address of the beginning of each data element, so
	 * we use this value as the address in our pointer arithmetic. It's
	 * important to treat the pointer value as unsigned! (I think).
	 *
	 * On the 32-bit (burrard) and 64-bit (n02) systems that I checked,
	 * the size of a long int always matches the size of a void * pointer,
	 * so we cast the data pointer to an unsigned long int for performing
	 * arithmetic on it. Ugh.
	 */
	/* According to VTune, this function has awful cache behavior (many
	 * last-level cache misses). It performs three dereferences, so I'm
	 * not sure if this is avoidable. */
	val_ptr = (uint64_t *)((unsigned long int)(v->data[idx]) + offset);
	val = *val_ptr;
	v_debug("got uint64_t %ju from index %ju of vector\n", val, idx);
	return val;
}

int vector_search_linear(const vector *v, size_t offset, uint64_t key, uint64_t *idx)
{
	uint64_t i, val;

	if (v == NULL) {
		v_error("v is NULL\n");
		return -1;
	}

	/* Only search up to count, not size!
	 * v->count is an unsigned long long, which should be the same as a
	 * uint64_t on most/all platforms.
	 */
	for (i = 0; i < v->count; i++) {
		val = vector_get_uint64_t(v, offset, i);
		if (val == key) {
			*idx = i;
			v_debug("found search key %ju at idx=%ju\n", key, *idx);
			return 0;
		}
	}

	v_debug("searched through all %llu elements, did not find search key "
			"%ju\n", v->count, key);
	return 1;
}

uint64_t vector_search_binary(const vector *v, size_t offset, uint64_t key,
		bool exact, vector_validator validator)
{
	uint64_t val, len, lower, upper, range, idx, candidate;

	candidate = UINT64_MAX;
	idx = 0;
	len = (uint64_t)(v->count);  //use count, not size!!!
	lower = 0;
	upper = len - 1;
	v_debug2("initial lower=%ju, upper=%ju, len=%ju\n", lower, upper, len);

	/* Special case to handle vector with length 1: without this, I ran into
	 * the following buggy behavior: when performing non-exact binary search
	 * for a key in a vector with a single element e whose value is GREATER
	 * than the key, the code in the while loop below will check idx = 0 the
	 * first time through, but will not return a non-exact match because it
	 * does not find that e is less than the key (this is what we want). Then,
	 * when we reach the !exact check after the while loop, we want to fail
	 * that condition and return NOT-FOUND, because there is no range in the
	 * vector where the key would currently be valid; however, with a vector
	 * of length 1, idx will equal 0 and will equal len-1, so we'll return
	 * 0 there, which is incorrect. Simply adding a check there for len > 1
	 * causes the binary search to fail in the opposite case, when the vector
	 * has a single element e that is greater than the key. Because of all
	 * of this, we just special-case the length = 1 case here, because there's
	 * just one element to check.
	 */
	if (len == 1) {
		val = vector_get_uint64_t(v, offset, 0);
		if (exact) {
			if (val == key) {
				return 0;
			} else {
				return UINT64_MAX;
			}
		} else {  //non-exact
			if (val <= key) {
				return 0;  //key is valid "to the right" of the single element
			} else {
				return UINT64_MAX;
			}
		}
	}

	while ((lower <= upper) && (lower != UINT64_MAX) && (upper != UINT64_MAX)) {
		range = upper - lower;
		idx = lower + (range / 2);
		v_debug2("len=%ju, lower=%ju, upper=%ju, range=%ju, idx=%ju\n",
				len, lower, upper, range, idx);
		val = vector_get_uint64_t(v, offset, idx);
		v_debug2("at idx=%ju got val=%ju (key=%ju)\n", idx, val, key);
		if (val == key) {
			/* Keep scanning to the right, in case there are multiple
			 * items with the same value:
			 */
			while(idx+1 < len &&
					vector_get_uint64_t(v, offset, idx+1) == key) {
				v_debug2("element at idx+1 (%ju) also matches, shifting right\n",
						idx+1);
				idx++;
			}
			v_debug2("returning LAST idx (%ju) that matches value\n", idx);
			candidate = idx;
			break;
		} else if (val < key) {  //idx is left of key; search right
			/* Check if element immediately to our right is greater than
			 * current element: if so, and if exact is false, then return
			 * the current element's index!
			 * In a binary search, we are guaranteed to examine the two
			 * elements that surround a search key that isn't present in
			 * the vector (TODO: right??!?!?). So, a corollary is that we
			 * don't need to scan any further to the right (like in the
			 * first if case above), because if we're not an exact match
			 * here and the element to this one's right is not greater
			 * than this one, we'll search the element to the right at
			 * a later step anyway.
			 */
			if ((idx < len - 1) && (!exact) &&
			    (vector_get_uint64_t(v, offset, idx+1) > key)) {
				v_debug2("v[%ju]=%ju, v[%ju]=%ju, so returning "
						"idx=%ju for range search\n", idx, val, idx+1, 
						vector_get_uint64_t(v, offset, idx+1), idx);
				candidate = idx;
				break;
			}
			lower = idx + 1;
		} else {  //idx is right of key; search left
			upper = idx - 1;
		}
	}

	/* If we're not performing an exact search, then check if we ended
	 * the search at the last element in the vector. If so, then the last
	 * element's value is less than the key, so we return the last element's
	 * index (since each element's value marks the BEGINNING of its validity
	 * range).
	 */
	if (!exact && idx == len - 1) {
		candidate = idx;
	}

	/* If validator is NULL, then we'll return candidate below - this is
	 * unchanged from how this code worked before. If validator is non-NULL,
	 * then we only perform the backwards linear search if we actually found
	 * some candidate index. */
	if (validator && candidate != UINT64_MAX) {
		/* If we reach this code, then candidate is exactly the same as the
		 * idx that was returned in the earlier version of this code. If the
		 * element at the candidate idx is already valid, then we'll return
		 * it the candidate idx. If not, then we'll perform a linear search
		 * backwards, until we hit the beginning of the vector, in which case
		 * we'll return UINT64_MAX for not-found. */
#ifdef VECTOR_ASSERT
		if (candidate >= v->count) {
			kp_die("bad candidate idx %ju; v->count is only %llu\n",
					candidate, v->count);
		}
#endif
		v_debug2("validator is non-NULL, performing linear search for "
				"valid element backwards from candidate idx=%ju\n",
				candidate);
		while (!validator(v->data[candidate])) {
			v_debug2("validator returned false for candidate idx=%ju, will "
					"try %ju next\n", candidate, candidate-1);
			if (candidate == 0) {
				/* After adding this code, this message was printed about 475
				 * times in evaluation runs with 8 threads, 25000 iterations,
				 * and a merge-frequency of 1. With a merge frequency of 10,
				 * we get the message 95198 times! Holy crap, we were being
				 * very wrong!!! */
				//v_print("hit beginning of vector without finding a valid "
				//		"entry: returning not-found where we used to return "
				//		"%ju!!\n", idx);
				candidate = UINT64_MAX;
				break;
			}
			candidate--;  //try again
		}
#ifdef VECTOR_ASSERT
		/* Single evaluation runs:
		 * 8 threads, 25000 iterations, merg_freq = 10:
		 *   31963 bad that used to be good, 3036512 still good:
		 *     31963 / 3,068,475 = 1% wrong, previouslu
		 * Merge freq 100: 217897 wrong, 2,628,411 total -> 8% wrong!
		 */
		//if (candidate == UINT64_MAX) {
		//	v_print("bad get that used to be good\n");
		//} else {
		//	v_print("good get still\n");
		//}
#endif
	}

	return candidate;
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
