/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Forked from: https://gist.github.com/953968
 */

#include <stdlib.h>
#include "vector64.h"
#include "vector_macros.h"
#include "../kp_common.h"
#include "../kp_recovery.h"
#include "../kp_macros.h"
  //hacky...

/* Factors that define when and how the vector is resized. The initial
 * size must be an integer, but the resize factor may be non-integral
 * (although I've only tested integer factors, namely 2.0).
 */
#define VECTOR64_INIT_SIZE 8
#define VECTOR64_RESIZE_FACTOR 2.0
#define VECTOR64_MAX_COUNT ((unsigned long)(0-2))

/* This structure MUST keep its size member adjacent to the data
 * member: on a vector resize, after the realloc() we need to flush
 * BOTH of these values (when use_nvm is true), and if they are kept
 * adjacent then this maximizes the likelihood that they are found in
 * the same cache line and hence only require one flush. The
 * vector64_resize_up() code currently depends on the following order of
 * members in the struct:
 *   array
 *   size
 *   count
 * Changing this order without changing the vector64_resize_up() code will
 * break things.
 */
struct vector64_ {
	unsigned long *array; //array of unsigned integers
	unsigned long size;   //number of array elements allocated
	unsigned long count;  //number of elements in use
	bool use_nvm;
	ds_state state;
};

int vector64_create(vector64 **v, unsigned long size, bool use_nvm)
{
	unsigned long init_size;
	v_debug("creating new vector64 with use_nvm=%s\n",
			use_nvm ? "true" : "false");

	/* Use calloc if use_nvm is true, otherwise just malloc: */
	kp_kpalloc((void **)v, sizeof(vector64), use_nvm);
	if (*v == NULL) {
		v_error("kp_kpalloc(vector64) failed\n");
		return -1;
	}

	init_size = size ? size : VECTOR64_INIT_SIZE;
	v_debug("using init_size=%lu for vector64\n", init_size);

	kp_kpalloc((void **)&((*v)->array), sizeof(unsigned long) * init_size,
			use_nvm);
	if ((*v)->array == NULL) {
		v_error("kp_kpalloc(array) failed\n");
		vector64_destroy(v);
#ifdef VECTOR_ASSERT
		if (*v != NULL) {
			v_die("vector_destroy() did not set *v to NULL as expected!\n");
		}
#endif
		return -1;
	}
	(*v)->size = init_size;
	(*v)->count = 0;
	(*v)->use_nvm = use_nvm;
	
	/* "CDDS": flush, set state, and flush again. */
	kp_flush_range((void *)*v, sizeof(vector64) - sizeof(ds_state), use_nvm);
	(*v)->state = STATE_ACTIVE;
	kp_flush_range((void *)&((*v)->state), sizeof(ds_state), use_nvm);

	v_debug("successfully created new vector64\n");
	return 0;
}

unsigned long vector64_count(const vector64 *v)
{
	if (v == NULL) {
		v_error("v is NULL\n");
		return VECTOR64_MAX;  //vector64_count isn't supposed to error, oh well
	}
	v_debug("returning count=%lu (size=%lu)\n", v->count, v->size);
	return v->count;
}

/* Increases the size of a vector. This function should be called when
 * adding an element to the vector, but v->count has hit v->size.
 *
 * Returns: 0 on success, -1 on error.
 */
static int vector64_resize_up(vector64 *v)
{
	unsigned long orig_size, new_size;
	int flush_size = 0;

	if (v == NULL) {
		v_die("v is NULL\n");
	}
	if (v->size != v->count) {
		v_error("this function should only be called when size (%lu) == "
				"count (%lu)\n", v->size, v->count);
		return -1;
	}

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
	 * the members in the struct is [array, size, count]!
	 */

#ifdef VECTOR64_RESIZE_PRINT
	v_print("v->size hit v->count = %lu; resizing up!!!\n",
			v->size);
#endif
	v_debug("v->size hit v->count = %lu; resizing up!!!\n", v->size);

	/* Check if multiplying v->size by VECTOR64_RESIZE_FACTOR will put
	 * it over the VECTOR64_MAX_COUNT:
	 */
	if (v->size > (VECTOR64_MAX_COUNT / VECTOR64_RESIZE_FACTOR)) {
		v_debug("vector64 size (%lu) times resize factor (%f) would "
				"overflow max count (%lu), so setting size directly "
				"to max count\n", v->size, VECTOR64_RESIZE_FACTOR,
				VECTOR64_MAX_COUNT);
		new_size = VECTOR64_MAX_COUNT;
	} else {
		new_size = v->size * VECTOR64_RESIZE_FACTOR;
	}
#ifdef VECTOR_RESIZE_PRINT
	v_print("calculated new_size=%lu\n", new_size);
#endif
	v_debug("calculated new_size=%lu\n", new_size);
	orig_size = v->size;

	/* We expect the flush_size to be 16 (8 bytes each for the unsigned long *
	 * and the unsigned long on a 64-bit system), but the compiler could
	 * possibly insert padding that changes this. On brief examination, no
	 * padding is inserted in practice, and both the array pointer and the
	 * size are flushed in a single flush...
	 * todo: could put the following code segment in its own "pcm_realloc()"
	 * function...
	 */
	if (v->use_nvm) {
		/* We only need to flush array and size; count comes _after_ size,
		 * so use it as the end of the range to flush. */
		flush_size = offsetof(vector64, count) - offsetof(vector64, array);
#ifdef VECTOR_ASSERT
		if (flush_size <
				(sizeof(unsigned long *) + sizeof(unsigned long))) {
			v_die("got unexpected flush_size %d! offsetof(count)=%zu, "
					"offsetof(array)=%zu\n", flush_size,
					offsetof(vector64, count), offsetof(vector64, array));
		}
		v_print("calculated flush_size %d from offsetof(count)=%zu, "
				"offsetof(array)=%zu\n", flush_size,
				offsetof(vector64, count), offsetof(vector64, array));
#endif
	}

	/* Leak window begin. Note that kp_realloc() doesn't flush internally,
	 * because we want to flush both the array and the size in the same
	 * cache line to get consistency.
	 *   Also note that we don't currently GUARANTEE this, if the compiler
	 *   happens to allocate v->array and v->size in two different cache
	 *   lines. Oh well...
	 */
	kp_realloc((void **)&(v->array), sizeof(unsigned long) * new_size, sizeof(unsigned long) * v->size,
			v->use_nvm);
	v->size = new_size;
	kp_flush_range(&(v->array), flush_size, v->use_nvm);  //flush both array and size!
	/* Leak window end. If we fail after the flush() has returned, then
	 * the next call to the vector append/insert functions will skip the
	 * resizing step and will not call this function.
	 */
	if (v->array == NULL) {
		v_error("kp_realloc(array) failed\n");
		/* Best effort on failure: reset size to what it was before,
		 * and let caller handle the rest.
		 */
		v->size = orig_size;
		return -1;
	}

	v_debug("re-allocated array, now has size %lu (%lu slots)\n",
			sizeof(unsigned long) * v->size, v->size);
	return 0;
}

/* Ideally, vector64_append would be consistent, durable, and _atomic_, which
 * would mean that it doesn't have to be _recovered_ after a failure. This
 * is probably possible by following the instructions in "A Lock-Free
 * Dynamically Resizable Array." However, this would require a significant
 * restructuring of the vector code that we already have, so we won't do
 * this for now. Instead, when the vector needs resizing, we focus on making
 * it _recoverable_, rather than atomic; when the vector doesn't need resizing,
 * then append is consistent and durable and _repeatable_, rather than atomic.
 *
 * Note that vector64_append is NOT thread-safe or "lock-free" - high-level
 * synchronization is needed so that only one append occurs at a time!
 *
 * Relevant links:
 *   http://www2.research.att.com/~bs/lock-free-vector.pdf
 *   https://parasol.tamu.edu/~peterp/slides/opodis06.pdf
 *   Intel patent, 8006064: "Lock-free vector utilizing a resource allocator...":
 *     http://www.google.com/patents/US8006064?printsec=abstract#v=onepage&q&f=false
 *   http://www.ibm.com/developerworks/aix/library/au-intelthreadbuilding/index.html?ca=drs-
 *   http://software.intel.com/en-us/blogs/2009/04/09/delusion-of-tbbconcurrent_vectors-size-or-3-ways-to-traverse-in-parallel-correctly/
 */
unsigned long vector64_append(vector64 *v, unsigned long e)
{
	int ret;

	/* HEADS UP: this function is mostly the same as vector_insert(), so
	 * if you update one, you should probably update the other! */

	if (v == NULL) {
		v_error("v is NULL\n");
		return VECTOR64_MAX;
	}
	if (v->count == VECTOR64_MAX_COUNT) {
		v_error("hit maximum vector64 length: v->count=%lu\n", v->count);
		return VECTOR64_MAX;
	}

	/* Check if resizing is needed. The resize function contains a small
	 * "leak window" during which if a failure occurs, we may have
	 * re-allocated the array to a larger size, but we will still remember
	 * the old, small size in the vector64 struct. However, if a failure
	 * occurs either before or after this small window, this code should
	 * work fine: either the vector will not have changed and we'll try
	 * to resize it again, or the vector will have been resized up and
	 * the size will no longer match the count, so we'll fail this
	 * condition during re-execution.
	 */
	if (v->size == v->count) {
		ret = vector64_resize_up(v);
		if (ret != 0) {
			v_error("vector64_resize_up() failed, unable to resize vector "
					"beyond current count %lu (ret=%d)\n", v->count, ret);
			return VECTOR64_MAX;
		}
	}

	/* The actual append part of vector_append() is atomic: we first
	 * fill in the element in the array, then increment the count.
	 * If we fail in-between these two steps, then vector_append() can
	 * just be called again and we'll overwrite the array element with the
	 * same value. We do, however, have to flush the written element before
	 * incrementing the count: we don't want the incremented count to hit
	 * memory before the new element does.
	 * Note again that this is ATOMIC, not REPEATABLE. After a failure, if
	 * the caller didn't get a return value from this function, then it
	 * can't be certain whether or not the append succeeded, so it can't
	 * just blindly repeat the append. Instead, the client must first do
	 * a vector_get() to check if the append happened or not.
	 */

	/* Use two flushes here to make this atomic - if we fail after the
	 * first set + flush, there are no real effects. */
	v->array[v->count] = e;
	kp_flush_range(&(v->array[v->count]), sizeof(unsigned long), v->use_nvm);
	/* Do we need a memory fence right here? Only if we're flushing (so
	 * the fence is already internal in kp_flush_range()); otherwise,
	 * we're not concerned about anybody else seeing the count and the
	 * element out-of-order... (right?). */
	v->count++;
	kp_flush_range((void *)&(v->count), sizeof(unsigned long), v->use_nvm);
	v_debug("stored new element %lu in slot %lu (now count=%lu, size=%lu)\n",
			v->array[(v->count)-1], v->count-1, v->count, v->size);

	return v->count;
}

unsigned long vector64_insert(vector64 *v, unsigned long e,
		bool allow_duplicates)
{
	unsigned long search_e, search_idx, insert_idx, shift_idx;
	int ret;

	/* HEADS UP: this function is mostly the same as vector_append(), so
	 * if you update one, you should probably update the other! */

	if (v == NULL) {
		v_error("v is NULL\n");
		return VECTOR64_MAX;
	}
	if (v->count == VECTOR64_MAX_COUNT) {
		v_error("hit maximum vector64 length: v->count=%lu\n", v->count);
		return VECTOR64_MAX;
	}
	v_debug("entered, with new element e=%lu, allow_duplicates=%s\n",
			e, allow_duplicates ? "true" : "false");

	/* First, perform a binary search for the to-be-inserted element in
	 * the vector. Set the "exact" parameter to false so that if the element
	 * is not already in the vector, insert_idx will be set to the index of
	 * the element closest to but less than the element. If the element is
	 * already found in the vector, the binary search will return the index
	 * of the LAST instance of the element if there are duplicates.
	 *
	 * Also, note that we perform the search before doing any resizing, in
	 * case we skip the insertion because it would lead to an unwanted
	 * duplicate element in the vector.
	 */
	insert_idx = VECTOR64_MAX;  //invalid
	if (v->count == 0) {
		/* Special easy case for insertion into empty array: */
		insert_idx = 0;
	} else {
		search_idx = vector64_search_binary(v, e, false);
		if (search_idx == VECTOR64_MAX) {
			/* We should only hit this case when the element to be inserted
			 * is less than every other element in the vector (otherwise,
			 * the non-exact range search would have returned _some_ index).
			 */
			v_debug("binary search returned not-found, meaning that the "
					"new element %lu should be inserted into the beginning "
					"of the vector (current beginning element=%lu)\n",
					e, v->array[0]);
#ifdef VECTOR_ASSERT
			if (v->array[0] < e) {
				v_die("unexpected: vector64_search_binary() returned not-"
						"found, but vector[0]=%lu, which is less than new "
						"element e=%lu\n", v->array[0], e);
			}
#endif
			insert_idx = 0;  //insert into first slot
		} else {
			search_e = v->array[search_idx];
			v_debug("binary search for element %lu returned index %lu, "
					"which currently contains element %lu\n",
					e, search_idx, search_e);
#ifdef VECTOR_ASSERT
			if (search_e > e) {  //sanity check
				v_die("binary search returned the index of an element "
						"%lu GREATER than the element to be inserted "
						"%lu!\n", search_e, e);
			}
#endif
			if (!allow_duplicates) {
				if (search_e == e) {
					v_debug("found element %lu in vector already and "
							"allow_duplicates is false, so just returning "
							"the index where the element was already "
							"located (%lu)\n", search_e, search_idx);
					return search_idx;
				} else {
					/* Insert after the element that's just less than the
					 * new element:
					 */
					insert_idx = search_idx + 1;
				}
			} else {
				/* Either insert after the element that's just-less than
				 * the new element, or insert after the LAST element that
				 * is EQUAL to the new element (that's what the binary
				 * search is supposed to return):
				 */
				insert_idx = search_idx + 1;
			}
			v_debug("will insert element %lu into vector at index %lu "
					"(current count=%lu, size=%lu)\n",
					e, insert_idx, v->count, v->size);
		}
	}
#ifdef VECTOR_ASSERT
	if (insert_idx == VECTOR64_MAX) {
		v_die("insert_idx was never set to something other than %lu\n",
				insert_idx);
	}
#endif

	/* Check if resizing is needed. The resize function contains a small
	 * "leak window" during which if a failure occurs, we may have
	 * re-allocated the array to a larger size, but we will still remember
	 * the old, small size in the vector64 struct. However, if a failure
	 * occurs either before or after this small window, this code should
	 * work fine: either the vector will not have changed and we'll try
	 * to resize it again, or the vector will have been resized up and
	 * the size will no longer match the count, so we'll fail this
	 * condition during re-execution.
	 */
	if (v->size == v->count) {
		ret = vector64_resize_up(v);
		if (ret != 0) {
			v_error("vector64_resize_up() failed, unable to resize vector "
					"beyond current count %lu (ret=%d)\n", v->count, ret);
			return VECTOR64_MAX;
		}
	}

	/* Start at the back of the array and shift elements forward one
	 * at a time. This is somewhat "repeatable" - if a failure occurs while
	 * we're doing this, then we can either remember the shift index, or
	 * look through the array for duplicates, and then resume where we left
	 * off. We flush after every single shift (ouch!) so that no data can
	 * be lost.
	 */
	shift_idx = v->count;
	while (shift_idx > insert_idx) {
		v_debug("shifting element %lu forward from %lu to %lu\n",
				v->array[shift_idx-1], shift_idx-1, shift_idx);
		v->array[shift_idx] = v->array[shift_idx-1];
		kp_flush_range(&(v->array[shift_idx]), sizeof(unsigned long),
				v->use_nvm);
		shift_idx--;
	}
	v_debug("now inserting new element %lu into idx=%lu\n",
			e, insert_idx);

	/* Use two flushes here to make this "repeatable" - if we fail after
	 * the first set + flush, there are no real effects (well, this was
	 * true for append... is it any different for insert??).
	 */
	v->array[insert_idx] = e;
	kp_flush_range(&(v->array[insert_idx]), sizeof(unsigned long), v->use_nvm);
	v->count++;
	kp_flush_range(&(v->count), sizeof(unsigned long), v->use_nvm);
	v_debug("stored new element %lu in slot %lu (now count=%lu, size=%lu)\n",
			v->array[insert_idx], insert_idx, v->count, v->size);
#ifdef VECTOR_DEBUG
	char *v_str = vector64_to_string(v);
	if (v_str) {
		v_debug("search vector contents: %s\n", v_str);
		free(v_str);
	}
#endif

	return insert_idx;
}

unsigned long vector64_set(vector64 *v, unsigned long idx, unsigned long e)
{
	unsigned long old_e;

	if (v == NULL) {
		v_error("v is NULL\n");
		return VECTOR64_MAX;
	}
	if (idx >= v->count) {
		if (VECTOR64_DIE_ON_OOB) {
			v_die("index %lu out-of-bounds, v->count=%lu\n", idx, v->count);
		}
		v_error("index %lu out-of-bounds, v->count=%lu\n", idx, v->count);
		return VECTOR64_MAX;
	}

	/* Set operation happens "atomically," as soon as the element that we
	 * set gets flushed to memory.
	 */
	old_e = v->array[idx];
	v->array[idx] = e;
	kp_flush_range(&(v->array[idx]), sizeof(unsigned long), v->use_nvm);
	v_debug("stored element %lu in slot %lu (count=%lu, size=%lu)\n",
			v->array[idx], idx, v->count, v->size);

	return old_e;
}

unsigned long vector64_get(const vector64 *v, unsigned long idx)
{
	unsigned long e;

	if (v == NULL) {
		v_error("v is NULL\n");
		return VECTOR64_MAX;
	}
	if (idx >= v->count) {
		if (VECTOR64_DIE_ON_OOB) {
			v_die("index %lu out-of-bounds, v->count=%lu\n", idx, v->count);
		}
		v_error("index %lu out-of-bounds, v->count=%lu\n", idx, v->count);
		return VECTOR64_MAX;
	}

	/* No need for flushing / failure recovery here; not changing anything,
	 * so user can just call vector64_get() again.
	 */
	e = v->array[idx];

	return e;
}

unsigned long vector64_delete(vector64 *v, unsigned long idx)
{
	unsigned long old_e;
	unsigned long i;
	int flush_size = 0;

	if (v == NULL) {
		v_error("v is NULL\n");
		return VECTOR64_MAX;
	}
	if (idx >= v->count) {
		if (VECTOR64_DIE_ON_OOB) {
			v_die("index %lu out-of-bounds, v->count=%lu\n", idx, v->count);
		}
		v_error("index %lu out-of-bounds, v->count=%lu\n", idx, v->count);
		return VECTOR64_MAX;
	}

	/* Remember the deleted element, then shift all of the other elements
	 * in the array down one slot. This is somewhat "repeatable" - if a
	 * failure occurs while we're doing this, then we can either remember
	 * the shift index (i.e. inside of the vector64 struct, which we currently
	 * don't to), or look through the array for duplicates, and then resume
	 * where we left off (but this is unreliable with integer elements). We
	 * flush after every single shift (ouch!) so that no data can be lost.
	 * This is similar (but opposite) to vector_insert() above.
	 */
	old_e = v->array[idx];
	v_debug("deleting element %lu from slot %lu\n", old_e, idx);
	for (i = idx; i < v->count - 1; i++) {
		v->array[i] = v->array[i+1];
		kp_flush_range(&(v->array[i]), sizeof(unsigned long), v->use_nvm);
		v_debug("shifted element %lu from slot %lu down to slot %lu\n",
				v->array[i], i+1, i);
	}
	v->count--;
	kp_flush_range((unsigned long *)&(v->count), sizeof(unsigned long),
			v->use_nvm);
	v_debug("now count=%lu, size=%lu (resize factor=%f)\n", v->count,
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
	 * will be half-filled after it is shrunk).
	 * //Also, we allow
	 * //the resize factor to be non-integral, so we cast the computation
	 * //results back to uint64_t; there could presumably be rounding errors
	 * //here, but during debugging such errors did not occur, and even if
	 * //they do, it shouldn't be a big deal.
	 */
	if ((v->size > VECTOR64_INIT_SIZE) &&
	    (v->count <= v->size / (VECTOR64_RESIZE_FACTOR * 2))) {
		v_debug("count %lu is <= %lu, shrinking array\n", v->count,
				(unsigned long)(v->size / (VECTOR64_RESIZE_FACTOR * 2)));
		v_debug("Todo: move this code into its own function, e.g. "
				"vector_resize_down()\n");

		/* See notes about flush_size in vector_append() above. */
		if (v->use_nvm) {
			flush_size = offsetof(vector64, count) - offsetof(vector64, array);
#ifdef VECTOR_ASSERT
			if (flush_size < 
					(sizeof(unsigned long *) + sizeof(unsigned long))) {
				v_die("got unexpected flush_size %d! offsetof(count)=%zu, "
						"offsetof(array)=%zu\n", flush_size,
						offsetof(vector64, count), offsetof(vector64, array));
			}
#endif
		}

		/* We set the vector's new size first, then set its array pointer, and
		 * then finally flush them both to memory (if use_nvm is true). See
		 * the notes in vector_append() for this. */
		/* Leak window begin: */
		size_t old_size = v->size;
		v->size /= VECTOR64_RESIZE_FACTOR;  //inverse of vector_append()
		kp_realloc((void **)&(v->array), sizeof(unsigned long) * v->size, sizeof(unsigned long) * old_size,
				v->use_nvm);
		kp_flush_range(&(v->array), flush_size, v->use_nvm);
		/* Leak window end. If we fail after the flush has returned, then
		 * the next call to vector_delete() will skip the resizing step. */
		if (v->array == NULL) {
			v_die("kp_realloc(array) failed\n");
		}
		v_debug("shrunk array, now has size %zu (%lu slots)\n",
				sizeof(unsigned long) * v->size, v->size);
	}

	return old_e;
}

unsigned long vector64_search_linear(const vector64 *v, unsigned long key)
{
	unsigned long i;

	if (v == NULL) {
		v_error("v is NULL\n");
		return VECTOR64_MAX;
	}

	/* Only search up to count, not size! */
	for (i = 0; i < v->count; i++) {
		if (v->array[i] == key) {
			v_debug("found search key %lu at idx=%lu\n", key, i);
			return i;
		}
	}

	v_debug("searched through all %lu elements, did not find search key "
			"%lu\n", v->count, key);
	return VECTOR64_MAX;
}

unsigned long vector64_search_binary(const vector64 *v, unsigned long key,
		bool exact)
{
	unsigned long val, len, lower, upper, range, idx;
#ifdef VECTOR_DEBUG
	char *v_str;
#endif

	idx = 0;
	len = v->count;
	lower = 0;
	upper = len - 1;
	v_debug("initial lower=%lu, upper=%lu, len=%lu\n", lower, upper, len);
#ifdef VECTOR_DEBUG
	v_str = vector64_to_string(v);
	if (v_str) {
		v_debug("search vector contents: %s\n", v_str);
		free(v_str);
	}
#endif

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
		val = v->array[0];
		if (exact) {
			if (val == key) {
				return 0;
			} else {
				return VECTOR64_MAX;
			}
		} else {  //non-exact
			if (val <= key) {
				return 0;  //key is valid "to the right" of the single element
			} else {
				return VECTOR64_MAX;
			}
		}
	}

	while ((lower <= upper) && (lower != VECTOR64_MAX) && (upper != VECTOR64_MAX)) {
		range = upper - lower;
		idx = lower + (range / 2);
		//v_debug("len=%lu, lower=%lu, upper=%lu, range=%lu, idx=%lu\n",
		//      len, lower, upper, range, idx);
		val = v->array[idx];
		//v_debug("at idx=%lu got val=%lu (key=%lu)\n", idx, val, key);
		if (val == key) {
			/* Keep scanning to the right, in case there are multiple
			 * items with the same value:
			 */
			while((idx+1 < len) && (v->array[idx+1] == key)) {
				//v_debug("element at idx+1 (%lu) also matches, shifting right\n",
				//      idx+1);
				v_warn("found multiple items with the same value (%lu) while "
						"scanning right in the array; is this expected???\n",
						key);
				idx++;
			}
			//v_debug("returning LAST idx (%lu) that matches value\n", idx);
			return idx;
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
			if ((idx < len - 1) && (!exact) && (v->array[idx+1] > key)) {
				//v_debug("v[%lu]=%lu, v[%lu]=%lu, so returning "
				//      "idx=%lu for range search\n", idx, val, idx+1, 
				//      v->array[idx+1], idx);
				return idx;
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
	 * range, and we assume that the final element is always valid up to
	 * infinity).
	 */
	if (!exact && idx == len - 1) {
		return idx;
	}

	return VECTOR64_MAX;
}

void vector64_destroy(vector64 **v)
{
	bool use_nvm;
	
	if (!v) {
		v_error("got NULL argument unexpectedly; returning\n");
		return;
	}

	if (*v) {
		use_nvm = (*v)->use_nvm;
		(*v)->state = STATE_DEAD;
		kp_flush_range((unsigned long *)&((*v)->state), sizeof(ds_state),
				use_nvm);

		if ((*v)->array) {
			v_debug("testing 0: (*v)->array = %p\n", (*v)->array);
			kp_free((void **)&((*v)->array), use_nvm);  //sets (*v)->array to NULL
#ifdef VECTOR_ASSERT
			v_debug("testing 1: (*v)->array = %p\n", (*v)->array);
			if (use_nvm && (*v)->array != NULL) {
				v_die("kp_free() did not set (*v)->array to NULL as "
						"expected!\n");
			}
#endif
		}
		v_debug("testing 2: *v = %p\n", *v);
		kp_free((void **)(v), use_nvm);  //sets *v to NULL after free
#ifdef VECTOR_ASSERT
		v_debug("testing 3: *v = %p\n", *v);
		if (use_nvm && *v != NULL) {
			v_die("kp_free() did not set *v to NULL as expected!\n");
		}
#endif
	}
	v_debug("freed vector64's array and vector64 struct itself\n");
}

size_t vector64_struct_size()
{
	return sizeof(vector64);
}

#define VECTOR64_MAX_STRLEN 2048
char *vector64_to_string(const vector64 *v)
{
	int len;
	unsigned long i;
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
		snprintf(littlestring, VECTOR64_MAX_STRLEN, "[%lu:%lu]", i,
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
