/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Forked from: https://gist.github.com/953968
 *
 * This implementation is "thread-safe," meaning that multiple clients
 * can create and use vectors that are completely independent of each
 * other; the memory-management functions (malloc, realloc, free) that
 * are used internally are thread-safe. However, this DOES NOT mean
 * that multiple readers/writers to the SAME vector will not step on each
 * other; the client must perform its own synchronization for the vector.
 *
 * This vector stores unsigned longs as its elements, which should be
 * 64-bits wide on a 64-bit Linux distribution (I know this) and 32-bits
 * wide on a 32-bit Linux distribution (I haven't confirmed this). A vector
 * can store up to 2^n - 1 elements, where n is the number of bits in an
 * unsigned long.
 */

#ifndef VECTOR64_H__
#define VECTOR64_H__

#include <stdint.h>
#include <stdbool.h>

/* Set to 1 if we should die on index out-of-bounds errors, or 0 if we
 * should just return an error.
 */
#define VECTOR64_DIE_ON_OOB 1

/* Opaque handle: */
struct vector64_;
typedef struct vector64_ vector64;
#define VECTOR64_MAX ((unsigned long)(0-1))

/* Allocates and initializes a vector. The vector should later be freed
 * by passing it to vector_free(). The initial vector size can be passed
 * to this function, or 0 can be passed and this function will use a default
 * size. Set use_nvm to true to perform the additional steps necessary for
 * use on non-volatile memory (i.e. initializing memory to 0, flushing, etc.).
 * Returns: 0 on success, -1 on error. On success, *v will be set to point
 * to the newly-allocated vector.
 */
int vector64_create(vector64 **v, unsigned long size, bool use_nvm);

/* Returns: the number of elements in the vector.
 * Important: be careful about calling vector64_count() directly in the
 * loop-condition of a for/while loop: it will be re-called on every loop!
 */
unsigned long vector64_count(const vector64 *v);

/* Appends an element to the vector. Valid element values range from 0 to
 * VECTOR64_MAX - 1.
 *
 * FAILURE properties: when resizing is necessary, this function contains
 * an internal memory leak window. Vector appends are atomic: the append
 * will either happen (and be flushed durably to memory if use_nvm is true)
 * or not, and no partial results/flushes are possible**. If the caller of
 * this function doesn't receive a return value, it is still possible that
 * the append succeeded, and the failure just occurred before the return
 * happened; so, after a failure, the caller should probably do a
 * vector_get() to check if the append actually happened or not (or check
 * the size before and after, etc.).
 *   **: assuming that the vector's internal data pointer and size value
 *       are kept in the same cache line, which is not necessarily
 *       guaranteed but has been empirically true so far.
 *
 * Returns: the new vector count on success, or VECTOR64_MAX on error.
 * On error, the vector may be corrupted and unusable.
 */
unsigned long vector64_append(vector64 *v, unsigned long e);

/* Inserts an element into the vector in sorted order. Valid element values
 * range from 0 to VECTOR64_MAX - 1. If allow_duplicates is set to false,
 * then this function will not insert the element into the vector a second
 * time if it is already present.
 *
 * Each vector should use either vector_insert or vector_append, but should
 * probably not mix them. This function performs a binary search to
 * efficiently find the slot to insert into, but then must shift every
 * element from the insertion slot to the back of the array down by one slot,
 * which is an O(n) operation :(
 *
 * Returns: the index that the element was inserted into, or VECTOR64_MAX
 * on error. If allow_duplicates is false and the element was already present
 * in the vector, this function will return the index where it was already
 * found.
 */
unsigned long vector64_insert(vector64 *v, unsigned long e,
		bool allow_duplicates);

/* Replaces the element at the specified index. Valid element values range
 * from 0 to VECTOR64_MAX - 1.
 * Returns: the element that was previously stored in the slot on success,
 * or VECTOR64_MAX on error.
 */
unsigned long vector64_set(vector64 *v, unsigned long idx, unsigned long e);

/* Gets the element at the specified index. If VECTOR64_DIE_ON_OOB is set
 * to true, then the only other error case for this function is if the pointer
 * that is passed to it is NULL, so if you're lazy, then you can skip
 * error-checking this function's return value.
 * Returns: the specified element, or VECTOR64_MAX on error.
 */
unsigned long vector64_get(const vector64 *v, unsigned long idx);

/* Removes the element at the specified index, and shifts all of the
 * remaining elements down in the vector.
 * Returns: the element that was stored at the specified index, or
 * VECTOR64_MAX on error.
 */
unsigned long vector64_delete(vector64 *v, unsigned long idx);

/* Performs a linear search through the vector for the key, suitable for
 * an unsorted vector.
 *
 * Returns: the index where the key was found, or VECTOR64_MAX if not found.
 */
unsigned long vector64_search_linear(const vector64 *v, unsigned long key);

/* Performs a binary search in a vector of unsigned long values. The vector
 * should be sorted in ASCENDING order.
 *
 * If the "exact" parameter is true,
 * this function will only return found if the exact unsigned long key is
 * found in the vector; otherwise, the values in the vector will be taken
 * as the BEGINNING of a RANGE of valid values for each element, and
 * the element whose range includes the key will be returned (the last
 * element is assumed to valid up to infinity).
 * 
 * Additionally, this function checks if there are multiple (adjacent,
 * because the vector is sorted) elements with a matching value/range,
 * and returns the LAST matching element that it finds (the one with the
 * highest index).
 *
 * Returns: the index where the key was found, or VECTOR64_MAX if not found.
 */
unsigned long vector64_search_binary(const vector64 *v, unsigned long key,
		bool exact);

/* Frees the vector's array and the vector struct itself.
 * To help make vectors a "consistent and durable data structure," this
 * function sets the value of *v to NULL after it has freed its memory.
 * This is why this function requires a vector** argument, rather than
 * just a vector*.
 */
void vector64_destroy(vector64 **v);

/* Returns: the size of the vector64 struct. */
size_t vector64_struct_size();

/* Creates a string with the contents of the vector, which must be 
 * freed by the caller (if it is non-null)!
 * Returns: pointer to the string, or NULL on error.
 */
char *vector64_to_string(const vector64 *v);

#endif  //VECTOR64_H

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
