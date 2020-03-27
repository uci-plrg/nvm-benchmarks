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
 * This vector stores generic void* as its elements, and can store up to
 * 2^n - 1 of them, where n is the number of bits in an unsigned long long.
 */

#ifndef VECTOR_H__
#define VECTOR_H__

#include <stdbool.h>
#include <stdint.h>

/* Set to 1 if we should die on index out-of-bounds errors, or 0 if we
 * should just return an error.
 */
#define VECTOR_DIE_ON_OOB 1

/* Opaque handle: */
struct vector_;
typedef struct vector_ vector;

/* Allocates and initializes a vector. The vector should later be freed
 * by passing it to vector_destroy(). The initial vector size can be passed
 * to this function, or 0 can be passed and this function will use a default
 * size. Set use_nvm to true to perform the
 * additional steps necessary for use on non-volatile memory (i.e.
 * initializing memory to 0, flushing, etc.).
 * Returns: 0 on success, -1 on error. On success, *v will be set to point
 * to the newly-allocated vector.
 */
int vector_create(vector **v, unsigned long long size, bool use_nvm);

/* Returns: the number of elements in the vector.
 * Important: be careful about calling vector_count() directly in the
 * loop-condition of a for/while loop: it will be re-called on every loop!
 */
unsigned long long vector_count(const vector *v);

/* Appends an element to the vector. For now, DON'T put any NULL pointers
 * into the vector (although this function doesn't check for this condition).
 * Note that currently, if the number of appended elements goes over the
 * maximum (2^n - 1, where n is the number of bits in an unsigned long long),
 * then undefined behavior will result (this error case is not checked
 * for).
 * FAILURE properties: when resizing is necessary, this function contains
 * an internal memory leak window. Vector appends are atomic: the append
 * will either happen (and be flushed durably to memory) or not, and no
 * partial results/flushes are possible**. If the caller of this function
 * doesn't receive a return value, it is still possible that the append
 * succeeded, and the failure just occurred before the return happened; so,
 * after a failure, the caller should probably do a vector_get() to check
 * if the append actually happened or not (or check the size before and
 * after, etc.).
 *   **: assuming that the vector's internal data pointer and size value
 *       are kept in the same cache line, which is not necessarily
 *       guaranteed but has been empirically true so far.
 *
 * NOTE: for version 3.1 (simultaneous merges with conflict detection), I
 * hacked this interface to return the element that used to be the last
 * element in the vector, before we appended this one. Hmmm...
 * The caller should probably set *previous_tail to NULL before calling
 * this function: we don't flush previous_tail after setting it, so the
 * caller should flush it itself if it needs to be durable, and pre-setting
 * it to NULL allows the caller to know if the store succeeded (I think...).
 *
 * Returns: 0 on success, -1 on error. On error, the vector may be corrupted
 * and unusable.
 */
int vector_append(vector *v, const void *e, void **previous_tail);

/* vector_comparator should take two vector elements and return 0 if e1 == e2,
 * return negative if e1 < e2, and return positive if e1 > e2. The function
 * should probably return negative if an error occurs... */
typedef int (*vector_comparator)(const void *e1, const void *e2);

/* Inserts an element into the vector in sorted order. Each vector should
 * user either vector_insert or vector_append, but should probably
 * not mix them, of course. This function actually performs a linear search
 * in the vector for the correct location to insert into, starting from the
 * _back_ of the vector; for version tables, this is expected to be the
 * most efficient.
 * Returns: the index that the element was inserted into, or UINT64_MAX on
 * error.
 */
uint64_t vector_insert(vector *v, const void *e, vector_comparator cmp);

/* Replaces the element at the specified index. For now, DON'T put any
 * NULL pointers into the vector (although this function doesn't check
 * this condition!).
 * Returns: 0 on success, -1 on error. On success, *old_e is set to point
 * to the element that was previously stored in the slot.
 */
int vector_set(vector *v, unsigned long long idx, void *e, void **old_e);

/* Gets the element at the specified index. If VECTOR_DIE_ON_OOB is set
 * to true, then the only other error case for this function is if the pointer
 * that is passed to it is NULL, so if you're lazy, then you can skip
 * error-checking this function's return value.
 * Returns: 0 on success, -1 on error. On success, *e is set to point to
 * the gotten element.
 */
int vector_get(const vector *v, unsigned long long idx, void **e);

/* Removes the element at the specified index, and shifts all of the
 * remaining elements down in the vector. Importantly, the element
 * itself is NOT freed; if e is non-NULL, then *e is set to point to
 * the element, so that the caller can free it.
 * Returns: 0 on success, -1 on error.
 */
int vector_delete(vector *v, unsigned long long idx, void **e);

/* Calls free() on all non-null pointers that are stored in the vector.
 * It does not remove these pointers from the vector however, so the
 * vector's element count will be unchanged.
 * USE THIS FUNCTION WITH CAUTION: it should probably only be called
 * just before calling vector_destroy(v).
 */
void vector_free_contents(vector *v);

/* Frees the vector's array and the vector struct itself. NOTE: if the
 * array contains pointers to other data, the data that is pointed to
 * is NOT freed!
 *
 * To help make vectors a "consistent and durable data structure," this
 * function sets the value of *v to NULL after it has freed its memory.
 * This is why this function requires a vector** argument, rather than
 * just a vector*.
 */
void vector_destroy(vector **v);

/* Returns: the size of the vector struct */
unsigned int vector_struct_size();

/* Performs a linear search through a vector by indexing into each element
 * at _offset_, which should be the offsetof a uint64_t value (no other type
 * will work!!!).
 *
 * Returns: 0 if found, 1 if not-found, -1 on error. If found, *idx is set
 * to the index of the found element.
 */
int vector_search_linear(const vector *v, size_t offset, uint64_t key,
		uint64_t *idx);

/* vector_validator should take a vector element and return true if it is
 * "valid" (for whatever the application's definition of valid is) or false
 * if it is invalid.
 *
 * For the kp_kvstore, valid means that the commit record that this VTE
 * belongs to is COMMITTED. */
typedef bool (*vector_validator)(const void *e1);

/* Performs a binary search inside of a vector. Because the vector stores
 * just void* pointers, the caller specifies the offset in the void* data
 * of a uint64_t type (no other type will work!!). This offset can be
 * specified using the offsetof operator:
 *     offsetof(data_structure, num64)
 * For the binary search to work, the vector must be sorted in ASCENDING
 * order by num64. If the last argument (exact) is true, then
 * this function will only return found if the exact uint64_t key is
 * found in the vector; otherwise, the values in the vector will be taken
 * as the BEGINNING of a RANGE of valid values for each element, and
 * the element whose range includes the key will be returned (the last
 * element is assumed to be valid up to infinity).
 * Additionally, this function checks if there are multiple (adjacent,
 * because the vector is sorted) elements with a matching value/range,
 * and returns the LAST matching element that it finds (the one with the
 * highest index).
 *
 * Last-minute hack: if validator is not NULL, then after finding a candidate
 * element via binary search, this function will perform a linear search
 * backwards through the vector to find the first element that is valid
 * according to the invalidator. If this behavior is not desired, then
 * set validator to NULL.
 *
 * Returns: the index where the key was found, or UINT64_MAX if not found.
 */
uint64_t vector_search_binary(const vector *v, size_t offset, uint64_t key,
		bool exact, vector_validator validator);

#endif  //VECTOR_H

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
