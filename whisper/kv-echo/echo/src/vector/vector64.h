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
 * This vector stores unsigned 64-bit integers (uint64_t) as its elements,
 * and can store up to 2^64 - 2 (UINT64_MAX - 1) of them.
 */

#ifndef VECTOR64_H__
#define VECTOR64_H__

#include <stdint.h>

/* Set to 1 if we should die on index out-of-bounds errors, or 0 if we
 * should just return an error.
 */
#define VECTOR64_DIE_ON_OOB 1

/* Opaque handle: */
struct vector64_;
typedef struct vector64_ vector64;

/* Allocates and initializes a vector. The vector should later be freed
 * by passing it to vector_free().
 * Returns: 0 on success, -1 on error. On success, *v will be set to point
 * to the newly-allocated vector.
 */
int vector64_alloc(vector64 **v);

/* Returns: the number of elements in the vector, or UINT64_MAX on error.
 * (The only error case for this function is if the pointer that is passed
 * to it is NULL, so if you're lazy, then you can skip error-checking this
 * function's return value.)
 * Important: be careful about calling vector64_count() directly in the
 * loop-condition of a for/while loop: it will be re-called on every loop!
 */
uint64_t vector64_count(vector64 *v);

/* Appends an element to the vector. Valid element values range from 0 to
 * UINT64_MAX-1.
 * Returns: 0 on success, -1 on error.
 */
int vector64_append(vector64 *v, uint64_t e);

/* Replaces the element at the specified index. Valid element values range
 * from 0 to UINT64_MAX-1.
 * Returns: the element that was previously stored in the slot on success,
 * or UINT64_MAX on error.
 */
uint64_t vector64_set(vector64 *v, uint64_t idx, uint64_t e);

/* Gets the element at the specified index. If VECTOR64_DIE_ON_OOB is set
 * to true, then the only other error case for this function is if the pointer
 * that is passed to it is NULL, so if you're lazy, then you can skip
 * error-checking this function's return value.
 * Returns: the specified element, or UINT64_MAX on error.
 */
uint64_t vector64_get(vector64 *v, uint64_t idx);

/* Removes the element at the specified index, and shifts all of the
 * remaining elements down in the vector.
 * Returns: the element that was stored at the specified index, or
 * UINT64_MAX on error.
 */
uint64_t vector64_delete(vector64 *v, uint64_t idx);

/* Performs a linear search through the vector for the key, suitable for
 * an unsorted vector.
 * Returns: on success, 0 is returned and *idx is set to the index where
 * the key was found. 1 is returned if the key was not found. -1 is
 * returned on error.
 */
int vector64_search_linear(vector64 *v, uint64_t key, uint64_t *idx);

/* Frees the vector.
 */
void vector64_free(vector64 *v);

/* Returns: the size of the vector struct */
unsigned int vector64_struct_size();

/* Creates a string with the contents of the vector, which must be 
 * freed by the caller (if it is non-null)!
 * Returns: pointer to the string, or NULL on error.
 */
char *vector64_to_string(vector64 *v);


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
