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

/* Set to 1 if we should die on index out-of-bounds errors, or 0 if we
 * should just return an error.
 */
#define VECTOR_DIE_ON_OOB 1

/* Opaque handle: */
struct vector_;
typedef struct vector_ vector;

/* Allocates and initializes a vector. The vector should later be freed
 * by passing it to vector_free().
 * Returns: 0 on success, -1 on error. On success, *v will be set to point
 * to the newly-allocated vector.
 */
int vector_alloc(vector **v);

/* Returns: the number of elements in the vector.
 * Important: be careful about calling vector_count() directly in the
 * loop-condition of a for/while loop: it will be re-called on every loop!
 */
unsigned long long vector_count(vector *v);

/* Appends an element to the vector.
 * Note that currently, if the number of appended elements goes over the
 * maximum (2^n - 1, where n is the number of bits in an unsigned long long),
 * then undefined behavior will result (this error case is not checked
 * for).
 * Returns: 0 on success, -1 on error.
 */
int vector_append(vector *v, void *e);

/* Replaces the element at the specified index.
 * Returns: 0 on success, -1 on error. On success, *old_e is set to point
 * to the element that was previously stored in the slot.
 */
int vector_set(vector *v, unsigned long long idx, void *e, void **old_e);

/* Gets the element at the specified index. If VECTOR64_DIE_ON_OOB is set
 * to true, then the only other error case for this function is if the pointer
 * that is passed to it is NULL, so if you're lazy, then you can skip
 * error-checking this function's return value.
 * Returns: 0 on success, -1 on error. On success, *e is set to point to
 * the gotten element.
 */
int vector_get(vector *v, unsigned long long idx, void **e);

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
 * just before calling vector_free(v).
 */
void vector_free_contents(vector *v);

/* Frees the vector's array and the vector struct itself. NOTE: if the
 * array contains pointers to other data, the data that is pointed to
 * is NOT freed!
 */
void vector_free(vector *v);

/* Returns: the size of the vector struct */
unsigned int vector_struct_size();

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
