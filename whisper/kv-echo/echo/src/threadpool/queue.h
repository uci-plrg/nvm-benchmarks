/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 */

/* A simple queue module. The queue is NOT thread-safe!
 *
 * This interface and implementation was first used for a non-volatile
 * memory project, so some artifacts of this are present in the code.
 * If this queue is to be used for some other project, then the memory
 * allocation functions can be modified to use the standard malloc and
 * free, etc., without too much difficulty.
 */

#ifndef QUEUE_H__
#define QUEUE_H__

#include <stdbool.h>

/* Opaque handle: */
struct _queue;
typedef struct _queue queue;

/* Allocates and initializes an empty queue. The use_nvm argument indicates
 * whether or not this queue will be stored in non-volatile memory.
 * Returns: 0 on success, -1 on error. On success, *q is set to point
 * to the new queue.
 */
int queue_create(queue **q, bool use_nvm);

/* Destroys the queue. If the queue is not empty, then the free function
 * that was passed along with each element at enqueue time will be called
 * on the element before the queue is destroyed.
 */
void queue_destroy(queue *q);

/* Signature for a function that frees a queue element. Queue elements
 * are just arbitrary pointers. */
typedef void (queue_element_free)(void *);

/* Adds an element to the end of the queue. This function does not make
 * a copy of the element, so after calling it, the caller must not use
 * the element until it is returned from a dequeue. A function to free
 * the queue element can also be specified; if this function is non-NULL,
 * then it will be called on the element if it is still in the queue
 * when queue_destroy() is called.
 * Returns: 0 on success, -1 on error.
 */
int queue_enqueue(queue *q, void *e, queue_element_free free_fn);

/* Removes the element from the front of the queue.
 * Returns: 0 on success, 1 if the queue was empty, -1 on error. On success,
 * *e will be set to the removed element.
 */
int queue_dequeue(queue *q, void **e);

/* Returns: the number of elements in the queue. */
unsigned int queue_length(queue *q);

/* Returns: true if the queue has no elements, false otherwise. */
bool queue_is_empty(queue *q);

#endif /* QUEUE_H__ */

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
