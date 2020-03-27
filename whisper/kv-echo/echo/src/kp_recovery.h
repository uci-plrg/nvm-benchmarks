/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Peter Hornyack & Katelin Bailey
 * 2/14/12
 *
 * Defines a "flush" operation that explicitly flushes data from all cache
 * levels of all processor cores. This operation requires x86 instructions
 * that were added with Intel's SSE2 extensions, which were first used in
 * Pentium 4 processors, so modern processors should support them.
 *
 * Inspired by "Consistent and Durable Data Structures for Non-Volatile
 * Byte-Addressable Memory" paper:
 *   http://www.usenix.org/event/fast11/tech/full_papers/Venkataraman.pdf
 *
 * Other references:
 *   Intel 64 and IA-32 Architectures Developer's Manual:
 *     http://www.intel.com/content/www/us/en/architecture-and-technology/64-ia-32-architectures-software-developer-manual-325462.html
 *   http://siyobik.info/main/reference/instruction/CLFLUSH
 *   http://lkml.indiana.edu/hypermail/linux/kernel/0707.2/2949.html
 */

#ifndef KP_RECOVERY_H__
#define KP_RECOVERY_H__

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

/* Define this variable to enable flushing; leave it undefined to turn
 * the functions in kp_recovery.c into no-ops.
 */
#define FLUSH_IT
//#define R_DEBUG
//#define R_ASSERT

/* Enumerates the different states that a data structure can be in.
 * ALLOCATED must be equivalent to 0, so that it is the default state
 * when a structure is allocated with calloc. After a data structure
 * has been allocated and initialized, the last step before it is
 * returned to the caller should be to set it to ACTIVE. Finally, when
 * a data structure is being destroyed, the first step in the destruction
 * process should be to set its state to DEAD.
 *
 * Data structures that use these states MUST put the state as the last
 * member of the struct, and should switch between states by using the
 * helper functions below!
 */
typedef enum ds_state_enum {
	STATE_ALLOCATED = 0,
	STATE_ACTIVE,
	STATE_DEAD
} ds_state;
#define ds_state_to_string(state)                \
	((state) == STATE_ALLOCATED ? "ALLOCATED" :  \
	 (state) == STATE_ACTIVE    ? "ACTIVE"    :  \
	 (state) == STATE_DEAD      ? "DEAD"      : "UNKNOWN!")

/* Add a function like this?:
 *   commit_state_change(void *ds, size_t ds_size, size_t state_offset,
 *       ds_state new_state)
 * That performs the following?:
 *   mfence
 *   sets [ds+state_offset] to new_state
 *   mfence
 *   flush_range(ds, ds_size)
 * ??
 *
 * This would work to "commit" all pointers and non-pointer fields in the
 * data structure, but what about sub-data structures that have already
 * been updated durably? Can we recover those changes (get back to previous
 * state?) coherently??
 */

#if 0
/* Sets the state member of a data structure to ACTIVE. For a data structure
 * such as:
 *   struct ds {
 *     int a;
 *     ds_state state;
 *   }
 * This function should be called using the offsetof macro, like this:
 *     set_ds_state_active((void *)ds_instance, offsetof(struct ds, state));
 * (offsetof is defined in stddef.h).
 *
 * This function uses memory barrier instructions internally to guarantee
 * that the state-change operation is not re-ordered.
 */
void set_ds_state_active(void *ds, size_t offset);

/* See the description of set_ds_state_dead() for an example of how to use
 * this function.
 */
void set_ds_state_dead(void *ds, size_t offset);
#endif

/* Allocates a memory region of the specified size and stores a pointer to
 * the memory in *ptr. If use_nvm is true, then the memory will be
 * initialized to zero and flushed to memory before this function returns
 * (if FLUSH_IT is defined).
 *
 * We may call this function from places where we only want calloc
 * on NVRAM, and only need malloc on VRAM (as selected by the use_nvm flag).
 * Currently, we'll use calloc for VRAM as well; it would be a minor
 * optimization to adjust this.
 *
 * On error, *ptr will be set to NULL. */
void kp_calloc(void **ptr, size_t size, bool use_nvm);

/* Allocates a memory region of the specified size and stores a pointer to
 * the memory in *ptr. No flushing is performed, even if use_nvm is true;
 * use this function when allocating a memory region for which it doesn't
 * matter if you know the state after a failure or not (e.g. a string
 * that is pointed to by some larger data structure, or ...), because if
 * a failure occurs then you'll just free it anyway.
 *   Basically, if a memory region doesn't have a "state" field, then
 *   you can probably allocate it with kp_malloc(); if the object needs
 *   to keep track of its state, then use kp_calloc().
 *
 * On error, *ptr will be set to NULL. */
void kp_malloc(void **ptr, size_t size, bool use_nvm);

/* Similar to kp_calloc and kp_malloc, but uses calloc when use_nvm is
 * true and uses malloc when use_nvm is false.
 */
void kp_kpalloc(void **ptr, size_t size, bool use_nvm);

/* Re-allocates the memory region pointed to by *ptr.
 * NOTE: currently, this function does NOT zero-allocate the new memory
 * region at all, nor does it flush it. The caller should do these things
 * if they are necessary. */
void kp_realloc(void **ptr, size_t new_size, size_t old_size, bool use_nvm);

/* Copies n bytes of memory from src to dest. If use_nvm is true, then
 * this function will also flush the memory range that was copied to
 * before returning (if FLUSH_IT is defined). */
void kp_memcpy(void *dest, const void *src, size_t n, bool use_nvm);

/* Copies up to n characters from src to dest. If use_nvm is true, then
 * this function will also flush the memory range that was copied to
 * before returning (if FLUSH_IT is defined). */
void kp_strncpy(char *dest, const char *src, size_t n, bool use_nvm);

/* Frees a memory region that was allocated using any of the kp_*alloc()
 * functions, and sets *ptr to NULL.The use_nvm flag passed to this function
 * must be set in the same way that it was when the memory was allocated. */
void kp_free(void **ptr, bool use_nvm);

/* Frees the memory region pointed to by *ptr. Additionally, *ptr is set
 * to NULL and flushed to memory (if FLUSH_IT is defined) before this function
 * returns.
 * If your function that calls pcm_free has a pointer argument, don't take
 * the address (&) of that pointer and pass it to pcm_free: the pointer
 * value is on the stack, so it will be set to NULL, but the caller's version
 * will not be!! Instead, the caller should take the address of the pointer
 * before passing it to your function.
 */
//void pcm_free(void **ptr);

/* This value represents the size of a single cache line, and is processor-
 * specific, so ideally it would be detected at runtime. It is defined
 * statically here for now though; on various machines I've checked (using
 * 'cat /proc/cpuinfo | grep clflush'), 64 bytes is the correct value.
 * Cache line sizes (or some kind of size...) can also be checked by:
 *   cat /sys/devices/system/cpu/cpu0/cache/index2/coherency_line_size
 * (these directories contain lots of other useful information about the
 * processor's caches too: total size, type (I vs. D), shared or not, etc.)
 */
#define CLFLUSH_SIZE 64

/* Flushes the memory contents in the address range [addr, addr+size-1]
 * from all of the cache levels of all processor cores back to main memory.
 * This function does not perform any error checking, so don't pass it any
 * NULL pointers, negative sizes, etc.
 *
 * flush_range() can be tricky to get right - remember, if you just set
 * some value or some member of a struct, then you probably want to take
 * the _address_ (e.g. the & operator) of the thing that you just set and
 * pass that as the argument to flush_range().
 * If you have a pointer to a struct that you want to flush, then you want
 * to pass the pointer itself to flush_range(), because the _value_ of the
 * pointer is the beginning address for the range to flush. If in doubt,
 * it may be easier to dereference the first member of the struct and then
 * take the address of that whole thing, but this is fragile because the
 * "first" member of the struct may change!
 *
 * Returns: the number of cache line flushes that had to be performed to
 * flush the entire range.
 */
unsigned int flush_range(const void *addr, const size_t size);

/* Convenience wrapper: if use_nvm is true, then this function simply
 * calls flush_range() as defined above. If use_nvm is false, then this
 * function is a noop. Also note that flush_range() is affected by the
 * FLUSH_IT definition.
 * Definition:
 *   void kp_flush_range(void *addr, size_t size, bool use_nvm); */
#define kp_flush_range(addr, size, use_nvm) do {  \
	if (use_nvm) {                                \
		flush_range(addr, size);                  \
	}                                             \
} while (0)

/* Just a convenience wrapper for a single memory fence: */
#define kp_mfence() do {  			\
	PM_FENCE();				\
	__asm__ __volatile__ ("mfence");       	\
} while (0)

/* Flushes all cache levels of all cores.
 */
//void flush_caches();

/* Resets the memory accounting statistics to zeros. Because I tacked on
 * the data/metadata accounting by instrumenting the memory allocation
 * functions in this module at a late point in development, this means that
 * the accounting is done on a global basis - if a master store is created,
 * then destroyed, then another is created, the second store will inherit
 * the stats counters from the first store if this function is not called
 * (if we perform allocation / de-allocation correctly, then all of the
 * counts should be reset to zeros anyway though... so this function is kind
 * of a hack in that regard). Therefore, we should just call this function
 * once for every new master store that is created - this means that our
 * memory accounting will work fine as long as only one master store is
 * active at a time. This is another way that this is kind of a hack... oh
 * well.
 */
void kp_reset_mem_accounting();

/* Fills in *mem_data and *mem_metadata with the global counts of currently
 * allocated data and metadata, respectively.
 */
void kp_get_mem_accounting(uint64_t *mem_data, uint64_t *mem_metadata);
 
/** MACROS: **/

/* Print normal and debug output to stdout, warning and error output to
 * stderr. Always flush after printing; this makes debugging etc. easier,
 * but possibly causes slowdown.
 */
#define r_print(f, a...)  do { \
	fprintf(stdout, "RECOVERY: %lu: " f, pthread_self(), ##a); \
	fflush(stdout); \
	} while(0)
#define r_warn(f, a...)  do { \
	fprintf(stderr, "**WARNING**: %lu: %s: " f, pthread_self(), __func__, ##a); \
	fflush(stderr); \
	} while(0)
#define r_error(f, a...)  do { \
	fprintf(stderr, "ERROR: %lu: %s: " f, pthread_self(), __func__, ##a); \
	fflush(stderr); \
	} while(0)
#define r_test(f, a...) fprintf(stdout, "TEST: %lu: " f, pthread_self(), ##a)

#ifdef R_DEBUG
#define r_debug(f, a...)  do { \
	fprintf(stdout, "DEBUG_R: %lu: %s: " f, pthread_self(), __func__, ##a); \
	fflush(stdout); \
	} while(0)
#else
#define r_debug(f, a...)  do { ; } while(0)
#endif

/* die by abort()ing; is exit(-1) better? */
#define r_die(f, a...)  do { \
	fprintf(stderr, "RECOVERY: Fatal error (%lu: %s): " f, pthread_self(), __func__, ##a); \
	fflush(stderr); \
	abort(); \
	} while(0)

#endif  //KP_RECOVERY_H

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
