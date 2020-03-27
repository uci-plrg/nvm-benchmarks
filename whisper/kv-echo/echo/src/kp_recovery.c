/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 *
 * Peter Hornyack & Katelin Bailey
 * 2/14/12
 *
 * See kp_recovery.h header file for detailed description and notes.
 */

#include "kp_recovery.h"
#include <string.h>
#include <clibpm.h>

#if 0
/* This is an internal helper function, for now.
 * TODO: should this function _flush_ after it changes the state? Should
 * that be a boolean argument to this function??
 */
void set_ds_state(void *ds, size_t offset, ds_state state)
{
	void *ptr = ds + offset;                //is this safe?? Safe enough?
	ds_state *state_ptr = (ds_state *)ptr;  //is this safe?? Safe enough?
	//r_debug("sizeof(void **) = %u; sizeof(unsigned long long) = %u\n",
	//		sizeof(void **), sizeof(unsigned long long));
	r_debug("ds ptr=%p, offset=%u, so setting state at ptr=%p to %d (was %d)\n",
			ds, offset, state_ptr, state, *state_ptr);

	kp_mfence();
	*state_ptr = state;       //TODO: just replace this with assembly!!!
	kp_mfence();
}

void set_ds_state_active(void *ds, size_t offset)
{
	set_ds_state(ds, offset, STATE_ACTIVE);
}

void set_ds_state_dead(void *ds, size_t offset)
{
	set_ds_state(ds, offset, STATE_DEAD);
}
#endif

void *calloc_v(size_t size)
{
	return calloc(1, size);
}

void *malloc_v(size_t size)
{
	return malloc(size);
}

void free_v(void *ptr)
{
	free(ptr);
}

/* Allocates a memory region of the indicated size and stores the pointer
 * to it in *ptr. The memory is initialized to zeros and flushed before
 * returning. *ptr itself is _not_ flushed; we're ignoring the reachability
 * problem.
 * Returns: on success, *ptr is set to point to the newly-allocated memory;
 * on failure, *ptr will be NULL!
 */
void pcm_calloc(void **ptr, size_t size)
{
	/* Important: we use calloc rather than malloc to initialize the new
	 * memory to zeros! We must then flush the zeros, because the caller
	 * has to be able to know the exact state of the memory when they get
	 * their hands on it.
	 * The caller of this function is expected to check that *ptr is not
	 * null (i.e. calloc failed).
	 *
	 * Idea: to ensure that caller can't see *ptr before the memory has
	 * been initialized to zeros, we could use a temporary pointer, then
	 * an mfence, and only then set the caller's pointer.
	 *   Is this necessary? Way too expensive??
	 */
	*ptr = pmalloc(size);
	flush_range(*ptr, size);  //only has an effect if FLUSH_IT defined
	pmemalloc_activate(*ptr);
}

void pcm_malloc(void **ptr, size_t size)
{
	/* When allocating non-zeroed-out memory, we don't have to flush it,
	 * even if we're on pcm - the caller is using this function because
	 * it is telling us that it doesn't care about the initial state of
	 * the memory (because this object doesn't have a "state" field or
	 * and pointers that need to be initialized to a known value!). */
	*ptr = pmalloc(size);
	pmemalloc_activate(*ptr);
}

void pcm_free(void **ptr)
{
	/* Re-did this function because we decided that we can't solve the
	 * "leak windows", so we won't even try. This means that free is
	 * just a straight free: we don't need to flush the memory or
	 * anything. We still set *ptr to NULL because a bunch of our code
	 * expects this to happen. */
	pfree(*ptr);
	*ptr = NULL;
#if 0
	/* We copy the pointer value, set it to NULL and flush it _before_
	 * freeing the memory - by doing this, if we fail during the window
	 * between flush_range and free, the error will manifest itself as
	 * a memory leak (because the caller will see ptr as NULL after recovery),
	 * rather than as a double-free (if we reversed the order and failed
	 * after freeing but before setting the pointer to NULL, and then the
	 * caller tried to call this function again).
	 */
	void *tmp = *ptr;
	/* Leak window begin: */
	*ptr = NULL;
	flush_range(ptr, sizeof(void *));  //flush the pointer, not the memory range
	  /* 'ptr' is the address of a pointer (to the just-freed memory):
	   * flush the pointer value which is stored at the address 'ptr'.
	   */
	free(tmp);
	/* Leak window end. */
#endif
}

void kp_calloc(void **ptr, size_t size, bool use_nvm)
{
	if (use_nvm) {
		pcm_calloc(ptr, size);
	} else {
		*ptr = calloc_v(size);
	}
}

void kp_malloc(void **ptr, size_t size, bool use_nvm)
{
	if (use_nvm) {
		pcm_malloc(ptr, size);
	} else {
		/* malloc_v is just malloc, but for profiling etc., may be helpful
		 * to keep it in the malloc_v wrapper function.
		 * No flushing / fences needed. */
		*ptr = malloc_v(size); // persistent ??
	}
}

void kp_kpalloc(void **ptr, size_t size, bool use_nvm)
{
	if (use_nvm) {
		pcm_calloc(ptr, size);  //will do flushing!
	} else {
		*ptr = malloc_v(size); // persistent ???
	}
}

void kp_realloc(void **ptr, size_t new_size, size_t old_size, bool use_nvm)
{
	void *oldptr = *ptr;
	/* See where this is used for resizing in vector.c: we don't need to
	 * flush the new memory region here or zero-allocate it, because the
	 * vector uses its size rather than the contents of the memory to know
	 * what's there. */
	DEBUG("ignoring use_nvm=%s for kp_realloc(); caller (vector object) "
			"should flush as necessary!\n", use_nvm ? "true" : "false");
	if(use_nvm){
	    PM_EQU((*ptr), (pmalloc(new_size)));
	    kp_memcpy(*ptr, oldptr, old_size, use_nvm);	
	}
	else{
		*ptr = realloc(*ptr, new_size);
	}
}

void kp_memcpy(void *dest, const void *src, size_t n, bool use_nvm)
{
	if(use_nvm)
		PM_DMEMCPY(dest, src, n);
	else
		memcpy(dest, src, n);
	if (use_nvm) {
		flush_range(dest, n);
		  /* dest is the address of the first byte of the destination
		   * memory range; flush_range() will start at this address and
		   * flush n bytes. */
	}
}

/* IMPORTANT: strncpy copies up to n bytes from src to dest; if there is
 * no null-zero within n bytes of src, then dest will not be null-terminated!
 * This means that you want to perform the sequence like this, I think
 *   len = strlen(key);
 *   kp_malloc((void **)&((*pair)->key), len+1, use_nvm);
 *   kp_strncpy((*pair)->key, key, len+1, use_nvm);
 * Importantly, don't just pass _len_ to kp_strncpy()!!!
 */
void kp_strncpy(char *dest, const char *src, size_t n, bool use_nvm)
{
#ifndef R_ASSERT
	strncpy(dest, src, n);
#else
	char *ret;
	if(use_nvm)
		ret = PM_DSTRNCPY(dest, src, n);
	else
		ret = strncpy(dest, src, n);
	if (ret != dest) {
		r_die("strncpy: dest=%p, but ret=%p\n", dest, ret);
	}
#endif
	if (use_nvm) {
		/* dest is the address of the first byte of the destination
		 * memory range; flush_range() will start at this address and
		 * flush n bytes: */
		flush_range(dest, n);
	}
}

void kp_free(void **ptr, bool use_nvm)
{
	/* calloc vs. malloc doesn't matter in this function, only use_nvm vs.
	 * not. */
	if ((unsigned long long)LIBPM <= (unsigned long long)*ptr \
		&& (unsigned long long)*ptr <= ((unsigned long long)LIBPM + (PMSIZE)))
 	{
		/* pcm_free() is the same as the else case now... */
		assert(use_nvm == true);
		pcm_free(ptr); 
	} else {
		free_v(*ptr);
		*ptr = NULL;
		  /* Probably not really necessary, but code that calls this function
		   * has been written with the assumption that *ptr comes back NULL,
		   * so don't change this. */
	}
}

#ifdef FLUSH_IT
unsigned int flush_range(const void *addr, const size_t size)
{
	unsigned long int ptr;
	bool exact_fit;
	unsigned long int addr_int = (unsigned long int)addr;  //for doing pointer arithmetic...
	unsigned int flushes;

	/* IMPORTANT: addr_int must be _unsigned_, because we perform modulus
	 * division on it, which will result in a negative value if addr_int
	 * happens to be signed. This causes us to flush the incorrect range
	 * (see the initial assignment of ptr below) when addr_int is signed.
	 * Such is the danger of pointer arithmetic...
	 *
	 * It appears that the size of a long int always matches the size of
	 * a void * pointer on a system, whether it's 32-bit or 64-bit; I
	 * verified this empirically on two systems (burrard and n02), but
	 * I haven't checked the C standard or anything.
	 */

#ifdef R_ASSERT
	if (addr == NULL) {
		r_error("addr is NULL, returning\n");
		return 0;
	}
	if (size == 0) {  //if we don't check this, one cache line may be flushed
		r_error("size is 0, returning\n");
		return 0;
	}
#endif

	/* This function utilizes two x86 assembly instructions: clflush and
	 * mfence. For more information about these instructions, see the Intel
	 * Developer's Manual (sections 5.6.4, 11.4.4, 3-135). In summary:
	 *   clflush: "Flushes and invalidates a memory operand and its
	 *     associated cache line from all levels of the processor’s cache
	 *     hierarchy."
	 *   mfence: "Serializes load and store operations."
	 * For more information on C inline assembly, see:
	 *   http://gcc.gnu.org/onlinedocs/gcc/Extended-Asm.html
	 *   http://gcc.gnu.org/onlinedocs/gcc/Constraints.html
	 *   http://ibiblio.org/gferg/ldp/GCC-Inline-Assembly-HOWTO.html
	 */

	/* Perform the first fence operation: no operands or input/output.
	 * Specify "volatile" so that gcc will not optimize this code away
	 * or re-order it.
	 */
	kp_mfence();

	/* Loop through the range in steps of size "CLFLUSH_SIZE" (defined
	 * in flush.h, typically 64 bytes). With the pointer and size
	 * calculations below, we should flush every cache line that contains
	 * some of the data; when the range bumps right up against the last
	 * byte of a cache line, we have an "exact_fit" special case that
	 * prevents us from flushing one too many lines. See test_flush.c
	 * for some test cases that verify this. Note that the clflush instruction
	 * "Invalidates the cache line that _contains_ the linear address
	 * specified with the source operand."
	 *
	 * At first, I declared ptr "volatile" to prevent gcc from performing
	 * optimizations related to the ptr address that we will pass to clflush.
	 * Usually, I think volatile just means "don't store this value in a
	 * register, get it from the cache/memory every time it's needed," so in
	 * our case here, I don't think that volatile would actually make any
	 * difference, so I removed it. In my test_flush.c code, running with
	 * and without volatile for ptr (and its cast below, "(volatile void *)")
	 * didn't make any difference.
	 *   http://stackoverflow.com/questions/7872175/c-volatile-variables-and-cache-memory
	 *   http://stackoverflow.com/questions/246127/why-is-volatile-needed-in-c
	 *
	 * Todo: a better way to do this would be to just pre-calculate the
	 * number of flushes that need to be performed, and then run the for
	 * loop for exactly that number of iterations... oops.
	 */
	if ((addr_int + size) % CLFLUSH_SIZE == 0) {
		exact_fit = true;
	} else {
		exact_fit = false;
	}
	ptr = addr_int - (addr_int % CLFLUSH_SIZE);
	//r_print("addr_int mod CLFLUSH_SIZE = %u mod %u = %u\n",
	//		addr_int, CLFLUSH_SIZE, addr_int % CLFLUSH_SIZE);
	r_debug("addr=%p (%lu), size=%u; calculated ptr=%p (%lu), exact_fit=%s\n",
			addr, addr_int, size, (void *)ptr, ptr,
			exact_fit ? "true" : "false");
#ifdef R_ASSERT
	if (ptr > addr_int + size) {
		/* We shouldn't fail the initial loop condition already. */
		r_die("Unexpected ptr value %p (%lu) is greater than addr_int %p (%lu) "
				"plus size (%u)!\n", (void *)ptr, ptr, (void *)addr_int,
				addr_int, size);
	}
#endif

	flushes = 0;
	for (;;) {
		if ((exact_fit && ptr == addr_int + size) ||
			(ptr > addr_int + size)) {
			break;
		}
		r_debug("calling clflush(%p) (%lu)\n", (void *)ptr, ptr);
		// PM_FLUSH(((void*)ptr), (64), (64));
		__asm__ __volatile__ (
				"clflush (%0)"
				:                    /* no output registers */
				: "r" ((void *)ptr)  /* "ptr" is input %0; "r" means
				                      * store in any register */
				);
		  /* I'm not exactly sure if the "ptr" input to clflush should be
		   * specified with the 'r' (register) constraint or the 'm' (memory)
		   * constraint; I think either would work, but 'r' should be faster
		   * (at the expense of using up a general-purpose register).
		   *
		   * The "volatile" keyword is used with asm to indicate that the
		   * statement must execute where we put it (e.g. it shouldn't be
		   * deleted as "unused," moved out of a loop, etc.). Our instructions
		   * have side effects that may not be clear to gcc, so we need it.
		   * "GCC will not delete a volatile asm if it is reachable." However,
		   * "An asm instruction without any output operands will be treated
		   * identically to a volatile asm instruction", so I guess this isn't
		   * really necessary.
		   */

		flushes++;
		ptr += CLFLUSH_SIZE;
	}

	/* Perform the final fence operation: */
	kp_mfence();

	/* Return the number of flushes that we performed. */
	r_debug("Flush range complete, %u lines were flushed.\n", flushes);
#ifdef R_ASSERT
	if (flushes == 0) {
		r_die("Didn't flush any cache lines - something is wrong!\n");
	}
#endif
	return flushes;
}

void flush_caches()
{
	r_die("not implemented\n");

	/* Can't use "wbinvd" instruction (invalidate cache with writeback):
	 * it's privileged (kernel-mode only)! Could implement it in a driver;
	 * or, could just load a bunch of data into cache to flush everything
	 * that's already in it.
	 * ...
	 */
}

#else  //ifdef FLUSH_IT false:
unsigned int flush_range(const void *addr, const size_t size)
{
	return 0;
}

void flush_caches()
{
	return;
}
#endif  //ifdef FLUSH_IT

void kp_reset_mem_accounting()
{
#if IS_GC_BRANCH
	kp_mem_data = 0;
	kp_mem_metadata = 0;
#endif
}

void kp_get_mem_accounting(uint64_t *mem_data, uint64_t *mem_metadata)
{
#if IS_GC_BRANCH
	if (mem_data) {
		*mem_data = kp_mem_data;
	}
	if (mem_metadata) {
		*mem_metadata = kp_mem_metadata;
	}
#else
	// this is a temporary place-holder...
	if (mem_data) {
		*mem_data = (uint64_t)1234;
	}
	if (mem_metadata) {
		*mem_metadata = (uint64_t)5678;
	}
#endif
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
