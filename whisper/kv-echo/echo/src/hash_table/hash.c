/* hash - hashing table processing.

   Copyright (C) 1998-2004, 2006-2007, 2009-2012 Free Software Foundation, Inc.

   Written by Jim Meyering, 1992.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* A generic hash table package.  */

/* Define USE_OBSTACK to 1 if you want the allocator to use obstacks instead
   of malloc.  If you change USE_OBSTACK, you have to recompile!  */

//PJH #include <config.h>

#include "hash.h"
#include "ht_macros.h"
#include "bitrotate.h"
#include "xalloc-oversized.h"
#include "../kp_common.h"    //hacky? PJH
#include "../kp_recovery.h"  //hacky? PJH

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#if USE_OBSTACK
# include "obstack.h"
# ifndef obstack_chunk_alloc
#  define obstack_chunk_alloc malloc
# endif
# ifndef obstack_chunk_free
#  define obstack_chunk_free free
# endif
#endif

struct hash_entry
{
	void *data;
	pthread_mutex_t *lock;  //PJH
	struct hash_entry *next;
};

struct hash_table
{
	/* The array of buckets starts at BUCKET and extends to BUCKET_LIMIT-1,
	   for a possibility of N_BUCKETS.  Among those, N_BUCKETS_USED buckets
	   are not empty, there are N_ENTRIES active entries in the table.  */
	struct hash_entry *bucket;
	struct hash_entry const *bucket_limit;
	size_t n_buckets;
	size_t n_buckets_used;
	size_t n_entries;

	/* PJH: we add a lock to the hash table to solve two problems:
	 *   1) So that insertions and deletions that cause re-hashing don't
	 *      mess up with concurrent gets and concurrent non-re-hashing
	 *      insertions.
	 *   2) So that we can lock individual hash table entries that have
	 *      just been looked up without a concurrent deletion removing
	 *      them from the hash table before the hash_lookup caller has
	 *      a chance to lock the entry. */
#ifdef REHASHING_ENABLED
	pthread_rwlock_t *rwlock;
#endif

	/* Tuning arguments, kept in a physically separate structure.  */
	const Hash_tuning *tuning;

	/* Three functions are given to 'hash_initialize', see the documentation
	   block for this function.  In a word, HASHER randomizes a user entry
	   into a number up from 0 up to some maximum minus 1; COMPARATOR returns
	   true if two user entries compare equally; and DATA_FREER is the cleanup
	   function for a user entry.  */
	Hash_hasher hasher;
	Hash_comparator comparator;
	Hash_data_freer data_freer;

	/* A linked list of freed struct hash_entry structs.  */
	struct hash_entry *free_entry_list;

#if USE_OBSTACK
	/* Whenever obstacks are used, it is possible to allocate all overflowed
	   entries into a single stack, so they all can be freed in a single
	   operation.  It is not clear if the speedup is worth the trouble.  */
	struct obstack entry_stack;
#endif
	bool use_nvm;
	ds_state state;
};

/* A hash table contains many internal entries, each holding a pointer to
   some user-provided data (also called a user entry).  An entry indistinctly
   refers to both the internal entry and its associated user entry.  A user
   entry contents may be hashed by a randomization function (the hashing
   function, or just "hasher" for short) into a number (or "slot") between 0
   and the current table size.  At each slot position in the hash table,
   starts a linked chain of entries for which the user data all hash to this
   slot.  A bucket is the collection of all entries hashing to the same slot.

   A good "hasher" function will distribute entries rather evenly in buckets.
   In the ideal case, the length of each bucket is roughly the number of
   entries divided by the table size.  Finding the slot for a data is usually
   done in constant time by the "hasher", and the later finding of a precise
   entry is linear in time with the size of the bucket.  Consequently, a
   larger hash table size (that is, a larger number of buckets) is prone to
   yielding shorter chains, *given* the "hasher" function behaves properly.

   Long buckets slow down the lookup algorithm.  One might use big hash table
   sizes in hope to reduce the average length of buckets, but this might
   become inordinate, as unused slots in the hash table take some space.  The
   best bet is to make sure you are using a good "hasher" function (beware
   that those are not that easy to write! :-), and to use a table size
   larger than the actual number of entries.  */

/* If an insertion makes the ratio of nonempty buckets to table size larger
   than the growth threshold (a number between 0.0 and 1.0), then increase
   the table size by multiplying by the growth factor (a number greater than
   1.0).  The growth threshold defaults to 0.8, and the growth factor
   defaults to 1.414, meaning that the table will have doubled its size
   every second time 80% of the buckets get used.  */
#define DEFAULT_GROWTH_THRESHOLD 0.8f
#define DEFAULT_GROWTH_FACTOR 1.414f

/* If a deletion empties a bucket and causes the ratio of used buckets to
   table size to become smaller than the shrink threshold (a number between
   0.0 and 1.0), then shrink the table by multiplying by the shrink factor (a
   number greater than the shrink threshold but smaller than 1.0).  The shrink
   threshold and factor default to 0.0 and 1.0, meaning that the table never
   shrinks.  */
#define DEFAULT_SHRINK_THRESHOLD 0.0f
#define DEFAULT_SHRINK_FACTOR 1.0f

/* Use this to initialize or reset a TUNING structure to
   some sensible values. */
static const Hash_tuning default_tuning =
{
	DEFAULT_SHRINK_THRESHOLD,
	DEFAULT_SHRINK_FACTOR,
	DEFAULT_GROWTH_THRESHOLD,
	DEFAULT_GROWTH_FACTOR,
	false
};

/* Information and lookup.  */

/* The following few functions provide information about the overall hash
   table organization: the number of entries, number of buckets and maximum
   length of buckets.  */

/* Return the number of buckets in the hash table.  The table size, the total
   number of buckets (used plus unused), or the maximum number of slots, are
   the same quantity.  */

//PJH:
size_t hash_get_hash_table_size()
{
	return sizeof(Hash_table);
}

size_t
hash_get_n_buckets (const Hash_table *table)
{
	ht_die("don't use this function without checking that it isn't made "
			"unusably inconsistent due to our locking scheme in hash_insert_"
			"if_absent()!\n");  //PJH
	return table->n_buckets;
}

/* Return the number of slots in use (non-empty buckets).  */

size_t
hash_get_n_buckets_used (const Hash_table *table)
{
	ht_die("don't use this function without checking that it isn't made "
			"unusably inconsistent due to our locking scheme in hash_insert_"
			"if_absent()!\n");  //PJH
	return table->n_buckets_used;
}

/* Return the number of active entries.  */

size_t
hash_get_n_entries (const Hash_table *table)
{
	ht_die("don't use this function without checking that it isn't made "
			"unusably inconsistent due to our locking scheme in hash_insert_"
			"if_absent()!\n");  //PJH
	return table->n_entries;
}

/* Return the length of the longest chain (bucket).  */

size_t
hash_get_max_bucket_length (const Hash_table *table)
{
	ht_die("don't use this function without checking that it isn't made "
			"unusably inconsistent due to our locking scheme in hash_insert_"
			"if_absent()!\n");  //PJH
	struct hash_entry const *bucket;
	size_t max_bucket_length = 0;

	for (bucket = table->bucket; bucket < table->bucket_limit; bucket++)
	{
		if (bucket->data)
		{
			struct hash_entry const *cursor = bucket;
			size_t bucket_length = 1;

			while (cursor = cursor->next, cursor)
				bucket_length++;

			if (bucket_length > max_bucket_length)
				max_bucket_length = bucket_length;
		}
	}

	return max_bucket_length;
}

/* Do a mild validation of a hash table, by traversing it and checking two
   statistics.  */

bool
hash_table_ok (const Hash_table *table)
{
	ht_die("don't use this function without checking that it isn't made "
			"unusably inconsistent due to our locking scheme in hash_insert_"
			"if_absent()!\n");  //PJH
	struct hash_entry const *bucket;
	size_t n_buckets_used = 0;
	size_t n_entries = 0;

	for (bucket = table->bucket; bucket < table->bucket_limit; bucket++)
	{
		if (bucket->data)
		{
			struct hash_entry const *cursor = bucket;

			/* Count bucket head.  */
			n_buckets_used++;
			n_entries++;

			/* Count bucket overflow.  */
			while (cursor = cursor->next, cursor)
				n_entries++;
		}
	}

	if (n_buckets_used == table->n_buckets_used && n_entries == table->n_entries)
		return true;

	return false;
}

void
hash_print_statistics (const Hash_table *table, FILE *stream)
{
	ht_die("don't use this function without checking that it isn't made "
			"unusably inconsistent due to our locking scheme in hash_insert_"
			"if_absent()!\n");  //PJH
	size_t n_entries = hash_get_n_entries (table);
	size_t n_buckets = hash_get_n_buckets (table);
	size_t n_buckets_used = hash_get_n_buckets_used (table);
	size_t max_bucket_length = hash_get_max_bucket_length (table);

	fprintf (stream, "# entries:         %lu\n", (unsigned long int) n_entries);
	fprintf (stream, "# buckets:         %lu\n", (unsigned long int) n_buckets);
	fprintf (stream, "# buckets used:    %lu (%.2f%%)\n",
			(unsigned long int) n_buckets_used,
			(100.0 * n_buckets_used) / n_buckets);
	fprintf (stream, "max bucket length: %lu\n",
			(unsigned long int) max_bucket_length);
}

/* Hash KEY and return a pointer to the selected bucket.
   If TABLE->hasher misbehaves, abort.  */
static struct hash_entry *
safe_hasher (const Hash_table *table, const void *key)
{
	size_t n = table->hasher (key, table->n_buckets);
	if (! (n < table->n_buckets))
		abort ();
	return table->bucket + n;
}

/* PJH: if ENTRY matches an entry already in the hash table, return the
 * entry from the table. Otherwise, return NULL. ADDITIONALLY, if the
 * callback_while_locked argument is not NULL, then this function will be
 * called on the looked-up entry BEFORE THE RDLOCK IS RELEASED. This enables
 * us to ensure that an entry is not deleted from the hash table before the
 * caller of hash_lookup() has a chance to use the entry. */
void *
hash_lookup (const Hash_table *table, const void *entry,
		Hash_lookup_action callback_while_locked)
{
#ifdef REHASHING_ENABLED
	kp_rdlock("hash table->rwlock", table->rwlock);  //PJH
#endif
	void *retptr = NULL;
	bool retbool;
	struct hash_entry const *bucket = safe_hasher (table, entry);
	struct hash_entry const *cursor;

	if (bucket->data == NULL) {
#ifdef REHASHING_ENABLED
		kp_rdunlock("hash table->rwlock", table->rwlock);  //PJH
#endif
		return NULL;
	}

	ht_debug("got bucket=%p, bucket->data=%p, bucket->next=%p\n", bucket,
			bucket->data, bucket->next);
	for (cursor = bucket; cursor; cursor = cursor->next) {
		ht_debug("checkpoint 1: entry=%p, cursor=%p, cursor->data=%p, "
				"cursor->next=%p\n", entry, cursor, cursor->data, cursor->next);
		if (entry == cursor->data || table->comparator (entry, cursor->data)) {
			ht_debug("found the entry, passing it to callback_while_locked() before "
					"rd-unlocking and returning\n");
#ifdef HT_ASSERT
			if (!callback_while_locked) {
				ht_die("callback_while_locked() is NULL; currently we don't ever "
						"expect it to be!\n");
			}
#endif
			retbool = callback_while_locked(cursor->data);
			ht_debug("checkpoint 2: entry=%p, cursor=%p, cursor->data=%p\n",
					entry, cursor, cursor->data);
			if (!retbool) {
				ht_error("callback_while_locked() failed! Returning NULL.\n");
#ifdef REHASHING_ENABLED
				kp_rdunlock("hash table->rwlock", table->rwlock);  //PJH
#endif
				return NULL;
			}

			/* PJH: IMPORTANT: we must copy the cursor->data pointer here before
			 * returning it (previously we just returned cursor->data directly).
			 * The reason is that as soon as we release the rdlock here, it's
			 * possible that some other thread is waiting to rehash the table,
			 * so they can grab the wrlock and rehash the entire table before
			 * we get a chance to return. Rehashing may set cursor->data to NULL,
			 * which causes us to return NULL even though we found and locked
			 * a vt, which eventually causes deadlock because nobody ever unlocks
			 * the vt. Yikes!
			 *
			 * retptr (cursor->data) points to a kp_ht_entry, which contains the
			 * pointer to the key and to the corresponding vt. Even if a rehash
			 * happens immediately after we copy retptr, it's still ok to return
			 * it to the caller; the caller just wants to get the vt, and doesn't
			 * care if/how the hash table changes underneath.
			 *
			 * When I made this change, I made a similar change in the other
			 * places in the hash table code where we returned a non-local-variable
			 * AFTER unlocking. I don't think these were actually causing any
			 * problems yet though. */
			retptr = cursor->data;
			kp_debug_lock("read-unlocking hash table->rwlock\n");
#ifdef REHASHING_ENABLED
			kp_rdunlock("hash table->rwlock", table->rwlock);  //PJH
#endif
			ht_debug("checkpoint 3: entry=%p, cursor=%p, cursor->data=%p, "
					"retptr=%p\n", entry, cursor, cursor->data, retptr);
#ifdef HT_ASSERT
			if (!retptr) {
				kp_die("about to return NULL even though we just locked the "
						"vt: cursor->data=%p, retptr=%p\n", cursor->data, retptr);
			}
#endif
			return retptr;
		}
	}

#ifdef REHASHING_ENABLED
	kp_rdunlock("hash table->rwlock", table->rwlock);  //PJH
#endif
	return NULL;
}

/* Walking.  */

/* The functions in this page traverse the hash table and process the
   contained entries.  For the traversal to work properly, the hash table
   should not be resized nor modified while any particular entry is being
   processed.  In particular, entries should not be added, and an entry
   may be removed only if there is no shrink threshold and the entry being
   removed has already been passed to hash_get_next.  */

/* Return the first data in the table, or NULL if the table is empty.  */

void *
hash_get_first (const Hash_table *table)
{
	void *retptr;
	struct hash_entry const *bucket;

	if (table->n_entries == 0)
		return NULL;

	/* PJH: it looks like this code should be safe except in the presence
	 * of re-hashing; so, take the readlock on the hash table while we loop
	 * (same as hash_lookup). For now (version 3.0), this function is only
	 * called for internal iterators on un-changing local kvstores anyway. */
#ifdef REHASHING_ENABLED
	kp_rdlock("hash table->rwlock", table->rwlock);  //PJH
#endif
	for (bucket = table->bucket; ; bucket++) {
		if (! (bucket < table->bucket_limit)) {
#ifdef REHASHING_ENABLED
			kp_rdunlock("hash table->rwlock", table->rwlock);  //PJH
#endif
			abort ();
		} else if (bucket->data) {
			retptr = bucket->data;
#ifdef REHASHING_ENABLED
			kp_rdunlock("hash table->rwlock", table->rwlock);  //PJH
#endif
			return retptr;
		}
	}
}

/* Return the user data for the entry following ENTRY, where ENTRY has been
   returned by a previous call to either 'hash_get_first' or 'hash_get_next'.
   Return NULL if there are no more entries.  */

void *
hash_get_next (const Hash_table *table, const void *entry)
{
	/* PJH: it looks like this code should be safe except in the presence
	 * of re-hashing; so, take the readlock on the hash table while we loop
	 * (same as hash_lookup). For now (version 3.0), this function is only
	 * called for internal iterators on un-changing local kvstores anyway. */
#ifdef REHASHING_ENABLED
	kp_rdlock("hash table->rwlock", table->rwlock);  //PJH
#endif

	void *retptr;
	struct hash_entry const *bucket = safe_hasher (table, entry);
	struct hash_entry const *cursor;

	/* Find next entry in the same bucket.  */
	cursor = bucket;
	do {
		if (cursor->data == entry && cursor->next) {
			retptr = cursor->next->data;
#ifdef REHASHING_ENABLED
			kp_rdunlock("hash table->rwlock", table->rwlock);  //PJH
#endif
			return retptr;
		}
		cursor = cursor->next;
	} while (cursor != NULL);

	/* Find first entry in any subsequent bucket.  */
	while (++bucket < table->bucket_limit) {
		if (bucket->data) {
			retptr = bucket->data;
#ifdef REHASHING_ENABLED
			kp_rdunlock("hash table->rwlock", table->rwlock);  //PJH
#endif
			return retptr;
		}
	}

	/* None found.  */
#ifdef REHASHING_ENABLED
	kp_rdunlock("hash table->rwlock", table->rwlock);  //PJH
#endif
	return NULL;
}

/* Fill BUFFER with pointers to active user entries in the hash table, then
   return the number of pointers copied.  Do not copy more than BUFFER_SIZE
   pointers.  */

size_t
hash_get_entries (const Hash_table *table, void **buffer,
		size_t buffer_size)
{
	ht_die("don't use this function without checking that it isn't made "
			"unusably inconsistent due to our locking scheme in hash_insert_"
			"if_absent()!\n");  //PJH
	size_t counter = 0;
	struct hash_entry const *bucket;
	struct hash_entry const *cursor;

	for (bucket = table->bucket; bucket < table->bucket_limit; bucket++)
	{
		if (bucket->data)
		{
			for (cursor = bucket; cursor; cursor = cursor->next)
			{
				if (counter >= buffer_size)
					return counter;
				buffer[counter++] = cursor->data;
			}
		}
	}

	return counter;
}

/* Call a PROCESSOR function for each entry of a hash table, and return the
   number of entries for which the processor function returned success.  A
   pointer to some PROCESSOR_DATA which will be made available to each call to
   the processor function.  The PROCESSOR accepts two arguments: the first is
   the user entry being walked into, the second is the value of PROCESSOR_DATA
   as received.  The walking continue for as long as the PROCESSOR function
   returns nonzero.  When it returns zero, the walking is interrupted.

PJH: on error (the processor function returns false), we changed this
function to return SIZE_MAX!
*/

size_t
hash_do_for_each (const Hash_table *table, Hash_processor processor,
		void *processor_data)
{
	/* PJH: it looks like this code should be safe except in the presence
	 * of re-hashing; so, take the readlock on the hash table while we loop
	 * (same as hash_lookup). For now (version 3.0), this function is only
	 * called when iterating over a "frozen" local kvstore (during commit)
	 * anyway. */
#ifdef REHASHING_ENABLED
	kp_rdlock("hash table->rwlock", table->rwlock);  //PJH
#endif
	size_t counter = 0;
	struct hash_entry const *bucket;
	struct hash_entry const *cursor;

	for (bucket = table->bucket; bucket < table->bucket_limit; bucket++)
	{
		/* Don't take the bucket lock - this function only processes the
		 * data that each entry points to, but the entries themselves
		 * can't be modified. */
		if (bucket->data)
		{
			for (cursor = bucket; cursor; cursor = cursor->next)
			{
				if (! processor (cursor->data, processor_data))
				{
#ifdef REHASHING_ENABLED
					kp_rdunlock("hash table->rwlock", table->rwlock);  //PJH
#endif
					return SIZE_MAX;  //PJH: this is the error condition!
					//return counter;
				}
				counter++;
			}
		}
	}

#ifdef REHASHING_ENABLED
	kp_rdunlock("hash table->rwlock", table->rwlock);  //PJH
#endif
	return counter;
}

/* Allocation and clean-up.  */

/* Return a hash index for a NUL-terminated STRING between 0 and N_BUCKETS-1.
   This is a convenience routine for constructing other hashing functions.  */

#if USE_DIFF_HASH

/* About hashings, Paul Eggert writes to me (FP), on 1994-01-01: "Please see
   B. J. McKenzie, R. Harries & T. Bell, Selecting a hashing algorithm,
   Software--practice & experience 20, 2 (Feb 1990), 209-224.  Good hash
   algorithms tend to be domain-specific, so what's good for [diffutils'] io.c
   may not be good for your application."  */

size_t
hash_string (const char *string, size_t n_buckets)
{
# define HASH_ONE_CHAR(Value, Byte) \
	((Byte) + rotl_sz (Value, 7))

	size_t value = 0;
	unsigned char ch;

	for (; (ch = *string); string++)
		value = HASH_ONE_CHAR (value, ch);
	return value % n_buckets;

# undef HASH_ONE_CHAR
}

#else /* not USE_DIFF_HASH */

/* This one comes from 'recode', and performs a bit better than the above as
   per a few experiments.  It is inspired from a hashing routine found in the
   very old Cyber 'snoop', itself written in typical Greg Mansfield style.
   (By the way, what happened to this excellent man?  Is he still alive?)  */

size_t
hash_string (const char *string, size_t n_buckets)
{
	size_t value = 0;
	unsigned char ch;

	for (; (ch = *string); string++)
		value = (value * 31 + ch) % n_buckets;
	return value;
}

#endif /* not USE_DIFF_HASH */

/* Return true if CANDIDATE is a prime number.  CANDIDATE should be an odd
   number at least equal to 11.  */

//PJH static bool _GL_ATTRIBUTE_CONST
static bool
is_prime (size_t candidate)
{
	size_t divisor = 3;
	size_t square = divisor * divisor;

	while (square < candidate && (candidate % divisor))
	{
		divisor++;
		square += 4 * divisor;
		divisor++;
	}

	return (candidate % divisor ? true : false);
}

/* Round a given CANDIDATE number up to the nearest prime, and return that
   prime.  Primes lower than 10 are merely skipped.  */

//PJH static size_t _GL_ATTRIBUTE_CONST
static size_t
next_prime (size_t candidate)
{
	/* Skip small primes.  */
	if (candidate < 10)
		candidate = 10;

	/* Make it definitely odd.  */
	candidate |= 1;

	while (SIZE_MAX != candidate && !is_prime (candidate))
		candidate += 2;

	return candidate;
}

void
hash_reset_tuning (Hash_tuning *tuning)
{
	*tuning = default_tuning;
}

/* If the user passes a NULL hasher, we hash the raw pointer.  */
static size_t
raw_hasher (const void *data, size_t n)
{
	/* When hashing unique pointers, it is often the case that they were
	   generated by malloc and thus have the property that the low-order
	   bits are 0.  As this tends to give poorer performance with small
	   tables, we rotate the pointer value before performing division,
	   in an attempt to improve hash quality.  */
	size_t val = rotr_sz ((size_t) data, 3);
	return val % n;
}

/* If the user passes a NULL comparator, we use pointer comparison.  */
static bool
raw_comparator (const void *a, const void *b)
{
	return a == b;
}


/* For the given hash TABLE, check the user supplied tuning structure for
   reasonable values, and return true if there is no gross error with it.
   Otherwise, definitively reset the TUNING field to some acceptable default
   in the hash table (that is, the user loses the right of further modifying
   tuning arguments), and return false.  */

static bool
check_tuning (Hash_table *table)
{
	const Hash_tuning *tuning = table->tuning;
	float epsilon;
	if (tuning == &default_tuning)
		return true;

	/* Be a bit stricter than mathematics would require, so that
	   rounding errors in size calculations do not cause allocations to
	   fail to grow or shrink as they should.  The smallest allocation
	   is 11 (due to next_prime's algorithm), so an epsilon of 0.1
	   should be good enough.  */
	epsilon = 0.1f;

	if (epsilon < tuning->growth_threshold
			&& tuning->growth_threshold < 1 - epsilon
			&& 1 + epsilon < tuning->growth_factor
			&& 0 <= tuning->shrink_threshold
			&& tuning->shrink_threshold + epsilon < tuning->shrink_factor
			&& tuning->shrink_factor <= 1
			&& tuning->shrink_threshold + epsilon < tuning->growth_threshold)
		return true;

	table->tuning = &default_tuning;
	return false;
}

/* Compute the size of the bucket array for the given CANDIDATE and
   TUNING, or return 0 if there is no possible way to allocate that
   many entries.  */

static size_t _GL_ATTRIBUTE_PURE
compute_bucket_size (size_t candidate, const Hash_tuning *tuning)
{
	if (!tuning->is_n_buckets)  //this condition is true with default tuning
	{
		ht_debug("computing new_candidate from candidate=%u, "
				"growth_threshold=%f\n", candidate, tuning->growth_threshold);
		float new_candidate = candidate / tuning->growth_threshold;
		if (SIZE_MAX <= new_candidate)
			return 0;
		candidate = new_candidate;
	}
	candidate = next_prime (candidate);
	ht_debug("computed number of buckets = %u\n", candidate);
	if (xalloc_oversized (candidate, sizeof (struct hash_entry *)))
		return 0;
	return candidate;
}

/* Allocate and return a new hash table, or NULL upon failure.  The initial
   number of buckets is automatically selected so as to _guarantee_ that you
   may insert at least CANDIDATE different user entries before any growth of
   the hash table size occurs.  So, if have a reasonably tight a-priori upper
   bound on the number of entries you intend to insert in the hash table, you
   may save some table memory and insertion time, by specifying it here.  If
   the IS_N_BUCKETS field of the TUNING structure is true, the CANDIDATE
   argument has its meaning changed to the wanted number of buckets.

   TUNING points to a structure of user-supplied values, in case some fine
   tuning is wanted over the default behavior of the hasher.  If TUNING is
   NULL, the default tuning parameters are used instead.  If TUNING is
   provided but the values requested are out of bounds or might cause
   rounding errors, return NULL.

   The user-supplied HASHER function, when not NULL, accepts two
   arguments ENTRY and TABLE_SIZE.  It computes, by hashing ENTRY contents, a
   slot number for that entry which should be in the range 0..TABLE_SIZE-1.
   This slot number is then returned.

   The user-supplied COMPARATOR function, when not NULL, accepts two
   arguments pointing to user data, it then returns true for a pair of entries
   that compare equal, or false otherwise.  This function is internally called
   on entries which are already known to hash to the same bucket index,
   but which are distinct pointers.

   The user-supplied DATA_FREER function, when not NULL, may be later called
   with the user data as an argument, just before the entry containing the
   data gets freed.  This happens from within 'hash_free' or 'hash_clear'.
   You should specify this function only if you want these functions to free
   all of your 'data' data.  This is typically the case when your data is
   simply an auxiliary struct that you have malloc'd to aggregate several
   values.  */
/* Returns: 0 on success, -1 on failure. */
int
hash_initialize (Hash_table **table, size_t candidate, const Hash_tuning *tuning,
		Hash_hasher hasher, Hash_comparator comparator,
		Hash_data_freer data_freer, bool use_nvm)
{
	int ret;
	struct hash_entry *bucket;

	if (hasher == NULL)
		hasher = raw_hasher;
	if (comparator == NULL)
		comparator = raw_comparator;

	kp_kpalloc((void **)table, sizeof(Hash_table), use_nvm);
	if (*table == NULL)
		return -1;

	if (!tuning)
		tuning = &default_tuning;
	PM_EQU(((*table)->tuning), (tuning));
	if (!check_tuning (*table))
	{
		/* Fail if the tuning options are invalid.  This is the only occasion
		   when the user gets some feedback about it.  Once the table is created,
		   if the user provides invalid tuning options, we silently revert to
		   using the defaults, and ignore further request to change the tuning
		   options.  */
		goto fail;
	}

	PM_EQU(((*table)->n_buckets), (compute_bucket_size (candidate, tuning)));
	if (!(*table)->n_buckets)
		goto fail;

	/* Force calloc! Don't use kp_kpalloc here. */
	kp_calloc((void **)&((*table)->bucket),
			(*table)->n_buckets * (sizeof *((*table)->bucket)),
			use_nvm);
	if ((*table)->bucket == NULL)
		goto fail;
	PM_EQU(((*table)->bucket_limit), ((*table)->bucket + (*table)->n_buckets)); // persistent
	PM_EQU(((*table)->n_buckets_used), (0));	// persistent
	PM_EQU(((*table)->n_entries), (0));	// persistent

	PM_EQU(((*table)->hasher), (hasher));	// persistent
	PM_EQU(((*table)->comparator), (comparator));	// persistent
	PM_EQU(((*table)->data_freer), (data_freer));	// persistent
	PM_EQU(((*table)->use_nvm), (use_nvm));		// persistent

	PM_EQU(((*table)->free_entry_list), (NULL));	// persistent
#if USE_OBSTACK
	obstack_init (table->entry_stack);
#endif

	/* PJH: calloc sets the buckets' data and next pointers to NULL, but now
	 * we need to allocate one pthread_mutex_t per bucket: */
	for (bucket = (*table)->bucket; bucket < (*table)->bucket_limit; bucket++) {
		ret = kp_mutex_create("bucket lock", &(bucket->lock));
		if (ret != 0 || !(bucket->lock)) {
			ht_error("kp_mutex_create() failed\n");
			//todo: should free mutexes created so-far here...
			goto fail;
		}
	}
	ht_debug("successfully created one mutex per hash table bucket.\n");

#ifdef REHASHING_ENABLED
	ret = kp_rwlock_create("hash table->rwlock", &((*table)->rwlock));
	if (ret != 0) {
		ht_error("kp_rwlock_create() returned error %d\n", ret);
		goto fail;
	}
	ht_debug("successfully created hash table's rwlock\n");
#endif

	/* "CDDS": flush, set state, and flush again. */
	kp_flush_range((void *)*table, sizeof(Hash_table) - sizeof(ds_state), use_nvm);
	PM_EQU(((*table)->state), (STATE_ACTIVE));
	kp_flush_range((void *)&((*table)->state), sizeof(ds_state), use_nvm);

	return 0;

fail:
	kp_free((void **)table, use_nvm);
	return -1;
}

/* Make all buckets empty, placing any chained entries on the free list.
   Apply the user-specified function data_freer (if any) to the datas of any
   affected entries.  */

void
hash_clear (Hash_table *table)
{
	struct hash_entry *bucket;

	/* PJH: for safety, this function grabs the write-lock on the hash
	 * table, but it shouldn't be necessary - we only (currently) call
	 * this function after a merge has happened and the local store
	 * is quiesced (we never call it on master stores). */

	ht_debug("TODO: make this function CDDS??\n");

#ifdef REHASHING_ENABLED
	kp_wrlock("hash table->rwlock", table->rwlock);  //PJH
#endif
	for (bucket = table->bucket; bucket < table->bucket_limit; bucket++)
	{
		if (bucket->data)
		{
			struct hash_entry *cursor;
			struct hash_entry *next;

			/* Free the bucket overflow.  */
			for (cursor = bucket->next; cursor; cursor = next)
			{
				if (table->data_freer)
					table->data_freer (cursor->data);
				cursor->data = NULL;

				next = cursor->next;
				/* Relinking is done one entry at a time, as it is to be expected
				   that overflows are either rare or short.  */
				cursor->next = table->free_entry_list;
				table->free_entry_list = cursor;
			}

			/* Free the bucket head.  */
			if (table->data_freer)
				table->data_freer (bucket->data);
			bucket->data = NULL;
			bucket->next = NULL;
		}
		/* Don't need to touch the bucket locks; they remain allocated
		 * (and un-locked). */
	}

	table->n_buckets_used = 0;
	table->n_entries = 0;

#ifdef REHASHING_ENABLED
	kp_wrunlock("hash table->rwlock", table->rwlock);  //PJH
#endif
	return;
}

/* Reclaim all storage associated with a hash table.  If a data_freer
   function has been supplied by the user when the hash table was created,
   this function applies it to the data of each entry before freeing that
   entry.  */

void
hash_free (Hash_table **table)
{
	struct hash_entry *bucket;
	struct hash_entry *cursor;
	struct hash_entry *next;

	if (!table) {
		ht_error("table is null\n");
		return;
	}

	if (*table) {
		(*table)->state = STATE_DEAD;
		kp_flush_range(&((*table)->state), sizeof(ds_state), (*table)->use_nvm);

		/* Call the user data_freer function.  */
		if ((*table)->data_freer && (*table)->n_entries)
		{
			for (bucket = (*table)->bucket; bucket < (*table)->bucket_limit; bucket++)
			{
				if (bucket->data)
				{
					for (cursor = bucket; cursor; cursor = cursor->next)
						(*table)->data_freer (cursor->data);
				}
				if (bucket->lock) {
					kp_mutex_destroy("bucket->lock", &(bucket->lock));
				} else {
					kp_warn("got a NULL bucket->lock pointer!\n");
				}
			}
		}

#if USE_OBSTACK
		obstack_free (&((*table)->entry_stack), NULL);
#else

		/* Free all bucket overflowed entries.  */
		for (bucket = (*table)->bucket; bucket < (*table)->bucket_limit; bucket++)
		{
			for (cursor = bucket->next; cursor; cursor = next)
			{
				next = cursor->next;
				kp_free((void **)&cursor, (*table)->use_nvm);
			}
		}

		/* Also reclaim the internal list of previously freed entries.  */
		for (cursor = (*table)->free_entry_list; cursor; cursor = next)
		{
			next = cursor->next;
			kp_free((void **)&cursor, (*table)->use_nvm);
		}
#endif

		/* Free the remainder of the hash table structure.  */
#ifdef REHASHING_ENABLED
		kp_rwlock_destroy("hash (*table)->rwlock", &((*table)->rwlock));  //PJH
#endif
		kp_free((void **)&((*table)->bucket), (*table)->use_nvm);
		kp_free((void **)table, (*table)->use_nvm);
	}
}

/* Insertion and deletion.  */

/* Get a new hash entry for a bucket overflow, possibly by recycling a
   previously freed one.  If this is not possible, allocate a new one.  */

static struct hash_entry *
allocate_entry (Hash_table *table)
{
	struct hash_entry *new;

	if (table->free_entry_list)
	{
		new = table->free_entry_list;
		table->free_entry_list = new->next;
		kp_flush_range((void *)&(table->free_entry_list),
				sizeof(struct hash_entry *), table->use_nvm);
	}
	else
	{
#if USE_OBSTACK
		new = obstack_alloc (&table->entry_stack, sizeof *new);
#else
		/* NOTE: if pointer flushing is necessary (e.g. for "reachability"),
		 * then this isn't going to do it: new is a local variable! */
		kp_kpalloc((void **)&new, sizeof(*new), table->use_nvm);
#endif
	}

	return new;
}

/* Free a hash entry which was part of some bucket overflow,
   saving it for later recycling.  */

static void
free_entry (Hash_table *table, struct hash_entry *entry)
{
	ht_debug("entered: entry=%p\n", entry);
	entry->data = NULL;
	entry->next = table->free_entry_list;
	kp_flush_range((void *)entry, sizeof(struct hash_entry), table->use_nvm);
	table->free_entry_list = entry;
	kp_flush_range((void *)&(table->free_entry_list),
			sizeof(struct hash_entry *), table->use_nvm);
}

/* This private function is used to help with insertion and deletion.  When
   ENTRY matches an entry in the table, return a pointer to the corresponding
   user data and set *BUCKET_HEAD to the head of the selected bucket.
   Otherwise, return NULL.  When DELETE is true and ENTRY matches an entry in
   the table, unlink the matching entry.  */

static void *
hash_find_entry (Hash_table *table, const void *entry,
		struct hash_entry **bucket_head, bool delete)
{
	struct hash_entry *bucket = safe_hasher (table, entry);
	struct hash_entry *cursor;

	*bucket_head = bucket;

	/* Test for empty bucket.  */
	if (bucket->data == NULL)
		return NULL;

	/* See if the entry is the first in the bucket.  */
	if (entry == bucket->data || table->comparator (entry, bucket->data))
	{
		void *data = bucket->data;

		if (delete)
		{
			ht_die("double-check this code path!\n");
			ht_die("make this CDDS!! Do flushing, etc.\n");
			if (bucket->next)
			{
				struct hash_entry *next = bucket->next;

				/* Bump the first overflow entry into the bucket head, then save
				   the previous first overflow entry for later recycling.  */
				*bucket = *next;
				free_entry (table, next);
			}
			else
			{
				bucket->data = NULL;
			}
		}

		return data;
	}

	/* Scan the bucket overflow.  */
	for (cursor = bucket; cursor->next; cursor = cursor->next)
	{
		if (entry == cursor->next->data
				|| table->comparator (entry, cursor->next->data))
		{
			void *data = cursor->next->data;

			if (delete)
			{
				ht_die("double-check this code path!\n");
				ht_die("make this CDDS!! Do flushing, etc.\n");
				struct hash_entry *next = cursor->next;

				/* Unlink the entry to delete, then save the freed entry for later
				   recycling.  */
				cursor->next = next->next;
				free_entry (table, next);
			}

			return data;
		}
	}

	/* No entry found.  */
	return NULL;
}

/* Internal helper, to move entries from SRC to DST.  Both tables must
   share the same free entry list.  If SAFE, only move overflow
   entries, saving bucket heads for later, so that no allocations will
   occur.  Return false if the free entry list is exhausted and an
   allocation fails.  */

static bool
transfer_entries (Hash_table *dst, Hash_table *src, bool safe)
{
	struct hash_entry *bucket;
	struct hash_entry *cursor;
	struct hash_entry *next;
	ht_debug("entered, rehashing now!\n");
	ht_die("Make this CDDS! Do flushing, etc.\n");
	for (bucket = src->bucket; bucket < src->bucket_limit; bucket++)
		if (bucket->data)
		{
			void *data;
			struct hash_entry *new_bucket;

			/* Within each bucket, transfer overflow entries first and
			   then the bucket head, to minimize memory pressure.  After
			   all, the only time we might allocate is when moving the
			   bucket head, but moving overflow entries first may create
			   free entries that can be recycled by the time we finally
			   get to the bucket head.  */
			for (cursor = bucket->next; cursor; cursor = next)
			{
				data = cursor->data;
				new_bucket = safe_hasher (dst, data);

				next = cursor->next;

				if (new_bucket->data)
				{
					/* Merely relink an existing entry, when moving from a
					   bucket overflow into a bucket overflow.  */
					cursor->next = new_bucket->next;
					new_bucket->next = cursor;
				}
				else
				{
					/* Free an existing entry, when moving from a bucket
					   overflow into a bucket header.  */
					new_bucket->data = data;
					dst->n_buckets_used++;
					free_entry (dst, cursor);
				}
			}
			/* Now move the bucket head.  Be sure that if we fail due to
			   allocation failure that the src table is in a consistent
			   state.  */
			data = bucket->data;
			bucket->next = NULL;
			if (safe)
				continue;
			new_bucket = safe_hasher (dst, data);

			if (new_bucket->data)
			{
				/* Allocate or recycle an entry, when moving from a bucket
				   header into a bucket overflow.  */
				struct hash_entry *new_entry = allocate_entry (dst);

				if (new_entry == NULL)
					return false;

				new_entry->data = data;
				new_entry->next = new_bucket->next;
				new_bucket->next = new_entry;
			}
			else
			{
				/* Move from one bucket header to another.  */
				new_bucket->data = data;
				dst->n_buckets_used++;
			}
			bucket->data = NULL;
			src->n_buckets_used--;
		}
	return true;
}

/* For an already existing hash table, change the number of buckets through
   specifying CANDIDATE.  The contents of the hash table are preserved.  The
   new number of buckets is automatically selected so as to _guarantee_ that
   the table may receive at least CANDIDATE different user entries, including
   those already in the table, before any other growth of the hash table size
   occurs.  If TUNING->IS_N_BUCKETS is true, then CANDIDATE specifies the
   exact number of buckets desired.  Return true iff the rehash succeeded.  */

bool
hash_rehash (Hash_table *table, size_t candidate)
{
	ht_die("entered rehashing function, but we want to avoid this!!\n");

	int ret;
	struct hash_entry *bucket;
	Hash_table storage;
	Hash_table *new_table;
	size_t new_size = compute_bucket_size (candidate, table->tuning);

	ht_die("make this CDDS!! Do flushing, etc.\n");
	if (!new_size)
		return false;
	if (new_size == table->n_buckets)
		return true;
	new_table = &storage;
	kp_kpalloc((void **)&(new_table->bucket),
			new_size * sizeof(*new_table->bucket), table->use_nvm);
	if (new_table->bucket == NULL) {
		ht_error("kp_kpalloc(new_table->bucket) failed, returning false\n");
		return false;
	}
	new_table->n_buckets = new_size;
	new_table->bucket_limit = new_table->bucket + new_size;
	new_table->n_buckets_used = 0;
	new_table->n_entries = 0;
	new_table->tuning = table->tuning;
	new_table->hasher = table->hasher;
	new_table->comparator = table->comparator;
	new_table->data_freer = table->data_freer;
	new_table->use_nvm = table->use_nvm;
	/* PJH: Re-hashing just got way more expensive: */
	for (bucket = new_table->bucket; bucket < new_table->bucket_limit; bucket++) {
		ret = kp_mutex_create("rehashed bucket lock", &(bucket->lock));
		if (ret != 0 || !(bucket->lock)) {
			ht_error("kp_mutex_create() failed\n");
			//todo: should free mutexes created so-far here...
			kp_free((void **)&(new_table->bucket), new_table->use_nvm);
			return false;
		}
	}

	/* In order for the transfer to successfully complete, we need
	   additional overflow entries when distinct buckets in the old
	   table collide into a common bucket in the new table.  The worst
	   case possible is a hasher that gives a good spread with the old
	   size, but returns a constant with the new size; if we were to
	   guarantee table->n_buckets_used-1 free entries in advance, then
	   the transfer would be guaranteed to not allocate memory.
	   However, for large tables, a guarantee of no further allocation
	   introduces a lot of extra memory pressure, all for an unlikely
	   corner case (most rehashes reduce, rather than increase, the
	   number of overflow entries needed).  So, we instead ensure that
	   the transfer process can be reversed if we hit a memory
	   allocation failure mid-transfer.  */

	/* Merely reuse the extra old space into the new table.  */
#if USE_OBSTACK
	new_table->entry_stack = table->entry_stack;
#endif
	new_table->free_entry_list = table->free_entry_list;

	if (transfer_entries (new_table, table, false))
	{
		/* Entries transferred successfully; tie up the loose ends.
		 * transfer_entries() moved each bucket's data and next pointers,
		 * but each bucket's lock should have been left untouched. We could
		 * potentially save some work here by transferring the mutexes in
		 * transfer_entries(), rather than allocating them all above and
		 * freeing the previous ones, but this is too much of a hassle. */
		for (bucket = table->bucket; bucket < table->bucket_limit; bucket++) {
			if (bucket->lock) {
				kp_mutex_destroy("previous bucket lock", &(bucket->lock));
			} else {
				ht_warn("got a NULL bucket->lock pointer while freeing all "
						"bucket locks!\n");
			}
		}
		kp_free ((void **)&(table->bucket), table->use_nvm);
		table->bucket = new_table->bucket;
		table->bucket_limit = new_table->bucket_limit;
		table->n_buckets = new_table->n_buckets;
		table->n_buckets_used = new_table->n_buckets_used;
		table->free_entry_list = new_table->free_entry_list;
		/* table->n_entries and table->entry_stack and table->rwlock already
		 * hold their value.  */
		return true;
	}

	/* We've allocated new_table->bucket (and possibly some entries),
	   exhausted the free list, and moved some but not all entries into
	   new_table.  We must undo the partial move before returning
	   failure.  The only way to get into this situation is if new_table
	   uses fewer buckets than the old table, so we will reclaim some
	   free entries as overflows in the new table are put back into
	   distinct buckets in the old table.

	   There are some pathological cases where a single pass through the
	   table requires more intermediate overflow entries than using two
	   passes.  Two passes give worse cache performance and takes
	   longer, but at this point, we're already out of memory, so slow
	   and safe is better than failure.  */
	table->free_entry_list = new_table->free_entry_list;
	if (! (transfer_entries (table, new_table, true)
				&& transfer_entries (table, new_table, false)))
		abort ();
	/* table->n_entries already holds its value.  */
	kp_free ((void **)&(new_table->bucket), new_table->use_nvm);
	ht_error("reached end of function, out of memory, returning false\n");
	return false;
}

#ifdef HT_ASSERT
/* Only print a rehashing warning once per table... */
static bool rehash_warned = false;
#endif

/* PJH: IMPORTANT: SEE NOTES BELOW ABOUT HOW THIS FUNCTION NEEDS TO WORK
 * WITH OUR HIGHER-LEVEL LOCKING IN THE KVSTORE!
 *   Basically, the kvstore needs to ensure that this insert function is
 *   never called by more than one thread at a time. If the kvstore can
 *   guarantee that, then the internal locking in the hash table will
 *   ensure that lookups are not blocked by inserts, except when re-
 *   hashing is needed!
 *
 Insert ENTRY into hash TABLE if there is not already a matching entry.
 Return -1 upon memory allocation failure.
 Return 1 if insertion succeeded.
 Return 0 if there is already a matching entry in the table,
 and in that case, if MATCHED_ENT is non-NULL, set *MATCHED_ENT
 to that entry.
 This interface is easier to use than hash_insert when you must
 distinguish between the latter two cases.  More importantly,
 hash_insert is unusable for some types of ENTRY values.  When using
 hash_insert, the only way to distinguish those cases is to compare
 the return value and ENTRY.  That works only when you can have two
 different ENTRY values that point to data that compares "equal".  Thus,
 when the ENTRY value is a simple scalar, you must use
 hash_insert_if_absent.  ENTRY must not be NULL.  */
int
hash_insert_if_absent (Hash_table *table, void const *entry,
		void const **matched_ent)
{
	void *data;
	struct hash_entry *bucket;

	/* The caller cannot insert a NULL entry, since hash_lookup returns NULL
	   to indicate "not found", and hash_find_entry uses "bucket->data == NULL"
	   to indicate an empty bucket.  */
	if (! entry)
		abort ();

	/* PJH: we really want to try not to have inserts block lookups; I think
	 * this is possible. First and foremost, this function should NEVER be
	 * called by more than one thread at a time: we have a higher-level
	 * lock (kv->rwlock) that is taken in the functions that call this
	 * function (kp_put_local() and kp_put_master()), so there is never
	 * more than one insertion at a time.
	 *
	 * The higher-level lock handles insert-insert conflicts, so now we
	 * only have to worry about insert-lookup conflicts. Looking at the
	 * insert code below, I'm pretty sure that the actual insert operation
	 * is ATOMIC, at the point where bucket->next is set to new_entry:
	 * assuming that this is implemented with a single mov instruction
	 * (is this a valid assumption?????????????????????????????????????),
	 * then the hash table code that performs lookups (and all of the other
	 * hash table code that I observed in a non-intensive audit) just loops
	 * "while(bucket->next)", so the insert will either be observed or not
	 * and there is no possibility of a partial insert being observed.
	 * Similar statements hold for the first bucket pointer, bucket->data,
	 * which is used on the first insertion into this particular bucket. The
	 * other data that may be modified by this function (n_entries,
	 * n_buckets_used) should only be _modified_ by other functions in
	 * the hash table code that are called while under the same higher-level
	 * kv->rwlock; they might be _read_ by some other functions in the hash
	 * table code, but on my brief audit I didn't notice any big problems
	 * with this.
	 *
	 * So, I think that this makes the locking inside of this function simple
	 * (ha). In the common case (insertions that DON'T require re-hashing),
	 * we don't have to do any locking at all, because we know that we're the
	 * only thread that is modifying the hash table, and our modifications
	 * happen atomically with respect to lookups. In the uncommon case, we
	 * need to halt all lookups while we re-hash the entire table, so we
	 * grab the wrlock in this function when re-hashing is necessary and
	 * release it when we're done. The functions that perform hash table
	 * lookups (besides this one) must take the rdlock during their lookup.
	 *
	 * Is there any possibility of deadlock with this strategy?
	 *   - Is it possible for thread A to try to acquire the mutex on a vt
	 *     while thread B holds it, and for thread B to try to acquire the
	 *     write-lock on the hash table while thread A holds the read-lock?
	 *       I don't think so: our code should always take the rdlock / wrlock
	 *       on the hash table BEFORE trying to acquire the VT's mutex. This
	 *       is currently true for insertions into the hash table (puts to
	 *       brand-new keys); we need to also make sure that these conditions
	 *       are true for removals from the hash table (when that gets
	 *       implemented...).
	 *   - Any other deadlock possibilities?
	 *       ...
	 *
	 * Whew. */

	/* If there's a matching entry already in the table, return that.
	 * This is essentially a lookup, so we read-lock the table first. */
#ifdef REHASHING_ENABLED
	kp_rdlock("hash table->rwlock", table->rwlock);  //PJH
#endif
	if ((data = hash_find_entry (table, entry, &bucket, false)) != NULL)
	{
		/* No flushing needed here: just returning what's already present
		 * in hash table. */
		if (matched_ent)
			*matched_ent = data;
#ifdef REHASHING_ENABLED
		kp_rdunlock("hash table->rwlock", table->rwlock);  //PJH
#endif
		return 0;
	}

	/* If the growth threshold of the buckets in use has been reached, increase
	   the table size and rehash.  There's no point in checking the number of
	   entries:  if the hashing function is ill-conditioned, rehashing is not
	   likely to improve it.  */

	/* Skip these checks entirely if HT_ASSERT is disabled: */
#ifdef HT_ASSERT
	if (table->n_buckets_used
			> table->tuning->growth_threshold * table->n_buckets)
	{
		/* Check more fully, before starting real work.  If tuning arguments
		   became invalid, the second check will rely on proper defaults.  */
		check_tuning (table);
		if (!rehash_warned &&
			(table->n_buckets_used > table->tuning->growth_threshold * table->n_buckets))
		{
			ht_warn("\n\nHIT CODE PATH THAT WOULD NORMALLY PERFORM HASH TABLE "
					"REHASHING, BUT SKIPPING IT!! n_buckets_used=%zu, "
					"growth_threshold=%f, n_buckets=%zu, product=%f\n\n\n",
					table->n_buckets_used, table->tuning->growth_threshold,
					table->n_buckets,
					table->tuning->growth_threshold * table->n_buckets);
			ht_die("exiting early due to hash table resizing\n");
			kp_print("\n\nHIT CODE PATH THAT WOULD NORMALLY PERFORM HASH TABLE "
					"REHASHING, BUT SKIPPING IT!!!!!!!\n\n\n");
			rehash_warned = true;
			/* Re-hashing needed! */
#ifdef REHASHING_ENABLED
			ht_debug("releasing rdlock and acquiring wrlock to perform "
					"rehashing!\n");
			/* BUG?: what bad things could happen in-between releasing the
			 * read-lock and acquiring the write-lock? */
#ifdef REHASHING_ENABLED
			kp_rdunlock("hash table->rwlock", table->rwlock);  //PJH
			kp_wrlock("hash table->rwlock", table->rwlock);  //PJH
#endif

			const Hash_tuning *tuning = table->tuning;
			float candidate =
				(tuning->is_n_buckets
				 ? (table->n_buckets * tuning->growth_factor)
				 : (table->n_buckets * tuning->growth_factor
					 * tuning->growth_threshold));

			if (SIZE_MAX <= candidate) {
				ht_error("SIZE_MAX=%u <= candidate=%f; returning -1\n",
						(unsigned int)SIZE_MAX, candidate);
#ifdef REHASHING_ENABLED
				kp_wrunlock("hash table->rwlock", table->rwlock);  //PJH
#endif
				return -1;
			}

			/* If the rehash fails, arrange to return NULL.  */
			if (!hash_rehash (table, candidate)) {
				ht_error("hash_rehash() failed, returning -1\n");
#ifdef REHASHING_ENABLED
				kp_wrunlock("hash table->rwlock", table->rwlock);  //PJH
#endif
				return -1;
			}

#ifdef REHASHING_ENABLED
			kp_wrunlock("hash table->rwlock", table->rwlock);  //PJH
#endif

			/* Update the bucket we are interested in. Take the read-lock
			 * again first! */
#ifdef REHASHING_ENABLED
			kp_rdlock("hash table->rwlock", table->rwlock);  //PJH
#endif
			if (hash_find_entry (table, entry, &bucket, false) != NULL)
				abort ();
#endif  //REHASHING_ENABLED
		}
	}
#endif  //HT_ASSERT

	/* ENTRY is not matched, it should be inserted. bucket has been set by
	 * hash_find_entry() to point to the bucket that should be inserted
	 * into, and we hold the read-lock on the store, so we know that
	 * rehashing will not occur at the moment. Now, take the exclusive
	 * lock on this bucket before continuing. Because we took the read-
	 * lock before finding our bucket, we don't have to worry about
	 * anybody re-hashing right now, so this bucket will always be the
	 * right bucket. If somebody else is inserting into this bucket
	 * right now, they will hold the lock and we will just wait for
	 * them to finish; a million people could insert into this particular
	 * bucket and it wouldn't change the correctness here. I think.
	 *
	 * Also, remember that the bucket lock does not stop gets from accessing
	 * this bucket concurrently. The append to a bucket's overflow appears
	 * to be atomic (the setting of bucket->next to the new entry instead
	 * of the previous head of the overflow list), so concurrent gets don't
	 * appear to be a problem while we're inserting. */

#ifdef HT_ASSERT
	if (!entry)
		ht_die("entry is NULL!\n");
#endif
	ht_debug("inserting new entry (data=%p) into bucket=%p; bucket->data=%p\n",
			entry, bucket, bucket->data);

	kp_debug_lock("locking bucket %p\n", bucket);
	kp_mutex_lock("bucket lock", bucket->lock);

	if (bucket->data)  //PJH: this is not the first entry in this bucket.
	{
		struct hash_entry *new_entry = allocate_entry (table);

		if (new_entry == NULL) {
			ht_error("allocate_entry() failed, returning -1\n");
			kp_debug_lock("unlocking bucket %p\n", bucket);
			kp_mutex_unlock("bucket lock", bucket->lock);
#ifdef REHASHING_ENABLED
			kp_rdunlock("hash table->rwlock", table->rwlock);  //PJH
#endif
			return -1;
		}

		/* Add ENTRY in the overflow of the bucket.  */

		PM_EQU((new_entry->data), ((void *) entry));
		PM_EQU((new_entry->next), (bucket->next));
		kp_flush_range((void *)new_entry, sizeof(struct hash_entry),
				table->use_nvm);

		PM_EQU((bucket->next), (new_entry));
		kp_flush_range((void *)&(bucket->next), sizeof(struct hash_entry *),
				table->use_nvm);
#ifdef HT_ASSERT
		if (! new_entry->data)
			ht_die("new_entry->data is NULL!\n");
#endif
		ht_debug("set bucket->next=%p, new_entry->data=%p, new_entry->next=%p\n",
				bucket->next, new_entry->data, new_entry->next);
		kp_debug_lock("unlocking bucket %p\n", bucket);
		kp_mutex_unlock("bucket lock", bucket->lock);
#ifdef REHASHING_ENABLED
		kp_rdunlock("hash table->rwlock", table->rwlock);  //PJH
#endif
		PM_EQU((table->n_entries), (table->n_entries+1));
		return 1;
	}

	//ELSE:
	/* Add ENTRY right in the bucket head.  */

	PM_EQU((bucket->data), ((void *) entry));
	kp_flush_range((void *)&(bucket->data), sizeof(void *), table->use_nvm);
	ht_debug("set bucket->data=%p (bucket->next=%p (should be nil))\n",
			bucket->data, bucket->next);
	kp_debug_lock("unlocking bucket %p\n", bucket);
	kp_mutex_unlock("bucket lock", bucket->lock);
#ifdef HT_ASSERT
	if (! bucket->data)
		ht_die("bucket->data is NULL!\n");
#endif

	/* Assume that these happen atomically, so no fine-grained locking
	 * needed... */
	PM_EQU((table->n_entries), (table->n_entries+1));
	PM_EQU((table->n_buckets_used), (table->n_buckets_used+1));
#ifdef REHASHING_ENABLED
	kp_rdunlock("hash table->rwlock", table->rwlock);  //PJH
#endif
	return 1;
}

/* hash_insert0 is the deprecated name for hash_insert_if_absent. */
int
hash_insert0 (Hash_table *table, void const *entry, void const **matched_ent)
{
	ht_die("don't use this function without carefully considering the "
			"implications on the remaining functions that are called by "
			"our kvstore! There is a delicate dance happening with the "
			"locking, etc. :)\n");  //PJH
	return hash_insert_if_absent (table, entry, matched_ent);
}

/* If ENTRY matches an entry already in the hash table, return the pointer
   to the entry from the table.  Otherwise, insert ENTRY and return ENTRY.
   Return NULL if the storage required for insertion cannot be allocated.
   This implementation does not support duplicate entries or insertion of
   NULL.  */

void *
hash_insert (Hash_table *table, void const *entry)
{
	ht_die("don't use this function without carefully considering the "
			"implications on the remaining functions that are called by "
			"our kvstore! There is a delicate dance happening with the "
			"locking, etc. :)\n");  //PJH

	void const *matched_ent;
	int err = hash_insert_if_absent (table, entry, &matched_ent);
	return (err == -1
			? NULL
			: (void *) (err == 0 ? matched_ent : entry));
}

/* If ENTRY is already in the table, remove it and return the just-deleted
   data (the user may want to deallocate its storage).  If ENTRY is not in the
   table, don't modify the table and return NULL.  */

void *
hash_delete (Hash_table *table, const void *entry)
{
	ht_die("Before using this function, make sure that it meets the same "
			"preconditions as hash_insert_if_absent() (namely that this "
			"function and that function are called under the SAME exclusive "
			"lock: at most one thread can call one of these functions "
			"concurrently!), and ensure that this function acquires its "
			"locks (hash table rwlock and entry->vt->lock) in the same order "
			"as existing insertion / lookup code, to avoid deadlock!\n");  //PJH
	ht_die("Also, before using this function, need to port the rwlocking and "
			"bucket locking operations from hash_insert_if_absent() into "
			"here!!!!\n");
	void *data;
	struct hash_entry *bucket;

	data = hash_find_entry (table, entry, &bucket, true);
	if (!data)
		return NULL;

	ht_die("make this CDDS! Add flushing, etc.\n");
	table->n_entries--;
	if (!bucket->data)
	{
		table->n_buckets_used--;

		/* If the shrink threshold of the buckets in use has been reached,
		   rehash into a smaller table.  */

		if (table->n_buckets_used
				< table->tuning->shrink_threshold * table->n_buckets)
		{
			/* Check more fully, before starting real work.  If tuning arguments
			   became invalid, the second check will rely on proper defaults.  */
			check_tuning (table);
			if (table->n_buckets_used
					< table->tuning->shrink_threshold * table->n_buckets)
			{
				const Hash_tuning *tuning = table->tuning;
				size_t candidate =
					(tuning->is_n_buckets
					 ? table->n_buckets * tuning->shrink_factor
					 : (table->n_buckets * tuning->shrink_factor
						 * tuning->growth_threshold));

				if (!hash_rehash (table, candidate))
				{
					/* Failure to allocate memory in an attempt to
					   shrink the table is not fatal.  But since memory
					   is low, we can at least be kind and free any
					   spare entries, rather than keeping them tied up
					   in the free entry list.  */
#if ! USE_OBSTACK
					struct hash_entry *cursor = table->free_entry_list;
					struct hash_entry *next;
					while (cursor)
					{
						next = cursor->next;
						//free (cursor);
						kp_free ((void **)&cursor, table->use_nvm);
						cursor = next;
					}
					table->free_entry_list = NULL;
#endif
				}
			}
		}
	}

	return data;
}

/* Testing.  */

#if TESTING

void
hash_print (const Hash_table *table)
{
	ht_die("don't use this function without checking that it isn't made "
			"unusably inconsistent due to our locking scheme in hash_insert_"
			"if_absent()!\n");  //PJH
	struct hash_entry *bucket = (struct hash_entry *) table->bucket;

	for ( ; bucket < table->bucket_limit; bucket++)
	{
		struct hash_entry *cursor;

		if (bucket)
			printf ("%lu:\n", (unsigned long int) (bucket - table->bucket));

		for (cursor = bucket; cursor; cursor = cursor->next)
		{
			char const *s = cursor->data;
			/* FIXME */
			if (s)
				printf ("  %s\n", s);
		}
	}
}

#endif /* TESTING */
