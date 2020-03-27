// clibpm

#include <clibpm.h>
#include <pthread.h>

pthread_mutex_t pmp_mutex;
static void *pmemalloc_reserve(size_t size);
static void pmemalloc_free(void *abs_ptr_);


void* pmalloc(size_t sz) {
  pthread_mutex_lock(&pmp_mutex);
/*  if(sz==32){ //32 is the size of vector, and vectors can be appended, so we need to reserve extra space 
      // to avoid the need for realloc
      sz=100*32; 
  }*/
  void* ret = pmemalloc_reserve(sz);
  pthread_mutex_unlock(&pmp_mutex);
  return ret;
}

void pfree(void *p) {
  pthread_mutex_lock(&pmp_mutex);
  pmemalloc_free(p);
  pthread_mutex_unlock(&pmp_mutex);
}


unsigned int get_next_pp() {
  pthread_mutex_lock(&pmp_mutex);
  unsigned int ret = sp->itr;
  PM_EQU((sp->itr), (sp->itr+1));
  pthread_mutex_unlock(&pmp_mutex);
  return ret;
}

struct static_info *sp;
int pmem_debug;
size_t pmem_orig_size;

// debug -- printf-like debug messages
void debug(const char *file, int line, const char *func, const char *fmt, ...) {
  va_list ap;
  int save_errno;

  //if (!Debug)
  //  return;

  save_errno = errno;
  fprintf(stderr, "debug: %s:%d %s()", file, line, func);
  if (fmt) {
    fprintf(stderr, ": ");
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
  }
  fprintf(stderr, "\n");
  errno = save_errno;
}

// fatal -- printf-like error exits, with and without errno printing
void fatal(int err, const char *file, int line, const char *func,
           const char *fmt, ...) {
  va_list ap;

  fprintf(stderr, "ERROR: %s:%d %s()", file, line, func);
  if (fmt) {
    fprintf(stderr, ": ");
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
  }
  if (err)
    fprintf(stderr, ": %s", strerror(err));
  fprintf(stderr, "\n");
  exit(1);
}

/*
struct clump {
  size_t size;  // size of the clump
  size_t prevsize;  // size of previous (lower) clump
  struct {
    off_t off;
    void *ptr_;
  } ons[PMEM_NUM_ON];
};
*/
void init_clump(struct clump *obj) {
    obj->size  = 0;
    obj->prevsize = 0;
}

// pool header kept at a known location in each memory-mapped file
struct pool_header {
  char signature[16]; /* must be PMEM_SIGNATURE */
  size_t totalsize; /* total file size */
  char padding[4096 - 16 - sizeof(size_t)];
};

void init_pool_header(struct pool_header* obj){
    obj->totalsize = 0;
}

// Global memory pool
void* pmp;

// display pmem pool
void pmemalloc_display() {
  struct clump* clp;
  size_t sz;
  size_t prev_sz;
  int state;
  clp = (struct clump *)ABS_PTR((struct clump *) PMEM_CLUMP_OFFSET);

  fprintf(stdout,
          "----------------------------------------------------------\n");
  while (1) {
    sz = clp->size & ~PMEM_STATE_MASK;
    prev_sz = clp->prevsize;
    state = clp->size & PMEM_STATE_MASK;

    fprintf(stdout, "%lu (%d)(%p)(%lu) -> ", sz, state, (struct clump *)REL_PTR(clp), prev_sz);

    if (clp->size == 0)
      break;

    clp = (struct clump *) ((uintptr_t) clp + sz);
  }

  fprintf(stdout, "\n");
  fprintf(stdout,
          "----------------------------------------------------------\n");

  fflush(stdout);
}

// pmemalloc_validate clump metadata
void pmemalloc_validate(struct clump* clp) {
  size_t sz;
  struct clump* next;

  sz = clp->size & ~PMEM_STATE_MASK;
  if (sz == 0)
    return;

  next = (struct clump *) ((uintptr_t) clp + sz);

  if (sz != next->prevsize) {
    DEBUG("clp : %p clp->size : %lu lastfree : %p lastfree->prevsize : %lu",
        clp, sz, next, next->prevsize);
    pmemalloc_display();
    exit(EXIT_FAILURE);
  }
}

// check pmem pool health
void check() {
  struct clump *clp, *lastclp;
  clp = (struct clump *)ABS_PTR((struct clump *) PMEM_CLUMP_OFFSET);

  lastclp =
      (struct clump *)ABS_PTR(
          (struct clump *) (pmem_orig_size & ~(PMEM_CHUNK_SIZE - 1)) - PMEM_CHUNK_SIZE);

  if (clp->size == 0)
    FATAL("no clumps found");

  while (clp->size) {
    size_t sz = clp->size & ~PMEM_STATE_MASK;
    clp = (struct clump *) ((uintptr_t) clp + sz);
  }

  if (clp != lastclp) {
    pmemalloc_display();
    FATAL("clump list stopped at %lx instead of %lx", clp, lastclp);
  }

}

/*
 * pmemalloc_recover -- recover after a possible crash
 *
 * Internal support routine, used during recovery.
 */
static void pmemalloc_recover(void *pmp) {
  struct clump *clp;
  int i;

  DEBUG("pmp=0x%lx", pmp);

  clp = PMEM(pmp, (struct clump *)PMEM_CLUMP_OFFSET);

  while (clp->size) {
    size_t sz = clp->size & ~PMEM_STATE_MASK;
    int state = clp->size & PMEM_STATE_MASK;

    DEBUG("[0x%lx]clump size %lx state %d", OFF(pmp, clp), sz, state);

    switch (state) {
      case PMEM_STATE_RESERVED:
        /* return the clump to the FREE pool */
        for (i = PMEM_NUM_ON - 1; i >= 0; i--)
          clp->on[i].off = 0;
        pmem_persist(clp, sizeof(*clp), 0);
        clp->size = sz | PMEM_STATE_FREE;
        pmem_persist(clp, sizeof(*clp), 0);
        break;

      case PMEM_STATE_ACTIVATING:
        /* finish progressing the clump to ACTIVE */
        for (i = 0; i < PMEM_NUM_ON; i++)
          if (clp->on[i].off) {
            uintptr_t *dest = PMEM(pmp, (uintptr_t * )clp->on[i].off);
            *dest = (uintptr_t) clp->on[i].ptr_;
            pmem_persist(dest, sizeof(*dest), 0);
          } else
            break;
        for (i = PMEM_NUM_ON - 1; i >= 0; i--)
          clp->on[i].off = 0;
        pmem_persist(clp, sizeof(*clp), 0);
        clp->size = sz | PMEM_STATE_ACTIVE;
        pmem_persist(clp, sizeof(*clp), 0);
        break;

      case PMEM_STATE_FREEING:
        /* finish progressing the clump to FREE */
        for (i = 0; i < PMEM_NUM_ON; i++)
          if (clp->on[i].off) {
            uintptr_t *dest = PMEM(pmp, (uintptr_t * )clp->on[i].off);
            *dest = (uintptr_t) clp->on[i].ptr_;
            pmem_persist(dest, sizeof(*dest), 0);
          } else
            break;
        for (i = PMEM_NUM_ON - 1; i >= 0; i--)
          clp->on[i].off = 0;
        pmem_persist(clp, sizeof(*clp), 0);
        clp->size = sz | PMEM_STATE_FREE;
        pmem_persist(clp, sizeof(*clp), 0);
        break;
    }

    clp = (struct clump *) ((uintptr_t) clp + sz);
    DEBUG("next clp %lx, offset 0x%lx", clp, OFF(pmp, clp));
  }
}

/*
 * pmemalloc_coalesce_free -- find adjacent free blocks and coalesce them
 *
 * Scan the pmeme pool for recovery work:
 *      - RESERVED clumps that need to be freed
 *      - ACTIVATING clumps that need to be ACTIVE
 *      - FREEING clumps that need to be freed
 *
 * Internal support routine, used during recovery.
 */
static void pmemalloc_coalesce_free(void *pmp) {
  struct clump *clp;
  struct clump *firstfree;
  struct clump *lastfree;
  size_t csize;

  DEBUG("pmp=0x%lx", pmp);

  firstfree = lastfree = NULL;
  csize = 0;
  clp = PMEM(pmp, (struct clump *)PMEM_CLUMP_OFFSET);

  while (clp->size) {
    size_t sz = clp->size & ~PMEM_STATE_MASK;
    int state = clp->size & PMEM_STATE_MASK;

    DEBUG("[0x%lx]clump size %lx state %d", OFF(pmp, clp), sz, state);

    if (state == PMEM_STATE_FREE) {
      if (firstfree == NULL)
        firstfree = clp;
      else
        lastfree = clp;
      csize += sz;
    } else if (firstfree != NULL && lastfree != NULL) {
      DEBUG("coalesced size 0x%lx", csize);
      firstfree->size = csize | PMEM_STATE_FREE;
      pmem_persist(firstfree, sizeof(*firstfree), 0);
      firstfree = lastfree = NULL;
      csize = 0;
    } else {
      firstfree = lastfree = NULL;
      csize = 0;
    }

    clp = (struct clump *) ((uintptr_t) clp + sz);
    DEBUG("next clp %lx, offset 0x%lx", clp, OFF(pmp, clp));
  }
  if (firstfree != NULL && lastfree != NULL) {
    DEBUG("coalesced size 0x%lx", csize);
    DEBUG("firstfree 0x%lx next clp after firstfree will be 0x%lx", firstfree,
          (uintptr_t )firstfree + csize);
    firstfree->size = csize | PMEM_STATE_FREE;
    pmem_persist(firstfree, sizeof(*firstfree), 0);
  }
}

// pmemalloc_init -- setup a Persistent Memory pool for use
void *pmemalloc_init(const char *path, size_t size) {
  int err;
  int fd = -1;
  struct stat stbuf;

  if (pthread_mutex_init(&pmp_mutex, NULL) != 0)
  {
      printf("\n mutex init failed\n");
      exit(0);
  }

  DEBUG("path=%s size=0x%lx", path, size);
  pmem_orig_size = size;

  if (stat(path, &stbuf) < 0) {
    struct clump cl;
    init_clump(&cl);
    struct pool_header hdr; 
    init_pool_header(&hdr);
    size_t lastclumpoff;

    if (errno != ENOENT)
      goto out;

    /*
     * file didn't exist, we're creating a new memory pool
     */
    if (size < PMEM_MIN_POOL_SIZE) {
      DEBUG("size %lu too small (must be at least %lu)", size,
          PMEM_MIN_POOL_SIZE);
      errno = EINVAL;
      goto out;
    }

    ASSERTeq(sizeof(cl), PMEM_CHUNK_SIZE);ASSERTeq(sizeof(hdr), PMEM_PAGE_SIZE);

    if ((fd = open(path, O_CREAT | O_RDWR, 0666)) < 0)
      goto out;

    if ((errno = posix_fallocate(fd, 0, size)) != 0)
      goto out;

    /*
     * location of last clump is calculated by rounding the file
     * size down to a multiple of 64, and then subtracting off
     * another 64 to hold the struct clump.  the last clump is
     * indicated by a size of zero (so no write is necessary
     * here since the file is initially zeros.
     */
    lastclumpoff = (size & ~(PMEM_CHUNK_SIZE - 1)) - PMEM_CHUNK_SIZE;

    /*
     * create the first clump to cover the entire pool
     */
    cl.size = lastclumpoff - PMEM_CLUMP_OFFSET;
    if (pwrite(fd, &cl, sizeof(cl), PMEM_CLUMP_OFFSET) < 0){
        DEBUG("[0x%lx] created clump, size 0x%lx", PMEM_CLUMP_OFFSET, cl.size);
        goto out;
    }
    /*
     * write the pool header
     */
    strcpy(hdr.signature, PMEM_SIGNATURE);
    hdr.totalsize = size;
    if (pwrite(fd, &hdr, sizeof(hdr), PMEM_HDR_OFFSET) < 0)
      goto out;

    if (fsync(fd) < 0)
      goto out;

  } else {

    if ((fd = open(path, O_RDWR)) < 0)
      goto out;

    size = stbuf.st_size;
  }

  /*
   * map the file
   */
  if ((pmp = pmem_map(fd, size)) == NULL) {
    DEBUG("fd : %d size : %lu \n", fd, size);
    perror("mapping failed ");
    goto out;
  }

  /*
   * scan pool for recovery work, five kinds:
   *    1. pmem pool file sisn't even fully setup
   *    2. RESERVED clumps that need to be freed
   *    3. ACTIVATING clumps that need to be ACTIVE
   *    4. FREEING clumps that need to be freed
   *    5. adjacent free clumps that need to be coalesced
   */
  pmemalloc_recover(pmp);
  pmemalloc_coalesce_free(pmp);

  return pmp;

  out: err = errno;
  if (fd != -1)
    close(fd);
  errno = err;
  return NULL;
}


// ROTATING FIRST FIT
struct clump* prev_clp = NULL;

// pmemalloc_reserve -- allocate memory, volatile until pmemalloc_activate()
static
void *pmemalloc_reserve(size_t size) {
  size_t nsize;

  if (size <= 64) {
    nsize = 128;
  } else {
      size_t temp = 63;
    nsize = 64 + ((size + 63) & ~temp);
  }

  //cerr<<"size :: "<<size<<" nsize :: "<<nsize<<endl;
  struct clump *clp;
  struct clump* next_clp;
  int loop = 0;
  DEBUG("size= %zu", nsize);

  if (prev_clp != NULL) {
    clp = prev_clp;
//    printf("prev_clp=%p\n", prev_clp);
  } else {
    clp = (struct clump *)ABS_PTR((struct clump *) PMEM_CLUMP_OFFSET);
  }

  DEBUG("pmp=%p clp= %p, size of clp=%d size of struct clump =%d", pmp, clp, sizeof(clp), sizeof(struct clump));

  /* first fit */
  check:  //unsigned int itr = 0;
  while (clp->size) {
//    DEBUG("************** itr :: %lu ", itr++);
    size_t sz = clp->size & ~PMEM_STATE_MASK;
    int state = clp->size & PMEM_STATE_MASK;
    DEBUG("size : %lu state : %d", sz, state);

    if (nsize <= sz) {
      if (state == PMEM_STATE_FREE) {
        void *ptr = (void *) (uintptr_t) clp + PMEM_CHUNK_SIZE
            - (uintptr_t) pmp;
        size_t leftover = sz - nsize;

        DEBUG("fit found ptr 0x%lx, leftover %lu bytes", ptr, leftover);
        if (leftover >= PMEM_CHUNK_SIZE * 2) {
          struct clump *newclp;
          newclp = (struct clump *) ((uintptr_t) clp + nsize);

          DEBUG("splitting: [0x%lx] new clump", (struct clump *)REL_PTR(newclp));
          /*
           * can go ahead and start fiddling with
           * this freely since it is in the middle
           * of a free clump until we change fields
           * in *clp.  order here is important:
           *  1. initialize new clump
           *  2. persist new clump
           *  3. initialize existing clump do list
           *  4. persist existing clump
           *  5. set new clump size, RESERVED
           *  6. persist existing clump
           */
          PM_EQU((newclp->size), (leftover | PMEM_STATE_FREE));
          PM_EQU((newclp->prevsize), (nsize));
          pmem_persist(newclp, sizeof(*newclp), 0);

          next_clp = (struct clump *) ((uintptr_t) newclp + leftover);
          PM_EQU((next_clp->prevsize), (leftover));
          pmem_persist(next_clp, sizeof(*next_clp), 0);

          PM_EQU((clp->size), (nsize | PMEM_STATE_RESERVED));
          pmem_persist(clp, sizeof(*clp), 0);

          //DEBUG("validate new clump %p", REL_PTR(newclp));
          //DEBUG("validate orig clump %p", REL_PTR(clp));
          //DEBUG("validate next clump %p", REL_PTR(next_clp));
        } else {
          DEBUG("no split required");

          PM_EQU((clp->size), (sz | PMEM_STATE_RESERVED));
          pmem_persist(clp, sizeof(*clp), 0);

          next_clp = (struct clump *) ((uintptr_t) clp + sz);
          PM_EQU((next_clp->prevsize), (sz));
          pmem_persist(next_clp, sizeof(*next_clp), 0);

          //DEBUG("validate orig clump %p", REL_PTR(clp));
          //DEBUG("validate next clump %p", REL_PTR(next_clp));
        }

        prev_clp = clp;
        return ABS_PTR(ptr);
      }
    }

    clp = (struct clump *) ((uintptr_t) clp + sz);
    DEBUG("next clump :: [0x%lx]", (struct clump *)REL_PTR(clp));
  }

  if (loop == 0) {
    DEBUG("LOOP ");
    loop = 1;
    clp = (struct clump *)ABS_PTR((struct clump *) PMEM_CLUMP_OFFSET);
    goto check;
  }
    
  printf("no free memory of size %lu available \n", nsize);
  printf("Increase the size of the PM pool:\n");
  printf("Increase PSEGMENT_RESERVED_REGION_SIZE in whisper/kv-echo/echo/include/pm_instr.h and rebuild echo\n");
  //display();
  errno = ENOMEM;
  exit(EXIT_FAILURE);
  return NULL;
}

// pmemalloc_free -- free memory, find adjacent free blocks and coalesce them
static
void pmemalloc_free(void *abs_ptr_) {

  if (abs_ptr_ == NULL)
    return;

  struct clump *clp, *firstfree, *lastfree, *next_clp;
  int first = 1, last = 1;
  size_t csize;
  size_t sz;

  firstfree = lastfree = NULL;
  csize = 0;

  DEBUG("ptr_=%lx", abs_ptr_);

  clp = (struct clump *) ((uintptr_t) abs_ptr_ - PMEM_CHUNK_SIZE);
  sz = clp->size & ~PMEM_STATE_MASK;
  DEBUG("size=%lu", sz);

  lastfree = (struct clump *) ((uintptr_t) clp + sz);
  //DEBUG("validate lastfree %p", REL_PTR(lastfree));
  if ((lastfree->size & PMEM_STATE_MASK) != PMEM_STATE_FREE)
    last = 0;

  firstfree = (struct clump *) ((uintptr_t) clp - clp->prevsize);
  //DEBUG("validate firstfree %p", REL_PTR(firstfree));
  if (firstfree == clp
      || ((firstfree->size & PMEM_STATE_MASK) != PMEM_STATE_FREE))
    first = 0;

  if (first && last) {
    DEBUG("******* F C L ");

    size_t first_sz = firstfree->size & ~PMEM_STATE_MASK;
    size_t last_sz = lastfree->size & ~PMEM_STATE_MASK;
    csize = first_sz + sz + last_sz;
    PM_EQU((firstfree->size), (csize | PMEM_STATE_FREE));
    pmem_persist(firstfree, sizeof(*firstfree), 0);

    next_clp = (struct clump *) ((uintptr_t) lastfree + last_sz);
    PM_EQU((next_clp->prevsize), (csize));
    pmem_persist(next_clp, sizeof(*next_clp), 0);

    prev_clp = firstfree;

    //DEBUG("validate firstfree %p", REL_PTR(firstfree));
  } else if (first) {
    DEBUG("******* F C  ");

    size_t first_sz = firstfree->size & ~PMEM_STATE_MASK;
    csize = first_sz + sz;
    PM_EQU((firstfree->size), (csize | PMEM_STATE_FREE));
    pmem_persist(firstfree, sizeof(*firstfree), 0);

    next_clp = lastfree;
    PM_EQU((next_clp->prevsize), (csize));
    pmem_persist(next_clp, sizeof(*next_clp), 0);

    prev_clp = firstfree;

    //DEBUG("validate firstfree %p", REL_PTR(firstfree));
    //DEBUG("validate lastfree %p", REL_PTR(firstfree));
  } else if (last) {
    DEBUG("******* C L ");
    size_t last_sz = lastfree->size & ~PMEM_STATE_MASK;

    csize = sz + last_sz;
    PM_EQU((clp->size), (csize | PMEM_STATE_FREE));
    pmem_persist(clp, sizeof(*clp), 0);

    next_clp = (struct clump *) ((uintptr_t) lastfree + last_sz);
    PM_EQU((next_clp->prevsize), (csize));
    pmem_persist(next_clp, sizeof(*next_clp), 0);

    prev_clp = clp;

    //DEBUG("validate firstfree %p", REL_PTR(firstfree));
    //DEBUG("validate clump %p", REL_PTR(clp));
  } else {
    DEBUG("******* C ");

    csize = sz;
    PM_EQU((clp->size), (csize | PMEM_STATE_FREE));
    pmem_persist(clp, sizeof(*clp), 0);

    //DEBUG("validate clump %p", REL_PTR(clp));
  }

}
