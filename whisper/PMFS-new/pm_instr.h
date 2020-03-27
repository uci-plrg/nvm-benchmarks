/*
 * Macros to instrument PM reads and writes
 * Author : Sanketh Nalli 
 * Contact: nalli@wisc.edu
 *
 * The value returned by code surrounded by {}
 * is the value returned by last statement in
 * the block. These macros do not perform any
 * operations on the persistent variable itself,
 * and hence do not introduce any extra accesses
 * to persistent memory. 
 * 
 */

#ifndef PM_INSTR_H
#define PM_INSTR_H

#include <linux/types.h>
extern atomic64_t tot_epoch_count;

#define __FILENAME__ 			\
	(strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)
extern unsigned int pmfs_tracemask;
#define pmfs_trace_printk(args ...)     \
    {                                   \
        if(pmfs_tracemask)              \
            trace_printk(args);         \
    }

#define pmfs_no_trace(args ...)	{;}

#ifdef __TRACE__
#define PM_TRACE                        pmfs_trace_printk
#else
#define PM_TRACE			pmfs_no_trace
#endif

/* Cacheable PM write */
#define PM_WRT_MARKER                   "PM_W"
#define PM_DI_MARKER	                "PM_DI"

/* Cacheable PM read */
#define PM_RD_MARKER                    "PM_R"

/* Un-cacheable PM store */
#define PM_NTI                          "PM_I"

/* PM flush */
#define PM_FLUSH_MARKER                 "PM_L"
#define PM_FLUSHOPT_MARKER              "PM_O"

/* PM Delimiters */
#define PM_TX_START                     "PM_XS"
#define PM_FENCE_MARKER                 "PM_N"
#define PM_COMMIT_MARKER                "PM_C"
#define PM_BARRIER_MARKER               "PM_B"
#define PM_TX_END                       "PM_XE"

/* PM Write macros */
/* PM Write to variable */
#define PM_STORE(pm_dst, bytes)                     \
    ({                                              \
        PM_TRACE("%s:%p:%lu:%s:%d\n",               \
                        PM_WRT_MARKER,              \
                        (pm_dst),                   \
                        bytes,			    \
                        __FILENAME__,               \
                        __LINE__);                  \
    })

#define PM_WRITE(pm_dst)                            \
    ({                                              \
        PM_TRACE("%s:%p:%lu:%s:%d\n",               \
                        PM_WRT_MARKER,              \
                        &(pm_dst),                  \
                        sizeof((pm_dst)),           \
                        __FILENAME__,               \
                        __LINE__);                  \
                                                    \
        pm_dst;                                     \
    })

#define PM_WRITE_P(pm_dst)                          \
    ({                                              \
        PM_TRACE("%s:%p:%lu:%s:%d\n",               \
                        PM_WRT_MARKER,              \
                        &(pm_dst),                  \
                        sizeof((pm_dst)),           \
                        __FILENAME__,               \
                        __LINE__);                  \
                                                    \
        &(pm_dst);                                  \
    })

#define PM_EQU(pm_dst, y)                           \
    ({                                              \
            PM_TRACE("%s:%p:%lu:%s:%d\n",           \
                        PM_WRT_MARKER,              \
                        &(pm_dst),                  \
                        sizeof((pm_dst)),           \
                        __FILENAME__,               \
                        __LINE__);                  \
            pm_dst = y;                             \
    })

#define PM_OR_EQU(pm_dst, y)                        \
    ({                                              \
            PM_TRACE("%s:%p:%lu:%s:%d\n",           \
                        PM_WRT_MARKER,              \
                        &(pm_dst),                  \
                        sizeof((pm_dst)),           \
                        __FILENAME__,               \
                        __LINE__);                  \
            pm_dst |= y;                            \
    })

#define PM_AND_EQU(pm_dst, y)                       \
    ({                                              \
            PM_TRACE("%s:%p:%lu:%s:%d\n",           \
                        PM_WRT_MARKER,              \
                        &(pm_dst),                  \
                        sizeof((pm_dst)),           \
                        __FILENAME__,               \
                        __LINE__);                  \
            pm_dst &= y;                            \
    })

#define PM_ADD_EQU(pm_dst, y)                       \
    ({                                              \
            PM_TRACE("%s:%p:%lu:%s:%d\n",           \
                        PM_WRT_MARKER,              \
                        &(pm_dst),                  \
                        sizeof((pm_dst)),           \
                        __FILENAME__,               \
                        __LINE__);                  \
            pm_dst += y;                            \
    })

#define PM_SUB_EQU(pm_dst, y)                       \
    ({                                              \
            PM_TRACE("%s:%p:%lu:%s:%d\n",           \
                        PM_WRT_MARKER,              \
                        &(pm_dst),                  \
                        sizeof((pm_dst)),           \
                        __FILENAME__,               \
                        __LINE__);                  \
            pm_dst -= y;                            \
    })

/* PM Writes to a range of memory */
#define PM_MEMSET(pm_dst, val, sz)                  \
    ({                                              \
            PM_TRACE("%s:%p:%lu:%s:%d\n",           \
                        PM_WRT_MARKER,              \
                        (pm_dst),                   \
                        (unsigned long)sz,          \
                        __FILENAME__,               \
                        __LINE__);                  \
            memset(pm_dst, val, sz);                \
    }) 

#define PM_MEMCPY(pm_dst, src, sz)                  \
    ({                                              \
            PM_TRACE("%s:%p:%lu:%s:%d\n",           \
                        PM_WRT_MARKER,              \
                        (pm_dst),                   \
                        (unsigned long)sz,          \
                        __FILENAME__,               \
                        __LINE__);                  \
            memcpy(pm_dst, src, sz);                \
    })              

#define PM_STRCPY(pm_dst, src)                      \
    ({                                              \
            PM_TRACE("%s:%p:%u:%s:%d\n",            \
                        PM_WRT_MARKER,              \
                        (pm_dst),                   \
                        min((int)PMFS_NAME_LEN,     \
                            (int)strlen((src))),    \
                        __FILENAME__,               \
                        __LINE__);                  \
            strcpy(pm_dst, src);                    \
    })

#define PM_MOVNTI(pm_dst, count, copied)            \
    ({                                              \
            PM_TRACE("%s:%p:%lu:%lu:%s:%d\n",       \
                        PM_NTI,                     \
                        (pm_dst),                   \
                        (unsigned long)copied,      \
                        (unsigned long)count,       \
                        __FILENAME__,               \
                        __LINE__                    \
                    );                              \
            0;                                      \
    })

#define PM_MOVNTI_DI(pm_dst, count, copied)         \
    ({                                              \
            PM_TRACE("%s:%p:%lu:%lu:%s:%d\n",       \
                        PM_DI_MARKER,               \
                        (pm_dst),                   \
                        (unsigned long)copied,      \
                        (unsigned long)count,       \
                        __FILENAME__,               \
                        __LINE__                    \
                    );                              \
            0;                                      \
    })


/* PM Read macros */
/* Return the data    of persistent variable */
#define PM_READ(pm_src)                             \
    ({                                              \
            PM_TRACE("%s:%p:%lu:%s:%d\n",           \
                        PM_RD_MARKER,               \
                        &(pm_src),                  \
                        sizeof((pm_src)),           \
                        __FILENAME__,               \
                        __LINE__);                  \
            (pm_src);                               \
    })     

/* Return the address of persistent variable */
#define PM_READ_P(pm_src)                           \
    ({                                              \
            PM_TRACE("%s:%p:%lu:%s:%d\n",           \
                        PM_RD_MARKER,               \
                        &(pm_src),                  \
                        sizeof((pm_src)),           \
                        __FILENAME__,               \
                        __LINE__);                  \
            &(pm_src);                              \
    })    

/* Return the address of persistent variable */
#define PM_RD_WR_P(pm_src)                          \
    ({                                              \
        PM_TRACE("%s:%p:%lu:%s:%d\n",               \
                        PM_RD_MARKER,               \
                        &(pm_src),                  \
                        sizeof((pm_src)),           \
                        __FILENAME__,               \
                        __LINE__);                  \
        PM_TRACE("%s:%p:%lu:%s:%d\n",               \
                        PM_WRT_MARKER,              \
                        &(pm_src),                  \
                        sizeof((pm_src)),           \
                        __FILENAME__,               \
                        __LINE__);                  \
        &(pm_src);                                  \
    })    

/* PM Reads to a range of memory */
#define PM_MEMCMP(pm_dst, src, sz)                  \
    ({                                              \
            PM_TRACE("%s:%p:%lu:%s:%d\n",           \
                        PM_RD_MARKER,               \
                        (pm_dst),                   \
                        (unsigned long)sz,          \
                        __FILENAME__,               \
                        __LINE__);                  \
            memcmp(pm_dst, src, sz);                \
    })


#define start_epoch()       ({;})
#define end_epoch()         ({;})

#define PM_TX_BEGIN()                               \
    ({                                              \
        PM_TRACE("%s:%s:%d\n",                      \
                PM_TX_START,                        \
                __FILENAME__,                       \
                __LINE__);                          \
    })

#define PM_TX_COMMIT()                              \
    ({                                              \
        PM_TRACE("%s:%s:%d\n",                      \
                PM_TX_END,                          \
                __FILENAME__,                       \
                __LINE__);                          \
    })

/* PM Persist operations 
 * (done/copied) followed by count to maintain 
 * uniformity with other macros
 */
#define PM_FLUSH(pm_dst, count, done)               \
    ({                                              \
        PM_TRACE("%s:%p:%u:%u:%s:%d\n",             \
                    PM_FLUSH_MARKER,                \
                    (pm_dst),                       \
                    done,                           \
                    count,                          \
                    __FILENAME__,                   \
                    __LINE__                        \
                );                                  \
    })
#define PM_FLUSHOPT(pm_dst, count, done)            \
    ({                                              \
        PM_TRACE("%s:%p:%u:%u:%s:%d\n",             \
                    PM_FLUSHOPT_MARKER,             \
                    (pm_dst),                       \
                    done,                           \
                    count,                          \
                    __FILENAME__,                   \
                    __LINE__                        \
                );                                  \
    })

#define PM_COMMIT()                                 \
    ({                                              \
        PM_TRACE("%s:%s:%d\n", PM_COMMIT_MARKER,    \
                    __FILENAME__, __LINE__);        \
    })
#define PM_BARRIER()                                \
    ({                                              \
        PM_TRACE("%s:%s:%d\n", PM_BARRIER_MARKER,   \
                    __FILENAME__, __LINE__);        \
    })

/*
 * We know that PMFS is careful in
 * issuing fences and only issues one
 * after a write to PM. Hence, there
 * are no null-epochs or stray fences.
 * Therefore, we don't need a per-thread
 * write counter, but a global fence counter
 * is a sufficient indicator of the number
 * of epochs in workload execution.
 */
#define PM_FENCE()                                  \
    ({                                              \
        PM_TRACE("%s:%s:%d\n", PM_FENCE_MARKER,     \
                    __FILENAME__, __LINE__);        \
	atomic64_inc(&tot_epoch_count);             \
    })

#endif /* PM_INSTR_H */
