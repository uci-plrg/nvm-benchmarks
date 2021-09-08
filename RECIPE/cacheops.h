/**
 * Contains cache operations.
 */

#ifndef CACHEOPS_H
#define CACHEOPS_H

#define BUGFIX 1

namespace PMCHECK {

static unsigned long write_latency_in_ns = 0;
static unsigned long cpu_freq_mhz = 2100;
static unsigned long cache_line_size = 64;

static inline void cpu_pause()
{
    __asm__ volatile ("pause" ::: "memory");
}

static inline unsigned long read_tsc(void)
{
    unsigned long var;
    unsigned int hi, lo;

    asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
    var = ((unsigned long long int) hi << 32) | lo;

    return var;
}

inline void mfence()
{
    asm volatile("mfence":::"memory");
}

inline void clflush(char *data, int len, bool front, bool back)
{
    volatile char *ptr = (char *)((unsigned long)data & ~(cache_line_size - 1));
    if (front)
        mfence();
    for (; ptr < data+len; ptr += cache_line_size){
        unsigned long etsc = read_tsc() +
            (unsigned long)(write_latency_in_ns * cpu_freq_mhz/1000);
#ifdef CLFLUSH
        asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
#elif CLFLUSH_OPT
        asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(ptr)));
#elif CLWB
        asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(ptr)));
#endif
        while (read_tsc() < etsc) cpu_pause();
    }
    if (back)
        mfence();
}

}
#endif