#include <iostream>
#include <chrono>
#include <random>
#include <thread>
#include <atomic>

using namespace std;

#include "clht.h"
#include "ssmem.h"
#include "../cacheops.h"

extern "C" {
    void * getRegionFromID(uint ID);
    void setRegionFromID(uint ID, void *ptr);
}


typedef struct thread_data {
    uint32_t id;
    clht_t *ht;
    uint64_t *keys;
    uint64_t n;
    uint num_thread;
} thread_data_t;

typedef struct barrier {
    pthread_cond_t complete;
    pthread_mutex_t mutex;
    int count;
    int crossing;
} barrier_t;

void barrier_init(barrier_t *b, int n) {
    pthread_cond_init(&b->complete, NULL);
    pthread_mutex_init(&b->mutex, NULL);
    b->count = n;
    b->crossing = 0;
}

void barrier_cross(barrier_t *b) {
    pthread_mutex_lock(&b->mutex);
    b->crossing++;
    if (b->crossing < b->count) {
        pthread_cond_wait(&b->complete, &b->mutex);
    } else {
        pthread_cond_broadcast(&b->complete);
        b->crossing = 0;
    }
    pthread_mutex_unlock(&b->mutex);
}

barrier_t barrier;
clht_t *hashtable = NULL;
uint64_t *counters = NULL;

void run(char **argv) {
    std::cout << "Simple Example of P-CLHT" << std::endl;
    int num_thread = atoi(argv[2]);
    uint64_t n = std::atoll(argv[1]);
    uint64_t *keys = new uint64_t[n];

    // Generate keys
    for (uint64_t i = 0; i < n; i++) {
        keys[i] = i + 1;
    }
    // Retreiving data structure from persistent memory.
    if (getRegionFromID(0) != NULL && getRegionFromID(1) != NULL) {
        hashtable = (clht_t*) getRegionFromID(0);
        counters = (uint64_t *) getRegionFromID(1);
    } else {
        hashtable = clht_create(512);
        setRegionFromID(0, hashtable);
        //Make sure counters and hashtable aren't in the same line:
        // 64 bytes + n*sizeof(uint64_t) + 64 bytes.
        counters = (uint64_t *)calloc(n + 16, sizeof(uint64_t));
        counters = &counters[8];
        setRegionFromID(1, counters);
    }  
    barrier_init(&barrier, num_thread);
    std::atomic<int> next_thread_id;
    next_thread_id.store(0);
    
    //Initializing tds...
    thread_data_t *tds = (thread_data_t *) malloc(num_thread * sizeof(thread_data_t));
    for (int i = 0; i < num_thread; i++){
            tds[i].id = next_thread_id.fetch_add(1);
            tds[i].ht = hashtable;
            tds[i].keys = keys;
            tds[i].n = n;
            tds[i].num_thread = num_thread;
    }
    {
        // Load
        auto starttime = std::chrono::system_clock::now();
        auto func = [](void * arg) -> void * {
            thread_data_t *tds = (thread_data_t *) arg;

            uint64_t start_key = tds->n / tds->num_thread * (uint64_t)tds->id;
            uint64_t end_key = start_key + tds->n / tds->num_thread;

            clht_gc_thread_init(tds->ht, tds->id);
            barrier_cross(&barrier);
            uint64_t index = start_key;
            // First read to see the actual values made out to the memory
            for (; index < start_key + counters[tds->id]; index++) {
                    uintptr_t val = clht_get(tds->ht->ht, tds->keys[index]);
                    if (val != tds->keys[index]) {
                        std::cout << "[CLHT] wrong key read: " << val << "expected: " << tds->keys[index] << std::endl;
                        //This write did not make out to the memory. So, we need to start inserting from here ... 
                        break;
                    }
            }
            // Now resuming adding keys to the tree.
            for (uint64_t i = index; i < end_key; i++) {
                counters[tds->id]++;
                clht_put(tds->ht, tds->keys[i], tds->keys[i]);
                PMCHECK::clflush((char*)&counters[tds->id], sizeof(counters[tds->id]), false, true);
            }
            return NULL;
        };

        pthread_t *threads = new pthread_t[num_thread];

        for (int i = 0; i < num_thread; i++){
            pthread_create(&threads[i], NULL, func, &tds[i]);
        }

        for (int i = 0; i < num_thread; i++)
            pthread_join(threads[i], NULL); 
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: load, %f ,ops/us\n", (n * 1.0) / duration.count());
        delete [] threads;
    }

    //BCD Should not free hashtable!!!
    //    clht_gc_destroy(hashtable);

    delete[] keys;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        printf("usage: %s [n] [nthreads]\nn: number of keys (integer)\nnthreads: number of threads (integer)\n", argv[0]);
        return 1;
    }
    run(argv);
    return 0;
}
