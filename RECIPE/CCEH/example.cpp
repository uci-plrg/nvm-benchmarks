#include <string>
#include <random>
#include <iostream>
#include <future>
#include <assert.h>
#include "src/CCEH.h"

using namespace std;

extern "C" {
    void * getRegionFromID(uint ID);
    void setRegionFromID(uint ID, void *ptr);
}

typedef struct thread_data {
    uint32_t id;
    CCEH *hashtable;
} thread_data_t;

CCEH* hashTable = NULL;
uint64_t *counters = NULL;

void run(char **argv) {
    std::cout << "Simple Example of CCEH" << std::endl;

    uint64_t n = std::atoll(argv[1]);
    uint64_t *keys = new uint64_t[n];

    // Generate keys
    for (uint64_t i = 0; i < n; i++) {
        keys[i] = i + 1;
    }

    int num_thread = atoi(argv[2]);

    if(getRegionFromID(0) == NULL){
        hashTable = new CCEH(2);
        clflush((char*)hashTable, sizeof(CCEH*), false, true);
        setRegionFromID(0, hashTable);
        
    } else {
        hashTable = (CCEH*) getRegionFromID(0);
        assert(hashTable);
    }
    if (getRegionFromID(1) == NULL) {
        //Make sure counters and hashtable aren't in the same line:
        // 64 bytes + n*sizeof(uint64_t) + 64 bytes.
        counters = (uint64_t *)calloc(n + 16, sizeof(uint64_t));
        counters = &counters[8];
        clflush((char*)counters, sizeof(uint64_t)*n, false, true);
        setRegionFromID(1, counters);
    } else {
        counters = (uint64_t *) getRegionFromID(1);
        assert(counters);
    }
    thread_data_t *tds = (thread_data_t *) malloc(num_thread * sizeof(thread_data_t));

    std::atomic<int> next_thread_id;

    {
        // Load
        auto starttime = std::chrono::system_clock::now();
        next_thread_id.store(0);
        auto func = [&]() {
            int thread_id = next_thread_id.fetch_add(1);
            tds[thread_id].id = thread_id;
            tds[thread_id].hashtable = hashTable;

            uint64_t start_key = n / num_thread * (uint64_t)thread_id;
            uint64_t end_key = start_key + n / num_thread;
            uint64_t index = start_key;
            // First read to see the actual values made out to the memory
            for (; index < start_key + counters[tds->id]; index++) {
                const char * val = tds[thread_id].hashtable->Get( keys[index]);
                if (val != (const char *)keys[index]) {
                    std::cout << "[CCEH] wrong key read: " << val << "expected: " << keys[index] << std::endl;
                    //This write did not make out to the memory. So, we need to start inserting from here ... 
                    break;
                }
            }
            // Now resuming adding keys to the tree.
            for (uint64_t i = index; i < end_key; i++) {
                assert(tds[thread_id].hashtable != NULL);
                    tds[thread_id].hashtable->Capacity();
                tds[thread_id].hashtable->Insert( keys[i], reinterpret_cast<const char*>( keys[i]));
                counters[thread_id]++;
                clflush((char*)&counters[thread_id], sizeof(counters[thread_id]), false, true);
            }
        };

        std::vector<std::thread> thread_group;

        for (int i = 0; i < num_thread; i++)
            thread_group.push_back(std::thread{func});

        for (int i = 0; i < num_thread; i++)
            thread_group[i].join();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: load, %f ,ops/us\n", (n * 1.0) / duration.count());
    }

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