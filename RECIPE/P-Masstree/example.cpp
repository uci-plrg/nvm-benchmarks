#include <iostream>
#include <chrono>
#include <random>
#include <atomic>
#include <thread>

using namespace std;

#include "masstree.h"

extern "C" {
    void * getRegionFromID(uint ID);
    void setRegionFromID(uint ID, void *ptr);
}

typedef struct thread_data {
    uint32_t id;
    masstree::masstree *tree;
    uint64_t n;
    uint num_thread;
} thread_data_t;

masstree::masstree *tree = NULL;
uint64_t *counters = NULL;
uint64_t *keys = NULL;

void initOrRecoverPersistentData(uint64_t n) {
    if(getRegionFromID(0) != NULL && getRegionFromID(1) != NULL && getRegionFromID(2) != NULL) {
        tree = (masstree::masstree *) getRegionFromID(0);
        assert(tree);
        counters = (uint64_t *) getRegionFromID(1);
        assert(counters);
        keys = (uint64_t *) getRegionFromID(2);
        assert(keys);
    } else {
        masstree::leafnode *init_root = new masstree::leafnode(0);
        tree = new masstree::masstree(init_root);
        setRegionFromID(0, tree);
        //Make sure counters and hashtable aren't in the same line:
        // 64 bytes + n*sizeof(uint64_t) + 64 bytes.
        counters = (uint64_t *)calloc(n + 16, sizeof(uint64_t));
        counters = &counters[8];
        masstree::clflush((char*)counters, sizeof(uint64_t)*n, true);
        setRegionFromID(1, counters);
        keys = new uint64_t[n];
        // Generate keys
        for (uint64_t i = 0; i < n; i++) {
            keys[i] = i + 1;
        }
        masstree::clflush((char*)keys, sizeof(uint64_t) * n , true);
        setRegionFromID(2, keys);
    }
}

void run(char **argv) {
    std::cout << "Simple Example of P-Masstree" << std::endl;

    uint64_t n = std::atoll(argv[1]);

    int num_thread = atoi(argv[2]);
    initOrRecoverPersistentData(n);

    thread_data_t *tds = (thread_data_t *) malloc(num_thread * sizeof(thread_data_t));
    std::atomic<int> next_thread_id;
    next_thread_id.store(0);
    for (int i = 0; i < num_thread; i++){
        tds[i].tree = tree;
        tds[i].n = n;
        tds[i].num_thread = num_thread;
    }
    {
        // Build tree
        auto starttime = std::chrono::system_clock::now();
        auto func = [&]() {
            int thread_id = next_thread_id.fetch_add(1);
            tds[thread_id].id = thread_id;

            uint64_t start_key = tds[thread_id].n / tds[thread_id].num_thread * (uint64_t)tds[thread_id].id;
            uint64_t end_key = start_key + tds[thread_id].n / tds[thread_id].num_thread;
            uint64_t index = start_key;
            // First read to see the actual values made out to the memory
            for (; index < start_key + counters[thread_id]; index++) {
                uint64_t *ret = reinterpret_cast<uint64_t *> (tree->get(keys[index]));
                if (ret == NULL || *ret != keys[index]) {
                    uint64_t readVal = ret? *ret : 0;
                    std::cout << "wrong value read: " << readVal << " expected:" << keys[index] << std::endl;
                    //This write did not make out to the memory. So, we need to start inserting from here ... 
                    break;
                }
            }
            // Now resuming adding keys to the tree.
            for (uint64_t i = index; i < end_key; i++) {
                counters[thread_id]++;
                tree->put(keys[i], &keys[i]);
                masstree::clflush((char*)&counters[thread_id], sizeof(counters[thread_id]), true);
            }
            return NULL;
        };
        std::vector<std::thread> thread_group;

        for (int i = 0; i < num_thread; i++)
            thread_group.push_back(std::thread{func});

        for (int i = 0; i < num_thread; i++)
            thread_group[i].join();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: insert,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: insert,%ld,%f sec\n", n, duration.count() / 1000000.0);
    }

    delete tds;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        printf("usage: %s [n] [nthreads]\nn: number of keys (integer)\nnthreads: number of threads (integer)\n", argv[0]);
        return 1;
    }
    run(argv);
    return 0;
}
