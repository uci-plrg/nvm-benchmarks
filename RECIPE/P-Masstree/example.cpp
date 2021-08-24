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
    void jaaru_recovery_procedure_begin();
    void jaaru_recovery_procedure_end();
}

typedef struct thread_data {
    uint32_t id;
    masstree::masstree *tree;
    uint64_t n;
    uint num_thread;
} thread_data_t;

uint64_t *counters = NULL;
uint64_t *keys = NULL;

void initOrRecoverPersistentData(uint64_t n) {
    if(getRegionFromID(0) != NULL && getRegionFromID(1) != NULL && getRegionFromID(2) != NULL) {
        masstreeptr = getRegionFromID(0);
        assert(masstreeptr);
#ifdef VERIFYFIXADV
        jaaru_recovery_procedure_begin();
        ((masstree::masstree*)masstreeptr)->recover();
        jaaru_recovery_procedure_end();
#endif
        counters = (uint64_t *) getRegionFromID(1);
        assert(counters);
        keys = (uint64_t *) getRegionFromID(2);
        assert(keys);
    } else {
        masstree::leafnode *init_root = new masstree::leafnode(0);
        masstreeptr = new masstree::masstree(init_root);
#ifdef VERIFYFIXADV
        ((masstree::masstree *)masstreeptr)->addLeafNode(init_root);
#endif
        setRegionFromID(0, masstreeptr);
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

    thread_data_t *tds = (thread_data_t *) calloc(num_thread, sizeof(thread_data_t));
    for (int i = 0; i < num_thread; i++){
        tds[i].tree = (masstree::masstree*)masstreeptr;
        tds[i].n = n;
        tds[i].num_thread = num_thread;
    }
    {
        // Build tree
        auto starttime = std::chrono::system_clock::now();
        auto func = [&](int thread_id) {
            tds[thread_id].id = thread_id;

            uint64_t start_key = tds[thread_id].n / tds[thread_id].num_thread * (uint64_t)tds[thread_id].id;
            uint64_t end_key = start_key + tds[thread_id].n / tds[thread_id].num_thread;
            uint64_t index = start_key;
            // First read to see the actual values made out to the memory
            for (; index < start_key + counters[thread_id]; index++) {
                uint64_t *ret = reinterpret_cast<uint64_t *> (tds[thread_id].tree->get(keys[index]));
                if (ret == NULL || *ret != keys[index]) {
                    uint64_t readVal = ret? *ret : 0;
                    std::cout << "wrong value read: " << readVal << " expected:" << keys[index] << std::endl;
                    //This write did not make out to the memory. So, we need to start inserting from here ... 
                    break;
                }
            }
            // Now resuming adding keys to the tree.
            for (uint64_t i = index; i < end_key; i++) {
                tds[thread_id].tree->put(keys[i], &keys[i]);
                counters[thread_id]++;
                masstree::clflush((char*)&counters[thread_id], sizeof(counters[thread_id]), true);
            }
            return NULL;
        };
        std::vector<std::thread> thread_group;

        for (int i = 0; i < num_thread; i++)
            thread_group.push_back(std::thread(func,i));

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
