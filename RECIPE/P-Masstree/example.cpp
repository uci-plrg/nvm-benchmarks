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
    uint64_t *keys;
    uint64_t n;
    uint num_thread;
} thread_data_t;

masstree::masstree *tree = NULL;

void run(char **argv) {
    std::cout << "Simple Example of P-Masstree" << std::endl;

    uint64_t n = std::atoll(argv[1]);
    uint64_t *keys = new uint64_t[n];

    // Generate keys
    for (uint64_t i = 0; i < n; i++) {
        keys[i] = i + 1;
    }

    int num_thread = atoi(argv[2]);

    printf("operation,n,ops/s\n");
    if (getRegionFromID(0) == NULL) {
        masstree::leafnode *init_root = new masstree::leafnode(0);
        tree = new masstree::masstree(init_root);
        setRegionFromID(0, tree);
    } else {
        tree = (masstree::masstree *) getRegionFromID(0);
    }
    thread_data_t *tds = (thread_data_t *) malloc(num_thread * sizeof(thread_data_t));
    std::atomic<int> next_thread_id;
    next_thread_id.store(0);
    for (int i = 0; i < num_thread; i++){
        tds[i].id = next_thread_id.fetch_add(1);
        tds[i].tree = tree;
        tds[i].keys = keys;
        tds[i].n = n;
        tds[i].num_thread = num_thread;
    }
    {
        // Build tree
        auto starttime = std::chrono::system_clock::now();
        auto func = [](void * arg) -> void * {
            thread_data_t *tds = (thread_data_t *) arg;

            uint64_t start_key = tds->n / tds->num_thread * (uint64_t)tds->id;
            uint64_t end_key = start_key + tds->n / tds->num_thread;
            for (uint64_t i = start_key; i < end_key; i++) {
                tree->put(tds->keys[i], &tds->keys[i]);
            }
        };
        pthread_t *threads = new pthread_t[num_thread];

        for (int i = 0; i < num_thread; i++) {
            pthread_create(&threads[i], NULL, func, &tds[i]);
        }
        for (int i = 0; i < num_thread; i++){
            pthread_join(threads[i], NULL);
        }
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: insert,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: insert,%ld,%f sec\n", n, duration.count() / 1000000.0);
        delete [] threads;
    }

    {
        // Lookup
        auto starttime = std::chrono::system_clock::now();
        next_thread_id.store(0);
        auto func = [](void * arg) -> void * {
            thread_data_t *tds = (thread_data_t *) arg;
            
            uint64_t start_key = tds->n / tds->num_thread * (uint64_t)tds->id;
            uint64_t end_key = start_key + tds->n / tds->num_thread;

            for (uint64_t i = start_key; i < end_key; i++) {
                uint64_t *ret = reinterpret_cast<uint64_t *> (tree->get(tds->keys[i]));
                if (*ret != tds->keys[i]) {
                    std::cout << "wrong value read: " << *ret << " expected:" << tds->keys[i] << std::endl;
                    throw;
                }
            }
        };

        pthread_t *threads = new pthread_t[num_thread];

        for (int i = 0; i < num_thread; i++) {
            pthread_create(&threads[i], NULL, func, &tds[i]);
        }
        for (int i = 0; i < num_thread; i++){
            pthread_join(threads[i], NULL);
        }    
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: lookup,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: lookup,%ld,%f sec\n", n, duration.count() / 1000000.0);
        delete [] threads;
    }
    delete tds;
    delete[] keys;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        printf("usage: %s [n] [nthreads]\nn: number of keys (integer)\nnthreads: number of threads (integer)\n", argv[0]);
        return 1;
    }
    run(argv);
    if(tree != NULL) {
        delete tree;
    }
    return 0;
}
