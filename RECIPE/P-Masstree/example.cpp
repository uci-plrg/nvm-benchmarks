#include <iostream>
#include <chrono>
#include <random>
#include <atomic>
#include <vector>
#include <thread>

using namespace std;

#include "masstree.h"

extern "C" {
    void restart();
}

typedef struct thread_data {
    uint32_t id;
    masstree::masstree *tree;
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
    if(tree == NULL){
        masstree::leafnode *init_root = new masstree::leafnode(0);
        tree = new masstree::masstree(init_root);
    }
    thread_data_t *tds = (thread_data_t *) malloc(num_thread * sizeof(thread_data_t));
    std::atomic<int> next_thread_id;
    {
        // Build tree
        auto starttime = std::chrono::system_clock::now();
        next_thread_id.store(0);
        auto func = [&]() {
            int thread_id = next_thread_id.fetch_add(1);
            tds[thread_id].id = thread_id;
            tds[thread_id].tree = tree;

            uint64_t start_key = n / num_thread * (uint64_t)thread_id;
            uint64_t end_key = start_key + n / num_thread;
            for (uint64_t i = start_key; i < end_key; i++) {
                tree->put(keys[i], &keys[i]);
            }
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

    {
        // Lookup
        auto starttime = std::chrono::system_clock::now();
        next_thread_id.store(0);
        auto func = [&]() {
            int thread_id = next_thread_id.fetch_add(1);
            tds[thread_id].id = thread_id;
            tds[thread_id].tree = tree;

            uint64_t start_key = n / num_thread * (uint64_t)thread_id;
            uint64_t end_key = start_key + n / num_thread;

            for (uint64_t i = start_key; i < end_key; i++) {
                uint64_t *ret = reinterpret_cast<uint64_t *> (tree->get(keys[i]));
                if (*ret != keys[i]) {
                    std::cout << "wrong value read: " << *ret << " expected:" << keys[i] << std::endl;
                    throw;
                }
            }
        };

        std::vector<std::thread> thread_group;

        for (int i = 0; i < num_thread; i++)
            thread_group.push_back(std::thread{func});

        for (int i = 0; i < num_thread; i++)
            thread_group[i].join();
            
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: lookup,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: lookup,%ld,%f sec\n", n, duration.count() / 1000000.0);
    }

    delete[] keys;
}

char ** argvptr;

int main(int argc, char **argv) {
    if (argc != 3) {
        printf("usage: %s [n] [nthreads]\nn: number of keys (integer)\nnthreads: number of threads (integer)\n", argv[0]);
        return 1;
    }
    argvptr = argv;
    run(argv);
    if(tree != NULL) {
        delete tree;
    }
    return 0;
}

void restart(){
    run(argvptr);
}
