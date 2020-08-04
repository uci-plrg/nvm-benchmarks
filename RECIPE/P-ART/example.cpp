#include <iostream>
#include <chrono>
#include <random>
#include <thread>
using namespace std;
#include "Tree.h"


extern "C" {
    void * getRegionFromID(uint ID);
    void setRegionFromID(uint ID, void *ptr);
}

typedef struct thread_data {
    uint32_t id;
    ART_ROWEX::Tree *tree;
} thread_data_t;


void loadKey(TID tid, Key &key) {
    return ;
}

ART_ROWEX::Tree *tree = NULL;

void run(char **argv) {
    std::cout << "Simple Example of P-ART" << std::endl;
    uint64_t n = std::atoll(argv[1]);
    uint64_t *keys = new uint64_t[n];
    std::vector<Key *> Keys;

    Keys.reserve(n);

    // Generate keys
    for (uint64_t i = 0; i < n; i++) {
        keys[i] = i + 1;
    }

    int num_thread = atoi(argv[2]);
    
    printf("operation,n,ops/s\n");
    if(getRegionFromID(0) == NULL){
        tree = new ART_ROWEX::Tree(loadKey, num_thread);
        setRegionFromID(0, tree);
    } else {
        tree = (ART_ROWEX::Tree*)getRegionFromID(0);
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
            auto t = tree->getThreadInfo(thread_id);
            for (uint64_t i = start_key; i < end_key; i++) {
                Keys[i] = Key::make_leaf(keys[i], sizeof(uint64_t), keys[i]);
                tree->insert(Keys[i], t);
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
            auto t = tree->getThreadInfo(thread_id);
            for (uint64_t i = start_key; i < end_key; i++) {
                uint64_t *val = reinterpret_cast<uint64_t *> (tree->lookup(Keys[i], t));
                if (*val != keys[i]) {
                    std::cout << "wrong value read: " << *val << " expected:" << keys[i] << std::endl;
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


int main(int argc, char **argv) {
    if (argc != 3) {
        printf("usage: %s [n] [nthreads]\nn: number of keys (integer)\nnthreads: number of threads (integer)\n", argv[0]);
        return 1;
    }
    run(argv);
    return 0;
}

