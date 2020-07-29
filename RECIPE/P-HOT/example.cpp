#include <iostream>
#include <chrono>
#include <random>

using namespace std;

#include <hot/rowex/HOTRowex.hpp>
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>

typedef struct IntKeyVal {
    uint64_t key;
    uintptr_t value;
} IntKeyVal;

template<typename ValueType = IntKeyVal *>
class IntKeyExtractor {
    public:
    typedef uint64_t KeyType;

    inline KeyType operator()(ValueType const &value) const {
        return value->key;
    }
};

typedef struct thread_data {
    uint32_t id;
    hot::rowex::HOTRowex<IntKeyVal *, IntKeyExtractor> *hot;
} thread_data_t;


void run(char **argv) {
    std::cout << "Simple Example of P-HOT" << std::endl;

    uint64_t n = std::atoll(argv[1]);
    uint64_t *keys = new uint64_t[n];

    // Generate keys
    for (uint64_t i = 0; i < n; i++) {
        keys[i] = i + 1;
    }

    int num_thread = atoi(argv[2]);
    
    printf("operation,n,ops/s\n");
    hot::rowex::HOTRowex<IntKeyVal *, IntKeyExtractor> mTrie;
    thread_data_t *tds = (thread_data_t *) malloc(num_thread * sizeof(thread_data_t));

    std::atomic<int> next_thread_id;
    {
        // Build tree
        auto starttime = std::chrono::system_clock::now();

        next_thread_id.store(0);
        auto func = [&]() {
            int thread_id = next_thread_id.fetch_add(1);
            tds[thread_id].id = thread_id;
            tds[thread_id].hot = &mTrie;

            uint64_t start_key = n / num_thread * (uint64_t)thread_id;
            uint64_t end_key = start_key + n / num_thread;
            for (uint64_t i = start_key; i < end_key; i++) {
                IntKeyVal *key;
                posix_memalign((void **)&key, 64, sizeof(IntKeyVal));
                key->key = keys[i];
                key->value = keys[i];
                if (!(mTrie.insert(key, thread_id))) {
                    fprintf(stderr, "[HOT] insert faile\n");
                    exit(1);
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
            tds[thread_id].hot = &mTrie;

            uint64_t start_key = n / num_thread * (uint64_t)thread_id;
            uint64_t end_key = start_key + n / num_thread;
            for (uint64_t i = start_key; i < end_key; i++) {
                idx::contenthelpers::OptionalValue<IntKeyVal *> result = mTrie.lookup(keys[i], thread_id);
                if (!result.mIsValid || result.mValue->value != keys[i]) {
                    printf("mIsValid = %d\n", result.mIsValid);
                    printf("Return value = %lu, Correct value = %lu\n", result.mValue->value, keys[i]);
                    exit(1);
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