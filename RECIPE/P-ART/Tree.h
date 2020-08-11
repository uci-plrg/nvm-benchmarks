//
// Created by florian on 18.11.15.
//

#ifndef ART_ROWEX_TREE_H
#define ART_ROWEX_TREE_H
#include "N.h"

using namespace ART;

namespace ART_ROWEX {

    class Tree {
    public:
        using LoadKeyFunction = void (*)(TID tid, Key &key);

    private:
        N *const root;

        void *checkKey(const Key *ret, const Key *k) const;

        void unlockSubTree( N *n);

        LoadKeyFunction loadKey;

        Epoche epoche;

    public:
        enum class CheckPrefixResult : uint8_t {
            Match,
            NoMatch,
            OptimisticMatch
        };

        enum class CheckPrefixPessimisticResult : uint8_t {
            Match,
            NoMatch,
            SkippedLevel
        };

        enum class PCCompareResults : uint8_t {
            Smaller,
            Equal,
            Bigger,
            SkippedLevel
        };
        enum class PCEqualsResults : uint8_t {
            BothMatch,
            Contained,
            NoMatch,
            SkippedLevel
        };
        static CheckPrefixResult checkPrefix(N* n, const Key *k, uint32_t &level);

        static CheckPrefixPessimisticResult checkPrefixPessimistic(N *n, const Key *k, uint32_t &level,
                                                                   uint8_t &nonMatchingKey,
                                                                   Prefix &nonMatchingPrefix,
                                                                   LoadKeyFunction loadKey);

        static PCCompareResults checkPrefixCompare(const N* n, const Key *k, uint32_t &level, LoadKeyFunction loadKey);

        static PCEqualsResults checkPrefixEquals(const N* n, uint32_t &level, const Key *start, const Key *end, LoadKeyFunction loadKey);

    public:

        Tree(LoadKeyFunction loadKey, uint number_of_threads);

        Tree(const Tree &) = delete;

        Tree(Tree &&t) : root(t.root), loadKey(t.loadKey), epoche(t.epoche) { }

        ~Tree();

        ThreadInfo getThreadInfo(uint64_t id);

        void *lookup(const Key *k, ThreadInfo &threadEpocheInfo) const;

        void recoverFromCrash();

        bool lookupRange(const Key *start, const Key *end, const Key *continueKey, Key *result[], std::size_t resultLen,
                         std::size_t &resultCount, ThreadInfo &threadEpocheInfo) const;

        void insert(const Key *k, ThreadInfo &epocheInfo);

        void remove(const Key *k, ThreadInfo &epocheInfo);
    };
}
#endif //ART_ROWEX_TREE_H
