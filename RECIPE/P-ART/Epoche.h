#ifndef ART_EPOCHE_H
#define ART_EPOCHE_H

#include <atomic>
#include <array>
#include "../cacheops.h"
#define BUGFIX 1

namespace ART {

    struct LabelDelete {
        std::array<void*, 32> nodes;
        uint64_t epoche;
        std::size_t nodesCount;
        LabelDelete *next;
    };

    class DeletionList {
        LabelDelete *headDeletionList = nullptr;
        LabelDelete *freeLabelDeletes = nullptr;
        std::size_t deletitionListCount = 0;

    public:

        std::atomic<uint64_t> localEpoche{0};
        size_t thresholdCounter{0};

        ~DeletionList();
        LabelDelete *head();

        void add(void *n, uint64_t globalEpoch);

        void remove(LabelDelete *label, LabelDelete *prev);

        std::size_t size();

        std::uint64_t deleted = 0;
        std::uint64_t added = 0;
    };

    class Epoche;
    class EpocheGuard;

    class ThreadInfo {
        friend class Epoche;
        friend class EpocheGuard;
        Epoche &epoche;
        DeletionList &deletionList;


        DeletionList & getDeletionList() const;
    public:

        ThreadInfo(Epoche &epoche, uint64_t id);

        ThreadInfo(const ThreadInfo &ti) : epoche(ti.epoche), deletionList(ti.deletionList) {
        }

        ~ThreadInfo();

        Epoche & getEpoche() const;
    };

    class Epoche {
        friend class ThreadInfo;
        std::atomic<uint64_t> currentEpoche{0};
        size_t startGCThreshhold;
        DeletionList *deletionLists;
        uint number_of_threads;

    public:
        Epoche(size_t startGCThreshhold, uint thread_num):
                startGCThreshhold(startGCThreshhold),
                number_of_threads(thread_num) 
        {
            deletionLists = new DeletionList[number_of_threads];
#ifdef BUGFIX
            PMCHECK::clflush((char*)deletionLists, sizeof(DeletionList)*number_of_threads, false, true);
            PMCHECK::clflush((char*)&deletionLists, sizeof(DeletionList*), false, true);
#endif
        }

        Epoche(Epoche &e): startGCThreshhold(e.startGCThreshhold), number_of_threads(e.number_of_threads) {}

        ~Epoche();

        void enterEpoche(ThreadInfo &epocheInfo);

        void markNodeForDeletion(void *n, ThreadInfo &epocheInfo);

        void exitEpocheAndCleanup(ThreadInfo &info);
        DeletionList &getDeletionList(uint64_t id){
            return deletionLists[id];
        }
        void showDeleteRatio();

    };

    class EpocheGuard {
        ThreadInfo &threadEpocheInfo;
    public:

        EpocheGuard(ThreadInfo &threadEpocheInfo) : threadEpocheInfo(threadEpocheInfo) {
            threadEpocheInfo.getEpoche().enterEpoche(threadEpocheInfo);
        }

        ~EpocheGuard() {
            threadEpocheInfo.getEpoche().exitEpocheAndCleanup(threadEpocheInfo);
        }
    };

    class EpocheGuardReadonly {
    public:

        EpocheGuardReadonly(ThreadInfo &threadEpocheInfo) {
            threadEpocheInfo.getEpoche().enterEpoche(threadEpocheInfo);
        }

        ~EpocheGuardReadonly() {
        }
    };

    inline ThreadInfo::~ThreadInfo() {
        deletionList.localEpoche.store(std::numeric_limits<uint64_t>::max());
    }
}

#endif //ART_EPOCHE_H
