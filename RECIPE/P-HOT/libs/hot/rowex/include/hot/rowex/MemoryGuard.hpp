#ifndef __HOT__ROWEX__MEMORY_GUARD__
#define __HOT__ROWEX__MEMORY_GUARD__

#include "hot/rowex/EpochBasedMemoryReclamationStrategy.hpp"

namespace hot { namespace rowex {

class MemoryGuard {
	EpochBasedMemoryReclamationStrategy* mMemoryReclamation;
	uint64_t threadID;
public:
	MemoryGuard(EpochBasedMemoryReclamationStrategy* memoryReclamation, uint64_t _threadID) : 
						mMemoryReclamation(memoryReclamation),
						threadID(_threadID) {
		mMemoryReclamation->enterCriticalSection(threadID);
	}

	MemoryGuard(EpochBasedMemoryReclamationStrategy* memoryReclamation) : mMemoryReclamation(memoryReclamation) {};

	~MemoryGuard() {
		mMemoryReclamation->leaveCriticialSection(threadID);
	}

	MemoryGuard(MemoryGuard const & other) = delete;
	MemoryGuard &operator=(MemoryGuard const & other) = delete;
};

}}

#endif // __HOT_MEMORY_GUARD__