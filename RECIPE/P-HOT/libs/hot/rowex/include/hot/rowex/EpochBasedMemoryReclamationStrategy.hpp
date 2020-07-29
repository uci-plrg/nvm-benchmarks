#ifndef __HOT_EPOCH_BASED_MEMORY_RECLAMATION_STRATEGY__
#define __HOT_EPOCH_BASED_MEMORY_RECLAMATION_STRATEGY__

#include <atomic>
#include <algorithm>
#include <array>
#include <thread>

#include "hot/rowex/ThreadSpecificEpochBasedReclamationInformation.hpp"
#define NUMBER_OF_THREAD 10

namespace hot { namespace rowex {

class EpochBasedMemoryReclamationStrategy {
	static uint32_t NEXT_EPOCH[3];
	static uint32_t PREVIOUS_EPOCH[3];

	std::atomic<uint32_t> mCurrentEpoch;
	ThreadSpecificEpochBasedReclamationInformation mThreadSpecificInformations [NUMBER_OF_THREAD];

private:
	EpochBasedMemoryReclamationStrategy() : mCurrentEpoch(0) {
	}

public:

	static EpochBasedMemoryReclamationStrategy* getInstance() {
		static EpochBasedMemoryReclamationStrategy instance;
		return &instance;
	}

	void enterCriticalSection(uint64_t threadID) {
		ThreadSpecificEpochBasedReclamationInformation & currentMemoryInformation = mThreadSpecificInformations[threadID];
		uint32_t currentEpoch = mCurrentEpoch.load(std::memory_order_acquire);
		currentMemoryInformation.enter(currentEpoch);
		if(currentMemoryInformation.doesThreadWantToAdvanceEpoch() && canAdvance(currentEpoch, threadID)) {
			mCurrentEpoch.compare_exchange_strong(currentEpoch, NEXT_EPOCH[currentEpoch]);
		}
	}

	bool canAdvance(uint32_t currentEpoch, uint64_t id) {
		uint32_t previousEpoch = PREVIOUS_EPOCH[currentEpoch];
		for (int i=0; i < NUMBER_OF_THREAD; i++){
			if(mThreadSpecificInformations[i].getLocalEpoch() == previousEpoch){
				return false;
			}
		}
		return true;
		
	}

	void leaveCriticialSection(uint64_t threadID) {
		ThreadSpecificEpochBasedReclamationInformation & currentMemoryInformation = mThreadSpecificInformations[threadID];
		currentMemoryInformation.leave();
	}

	void scheduleForDeletion(HOTRowexChildPointer const & childPointer, uint64_t threadID) {
		mThreadSpecificInformations[threadID].scheduleForDeletion(childPointer);
	}
};

uint32_t EpochBasedMemoryReclamationStrategy::NEXT_EPOCH[3] = { 1, 2, 0 };
uint32_t EpochBasedMemoryReclamationStrategy::PREVIOUS_EPOCH[3] = { 2, 0, 1 };

} }



#endif