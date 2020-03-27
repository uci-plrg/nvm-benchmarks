CXX=/scratch/nvm/benchmarks/RECIPE/g++
SOURCE = unordered_set_malloc.cpp vector_capacity.cpp bloom_filter.cpp std_hash_speed.cpp dcas_test.cpp partial_order_sort.cpp copy_backward.cpp new_zero_test.cpp thread_local_test.cpp
CXXFLAG = -O3 --std=gnu++11 -g -Wall

all: $(SOURCE)
	$(CXX) vector_capacity.cpp -o ./bin/vector_capacity $(CXXFLAG)
	$(CXX) unordered_set_malloc.cpp -o ./bin/unordered_set_malloc $(CXXFLAG)
	$(CXX) bloom_filter.cpp -o ./bin/bloom_filter $(CXXFLAG)
	$(CXX) std_hash_speed.cpp -o ./bin/std_hash_speed $(CXXFLAG)
	$(CXX) dcas_test.cpp -o ./bin/dcas_test $(CXXFLAG) -mcx16
	$(CXX) std_sort_malloc.cpp -o ./bin/std_sort_malloc $(CXXFLAG)
	$(CXX) sorted_small_set_test.cpp -o ./bin/sorted_small_set_test $(CXXFLAG)
	$(CXX) partial_order_sort.cpp -o ./bin/partial_order_sort $(CXXFLAG)
	$(CXX) copy_backward.cpp -o ./bin/copy_backward.cpp $(CXXFLAG)
	$(CXX) offsetof_test.cpp -o ./bin/offset_of $(CXXFLAG)
	$(CXX) new_zero_test.cpp -o ./bin/new_zero_test $(CXXFLAG)
	$(CXX) thread_local_test.cpp -o ./bin/thread_local_test $(CXXFLAG)

clean:
	rm -f ./bin/*
