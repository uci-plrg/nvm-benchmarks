LIBS := -lrt -ltbb -lpthread
CXX=/scratch/nvm/benchmarks/RECIPE/g++
CFLAGS := -O3 -std=c++17 -mrtm

all:
	$(CXX) $(CFLAGS) -c FPTree_lock.cpp $(INCLUDES) 
	$(CXX) $(CFLAGS) -c main.cpp 
	$(CXX) $(CFLAGS) -c main.cpp -o main_mixed.o -DMIXED
	$(CXX) $(CFLAGS) -o FPTree_lock main.o FPTree_lock.o $(LIBS)
	$(CXX) $(CFLAGS) -o FPTree_lock_mixed main_mixed.o FPTree_lock.o $(LIBS) 

clean:
	rm -f FPTree_lock.o main.o main_mixed.o FPTree_lock FPTree_lock_mixed
