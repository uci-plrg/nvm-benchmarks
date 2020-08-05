.PHONY: all clean
.DEFAULT_GOAL := all
CXX=/scratch/nvm/benchmarks/g++
#CXX=g++
LIBS=-lpthread
INCLUDES=-I./include
CFLAGS=-O0 -std=c++11 -g -DCLFLUSH_OPT=1

output = example

all: main

main: example.cpp
	$(CXX) $(CFLAGS) -o $(output) example.cpp $(LIBS)

clean:
	rm $(output)
