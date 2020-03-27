.PHONY: all clean
.DEFAULT_GOAL := all
CXX=/scratch/nvm/benchmarks/RECIPE/g++
LIBS=-lrt -lm -lpthread
INCLUDES=-I./include
CFLAGS=-O0 -std=c++11 -g

output = test

all: main

main: main.cpp
	$(CXX) $(CFLAGS) -o test main.cpp $(LIBS)

clean:
	rm $(output)
