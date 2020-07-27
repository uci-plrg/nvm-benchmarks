#/bin/bash

/scratch/nvm/llvm-project/build/bin/clang -Xclang -load -Xclang /scratch/nvm/llvm-project/build/lib/libPMCPass.so -Wno-unused-command-line-argument -fheinous-gnu-extensions -L/scratch/nvm/pmcheck/bin -lpmcheck $@
