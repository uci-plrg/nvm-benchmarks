#!/bin/bash
export LD_LIBRARY_PATH=/scratch/bdemsky/pmcheck/bin/
# For Mac OSX
export DYLD_LIBRARY_PATH=/scratch/bdemsky/pmcheck/bin/

echo $@
$@
