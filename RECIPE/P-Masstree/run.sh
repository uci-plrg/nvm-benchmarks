#!/bin/bash
export LD_LIBRARY_PATH=/scratch/nvm/pmcheck/bin/
# For Mac OSX
export DYLD_LIBRARY_PATH=/scratch/nvm/pmcheck/bin/

echo $@
$@
