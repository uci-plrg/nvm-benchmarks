#!/bin/bash
# 
# Script for stress testing some version of the store or evaluation.
# Runs the given evaluation ("command") with default parameters until it dies. 
# Allows the collection of coredump when dead.  Will otherwise run forever.
#
# Katelin Bailey and Peter Hornyack
# University of Washington
# Copyright 2011-2014
#


OUTFILE="./output/repeat_eval.out"
USEVALGRIND=0

command="./evaluation"        # evaluation command to run

# Default evaluation parameters
numcpus=16                    # max number of cpus to expect/pin
iters=25000                   # number of iterations to amortize costs over
keysize=128                   # keysize in bytes
valsize=1024                  # valuesize (string length) in bytes
mergefreq=8                   # how many commands per transaction
arg5=0.1                      # probability command will be a put
arg6=0.7                      # probability command will be a put to old key
numthreads=16                 # max number of threads to expect/use
outfile=$2                    # stored output, if need results. may be empty

# Preface
date
ulimit -c unlimited
echo "Set coredump size to unlimited"

# Set running Parameters
sleeptime=1
date=""
dead=0
count=1

while [[ "$dead" -eq "0" ]]; do

    #Cleanup from previous run
    echo ""
    sleep $sleeptime  # for some reason rm -f on OUTFILE doesn't always work
    rm -f $OUTFILE
    rm -f kpvm_logged_errors.out
    sleep $sleeptime

    #Setup for this run
    date=`date`
    echo "loop number: $count - $date"
    
    if [[ "$USEVALGRIND" -ne "0" ]]; then
        # append .count to valgrind output - it takes forever to get, so it's
	# valuable and we don't want to overwrite it.
	echo "Running: valgrind $command $numcpus $iters $keysize $valsize" \
             " $mergefreq $arg5 $arg6 $numthreads &> $OUTFILE.$count"
	time valgrind $command $numcpus $iters $keysize $valsize $mergefreq \
             $arg5 $arg6 $numthreads &> $OUTFILE.$count
    else
	echo "Running: $command $numcpus $iters $keysize $valsize $mergefreq"\
             " $arg5 $arg6 $numthreads &> $OUTFILE"
	time $command $numcpus $iters $keysize $valsize $mergefreq \
	     $arg5 $arg6 $numthreads &> $OUTFILE
    fi

    #Check for liveness
    dead=$?
    let "count += 1"
done

date
echo "died, should find a coredump file"
exit 0
