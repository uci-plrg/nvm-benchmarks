#!/bin/bash
action=$1
trace=$2

if [ "$trace" == "--trace" ]
then
	trace="-n"
	sudo="sudo"
fi

export LD_LIBRARY_PATH=./malloc/lib/:../lib/:$LD_LIBRARY_PATH

time="time"

if [ "$action" == "-h" ] 
then
	./evaluation/evaluation -h
elif [ "$action" == "--small" ]
then
	$sudo $time ./evaluation/evaluation $trace --kpvm-dram 2 8 128 1024 8 8 7 10000 2
elif [ "$action" == "--med" ]
then
	$sudo $time ./evaluation/evaluation $trace --kpvm-dram 2 16 128 1024 8 8 7 100000 2
elif [ "$action" == "--large" ]
then
	$sudo $time ./evaluation/evaluation $trace --kpvm-dram 4 16 128 1024 8 8 7 1000000 2

fi

#./evaluation/evaluation -n --kpvm-dram 2 3 128 1024 8 8 7 400 2
# trace
#time ./evaluation/evaluation -n --kpvm-dram 2 16 128 1024 8 8 7 1024000 4
# no trace
#./evaluation/evaluation --kpvm-dram 2 16 128 1024 8 8 7 1024000 4
#./evaluation/evaluation -n --kpvm-dram 2 16 128 1024 8 8 7 1024000 4
# <cpus> <iters> <ksize> <vsize> <merge_every> <put_prob> <update_prob> <num_operations> <threads> 
#PREPOP number is a macro hardcoded in evaluation.c, which determines number of prepopulated keys
