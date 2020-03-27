#!/bin/bash
#
# Script for gathering all the comparison datapoints needed for thorough analysis.
# Provides options for comparing across ranges of valuesize, keysize, trans size,
# number of concurrent threads, etc. 
#
# Katelin Bailey and Peter Hornyack
# Created September 2011
# University of Washington
# Copyright 2011
#

# Todo: OPROF, GPROF, and VMLINUX (at least) should be command-line args

EVALCMD="numactl --membind=2 ./evaluation"
#EVALCMD="./evaluation"
OPTIONS=$*
EVALFLAGS=""
EVALDIR="output"

GPROF="false"  # set to "true" to enable gprof
GPROFCMD="gprof"
GPROFPREFIX="gprof"
KP_KVSTOREOBJ="../obj/kp_kvstore.o"

usage() {
		echo ""
		echo "USAGE: ./evaluation_harness.sh "
		echo " [OPTION] <store=string>[value1=num][value2=num]..."
		echo "Options:"
		echo " (none)      : does all runs, all benchmarks with default values "
		echo " --defaults  : prints the default values "
		echo " --debug     : prints a single run with small default values"
		echo " --debug_thd : prints a single (threaded)run with default values"
		echo " --no_vsize  : omits value size progression"
		echo " --no_ksize  : omits key size progression"
		echo " --no_mfreq  : omits merge frequency progression"
		echo " --no_thread : omits threaded runs"
		echo " --no_wset   : omits working set size progression"
		echo "Possible default values to set:"
		echo " iters, ksize, vsize, mfreq, threads,"
		echo "  put_prob, update_prob "
    echo "Example: "
    echo "  ./evaluation_harness --no_ksize --iters=100 --put_prob=40"
		echo ""
		exit
}


# Function that runs the evaluation command. The first argument must be
# the prefix for the output file (i.e. the "name" of the test being run
# with this command). The remaining arguments are the "parameters" passed
# to the evaluation command.
run_test () {
	test_name="${1}"
	shift  #discard arg
	test_params=""
	until [[ -z "$1" ]]; do
		test_params="${test_params}${1} "
		shift
	done
	
	command="${EVALCMD} ${EVALFLAGS} ${test_params}"
	outfile="${EVALDIR}/${test_name}.out"
	echo "Command: ${command} >> ${outfile}"
	${command} >> ${outfile}  # run the evaluation

	# An executable that is built with gprof ('-pg' flag to gcc) will
	# automatically emit profiling information in file gmon.out; gprof then
	# loads the symbols from the executable and combines it with this
	# profiling information, and sends a report to stdout.
	if [[ ${GPROF} == "true" || ${GPROF} == "TRUE" ]]; then
		command="${GPROFCMD} ${EVALCMD}"
		outfile="${EVALDIR}/${GPROFPREFIX}.${test_name}.out"
		${command} > ${outfile}
		echo "gprof output in file: ${outfile}"
	fi

	return 0
}

# Options for which tests to run by default
opt_debug="false"
opt_debug_thd="false"
opt_no_vsize="false"
opt_no_ksize="false"
opt_no_mfreq="false"
opt_no_thread="false"
opt_no_wset="false"

# Options for which values to run with by default
def_iter=50000
def_ksize=128
def_vsize=1024
def_merge=8
def_put=0.1
def_update=0.7
def_threads=1
def_store="kpvm-dram"
def_cpus=16


#Prefix
if [[ ! -e "${EVALDIR}" ]]; then
	mkdir -pv ${EVALDIR}
fi
echo "Running evaluations, storing results in directory: ${EVALDIR}"

#parse_options
for opt in $OPTIONS
do
		if [ "$opt" == "--help" ]; then
				usage
		elif [ "$opt" == "--defaults" ]; then
				echo "iterations $def_iter"
				echo "keysize $def_ksize" 
				echo "valuesize $def_vsize"
				echo "mergefreq $def_merge"
				echo "putprob $def_put" 
				echo "updateprob $def_update" 
				echo "threads $def_threads" 
				exit
		elif [ "$opt" == "--debug" ]; then
				opt_debug="true"
		elif [ "$opt" == "--debug_thd" ]; then
				opt_debug_thd="true"
		elif [ "$opt" == "--no_vsize" ]; then
				opt_no_vsize="true"
		elif [ "$opt" == "--no_ksize" ]; then
				opt_no_ksize="true"
		elif [ "$opt" == "--no_mfreq" ]; then
				opt_no_mfreq="true"
		elif [ "$opt" == "--no_wset" ]; then
				opt_no_wset="true"
		elif [ "$opt" == "--no_thread" ]; then
				opt_no_thread="true"
		else
				base=$(echo $opt | cut --delimiter='=' -f 1)
				val=$(echo $opt | cut --delimiter='=' -f 2)
				if [ "$base" == "iters" ]; then
 						def_iter=$val
						echo $def_iter
				elif [ "$base" == "store" ]; then
						def_store=$val
				elif [ "$base" == "cpus" ]; then
						def_cpus=$val
				elif [ "$base" == "ksize" ]; then
						def_ksize=$val
				elif [ "$base" == "vsize" ]; then
						def_vsize=$val
				elif [ "$base" == "mfreq" ]; then
						def_merge=$val
				elif [ "$base" == "put_prob" ]; then
						def_put="0.$val"
				elif [ "$base" == "update_prob" ]; then
						def_update="0.$val"
				elif [ "$base" == "threads" ]; then
								def_threads=$val
				else
						usage
				fi
		fi
done		

date


# PARAMS
#   <store> <iterations> <keysize> <valuesize> <mergefreq> 
#     <putprob> <updateprob> <threads> 

# DEBUG RUN(S)
if [ "$opt_debug" == "true" ]
then
		params="--$def_store $def_cpus 100  $def_ksize $def_vsize  4 $def_put	$def_update 1";	name="single_debug";	run_test ${name} ${params}
		exit
elif [ "$opt_debug_thd" == "true" ]
then
		params="--$def_store $def_cpus 100  $def_ksize $def_vsize  4 $def_put	$def_update 2";	name="double_debug";	run_test ${name} ${params}
		exit
else


# KATELIN- TEST OF VALUE SIZES
		if [ "$opt_no_vsize" == "false" ]; then
				echo "--> Starting progression 1a/5, value size ramp up (in cache)"
				for i in 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768 65536 131072				
				do
				    declare -i n
				    n=524288/$i
						params="--$def_store $def_cpus $n $def_ksize $i $def_merge $def_put $def_update $def_threads";
						name="valuesize_sm_$i";
						run_test ${name} ${params}
				done
		else
				echo "--> Omitting progression 1/5, value size ramp up"
		fi

		if [ "$opt_no_vsize" == "false" ]; then
				echo "--> Starting progression 1b/5, value size ramp up (out of cache)"
				for i in 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768 65536 131072
				do
				    declare -i n
				    n=4194304/$i
						params="--$def_store $def_cpus $n $def_ksize $i $def_merge $def_put $def_update $def_threads";
						name="valuesize_lg_$i";
						run_test ${name} ${params}
				done
		else
				echo "--> Omitting progression 1/5, value size ramp up"
		fi

		if [ "$opt_no_vsize" == "false" ]; then
				echo "--> Starting progression 1c/5, value size ramp up (normal)"
				for i in 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768 65536 131072
				do
						params="--$def_store $def_cpus $def_iter $def_ksize $i $def_merge $def_put $def_update $def_threads";
						name="valuesize_$i";
						run_test ${name} ${params}
				done
		else
				echo "--> Omitting progression 1/5, value size ramp up"
		fi

		
# KATELIN- TEST OF KEY SIZES
		if [ "$opt_no_ksize" == "false" ]; then
				echo "--> Starting progression 2/5, key size ramp up"
				for i in 8 16 32 64 128 256 512
				do
						params="--$def_store $def_cpus $def_iter $i $def_vsize $def_merge $def_put $def_update $def_threads";
						name="keysize_$i";
						run_test ${name} ${params}
				done
		else
				echo "--> Omitting progression 2/5, key size ramp up"
		fi
		
# KATELIN- TEST OF MERGE FREQUENCIES
		if [ "$opt_no_mfreq" == "false" ]; then
				echo "--> Starting progression 3/5, merge frequency"
				for i in 1 2 4 8 16 32 64 128 256 512 1024 2048
				do
						params="--$def_store $def_cpus $def_iter $def_ksize $def_vsize $i $def_put $def_update $def_threads";
						name="mergefreq_$i";
						run_test ${name} ${params}
				done
		else
				echo "--> Omitting progression 3/5, merge frequency"
		fi
		
# KATELIN- TEST OF THREAD RAMP UP
		if [ "$def_cpus" == "48" ]; then
				thread_lineup="1 4 8 12 16 20 24 28 32 36 40 44 48"
		elif [ "$def_cpus" == "16" ]; then
				thread_lineup="1 2 4 6 8 10 12 14 16"
		elif [ "$def_cpus" == "2" ]; then
				thread_lineup="1 2"
		fi
		if [ "$opt_no_thread" == "false" ]; then
				echo "--> Starting progression 4/5, thread ramp up"
				for i in $thread_lineup
				do
						params="--$def_store $def_cpus $def_iter $def_ksize $def_vsize $def_merge $def_put $def_update $i";
						name="threaded_$i";
						run_test ${name} ${params}
				done
		else
				echo "--> Omitting progression 4/5, thread ramp up"
		fi


# KATELIN- TEST OF WORKING SIZERAMP UP
		if [ "$opt_no_wset" == "false" ]; then 
				echo "--> Starting progression 5/5, Working Size ramp up"
				for i in 100 1000 10000 50000 100000 500000 1000000 #5000000 10000000
				do
						params="--$def_store $def_cpus $i $def_ksize 32 $def_merge $def_put $def_update $def_threads";
						name="iterations_$i";
						run_test ${name} ${params}
				done
		else
				echo "--> Omitting progression 5/5, workingsetsize"
		fi
fi

date

exit 0

