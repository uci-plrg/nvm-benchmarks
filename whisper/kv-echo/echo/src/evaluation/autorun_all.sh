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

# katelin@EM1:
AUTORUN_DIR="/home/katelin/autoruns"

PREFIX=$(date +%Y.%m.%d-%H:%M)
EVALDIR="output"
EXECDIR=$(pwd)
OUTPUT="${EVALDIR}/*.out"
TRACES="${EVALDIR}/*.out.trace"

HARNESS_FILE="evaluation_harness.sh"
if [ ! -e $HARNESS_FILE ]
then
		echo "$HARNESS_FILE not found"
		exit
fi

ulimit -c 0

echo "======================= COMPILATION =================================="
cd ..
#make clean;
#./make-tcmalloc.sh
#make evaluation;

source source-me
cd evaluation

rm /home/katelin/leveldb_client_name
rm /mnt/pmfs/leveldb_client_name

echo "======================= EVALUATION =================================="
for i in 1
do
    # For each...
    for s in  'kpvm-dram' 'masstree' 'leveldb-ramdisk'
    #for s in 	'kpvm-dram' 'masstree' 'leveldb-ramdisk' 'leveldb-ssd' 'leveldb-disk'
    do
				printf "\n Starting $s run $i at %s\n" $(date +%T)
				DIR="$AUTORUN_DIR/$PREFIX-$s-run$i" 
				if [ ! -D $EVAL_FILE ]
				then
						echo "directory already exists"
						exit
				fi
				"./$HARNESS_FILE" store=$s cpus=16 --no_ksize --no_mfreq --no_thread --no_wset
#				"./$HARNESS_FILE" store=$s cpus=2 --no_vsize --no_ksize --no_mfreq --no_wset --no_thread
				printf "\tconcluding $s run $i at %s\n" $(date +%T)

				# move into results tree
				mkdir  $DIR
				mv $OUTPUT $DIR
				mv $TRACES $DIR

        # Record some vital data for later
				hostname >> "$DIR/HOSTNAME"
				svnversion ./ >> "$DIR/REPOSITORY_VERSION"
				cp $HARNESS_FILE "$DIR/EVALUATION_HARNESS"
				cp nohup.out "$DIR/NOHUP"
				cp /data/devel/sdv-tools/sdv-release/ivt_pm_sdv.log "$DIR/PM_SET"
				"./$HARNESS_FILE" --defaults >> "$DIR/DEFAULTS"

    done
done


#cd $AUTORUN_DIR
#../scripts/do_all_graphing.sh $PREFIX
