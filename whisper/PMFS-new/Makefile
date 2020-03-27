#
# Makefile for the linux pmfs-filesystem routines.
#
# TO COMPILE :
# make CPPFLAGS="-D__TRACE__"
#
# TO RUN AND TRACE :
# insmod pmfs.ko measure_timing=0 
# cd /sys/kernel/debug/tracing
# echo 0 > tracing_on
# echo pmfs_mount > set_ftrace_filter	# we don't want all pmfs_functions
# echo function > current_tracer	# enables trace_printks
# echo 1 > tracing_on
# cat trace_pipe
# mount -t pmfs -o init,tracemask=1,jsize=1G /dev/pmem0 /mnt/pmfs
# # Increase journal size so that we don't run into trouble later
# Happy tracing !
#
# TO UNMOUNT :
# umount /mnt/pmfs
# rmmod pmfs
#
# TO REMOUNT :
# insmod pmfs.ko measure_timing=0 
# mount -t pmfs -o tracemask=1 /dev/pmem0 /mnt/pmfs
# 

KCPPFLAGS= ${CPPFLAGS}
export KCPPFLAGS

obj-m += pmfs.o

pmfs-y := bbuild.o balloc.o dir.o file.o inode.o namei.o super.o symlink.o ioctl.o pmfs_stats.o journal.o xip.o wprotect.o

all:
	make -C /lib/modules/$(shell uname -r)/build M=`pwd`

clean:
	make -C /lib/modules/$(shell uname -r)/build M=`pwd` clean
