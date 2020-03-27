import gzip
import time
import os
import sys
import subprocess
import errno
import csv
import argparse
import traceback
import gc
import bisect
import numpy as np
import ConfigParser
import intervaltree as it
import matplotlib.pyplot as plt
from pylab import *
from os import listdir
from os.path import isfile, join
from multiprocessing import Pool, TimeoutError, Process, Queue, Lock
from ep_stats import ep_stats

debugl = [1,2,3,4]

parser = argparse.ArgumentParser(prog="eliza", description="A tool to analyze epochs")
parser.add_argument('-d', dest='debug', default = 0, help="Debug levels", choices=debugl)
parser.add_argument('-r', dest='logdir', help="Log directory containing per-thread epoch logs")
parser.add_argument('-v', '--version', action='version', version='%(prog)s v0.1', help="Display version and quit")

try:
	args = parser.parse_args()
except:
	sys.exit(0)

wpid = -1 # Worker pid
est = None
lookback_time = 0.005 # 5 ms
# Keep the two separate so that you can collectively read in lines later
# if the files are too big
f_to_epochs_m = {}
f_to_lnnums_m = {}

def make_index(logdir, f):
	global f_to_epochs_m
	f2e = []
	with open(logdir + '/' + f, 'r') as fp:
		for lno,l in enumerate(fp):
			try:
				# this may fail due to out-range index, hence "try"
				epstr = l.split(';')[2]
				stime = est.get_stime_from_str(epstr)
				etime = est.get_etime_from_str(epstr)
				f2e.append((stime, etime, lno))
			except:
				continue
	fp.close()
	f_to_epochs_m[f] = f2e
	''' 
		We could have one giant index for all epochs in the env
		indexed using their start times. But then concurrent epochs
		from two different guest threads with the same start time will 
		contend for a space in the index and I am not sure how python
		handles such contention - chaining, or what ? I am assuming only
		one of the contending epochs gets to reside in the index and this can
		make me miss some dependencies. It doesn't matter if there are plenty
		of dependencies already and missing one or two doesn't matter. But at 
		this point we don't know so to be safe we will not have one index
		for all epochs in the env but one index per guest thread.
	'''
def make_lnmap(logdir, f):
	global f_to_lnnums_m
	lno_to_lines_m = {}
	with open(logdir + '/' + f, 'r') as fp:
		for lno,l in enumerate(fp):
			lno_to_lines_m[lno] = l.split(';')[1].split(',')		
	fp.close()
	f_to_lnnums_m[f] = lno_to_lines_m
	
def cal_cross_thd_dep(pid, args):

	logdir = args[0]
	logfile = args[1]
	global wpid
	global est
	global lookback_time
	global f_to_lnnums_m
	global f_to_epochs_m
	wpid = pid
	est = ep_stats()
	print "Worker " + str(wpid) + " analyzing " + logfile

	onlyfiles = [f for f in listdir(logdir) if isfile(join(logdir, f)) and 'txt' in f and f != logfile]
	
	for f in onlyfiles:
		make_index(logdir, f)
		make_lnmap(logdir, f)

	print "''' Core analysis begins here '''"
	with open(logdir + '/' + logfile, 'r') as fp:
		for lno,l in enumerate(fp):
			try:
				epstr = l.split(';')[2] # this may fail due to out-range index
				stime = est.get_stime_from_str(epstr)
				etime = est.get_etime_from_str(epstr)
			except:
				continue

			# A list of concurrent epochs per epoch guest thread		
			concurrent_epochs = [] 
			for f,f2e in f_to_epochs_m.iteritems():
				# TODO : make this modular !
				pos_ = bisect.bisect_left(f2e, (stime,))
				try:
					if pos_ != 0:
						while f2e[pos_][1] > stime and pos_ != 0:
							pos_ -= 1
					else:
						continue
						# This algo needs to change
				except:
					# print pos, "premature exit. its ok. the EOF has been reached"
					sys.exit(0)
					
				stime_,etime_,lno_ = f2e[pos_]
				if etime_ > stime:
					print f, f2e[pos_], stime
					sys.exit(0)
					# assert etime_ <= stime # Assertion 1
				''' 
					If the time line of a thread goes frm
					left to right, __stime is the start time of 
					the most recent epoch any where in the system
				'''
			
				''' Making sure that __stime is within some time interval '''
				if stime - lookback_time <= stime_:
					pos__ = bisect.bisect_left(f2e, (stime - lookback_time,))
					stime__,etime__,lno__ = f2e[pos__]
					assert stime - lookback_time <= stime__  # Assertion 2
					assert stime__ <= stime
					'''
					if __stime_lb >= stime:
						print ">>>>",__stime_lb, stime, wpid, logfile
						sys.exit(0)
					'''
					# Assertion 1 and 2 are extremely important
					concurrent_epochs.append((f, lno__, lno_, stime__, etime_))

		
		# This is "if" condition is for debugging
		# Otherwise the code below it can be used
			if wpid == 0:
				if len(concurrent_epochs) > 0:
					keys = l.split(';')[1].split(',')
					print str(stime-lookback_time) + '-' + str(stime), keys

					for t in concurrent_epochs:
						print "Concur", str(t)
						
						f = t[0] # f
						assert f in f_to_lnnums_m
						lno_to_lines_m = f_to_lnnums_m[f]	
						start_lno = int(t[1]) #lno__
						end_lno = int(t[2]) #lno_
						assert end_lno >= start_lno
						assert start_lno in lno_to_lines_m
						assert end_lno in lno_to_lines_m

						# if you want to reduce memory footprint then don't assume
						# the lines are already present, read them in selectively
						__lno = start_lno
						while __lno <= end_lno:
							for ep_addr in keys:
								if ep_addr in lno_to_lines_m[__lno]:								
									print "Deps", str(t), ep_addr
								else:
									a = 1
							__lno += 1
						assert __lno - 1 == end_lno
						
datadir = '/dev/shm/'
colmap = {}
colmap['etype'] = 0
colmap['epoch_esize'] = 1
colmap['epoch_wsize'] = 2
colmap['epoch_cwsize'] = 3
colmap['epoch_duration'] = 4
colmap['epoch_page_span'] = 6
colmap['epoch_dist_from_mrd'] = 7
colmap['epoch_dist_from_lrd'] = 8
marker = ['ro-', 'bs-', 'g^-', 'kD-']
plain  =   ['r-', 'b-', 'g-', 'k-']
logdir = '/dev/shm/' + args.logdir
cfg = ConfigParser.ConfigParser()
cfg.read('data.ini')

onlyfiles = [f for f in listdir(logdir) if isfile(join(logdir, f)) and '.txt' in f]
pmap = {}
pid = 0
max_pid = 8

for logfile in onlyfiles:
	'''
		Launch a process for each file, which is a thread log with
		the filename and logdir as argument
		
		Laucn a maximum of four processes
		
		Have each process index all files except the one passed to it as argument
		
		I will write the later algo later
	'''

	pmap[pid] = Process(target=cal_cross_thd_dep, args=(pid, [logdir, logfile]))
	pmap[pid].start()
	print "Parent started worker ", pid
	pid += 1;
	
	if pid == max_pid:
		print "Max number of processes reached"
		for pid,p in pmap.items():
			print "Parent waiting for worker", pid
			p.join()
		
		pid = 0
	else:
		continue
