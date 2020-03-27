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
lmaps = [] # list of maps 
est = None
lookback_time = 0.005 # 5 ms
# Keep the two separate so that you can collectively read in lines later
# if the files are too big
f_to_itree_m = {}
f_to_lnos_m = {}

def make_itree(logdir, f):
	global f_to_itree_m
	__itree = it.IntervalTree()
	with open(logdir + '/' + f, 'r') as fp:
		for lno,l in enumerate(fp):
			try:
				# this may fail due to out-range index, hence "try"
				epstr = l.split(';')[2]
				stime = est.get_stime_from_str(epstr)
				etime = est.get_etime_from_str(epstr)
				__itree[stime:etime] = lno
			except:
				continue
	fp.close()
	f_to_itree_m[f] = __itree
	
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
	
def make_lmap(logdir, f):
	global f_to_lnos_m
	lno_to_lines_m = {}
	with open(logdir + '/' + f, 'r') as fp:
		for lno,l in enumerate(fp):
			lno_to_lines_m[lno] = l.split(';')[1].split(',')		
	fp.close()
	f_to_lnos_m[f] = lno_to_lines_m

def cal_cross_thd_dep(pid, args):

	logdir = args[0]
	logfile = args[1]
	global wpid
	global est
	global lookback_time
	global f_to_lnos_m
	global f_to_itree_m
	wpid = pid
	est = ep_stats()
	print "Worker " + str(wpid) + " analyzing " + logfile

	onlyfiles = [f for f in listdir(logdir) if isfile(join(logdir, f)) and 'txt' in f and f != logfile]
	
	for f in onlyfiles:
		make_itree(logdir, f)
		make_lmap(logdir, f)

	sys.exit(0) # It takes 32 seconds until this point. Cannot use this !
	''' Core analysis begins here ''' 
	fp = open(logdir + '/' + logfile, 'r')
	for l in fp:
		
		try:
			# this may fail due to out-range index
			epstr = l.split(';')[2]
			stime = est.get_stime_from_str(epstr)
			etime = est.get_etime_from_str(epstr)
		except:
			continue
		
		# A list of competing epochs for one guest thread, from all other threads
		concurrent_epochs = []
		for f,__itree in f_to_itree_m.iteritems():
			concurrent_epochs.append((f,list(__itree.search(stime - lookback_time,stime,strict=True))))

		if wpid == 0: # This needs to go
			if len(concurrent_epochs) > 0:
				keys = l.split(';')[1].split(',')
				print str(stime-lookback_time) + '-' + str(stime), keys
				
				for (f,l_iv) in concurrent_epochs:
					print "Concur", f, l_iv
					lno_to_lines_m = f_to_lines_m[f]

					concurrent_addr_m = {}
					for iv in l_iv:
						lno = iv.data
						for addr in lno_to_lines_m[lno]:
							concurrent_addr_m[addr] = 0

					for addr in keys:
							if addr in concurrent_addr_m:
								print "Deps", f, addr
							else:
								a = 1
			

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

	
	
'''
for graph in cfg.sections():
	
	print "\nPlotting " + str(graph)

	optval = {}
	for option in cfg.options(graph):
		optval[option] = cfg.get(graph, option)

	if optval['graph_type'] == 'cdf':

		plt.clf()
		plt.xlabel(str(optval['xlabel']))
		plt.ylabel(str(optval['ylabel']))
		plt.title(str(optval['heading']))
		plt.grid(True)
		gname = 'png/' + graph + '.png'		
		
		if optval['values'] in colmap:
			col = colmap[optval['values']]
		else:
			print "value for config option 'values' unsupported"
			sys.exit(0)
		
		styles = marker
		if 'marker' in optval:
			if optval['marker'] == 'False':
				styles = plain
			else:
				print "value for config option 'marker' unsupported"
				sys.exit(0)
		n_si = len(styles)
		

		si = -1
		for fn in optval['data_files'].split(','):			
			si += 1
			buf = []
			buflen = 0				
			try:
				fd = open(datadir + fn, 'r')
			except:
				print "cannot open " + datadir + fn
				sys.exit(0)				

			csvfd = csv.reader(fd)
			for row in csvfd:
				insert(buf, buflen, row, col)			
			fd.close()
			print "Done reading " + str(fn)

			# Eliminate zeros
			buf = filter(lambda i: i > 0, buf)
			plot_cdf(buf,styles[si%n_si], int(optval['xcut']), int(optval['ycut']), fn.split('-')[0])

		
		plt.legend(loc='lower right')
		plt.savefig(gname, format='png', dpi=100)

	else:
		print "value for config option 'graph_type' unsupported"
		sys.exit(0)
'''
