from ftread import ftread
from utread import utread
from smt import smt
from ep_stats import ep_stats
from multiprocessing import Pool, TimeoutError, Process, Queue, Lock
from multiprocessing.sharedctypes import Value, Array
from ctypes import Structure

import gzip
import time
import os
import subprocess
import sys
import errno
import csv
import argparse
import traceback
import gc

ttypes = (['ftrace', 'utrace'])
verbosity = (['1','2','3','4','5'])
reuse_t = ['1','2']

parser = argparse.ArgumentParser(prog="eliza", description="A tool to analyze epochs")
parser.add_argument('-f', dest='tfile', required=True, help="Gzipped trace file")
parser.add_argument('-y', dest='ttype', required=True, help="Type of trace file", choices=ttypes)
parser.add_argument('-w', dest='workers', default = 1, help="Number of workers")
parser.add_argument('-b', dest='db', action='store_true', default=False, help="Create database")
parser.add_argument('-o', dest='flow', action='store_true', default=False, help="Get control flow of an epoch")
parser.add_argument('-nz', dest='anlz', action='store_false', default=True, help="Ignore analysis")
parser.add_argument('-ni', dest='nti', action='store_false', default=True, help="Ignore movnti")
parser.add_argument('-nf', dest='clf', action='store_false', default=True, help="Ignore clflush/clwb")
parser.add_argument('-ns', dest='st', action='store_false', default=True, help="Ignore stores")
parser.add_argument('-nl', dest='ld', action='store_false', default=True, help="Ignore loads")
parser.add_argument('-u', dest='reuse', default= 0, help="Calculate reuse [1 = Inside an epoch, 2 = Inside a thread, 3 = 1 & 2]", choices=reuse_t)
parser.add_argument('-p', '--print', dest='pt', default=0, help="Set verbosity and print trace for debugging (recommended value is 2)", choices=verbosity)
parser.add_argument('-v', '--version', action='version', version='%(prog)s v0.1', help="Display version and quit")

try:
	args = parser.parse_args()
except:
	sys.exit(0)

def digest(usrargs, sysargs):
	
	n_tl  = 0
	BATCH = 100000	  # Worker emits stmt after processing BATCH entries
	tid   = -1
	m_threads = {} #tls
	est   = ep_stats()
	
	# pmap[wpid] = Process(target=digest, args=(args, [wpid, [], 100000, w, None, [-1], logdir]))
	tfile = usrargs.tfile
	ttype = usrargs.ttype
	pt    = int(usrargs.pt)

	pid   = sysargs[0]
	exclude = sysargs[1]
	include = sysargs[5] # List of host tids you want to examine, for internal use only

	if pt > 0:
		op = open("/dev/shm/" + \
			str(os.path.basename(tfile).split('.')[0]) + \
			'_' + str(pid) + '.t', 'w')	
			
	if ttype == 'ftrace':
		tread = ftread(usrargs, sysargs)
	elif ttype == 'utrace':
		tread = utread(usrargs, sysargs)

	try:
		if 'gz' in tfile:
			cmd = "zcat " + tfile
		else:
			print "Please provide a compressed gzipped trace file"
			sys.exit(0)
	except:
		print "Unable to open ", tfile
		sys.exit(errno.EIO)
	

	try: #for i in range(0,1):
		for tl in os.popen(cmd, 'r', 32768): # input is global
			
			te = tread.get_tentry(tl)
			
			if te is None:
				continue

			if te.get_tid() in exclude:
				continue

			n_tl += 1;

			if(n_tl % BATCH == 0):
				print "Worker ", pid, "completed ", \
				str("{:,}".format(n_tl)) , " trace entries"

			if pt > 0: # pt = 1
				if -1 in include:
					# Write all
					op.write(tl)
					op.flush()
				elif te.get_tid() in include:
					# Write only specific tids
					op.write(tl)
					op.flush()

			if te.get_tid() != tid:
				tid = te.get_tid()
				if tid not in m_threads:
					m_threads[tid] = smt(tid, usrargs, sysargs)
				curr = m_threads[tid]			

			if pt > 1: # pt = 2
				l = te.te_list()
				op.write('te = ' + str(l) + '\n')
				op.flush()

			# curr.update_call_chain(caller, callee)
		
			ep = curr.do_tentry(te)

			if ep is not None:
				if pt > 2: # pt = 3
					op.write('ep = ' + str(ep.ep_list()) + '\n')
					op.flush()

				t = est.get_tuple(ep)
				if pt > 3: # pt = 4
					op.write('tu[' + str(t_buf_len - 1) + '] = ' \
					+ str(t_buf[t_buf_len-1]) + '\n')
					op.flush()
	except:
		sys.exit(0)

if __name__ == '__main__':
		
		try:
			w = int(args.workers)
		except:
			print "The number of workers must be an integer"
			sys.exit(1)
			
		if w <= 0:
			print "The number of workers must be greater than 0"
			sys.exit(1)
			
		pmap = {}
		shmmap = {}
		qs = {}
		datadir = './results/'
		try:
			os.mkdir(datadir)
		except:
			# this will mostly fail because the dir exists
			print ''
		
		resdir = str(time.strftime("%d%b%H%M%S")).lower() + \
				'-' + str(os.path.basename(args.tfile.split('.')[0]))

		logdir = datadir + resdir

		os.mkdir(logdir)

		for wpid in range(0, w):
			pmap[wpid] = Process(target=digest, args=(args, [wpid, [], \
										100000, w, None, [-1], logdir]))
			pmap[wpid].start()
			print "Parent started worker ", wpid
		
		for pid,p in pmap.items():
			print "Parent waiting for worker", pid
			p.join()
		

		if args.anlz is True:
			''' Put the analysis module here '''
			cmd = 'python analyze.py -r ' + str(resdir)
			os.system(cmd)
			
			''' Put the dependency checker here '''
			if 'exim' in args.tfile:
				cmd = 'pypy cross_for_exim.py -t 50 -d 1 -r ' + str(resdir)
				os.system(cmd)
			else:
				cmd = 'pypy cross_thd_dep_memopt.py -t 50 -d 1 -r ' + str(resdir)
				os.system(cmd)

		else:
			print "No analysis performed"
		

