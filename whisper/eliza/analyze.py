import gzip
import time
import os
import sys
import errno
import csv
import argparse
import traceback
import gc
import numpy as np
import ConfigParser
# import matplotlib.pyplot as plt
# from pylab import *
from os import listdir
from os.path import isfile, join

ttypes = (['ftrace', 'utrace'])
debugl = ['1','2','3','4']

parser = argparse.ArgumentParser(prog="eliza", description="A tool to analyze epochs")
parser.add_argument('-p', '--print', dest='pt', action='store_true', default=False, help="Print trace")
parser.add_argument('-v', '--version', action='version', version='%(prog)s v0.1', help="Display version and quit")
parser.add_argument('-r', dest='result_dir', help="Result directory in results/ containing per-thread traces")

try:
	args = parser.parse_args()
except:
	#parser.exit(status=0, message=parser.print_help())
	sys.exit(0)

logdir = 'results/' + args.result_dir

stf = 0
try:
	stf = open(logdir + "/stats", 'w')
except:
	print "Could not open stat file", logdir, "/stats"
	sys.exit(0)

'''
for i in range(0,w):
	cq = '/dev/shm/.' + str(os.path.basename(tfile.split('.')[0])) + '_' + str(i) + '.q'
	f = open(cq, 'r')
	csvr = csv.reader(f)
	for row in csvr:
		insert(row)
	f.close()
'''		
		
'''
		t =  ((0) etype, (1) esize, (2) wsize, (3) cwsize,
		        (4) stime, (5) etime, (6) r1, (7) r2, (8) r3 ,(9) r4, 
		        (10) tid)
		r1 = page_span t[6]
		r2 = min dist between dep epochs t[7]
		r3 = max dist between dep epochs t[8]

		def cdf(arr, fname, xaxis, heading, content)

'''
def plot_cdf(arr, style, xcut, ycut, lbl):
	m = {}
	mx = max(arr)
	l = len(arr)
	fl = float(l)
	
	''' 
		Divide the array into buckets 
		or calculate histogram !
	'''
	# np.histogram
	for v in arr:
		if v not in m:
			m[v] = 0
		m[v] = m[v] + 1

	skeys = sorted(m.keys())
	
	X = [x for x in skeys]
	Y = np.cumsum([100*float(m[x])/fl for x in skeys])
	
	A = []
	B = []

	for i in range(0, len(X)-1):
		if X[i] < xcut and Y[i] < ycut:
			A.append(X[i])
			B.append(Y[i])
	
	X = A
	Y = B

	''' 
		State machine interface 
		Consider using OO interface later
	'''
	# xticks(np.arange(0.0, 200, 5.0))
	# yticks(np.arange(0.0, 120.0, 5.0))
	plt.plot(X,Y, style, label=lbl)

def insert(buf, buflen, row, col):
	
	'''
		t =  ((0) etype, (1) esize, (2) wsize, (3) cwsize,
		        (4) stime, (5) etime, (6) r1, (7) r2, (8) r3 ,(9) r4, 
		        (10) tid)
	'''
			
	buf.append(float(row[col])) # Not buf += t
	buflen += 1;

def cdf(arr, fname, xaxis, heading, content, clear):
	if clear == 1:
		clf()
	m = {}
	mx = max(arr)
	l = len(arr)
	fl = float(l)
	
	''' 
		Divide the array into buckets 
		or calculate histogram !
	'''
	# np.histogram
	for v in arr:
		# Ugly hack to draw a graph for 2mrw
		# if v != mx:
		if True is True:
			if v not in m:
				m[v] = 0
			m[v] = m[v] + 1

	skeys = sorted(m.keys())
	
	X = [x for x in skeys]
	Y = np.cumsum([100*float(m[x])/fl for x in skeys])
	
	A = []
	B = []
	for i in range(0, len(X)):
		if Y[i] < 99.5:
			A.append(X[i])
			B.append(Y[i])
	
	X = A
	Y = B

	''' 
		State machine interface 
		Consider using OO interface later
	'''
	# xticks(np.arange(0.0, 200, 5.0))
	# yticks(np.arange(0.0, 120.0, 5.0))
	ylabel('Percentage')
	xlabel(xaxis)
	title(heading + ' (' + fname + ')')
	plot(X,Y)
	
	''' Automate this saving ''' 
	fig = gcf()
	fig.set_size_inches(24,12.75)
	''' Graphs are saved in png/ in eliza '''
	gname = 'png/' + os.path.basename(fname).split('.')[0] + '-' + content + '.png'
	savefig(gname, format='png', dpi=100)


def classify(_map, count):
		
		global stf
		for k in sorted(_map.keys()):
			abs_fq = _map[k]
			rel_fq = float(round(100*float(abs_fq)/float(count),4))
			stf.write(str(k) + "		" + str(abs_fq) + "		" + str(rel_fq)+"\n")
			#stf.write(str(k) + "		" + str("{:,}".format(abs_fq)) + "		" + str(rel_fq)+"\n")

def get_epoch_level_info(only_txt_files):

	# get_size_distribution(only_csv_files)
	# get_dirty_byte_distribution_true
	# get_dirty_byte_distribution_singleton

	global logdir
	global stf
	
	#	t =  ((0) etype, (1) esize, (2) wsize, (3) cwsize,
	#	        (4) stime, (5) etime, (6) r1, (7) r2, (8) r3 ,(9) r4, 
	#	        (10) tid)
	#	r1 = page_span t[6]
	#	r2 = min dist between dep epochs t[7]
	#	r3 = max dist between dep epochs t[8]

	#	def cdf(arr, fname, xaxis, heading, content)

	
	sz_map = {}
	dy_map_true = {}
	dy_map_single = {}
	count = 0
	tot_count = 0
	true_ep_count = 0
	singleton_count = 0
	for f in only_txt_files:
		fname = logdir + '/' + f
		print "Collecting epoch-level info of", fname

		with open(fname, 'r') as fp:
			for te in fp:

				if 'PM_TX' in te:
					continue
				
				if '{' not in te or '}' not in te:
					# Some trace entries (te) may be incomplete due
					# to interrupts
					continue

				ts = te.split(';')[3]
				tl = ts.split(',')
				ep_type = tl[0]
			
				if ep_type == 'null':
					continue

				try:
					sz = float(tl[2])
				except:
					continue
			
				if ep_type == 'true' or ep_type == 'singleton':
					try :
						dirty_true = float(tl[8])
					except:
						print te, ep_type
						continue
					true_ep_count += 1

					if dirty_true in dy_map_true:
						dy_map_true[dirty_true] += 1
					else:
						dy_map_true[dirty_true] = 1
	
					if ep_type == 'singleton':
						dirty_single = dirty_true
						singleton_count += 1
						if dirty_single in dy_map_single:
							dy_map_single[dirty_single] += 1
						else:
							dy_map_single[dirty_single] = 1

				if sz in sz_map:
					sz_map[sz] += 1
				else:
					sz_map[sz] = 1
				
				count += 1
				if (count % 1000000) == 0:
					tot_count += 1000000
					print "Analyzed ", tot_count,  " epochs" #str("{:,}".format(tot_count)), " epochs"

		
	stf.write("Epoch sizes		Abs.freq.		Rel. freq.\n")
	stf.write("=========================================================\n")
	classify(sz_map, count)
	stf.write("\n\n")

	stf.write("True Epoch dirty		Abs.freq.		Rel. freq.\n")
	stf.write("=========================================================\n")
	classify(dy_map_true, true_ep_count)
	stf.write("\n\n")
				
	stf.write("Singleton dirty		Abs.freq.		Rel. freq.\n")
	stf.write("=========================================================\n")
	classify(dy_map_single, singleton_count)
	stf.write("\n\n")

	stf.write("\nTotal true epochs including singletons " + str(true_ep_count))
	stf.write("\n\n")
		
def get_thread_level_info(only_txt_files):
	
	global logdir
	global stf
	tot_tx = 0
	tot_working_set = {}
	n_tx_per_thread = {}
	n_ep_per_tx     = {}
	arr_n_true_ep = []
	for f in only_txt_files:
		fname = logdir + '/' + f
		print "Collecting working set of", fname

		with open(fname, 'r') as fp:
			for lno,te in enumerate(fp):
				# Ignore end-of-tx entries
				# Focus on end-of-epoch entries

				if 'PM_TX' not in te:

					try :
						l_addr = te.split(';')[1].split(',')
						for addr in l_addr:
							if addr not in tot_working_set:
								tot_working_set[addr] = 0
							tot_working_set[addr] += 1
					except:
						continue
				else:
					tot_tx += 1
					if f not in n_tx_per_thread:
						n_tx_per_thread[f] = 0
					n_tx_per_thread[f] += 1		# Count of tx in this thread
					
					try:
						n_true_ep = te.split(',')[3] # Num of true eps in this tx
						if n_true_ep not in n_ep_per_tx:
							n_ep_per_tx[n_true_ep] = 0
						n_ep_per_tx[n_true_ep] += 1
						arr_n_true_ep.append(int(n_true_ep))
					except:
						continue
	
	print "Combining working sets of all threads..."
	#for addr,freq in tot_working_set.items():
	#	print addr, freq
	print "Working set size ", str(len(tot_working_set.keys()) * 64), " bytes"
	stf.write("\nWorking set size " + str(len(tot_working_set.keys()) * 64) + " bytes\n")
	#print "Working set size ", str("{:,}".format(len(tot_working_set.keys()) * 64)), " bytes"

	stf.write("Thread ID		Number of Tx per thread\n")
	stf.write("=========================================================\n")
	for k,v in n_tx_per_thread.items():
		stf.write(str(k) + " " + str(v) + "\n")
	stf.write("\n\n")

	stf.write("Ep count per tx		Abs.freq.		Rel. freq.\n")
	stf.write("=========================================================\n")
	classify(n_ep_per_tx, tot_tx)
	arr_n_true_ep.sort()
	arr = np.array(arr_n_true_ep)
	stf.write("\n95-percentile TX sizes " + str(np.percentile(arr, 95)));
	stf.write("\n\n")
	

print '''Gathering epoch level statistics '''
only_txt_files = [f for f in listdir(logdir) if isfile(join(logdir, f)) and '.txt' in f]
get_epoch_level_info(only_txt_files)
print '''DONE Gathering epoch level statistics '''

print '''Gathering thread level statistics '''
only_txt_files = [f for f in listdir(logdir) if isfile(join(logdir, f)) and '.txt' in f]
get_thread_level_info(only_txt_files)
print '''DONE Gathering thread level statistics '''
# get_working_set(only_txt_files)
# get_number_of_tx_per_thread
# get_number_of_epochs_per_tx

# cross_thread_conflicts ?
# self_thread_conflicts ?
