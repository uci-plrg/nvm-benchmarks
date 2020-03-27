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
import matplotlib.pyplot as plt
from pylab import *


ttypes = (['ftrace', 'utrace'])
debugl = [1,2,3,4]

parser = argparse.ArgumentParser(prog="eliza", description="A tool to analyze epochs")
parser.add_argument('-f', dest='tfile', required=True, help="Gzipped trace file")
parser.add_argument('-y', dest='ttype', help="Type of trace file", choices=ttypes)
parser.add_argument('-d', dest='debug', default = 0, help="Debug levels", choices=debugl)
parser.add_argument('-w', dest='workers', default = 1, help="Number of workers")
parser.add_argument('-b', dest='db', action='store_true', default=False, help="Create database")
parser.add_argument('-o', dest='flow', action='store_true', default=False, help="Get control flow of an epoch")
parser.add_argument('-nz', dest='anlz', action='store_false', default=True, help="Analyze and collect some stats")
parser.add_argument('-p', '--print', dest='pt', action='store_true', default=False, help="Print trace")
parser.add_argument('-v', '--version', action='version', version='%(prog)s v0.1', help="Display version and quit")

'''
try:
	args = parser.parse_args()
except:
	# parser.exit(status=0, message=parser.print_help())
	sys.exit(0)


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
cfg = ConfigParser.ConfigParser()
cfg.read('data.ini')

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


''' ******************** Obsolete ********************** '''

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

	
def analyze():

	''' get 95-th percentile epoch size '''
	global buf
	global buflen
	global tfile
	''' Stats are saved in stat/ in eliza '''
	fname = 'stat/' + os.path.basename(tfile.split('.')[0]) + '_' + str(time.strftime("%d%b%H%M%S")).lower()  + '.stat'

	'''
		t =  ((0) etype, (1) esize, (2) wsize, (3) cwsize,
		        (4) stime, (5) etime, (6) r1, (7) r2, (8) r3 ,(9) r4, 
		        (10) tid)
		r1 = page_span t[6]
		r2 = min dist between dep epochs t[7]
		r3 = max dist between dep epochs t[8]

		def cdf(arr, fname, xaxis, heading, content)

	'''

	tt = [t[2] for t in buf]
	cdf(tt, fname, "Epoch size in terms of 64B cache-blocks", \
	"CDF of epoch sizes", \
	'ep_size', 1)
	
	tt = [t[7] for t in buf]
	tt = filter(lambda i: i != 0, tt)
	cdf(tt, fname, 'Minimum distance between dependent epochs', \
	'CDF of minimum distance between dependent epochs', \
	'cdf_min_dist', 1)

	tt = [t[8] for t in buf]
	tt = filter(lambda i: i != 0, tt)
	cdf(tt, fname, 'Maximum distance between dependent epochs', \
	'CDF of maximum distance between dependent epochs', \
	'cdf_max_dist', 0)

	a = np.array(tt)
	a = a[a != 0.0]
	p95 = np.percentile(a, 95)
	p99 = np.percentile(a, 99)
	p5  = np.percentile(a, 5)
	med = np.median(a)
	avg_s = np.mean(a)
	max_sz = np.amax(a)
	min_sz = np.amin(a) # amino
	tot = buflen
	nlpcnt = float(tnull)/float(buflen) * 100
		
	''' get avg duration in secs '''
	d = np.array([t[5]-t[4] for t in buf])
	avg_d = np.mean(d)
		
	''' get 95-th percentile page span '''
	psa = np.array([t[6] for t in buf])
	psa95 = np.percentile(psa, 95)
	max_psa = np.amax(psa)
	
	tid_m = {}
	for t in buf:
		tid_m[t[10]] = 0
	

	print "Stat file :", fname
	f = open(fname, 'w')
	if f is not None :
		''' Better way of reporting stats ? '''
		f.write("Trace file        : "  + str(tfile) + "\n")
		f.write("Total epochs      : "  + str("{:,}".format(buflen)) + "\n")
		f.write("Total null epochs : "  + str("{:,}".format(tnull)) + " (" + str(nlpcnt) + "%)\n")
		f.write("Total threads     : "  + str(len(tid_m)) + "\n")
		f.write("Average duration  : "  + str(avg_d) + " secs \n")
		f.write("5-tile epoch size : "  + str(p5)    + "\n")
		f.write("95-tile epoch size: "  + str(p95)   + "\n")
		f.write("99-tile epoch size: "  + str(p99)   + "\n")
		f.write("Median epoch size : "  + str(med) + "\n")
		f.write("95-tile page span : "  + str(psa95)   + "\n")
		f.write("Max page span     : "  + str(max_psa)   + "\n")
		f.write("Average epoch sz  : "  + str(avg_s) + "\n")
		f.write("Max epoch size    : "  + str(max_sz) + "\n")
		f.write("Min epoch size    : "  + str(min_sz) + "\n")
		f.write("\n* All epoch sizes are in terms of 64B cache blocks \n")		
		f.close()
	cmd = 'cat ' + fname
	os.system(cmd)


# analyze()
