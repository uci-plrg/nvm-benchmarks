import gzip
import os
import sys


if len(sys.argv) == 1:
	print "Usage : python gkf.py <trace file>"
	sys.exit(0)
else:
	tfile = sys.argv[1]
	tf = gzip.open(tfile, 'rb')

pcs = set([])
try:
	for tl in tf:
		l = tl.split()
		if 'VERSION' in l or 'PCIDEV' in l:
			continue

		if 'UNKNOWN' in l:
			pc = l[5]
		else:
			pc = l[6]
		pcs.add(pc)
except:
	for i in list(pcs):
		print i
