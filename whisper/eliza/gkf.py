import gzip
import os
import sys


if len(sys.argv) == 1:
	print "Usage : python gkf.py <trace file>"
	sys.exit(0)
else:
	tfile = sys.argv[1]
	tf = gzip.open(tfile, 'rb')

try:
	for tl in tf:
		sys.stdout.write(tl)
except:
	sys.exit(0)
