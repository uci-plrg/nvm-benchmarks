from epoch import epoch
import sqlite3
import sys
import errno
import time
import os

class etable:
	
	'''
	Created a database identified using timestamp
	which is to never re-write an old database
	'''
	
	BUFSIZE = 1000000
	
	def __init__(self, name, db):
		assert name is not None
		self.db = db
		self.buf = []
		self.buflen = 0
		self.tno = 0
		self.tname = str(os.path.basename(name))
		self.dbname = 'db/' + self.tname + "_" + \
					str(time.strftime("%d%b%H%M%S")).lower() + '.db'

		if self.db is False:
			return
		self.conn = sqlite3.connect(self.dbname)
		self.c = self.conn.cursor()
		self.c.execute('''create table if not exists '%s'
			(id text, type text, 
			 esize real, wsize real, cwsize integer,
			 stime real, etime real,
			 tid integer, 
			 r1 real, r2 real, r3 real, r4 real,
			 primary key (id))''' % self.tname)
			 
		'''
			r* are reserved fields for future use
		'''
	def get_dbname(self):
		return str(os.path.basename(self.dbname))

	def insert(self, ep):
		if self.db is False:
			return
			
		etype = ep.get_epoch_type()
		eid = self.tno
		self.tno += 1
		cwsize = ep.get_cwrt_set_sz()
		wsize  = float(cwsize) + ep.get_nwrt_set_sz()
		esize  = wsize + float(ep.get_rd_set_sz())
		stime = ep.get_start_time()
		etime = ep.get_end_time()
		tid = ep.get_tid()
		r1 = r2 = r3 = r4 = 0.0
		c = self.c
		tname = self.tname

		
		t =  (eid, etype, esize, wsize, cwsize,
		        stime, etime, r1, r2, r3 ,r4, tid)
		
		self.buf.append(t) # Not buf += t
		self.buflen += 1;
		if (self.buflen == self.BUFSIZE):
		#	print "Inserting %d tuples" % self.buflen
			self.drain()
			assert self.buflen == 0
		
	def drain(self):
		c = self.c
		tname = self.tname
		try:
			'''This should always succeed as we create a new database each time '''
			c.executemany("insert into '%s' values (?,?,?,?,?,?,?,?,?,?,?,?)" % tname, self.buf)
			self.buflen = 0
			self.buf = []
			self.conn.commit()
		except:
			self.conn.commit()
			#print "fatal : cannot insert into db"
			sys.exit(errno.EIO)

	def commit(self):
		if self.db is False:
			return
		self.drain()

