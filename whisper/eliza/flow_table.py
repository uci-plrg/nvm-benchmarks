import sqlite3
import sys
import os
import time

class flow_table:
	def __init__(self, tname, flow):
		self.flow = flow
		if self.flow is False:
			return
			
		self.tname = 'db/' + str(os.path.basename(tname)) + '_' + \
					str(time.strftime("%d%b%H%M%S")).lower() + '_ft'
		self.tno = 0
		self.dbname = self.tname + '.db'
		self.conn = sqlite3.connect(self.dbname)
		self.c = self.conn.cursor()
		self.c.execute('''create table if not exists '%s'
			(id integer, tid integer, stime real, etime real, cchain text,
			 primary key (id))''' % self.tname)

	def insert(self, cchain, tid, stime, etime):
		if self.flow is False:
			return
			
		t =  (self.tno, tid, stime, etime, cchain)
		self.tno += 1
		tname = self.tname
		c = self.c
		try:
			c.execute("insert into '%s' values (?,?,?,?,?)" % tname, t)
		except:
			#print "failure to insert call chain in flow db"
			self.commit()
			sys.exit(-1)

	def commit(self):
		if self.flow is False:
			return
		self.conn.commit()
