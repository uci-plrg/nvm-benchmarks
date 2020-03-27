from tentry import tentry
import sys
class utread:
	
	delim = ':'
	psegment_start = 0x100000000000
	psegment_size  = 0x10000000000
	psegment_end   = psegment_start + psegment_size
	
	def __init__(self, usrargs, sysargs):
		self.pid = sysargs[0]
		self.n = sysargs[3]
		self.lno = 0
		
		self.nti = usrargs.nti
		self.clf = usrargs.clf
		self.st = usrargs.st
		self.ld = usrargs.ld
		
		self.te = tentry()
		
		self.te_pm_ref = list(self.te.get_pm_ref())
		self.other_te_types = list(self.te.get_types() - self.te.get_pm_ref())
		
		self.NTI = list(self.te.n_write_ops)
		self.CLF = list(self.te.flush_ops)
		self.LD  = list(self.te.c_read_ops)
		self.ST  = list(self.te.c_write_ops)
		
		'''
		Time stamps in user space are too long since they are number
		of seconds in since epoch - Jan 1, 1970. Wth ?
		We only need relative times and not absolute ones.
		'''
		self.prev_t = 0
		return None
	
	def get_tentry(self, tl):
		self.lno += 1
		assert tl is not None
		te = tentry()
		
		''' Are there any trace entries to be avoided ? '''
		if self.nti is False:
			for t in self.NTI:
				if t in tl:
					return None
		if self.clf is False:
			for t in self.CLF:
				if t in tl:
					return None
		if self.st is False:
			for t in self.ST:
				if t in tl:
					return None	
		if self.ld is False:
			for t in self.LD:
				if t in tl:
					return None

		''' Most frequently occuring tentry types '''
		for te_type in self.te_pm_ref:
			if te_type in tl:
				te.set_type(te_type)
				break
		
		''' Rarely occurring tentry types '''
		if te.is_valid() is False:
			for te_type in self.other_te_types:
				if te_type in tl:
					te.set_type(te_type)
					break
				
		if te.is_valid() is False:
			return None

		# Change here to accomodate user mode traces
		# Add a filter for Mnemosyne
		l = tl.split(':')
		if True is True:
			te.set_tid(int(l[0]))
			# Rule for workers to decide whether to proceed or not
			if te.get_tid() % self.n != self.pid:
				del te
				return None

			''' I didn't know that Sanketh had put this code here ! Great ! '''
			te_time = int(l[1])
			if te_time >= self.prev_t:
				self.prev_t = te_time
			else:
				print "TIME CHECK FAIL", self.lno, te.te_type, te.get_tid(), te_time, self.prev_t
				# assert 0
				te_time = self.prev_t
				self.prev_t = te_time
				
			te.set_time(te_time)
			te.set_callee('null')
			
			if te.need_arg():
				# Check for M, very crucial !
				if self.psegment_start <= int(l[3],16) and \
					int(l[3],16) <= self.psegment_end:
					te.set_addr(l[3])
					te.set_size(int(l[4]))
					
					if (te.is_movnti() is True) or (te.is_flush() is True):
						te.set_caller(l[6])
						te.set_pc(int(l[7]))
					else:
						te.set_caller(l[5])
						te.set_pc(int(l[6]))

				else:
					del te
					return None
			#else:
				#te.set_callee(l[4])
				# te.set_caller(l[5].split('-')[1])
					
			return te
		else:
			return None
