from epoch import epoch
from tentry import tentry
import sys
import math
import os,csv,gzip
from ep_stats import ep_stats


class tx:
	def __init__(self, tidargs, usrargs, sysargs):

		self.ep     = None 
		self.ep_ops = None 
		self.true_ep_count = 0 # Count of true epochs
		self.null_ep_count = 0 # Count of null epochs
		self.cwrt_set = {} # ?
		self.call_chain = []
		self.inter_arrival_duration = 0
		self.prev_ep_end = 0

		self.tid  = tidargs[0]
		self.txid = tidargs[1]
		self.__tx_start = tidargs[2]
		self.__tx_end   = 0.0
		self.logfile = tidargs[5]
		self.log = None
		
		self.ppid   = sysargs[0] # ?
		self.logdir = sysargs[6] # ?

		self.usrargs = usrargs
		self.flow = self.usrargs.flow

		self.est = ep_stats()
		self.ptype = [0,0,0,0]
		self.data_writes = 0
		self.meta_writes = 0


		
	def sanity(self, sa, sz, r):
			return
			sa = int(sa, 16)
			ea = sa + sz - 1 # This prevents an off-by-one error
			ecl = ea & ~(63)
			if ecl !=r :
				print hex(ecl), hex(r)
			assert ecl == r

	def log_open(self):
		while self.log is None:
			self.log = open(self.logfile, 'a')

					
	def log_close(self):
		if self.log is not None:
			self.log.close()
		self.log = None

	def log_write(self, s):
		self.log_open()
		if self.log is not None:
			self.log.write(s)
		self.log_close()			
	
	def log_start_entry(self):
		self.log_open()
		if self.log is not None:
			self.log.write('{;')
		self.log_close()			

	def log_end_entry(self):
		self.log_open()
		if self.log is not None:
			self.log.write('}\n')
		self.log_close()		

	def log_insert_entry(self, lentry):
		self.log_open()
		if self.log is not None:
			self.log.write(str(lentry) + ';')
		self.log_close()
	
	def do_tentry(self, te):

		assert te.is_valid() is True

		ret = None
		te_type = te.get_type()
		log = self.log
		est = self.est
		
		if self.ep is None: 
			if te.is_write():
				''' The beginning of a new epoch '''
				if self.prev_ep_end > 0:
					assert self.true_ep_count > 0
					self.inter_arrival_duration += te.get_time() - 	\
													self.prev_ep_end
													
				''' No epoch number should start from 0 '''
				self.true_ep_count += 1
				self.ep = epoch([self.true_ep_count, te.get_time()], 	\
								[self.tid, None, self.cwrt_set, self.txid, self.logfile], \
								self.usrargs) 
								#self.tid, te.get_time(), log)
				self.ep_ops = self.ep.get_ep_ops()
				
				try:
					r = self.ep_ops[te_type](te)
				except:
					print "TXN_ERR1", te.te_list()
					return None# assert 0
				''' Size has to be greater than 0'''
				self.sanity(te.get_addr(), te.get_size(), r)

				assert self.ep.is_true()
		else: 
			# True epoch
			if te.is_fence():
				# The end of another epoch
				self.prev_ep_end = te.get_time()
				
				self.ep.end_epoch(te)
				
				''' Get some stuff about the epoch '''
				self.data_writes += self.ep.get_data_writes()
				self.meta_writes += self.ep.get_meta_writes()

				# Cannot delegate this to the epoch
				self.log_start_entry()
				for e in self.ep.get_info():
					self.log_insert_entry(e)
				self.log_insert_entry(est.get_str(self.ep))
				self.log_end_entry()

				ret = self.ep
				self.ep = None
				self.ep_ops = None

			else:
				try:	
					r = self.ep_ops[te_type](te)
				except:
					print "TXN_ERR2", te.te_list()
					return None #sys.exit(0) #assert 0
				if(te.get_size() > 0):
					self.sanity(te.get_addr(), te.get_size(), r)
				
		return ret
		'''
		if te.is_write() is True:
			if self.ep.is_null():
				self.ep.set_tid(self.tid)
				self.ep.set_time(te.get_time())
			
			assert te_type in self.ep_ops
			
			ep_op = self.ep_ops[te_type]
			r = ep_op(te)
			if(te.get_size()):
				self.sanity(te.get_addr(), te.get_size(), r)
				# Do a sanity check only when u have memory accesses
			assert self.ep.is_true() is True
		elif te.is_fence():
			if self.ep.is_null(): # null epoch
				self.ep.set_tid(self.tid)
				self.ep.set_time(te.get_time())
				self.ep.end_epoch(te)
				ret = self.ep
				self.ep.reset()
			else:
				assert self.ep.is_true() is True
				self.ep.end_epoch(te)
				ret = self.ep
				self.ep.reset()
		else:
			if self.ep.is_true():
				ep_op = self.ep_ops[te_type]
				r = ep_op(te)
				if(te.get_size() > 0):
					self.sanity(te.get_addr(), te.get_size(), r)
				# Do a sanity check only when u have memory accesses
		return ret
		'''
	
	def update_call_chain(self, caller, callee):
		if self.flow is False:
			return
			
		l = len(self.call_chain)
		
		if caller != 'null':
			if l == 0:
				self.call_chain.append(caller)
			elif caller != self.call_chain[l-1]:
				self.call_chain.append(caller)
		
		if callee == 'null':
			# print "(update_call_chain)", self.tid, self.time
			# callee cannot be null because the processor always is 
			# inside a callee !
			assert callee != 'null'
		else:
			if l == 0:
				self.call_chain.append(callee)
			elif callee != self.call_chain[l-1]:
				self.call_chain.append(callee)
	
	def get_call_chain(self):
		if self.flow is False:
			return None
			
		if len(self.call_chain) == 0:
			# print "(get_call_chain)", tid
			assert len(self.call_chain) != 0
		else:
			call_str = 'S'
			m = "->"
			for f in self.call_chain:
				call_str += m
				call_str += f
			
		call_str += m
		call_str += 'E'	
		self.call_chain = []
		return call_str
	
	def clear_call_chain(self):
		self.call_chain = []
		
	def get_tid(self):
		return self.tid

	def get_txid(self):
		return self.txid
	
	def tx_end(self, te):

		#try:
		for i in range(0,1):
			assert self.__tx_end == 0.0
		
			if self.ep is not None:
				self.prev_ep_end = te.get_time()
				# This code is here only because of M, due to its bad design
				self.ep.end_epoch(te)
				ret = self.ep
				self.ep = None
				self.ep_ops = None

				self.log_insert_entry(self.est.get_str(ret))
				self.log_end_entry()
				
			self.__tx_end = te.get_time()
			assert self.__tx_end >= self.__tx_start
			iad = float(self.inter_arrival_duration)
			c = float(self.true_ep_count)
			avg_iad = 0.0
			
			if c > 0:
				avg_iad = round(iad/c, 2)

			'''
			t3byt2 = 0.0 # For M, 
			t2byt3 = 0.0 # For PMFS, N-store, Echo, Redis

			if self.ptype[2] > 0:
				t3byt2 = float(self.ptype[3]) / float(self.ptype[2])
			if self.ptype[3] > 0:
				t2byt3 = float(self.ptype[2]) / float(self.ptype[3])
			'''
			tot_writes = self.data_writes + self.meta_writes
			m2d_pmfs = 0.0
			if self.data_writes > 0:
				# For PMFS, N-store, Echo, Redis
				tmp = float(self.meta_writes) / float(self.data_writes)
				m2d_pmfs = round(tmp, 2)
			m2d_nemo = 0.0
			if self.meta_writes > 0:
				# For Mnemosyne
				tmp = float(self.data_writes) / float(self.meta_writes)
				m2d_nemo = round(tmp, 2)

			le = ['PM_TX', self.__tx_start, self.__tx_end, \
					self.true_ep_count, self.null_ep_count, avg_iad, \
					m2d_pmfs, m2d_nemo, self.meta_writes, self.data_writes]
			self.log_start_entry()
			for li in le:
				# for each list item in list entries
				s = str(li) 
				s += ','
				self.log_write(s)
			self.log_write(';')
			self.log_end_entry()
		#except:
		#	print self.tid, self.txid, self.__tx_start, self.__tx_end
		#	assert False

		return None

						
			
