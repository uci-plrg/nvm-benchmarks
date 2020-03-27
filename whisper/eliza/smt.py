from txn import tx
from tentry import tentry
import sys
import os,csv,gzip


class smt:
	def __init__(self, tid, usrargs, sysargs):

		self.tid = tid
		self.tx = None
		self.txid = 0
		self.tx_count = 0
		self.tx_stack = [] # For nested txns, may shift to real stack
		
		self.cwrt_set = {} # ?

		''' Open a per-thread log file here '''
		self.usrargs = usrargs
		self.tfile   = usrargs.tfile

		self.sysargs = sysargs
		self.logdir  = sysargs[6]

		''' Per-thread log file '''
		self.logfile = self.logdir + '/' + \
					  str(os.path.basename(self.tfile.split('.')[0])) +\
					  '-' + str(self.tid) + '.txt'
		
	def do_tentry(self, te):
		'''
			A thread can receive a compound operation or a simple
			operation. A compound operation is an operation on a range
			of memory specified by the starting address of the range, 
			the size of the range and the type of operation. The types
			can be read, write, movnti or clflush.
			
			A simple operation is an operation on a 8-byte range.
			Mulitple consecutive simple operations form a compound one.
		'''
		assert te.is_valid() is True
		te_type = te.get_type()

		if te.is_tx_start():
			
			''' We flatten any nested transactions '''
			if self.tx is None:
				assert self.tx_count == 0
				self.tx_count += 1
				self.txid += 1
				''' Create a new txn context '''
				self.tx = tx([self.tid, self.txid, te.get_time(), 	\
								None, self.cwrt_set, self.logfile],	\
								self.usrargs, self.sysargs)
				assert self.tx is not None

			return self.tx.do_tentry(te)
			
			''' 
				For nested transactions 
				Abandoning this code in favor of PMFS.
				For userspace, you may plug it back in
			
			if self.tx is not None:
				self.tx_stack.append(self.tx)
				
			self.txid += 1
			self.tx = tx([self.tid, self.txid, te.get_time(), 	\
							self.log, self.cwrt_set], 			\
							self.usrargs, self.sysargs)
			assert self.tx is not None
			return self.tx.do_tentry(te)
			'''
		elif te.is_tx_end():

			if self.tx_count > 0:
				self.tx_count -= 1
			
			if self.tx_count == 0 and (self.tx is not None): 
				self.tx.tx_end(te)
				self.tx = None
				
			return None
			
			'''
				For nested transactions 
				Abandoning this code in favor of PMFS.
				For userspace, you may plug it back in

			if self.tx is None:
				return None
				# Houston, something is wrong and we lost the start of the txn
				# So ignore and proceed
				
			try:
				ret = self.tx.tx_end(te)
			except:
				print "THD_ERR1", te.te_list()
				sys.exit(0)

			self.tx = None
			if len(self.tx_stack) > 0:
				self.tx = self.tx_stack.pop()
			return ret
			'''
		else:
			''' We don't care about operations outside a txn '''
			if self.tx is None:
				return None
			else:
				return self.tx.do_tentry(te)
									
	def update_call_chain(self, caller, callee):
		return self.tx.update_call_chain(caller, callee)
	
	def get_call_chain(self):
		return self.tx.get_call_chain()
	
	def clear_call_chain(self):
		self.tx.clear_call_chain()
		
	def get_tid(self):
		return self.tid
		
	def get_tid(self):
		if tx is not None:
			assert tx.get_txid() == self.txid
		return self.txid

	def close_thread(self):
		if self.log is not None:
			self.log.close()
				
			
