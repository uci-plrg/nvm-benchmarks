class tentry:

					
	c_write_ops = set(['PM_W', 'PM_D'])
	c_read_ops  = set(['PM_R'])
	n_write_ops = set(['PM_I'])
	flush_ops   = set(['PM_L', 'PM_O'])
	commit_ops  = set(['PM_C'])
	fence_ops   = set(['PM_N', 'PM_B'])
	tx_delims   = set(['PM_XS', 'PM_XE'])
	
	te_types    = set().union(
						c_write_ops,
						c_read_ops, 
						n_write_ops, 
						flush_ops,
					    commit_ops, 
					    fence_ops, 
					    tx_delims)
					    
	te_pm_ref   = set().union(
						c_write_ops,
						n_write_ops,
						c_read_ops)
	
	delims      = set().union(
						commit_ops,
						fence_ops, 
						tx_delims)
							
	def __init__(self):
		self.tid = 0
		self.time = 0.0
		self.te_type = 'INV'
		self.addr = '0'
		self.size = 0
		self.caller = 'null'
		self.callee = 'null'
		self.pc = 0


	def reset(self):
		self.tid = 0
		self.time = 0.0
		self.te_type = 'INV'
		self.addr = '0'
		self.size = 0
		self.caller = 'null'
		self.callee = 'null'


	def get_types(self):
			return self.te_types
			
	def get_pm_ref(self):
			return self.te_pm_ref
	
	def is_valid(self):
		if self.te_type != 'INV':
			return True
		else:
			return False
	
	def invalidate(self):
		self.te_type = 'INV'
		
	def get_tid(self):
		return self.tid

	def get_time(self):
		return self.time
		
	def get_type(self):
		return self.te_type
			
	def get_addr(self):
		return self.addr
	
	def get_size(self):
		return self.size
		
	def get_callee(self):
		return self.callee
		
	def get_caller(self):
		return self.caller
		
	def set_type(self,te_type):
		self.te_type = te_type
		
	def set_tid(self, tid):
		self.tid = tid
		
	def set_time(self, time):
		self.time = time
	
	def set_addr(self, addr):
		self.addr = addr
	
	def set_size(self, size):
		self.size = size
		
	def set_callee(self, fun):
		self.callee = fun
	
	def set_caller(self, fun):
		self.caller = fun
		
	def set_pc(self, pc):
		self.pc = int(pc)
		
	def get_pc(self):
		return self.pc
		
	def te_list(self):
		l = []
		l.append(str(self.tid))
		l.append(str(self.time))
		l.append(self.te_type)
		l.append(str(self.addr))
		l.append(str(self.size))
		l.append(self.callee)
		l.append(self.caller)
		l.append(self.pc)
		return l
		
	def get_tuple(self):
		return (str(self.addr), str(self.size))

	def get_str(self):
		return str(self.addr) + ',' + str(self.size)

	def need_arg(self):
		if self.te_type not in self.delims:
			return True
		else:
			return False
	
	def is_fence(self):
		if self.te_type in self.fence_ops:
			return True
		else:
			return False
	
	def is_write(self):
		if (self.te_type in self.c_write_ops) or \
				(self.te_type in self.n_write_ops):
			return True
		else:
			return False

	def is_movnti(self):
		if (self.te_type in self.n_write_ops):
			return True
		else:
			return False

	def is_flush(self):
		if (self.te_type in self.flush_ops):
			return True
		else:
			return False

	def is_tx_start(self):
		if (self.te_type in self.tx_delims and \
				self.te_type == 'PM_XS'):
			return True
		else:
			return False

	def is_tx_end(self):
		if (self.te_type in self.tx_delims and \
				self.te_type == 'PM_XE'):
			return True
		else:
			return False
