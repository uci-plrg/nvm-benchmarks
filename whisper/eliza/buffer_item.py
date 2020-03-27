class buffer_item.py:
	"""Cacheline"""
	cstates = set(['clean', 'dirty', 'invalid'])
	
	def __init__(self, addr):
		self.addr = addr
		self.state = 'invalid'
		assert self.addr is not None
		assert self.state in self.cstates
		
	def get_addr(self):
		return self.addr
	
	def get_state(self):
		assert self.state in self.cstates
		return self.state
		
	def set_state(self, state):
		assert state in self.cstates
		assert self.state in self.cstates
		self.state = state
