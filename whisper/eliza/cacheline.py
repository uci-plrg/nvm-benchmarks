class cacheline:
	"""Cacheline"""
	cstates = set(['clean', 'dirty', 'invalid'])
	CSIZE = 64
	CMASK = 0xffffffffffffffc0
	COFF  = 0x3f
		
	def __init__(self, addr):
		self.addr = addr
		self.state = 'invalid'
		self.dbufs = set()
		self.dbytes = set()
		assert self.addr is not None
		assert self.state in self.cstates
		
	def dirty_all(self):
		self.dbufs.add(8)
			
	def set_dirty_bit(self):
		self.dirty_all()

	def dirty(self, b_idx):
		assert b_idx > -1 and b_idx < 8
		self.dbufs.add(b_idx)

	def dirty_bytes(self, ar, sz):
		# return 
		dl = []
		if not (hex(ar & self.CMASK) == hex(self.addr & self.CMASK)):
			# The addr doesn't belong to this cache line !
			print self.dirty_bytes.__name__, ': <', hex(ar), '/', hex(ar & self.CMASK), 'in', hex(self.addr), sz, '>'
			assert hex(ar & self.CMASK) == hex(self.addr & self.CMASK)
		for i in range(0, sz):
			if not(-1 < (ar - self.addr) and (ar - self.addr)  < 64):
				''' failure condition '''
				# print self.dirty_bytes.__name__, (ar - self.addr)
				# print '<', ar, '=', hex(ar), 'in', hex(ar & self.CMASK),'> <', self.addr, '=', hex(self.addr), 'in', hex(self.addr & self.CMASK), '>'
				print self.dirty_bytes.__name__, ': <', hex(ar), '/', hex(ar & self.CMASK), 'in', hex(self.addr), sz, '>', dl
				assert -1 < (ar - self.addr) and (ar - self.addr) < 64
			self.dbytes.add(ar - self.addr)
			ar += 1 # Increment address by 1, not by i else you get (ar+1), (ar+1)+2, ((ar+1)+2)+3 --> GOOD OBSERVATION !
			dl.append((hex(ar),i))
	
	def get_dirty_nbytes(self):
		l = len(list(self.dbytes))
		assert 0 <= l and l <= 64
		return l
	''' 
		This informs whether cache line is dirty or not	
		In case of partially dirty lines, it informs how many
		8-byte buffer items are dirty
	'''
	def get_dirtyness(self):
		if 8 in self.dbufs:
			return 8
		else:
			return len(self.dbufs)
			
	def get_addr(self):
		return self.addr
	
	def get_state(self):
		assert self.state in self.cstates
		return self.state
		
	def set_state(self, state):
		assert state in self.cstates
		assert self.state in self.cstates
		self.state = state
