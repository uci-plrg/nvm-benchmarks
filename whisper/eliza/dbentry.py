from epoch import epoch
class dbentry:
	def __init__(self, ep):
		self.eid = str(ep.get_tid()) + "." + str(ep.get_end_time())
		self.etype = ep.get_epoch_type()
		self.size = ep.get_size()
		self.stime = ep.get_start_time()
		self.etime = ep.get_end_time()
		self.tid = ep.get_tid()
		
	def get_tuple(self):
		return (self.eid, self.etype, self.size, self.stime,
				self.etime, self.tid)

