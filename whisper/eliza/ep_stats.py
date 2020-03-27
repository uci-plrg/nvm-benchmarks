from epoch import epoch
class ep_stats:

	''' Epoch stat collector '''
	def __init__(self):

		# assert fname is not None
		# self.dbname = "db/" + dbname + ".db"
		# self.fname = "stat/" + dbname + ".stat"
		# self.fname = fname
		self.buf = []
		self.buflen = 0
		self.tno = 0
		self.tnull = 0
	
	def get_tuple(self, ep):

		# Obsolete, use get_str()
		# Or update to use

		etype = ep.get_epoch_type()
		if ep.is_null() is True:
			self.tnull += 1

		cwsize = ep.get_cwrt_set_sz()
		wsize  = float(cwsize) + ep.get_nwrt_set_sz()
		esize  = wsize + float(ep.get_rd_set_sz())
		stime = ep.get_start_time()
		etime = ep.get_end_time()
		tid = ep.get_tid()
		r1 = ep.get_page_span()
		r2 = ep.get_dist_from_mrd()
		r3 = ep.get_dist_from_lrd()
		if cwsize > 0:
			r4 = round(float(float(ep.get_dirty_nbytes()) / float(cwsize*epoch.CSIZE)),2)
		else:
			r4 = 0.0
		assert 0.0 <= r4 and r4 <= 1.0
		r4 = round((r4 * 100.0), 2)
		# r4 = ep.get_dirty_nbytes()
		'''
		t =  ((0) etype, (1) esize, (2) wsize, (3) cwsize,
		        (4) stime, (5) etime, (6) r1, (7) r2, (8) r3 ,(9) r4, 
		        (10) tid)
		'''
		t =  (etype, esize, wsize, cwsize,
		        stime, etime, r1, r2, r3 ,r4, tid)
		
		return t
		
	def get_str(self, ep):

		etype = ep.get_epoch_type()
		if ep.is_null() is True:
			self.tnull += 1
			
		cwsize = ep.get_cwrt_set_sz()
		wsize  = float(cwsize) + ep.get_nwrt_set_sz()
		esize  = wsize + float(ep.get_rd_set_sz())
		stime = ep.get_start_time()
		etime = ep.get_end_time()
		tid = ep.get_tid()
		r1 = ep.get_page_span()
		r2 = ep.get_dist_from_mrd()
		r3 = ep.get_dist_from_lrd()
		if cwsize > 0:
			r4 = round(float(float(ep.get_dirty_nbytes()) / float(cwsize*epoch.CSIZE)),2)
		else:
			r4 = 0.0
		assert 0.0 <= r4 and r4 <= 1.0
		r4 = round((r4 * 100.0), 2)
		
		return  str(etype) + ',' + str(esize) + ','  + str(wsize) + ',' \
				+ str(stime) + ',' + str(etime) + ',' \
				+ str(r1) + ',' + str(r2) + ',' + str(r3) + ',' + str(r4)
				
		''' r4 is the fraction of dirty bytes in an epoch '''


		#return  str(etype) + ',' + str(esize) + ','  + str(wsize) + ','  \
		#		+ str(cwsize) + ','  + str(r1) + ',' + str(r2) + ',' \
		#		+ str(etime-stime)
	
	def get_stime_from_str(self, epstr):
		assert epstr is not None
		return float(epstr.split(',')[3])

	def get_etime_from_str(self, epstr):
		assert epstr is not None
		return float(epstr.split(',')[4])
		
