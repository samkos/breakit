#!/sw/xc40/python/2.7.11/sles11.3_gnu5.1.0/bin/python
from breakit import *
import glob

class my_breakit(breakit):

  def __init__(self):
    breakit.__init__(self)



  def my_timing(self,dir,ellapsed_time,status):

    if self.DEBUG:
      print dir,status
    for filename in glob.glob(dir+'/*/results*log'):
      try:
        f = open(filename).readlines()
        if len(f)<2:
          if status=="COMPLETED":
            return "?%s/%s" %(ellapsed_time,status[:2])
          else:
            return "NOLOG/"+status[:2] 
        l = f[-2][:-1]
        l = l.replace("Total time :","")
        if self.DEBUG:
          print l,filename
        return int(float(l))
      except:
        self.dump_exception('user_defined_timing')
        job_out = "___".join(open(dir+'/job.out').readlines())
        if job_out.find("CANCELLED")>-1:
          return "CANCELLED/"+status[:2]
        if self.DEBUG:
          print "pb on ",filename
        return "PB/"+status[:2]
    return "!%s" % ellapsed_time

if __name__ == "__main__":
    my_breakit()
      
