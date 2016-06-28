#!/sw/xc40/python/2.7.9/cnl5.2_gnu4.9.3/bin/python
from breakit import *
import glob
from env import *
import math

class my_breakit(breakit):

  def __init__(self):
    breakit.__init__(self,app_name='my_app')



  def my_timing(self,dir,ellapsed_time,status):
    # default behavior
    return "%s/%s" % (ellapsed_time,status)

    # file to scan
    file_to_grep = dir+'/job.out'

    # the file does not exists
    if not(os.path.exists(file_to_grep)):
       return "!%s/%s" % (ellapsed_time,status)
        
    # or .... greping result in a file
    try:
      res = greps("Total Time",file_to_grep,-2)
      Total_time        = int(float(res[0])*100.)/100. 
      self.log_debug("Total Time: %s" % (Total_time))
      return "%s/%s" % (Total_time,status)
    except:
      self.dump_exception('user_defined_timing in dir %s' % dir)
      # file exists but no Total Time is printed
      return "NO_TIME/"+status

if __name__ == "__main__":
    my_breakit()
