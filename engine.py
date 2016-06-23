#!/usr/bin/python

import glob,os,re
import getopt, traceback
import time, datetime, sys, threading
import subprocess
import logging
import logging.handlers
import warnings
import shutil
from os.path import expanduser
from env     import *
import fcntl
import getpass

import argparse

LOCK_EX = fcntl.LOCK_EX
LOCK_SH = fcntl.LOCK_SH
LOCK_NB = fcntl.LOCK_NB

ENGINE_VERSION = '0.14'

class LockException(Exception):
    # Error codes:
    LOCK_FAILED = 1


  
class engine:

  def __init__(self,app_name="app",app_version="?",app_dir_log=False,engine_version_required=ENGINE_VERSION):
    #########################################################################
    # set initial global variables
    #########################################################################


    self.SCRIPT_NAME = os.path.basename(__file__)

    self.APPLICATION_NAME=app_name
    self.APPLICATION_VERSION=app_version
    self.ENGINE_VERSION=ENGINE_VERSION

    self.LOG_PREFIX=""
    self.LOG_DIR = app_dir_log
   
    self.WORKSPACE_FILE = ".%s.pickle" % app_name
    self.JOB_DIR = os.path.abspath("./.%s/RESULTS" % app_name)
    self.SAVE_DIR = os.path.abspath("./.%s/SAVE" % app_name)
    self.LOG_DIR = os.path.abspath("./.%s/LOGS" % app_name)


    # saving scheduling option in the object
    
    self.MAIL_COMMAND = MAIL_COMMAND
    self.SUBMIT_COMMAND = SUBMIT_COMMAND
    self.SCHED_TYPE = SCHED_TYPE
    self.DEFAULT_QUEUE = DEFAULT_QUEUE
    

    # initilization
    
    self.welcome_message()

    # checking
    self.check_python_version()
    self.check_engine_version(engine_version_required)

    # parse command line to eventually overload some default values
    self.parser = argparse.ArgumentParser()
    self.initialize_parser()
    self.args = self.parser.parse_args()

    
    # initialize logs
    self.initialize_log_files()

    # initialize scheduler
    self.initialize_scheduler()


    self.env_init()
    
  def start(self):

    self.log_debug('[Engine:start] entering')
    engine.run(self)


  #########################################################################
  # check for tne option on the command line
  #########################################################################


  def initialize_parser(self):
    self.parser.add_argument("-i","--info",  action="count", default=0, help=argparse.SUPPRESS)
    self.parser.add_argument("-d","--debug", action="count", default=0, help=argparse.SUPPRESS)

    # self.parser.add_argument("--kill", action="store_true", help="Killing all processes")
    # self.parser.add_argument("--scratch", action="store_true", help="Restarting the whole process from scratch cleaning everything")
    # self.parser.add_argument("--restart", action="store_true", help="Restarting the process from where it stopped")

    self.parser.add_argument("--kill", action="store_true", help=argparse.SUPPRESS)
    self.parser.add_argument("--scratch", action="store_true", help=argparse.SUPPRESS)
    self.parser.add_argument("--restart", action="store_true", help=argparse.SUPPRESS)

    self.parser.add_argument("--log-dir", type=str, help=argparse.SUPPRESS)
    self.parser.add_argument("--mail", type=str, help=argparse.SUPPRESS)
    self.parser.add_argument("--fake", action="store_true", help=argparse.SUPPRESS)
    self.parser.add_argument("--dry", action="store_true", help=argparse.SUPPRESS)
    self.parser.add_argument("--pbs", action="store_true", help=argparse.SUPPRESS)
    self.parser.add_argument("-x","--exclude-nodes", type=str , help=argparse.SUPPRESS)
    self.parser.add_argument("--reservation", type=str , help='SLURM reservation')
    self.parser.add_argument("-p","--partition", type=str , help='SLURM partition')
        

  #########################################################################
  # main router
  #########################################################################

  def run(self):

    if self.args.info>0:
        self.log.setLevel(logging.INFO)
    if self.args.debug>0:
        self.log.setLevel(logging.DEBUG)

    if self.args.scratch:
        self.log_info("restart from scratch")
        self.log_info("killing previous jobs...")
        self.kill_jobs()
        self.log_info("deleting JOBS and SAVE DIR")
        shutil.rmtree(self.JOB_DIR)
        shutil.rmtree(self.SAVE_DIR)
                                    
    if self.args.kill:
        self.kill_jobs()
        sys.exit(0)
        
  #########################################################################
  # set self.log file
  #########################################################################


  def initialize_log_files(self):
      
    if not(self.LOG_DIR):
      self.LOG_DIR= "/scratch/%s/logs/.%s" % (getpass.getuser(),self.APPLICATION_NAME)


    if self.args.log_dir:
        self.LOG_DIR = self.args.log_dir

    self.LOG_DIR = expanduser("%s" % self.LOG_DIR)
    
    for d in [ self.LOG_DIR]:
      if not(os.path.exists(d)):
        os.makedirs(d)
        

    self.log = logging.getLogger('%s.log' % self.APPLICATION_NAME)
    self.log.propagate = None
    self.log.setLevel(logging.ERROR)
    self.log.setLevel(logging.INFO)
    console_formatter=logging.Formatter(fmt='[%(levelname)-5.5s] %(message)s')
    formatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")

    self.log_file_name = "%s/" % self.LOG_DIR+'%s.log' % self.APPLICATION_NAME
    open(self.log_file_name, "a").close()
    handler = logging.handlers.RotatingFileHandler(
         self.log_file_name, maxBytes = 20000000,  backupCount = 5)
    handler.setFormatter(formatter)
    self.log.addHandler(handler)


    consoleHandler = logging.StreamHandler(stream=sys.stdout)
    consoleHandler.setFormatter(console_formatter)
    self.log.addHandler(consoleHandler)



  def log_debug(self,msg,level=0,dump_exception=0):
    if level<=self.args.debug:
      if len(self.LOG_PREFIX):
          msg = "%s:%s" % (self.LOG_PREFIX,msg)
      self.log.debug(msg)
      if (dump_exception):
        self.dump_exception()
      #self.log.debug("%d:%d:%s"%(self.args.debug,level,msg))

  def log_info(self,msg,level=0,dump_exception=0):
    if level<=self.args.info:
      if len(self.LOG_PREFIX):
          msg = "%s:%s" % (self.LOG_PREFIX,msg)
      self.log.info(msg)
      if (dump_exception):
        self.dump_exception()
      #self.log.debug("%d:%d:%s"%(self.args.debug,level,msg))

  def dump_exception(self,where=None):
    if where:
      print '\n#######!!!!!!!!!!######## Exception occured at ',where,'############'
    exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
    traceback.print_exception(exceptionType,exceptionValue, exceptionTraceback,\
                                file=sys.stdout)
    print '#'*80


  def set_log_prefix(self,prefix):
      self.LOG_PREFIX = prefix
    
  #########################################################################
  # initialize job environment
  #########################################################################

  def env_init(self):

    self.log_debug('initialize environment ',1)

    for d in [self.SAVE_DIR,self.LOG_DIR]:
      if not(os.path.exists(d)):
        os.makedirs(d)
        self.log_debug("creating directory "+d,1)
    
    for f in glob.glob("*.py"):
      self.log_debug("copying file %s into SAVE directory " % f,1)
      os.system("cp ./%s  %s" % (f,self.SAVE_DIR))

    # for f in self.FILES_TO_COPY:
    #   os.system("cp %s/%s %s/" % (self.INITIAL_DATA_DIR,f,self.JOB_DIR_0))

    self.log_info('environment initialized successfully',1)

  #########################################################################
  # initialize_scheduler
  #########################################################################
  def initialize_scheduler(self):
    global SCHED_TYPE

    if SCHED_TYPE=="pbs" or self.args.pbs:
      self.PBS = True
      self.SCHED_TAG = "#PBS"
      self.SCHED_SUB = "qsub"
      self.SCHED_ARR = "-t"
      self.SCHED_KIL = "qdel"
      self.SCHED_Q = "qstat"
      self.SCHED_DEP = "-W depend=afteranyarray"
    else:
      self.SCHED_TAG = "#SBATCH"
      self.SCHED_SUB = "sbatch"
      self.SCHED_ARR = "-a"
      self.SCHED_KIL = "scancel"
      self.SCHED_Q = "squeue"
      self.SCHED_DEP = "--dependency=afterany"
      self.SCHED_DEP_OK = "--dependency=afterok"
      self.SCHED_DEP_NOK = "--dependency=afternotok"


  #########################################################################
  # welcome message
  #########################################################################

  def welcome_message(self):
      """ welcome message"""
      
      print
      print("          ########################################")
      print("          #                                      #")
      print("          #   Welcome to %11s version %3s!#" % (self.APPLICATION_NAME, self.APPLICATION_VERSION))
      print("          #    (using ENGINE Framework %3s)     #" % self.ENGINE_VERSION)
      print("          #                                      #")
      print("          ########################################")
      print("       ")


      print   ("\trunning on %s (%s) " %(MY_MACHINE_FULL_NAME,MY_MACHINE))
      print   ("\t\tpython " + " ".join(sys.argv))
      self.MY_MACHINE = MY_MACHINE

  #########################################################################
  # dumping error report ...
  #########################################################################
      
   
  def error_report(self,message = None, error_detail = "", exit=True,exception=False):
      """ helping message"""
      if message:
        message = str(message)+"\n"
        if len(error_detail):
          print "[ERROR] Error %s : " % error_detail
        for m in message.split("\n"):
          try:
            #print "[ERROR]  %s : " % m
            self.log.error(m)
          except:
            print "[ERROR Pb processing] %s" % m
        print "[ERROR] type python %s -h for the list of available options..." % \
          self.APPLICATION_NAME
      else:
        try:
          self.usage(exit=False)
        except:
          self.dump_exception()
          print "\n  usage: \n \t python %s \
               \n\t\t[ --help ] \
               \n\t\t[ --info  ] [ --info-level=[0|1|2] ]  \
               \n\t\t[ --debug ] [ --debug-level=[0|1|2] ]  \
             \n"  % self.APPLICATION_NAME

      if not(exception==False):
        self.dump_exception()
          
      if exit:
        sys.exit(1)


  #########################################################################
  # checking methods
  #########################################################################
      

  def check_python_version(self):
    try:
      subprocess.check_output(["ls"])
    except:
      self.error_report("Please use a more recent version of Python > 2.7.4")



  def check_engine_version(self,version):
    current = int(("%s" % self.ENGINE_VERSION).split('.')[1])
    asked   = int(("%s" % version).split('.')[1])
    if (asked>current):
        self.error_report("Current Engine version is %s while requiring %s, please fix it!" % (current,asked))


  #########################################################################
  # locking methods
  #########################################################################

  def lock(self, file, flags):
      try:
        fcntl.flock(file.fileno(), flags)
      except IOError, exc_value:
        #  IOError: [Errno 11] Resource temporarily unavailable
        if exc_value[0] == 11:
          raise LockException(LockException.LOCK_FAILED, exc_value[1])
        else:
          raise
    
  def unlock(self,file):
    fcntl.flock(file.fileno(), fcntl.LOCK_UN)


  def take_lock(self,filename,write_flag="a+"):
    install_lock = open(filename,write_flag)
    self.lock(install_lock, LOCK_EX)
    return install_lock

  def release_lock(self,install_lock):
    install_lock.close()
    
  #########################################################################
  # os.system wrapped to enable Trace if needed
  #########################################################################

  def wrapped_system(self,cmd,comment="No comment",fake=False):

    self.log_debug("\tcurrently executing /%s/ :\n\t\t%s" % (comment,cmd))

    output='FAKE EXECUTION'
    if not(fake) and not(self.args.fake):
      #os.system(cmd)
      #subprocess.call(cmd,shell=True,stderr=subprocess.STDOUT)
      proc = subprocess.Popen(cmd, shell=True, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
      output = ""
      while (True):
          # Read line from stdout, break if EOF reached, append line to output
          line = proc.stdout.readline()
          #line = line.decode()
          if (line == ""): break
          output += line
      if len(output):
        self.log_debug("output=+"+output,3)
    return output


  #########################################################################
  # send mail back to the user
  #########################################################################

  def init_mail(self):

    if not(self.args.mail):
      return False
    
    # sendmail only works from a node on shaheen 2 via an ssh connection to cdl via gateway...

    if not(self.args.mail_TO):
      self.args.mail_TO = getpass.getuser()

  def send_mail(self,title,msg,to=None):

    if not(self.args.mail):
      return False

    if not(self.MAIL_COMMAND):
      self.error_report("No mail command available on this machine")

    # sendmail only works from a node on shaheen 2 via an ssh connection to cdl via gateway...

    mail_file = os.path.abspath("./mail.txt")

    f = open(mail_file,'w')
    f.write(msg)
    f.close()

    if not(to):
        to = self.args.mail_TO
        
    cmd = (self.MAIL_COMMAND+"2> /dev/null") % (title, to, mail_file)
    self.log_debug("self.args.mail cmd : "+cmd,2)
    os.system(cmd)



  #########################################################################
  # create template (matrix and job)
  #########################################################################
  def create_template(self,l):

    print

    for filename_content in l.split("__SEP1__"):
      filename,content = filename_content.split("__SEP2__")
      if os.path.exists(filename):
        print "\t file %s already exists... skipping it!" % filename
      else:
        dirname = os.path.dirname(filename)
        if not(os.path.exists(dirname)):
          self.wrapped_system("mkdir -p %s" % dirname,comment="creating dir %s" % dirname)
        executable = False
        if filename[-1]=="*":
          filename = filename[:-1]
          executable = True
        if os.path.exists(filename):
          print "\tfile %s already exists... skipping it!" % filename
        else:
          f = open(filename,"w")
          f.write(content)
          f.close()
          if executable:
            self.wrapped_system("chmod +x %s" % filename)
          self.log_info("file %s created " % filename)

    sys.exit(0)


if __name__ == "__main__":
  D = application("my_app")
