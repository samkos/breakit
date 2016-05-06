import getopt, sys, os, socket, traceback

import logging
import logging.handlers
import warnings

import math
import time
import subprocess
import re
import copy
import shlex
import pickle
import getpass
import datetime
import string
import shutil
import getpass

from pysam import engine
from env import *

ERROR = -1


class break_it(engine):

  def __init__(self):


    self.WORKSPACE_FILE = "nodecheck.pickle"
    self.check_python_version()
    self.CLEAN = False
    self.ALL = True
    self.APP_NAME  = "breakit"
    self.VERSION = "0.1"
    
    self.JOB_DIR = os.path.abspath("./JOBS/RESULTS")
    self.SAVE_DIR = os.path.abspath("./JOBS/SAVE")
    self.MY_EXEC = sys.argv[0]
    self.MY_EXEC_SAVED = self.SAVE_DIR+"/"+os.path.basename(sys.argv[0])
    self.INITIAL_DATA_DIR = "."

    if os.path.exists(self.WORKSPACE_FILE):
      self.load_workspace()

    engine.__init__(self,self.APP_NAME,self.VERSION,"/scratch/%s/logs/" % getpass.getuser())

    self.run()
      
  #########################################################################
  # run_test()
  #########################################################################
  def run(self):
    #
    self.initialize_scheduler()
    self.prepare_computation()
    job = self.create_job_template()
    print job
      
  #########################################################################
  # initialize_scheduler
  #########################################################################
  def initialize_scheduler(self):
    global SCHED_TYPE

    if SCHED_TYPE=="pbs" or self.PBS:
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


  def job_header_amend(self):
    job = ""
    #job = job + "__SCHEDULER_DEPENDENCY__\n"
    if self.PBS:
      job = job + self.SCHED_TAG+" -o %s/__JOB_NAME__.out\n" % self.SAVE_DIR
      job = job + self.SCHED_TAG+" -e %s/__JOB_NAME__.err\n" % self.SAVE_DIR
      job = job + self.SCHED_TAG+" -N __JOB_NAME__\n"
      job = job + """\ntask_id=`printf "%03d" "$PBS_ARRAYID"`\n"""
    else:
      job = job + self.SCHED_TAG+" -o %s/__JOB_NAME__.out-%%a\n" % self.SAVE_DIR
      job = job + self.SCHED_TAG+" -e %s/__JOB_NAME__.err-%%a\n" % self.SAVE_DIR
      job = job + self.SCHED_TAG+" --job-name=__JOB_NAME__\n"
      job = job + """\ntask_id=`printf "%03d" "$SLURM_ARRAY_TASK_ID"`\n"""
    return job


  #########################################################################
  # starting the process
  #########################################################################

  def prepare_computation(self):

    self.log.info("")
    self.log.info("="*60)
    self.log.info("Setting environment...")

    self.log_debug("======== STARTING BREAKIT ==========")
    self.log_debug("now : "+getDate())

    self.check_python_version()

    if (os.path.exists(self.JOB_DIR)):
      if not(self.SCRATCH or self.RESTART or self.KILL): 
        self.error_report(error_detail="RESULT_DIR_EXISTING",\
                message = \
                "Result directory %s already exists from a previous run "%self.JOB_DIR + \
                "\nplease rename it, add --restart or --scratch to your command line" +\
                "\n\t\t" + " ".join(sys.argv)+" --restart" + \
                "\n\t\t" + " ".join(sys.argv)+" --scratch")
      if  self.SCRATCH: 
        self.log_info("restart from scratch")
        self.log_info("killing previous jobs...")
        self.kill_jobs()
        shutil.rmtree(self.JOB_DIR)
        shutil.rmtree(self.SAVE_DIR)
        for self.log_file in glob.glob("%s/level*.log" % self.LOG_DIR):
          os.unlink(self.log_file)

    for d in [self.JOB_DIR,self.SAVE_DIR,self.LOG_DIR]:
      if not(os.path.exists(d)):
        os.makedirs(d)
        self.log_debug("creating directory %s" % d,1)

        

  #######################################################################
  # create a job template
  #########################################################################

  def create_job_template(self):

    self.log_debug('creating job template ',1)
    job = ""
    
    job_name = self.JOB

    if not(os.path.exists(job_name)):
        self.error_report("Template job file %s missing..." % job_name)
    job_header = greps(self.SCHED_TAG,job_name)

    if not(job_header):
        self.error_report("Template job file %s does not contain any %s line..." % (job_name,self.SCHED_TAG))

    nb_header_lines = len(job_header)
    for l in open(job_name,"r").readlines():

      job = job + l

      if l.find(self.SCHED_TAG)>=0:
        nb_header_lines = nb_header_lines - 1
        if (nb_header_lines) == 0:

          finalize_cmd = "python -u ../../../SAVE/%s.py --finalize --self.log-dir=../../../LOGS" %           self.APP_NAME

          job = job + self.job_header_amend()
          job = job + "mkdir -p "+self.JOB_DIR__+"/$task_id\n\n"
          job = job + "cd %s/$task_id \n\n" % __self.JOB_DIR__
          job = job + "python -u ../../../SAVE/%s.py --feeds --self.log-dir=../../../LOGS" % \
              self.APP_NAME

          if self.FAKE:
            job = job + " --fake"
            finalize_cmd = finalize_cmd + " --fake"
          if self.PBS:
            job = job + " --pbs"
            finalize_cmd = finalize_cmd + " --pbs"
          if not(self.DEBUG==False):
            job = job + " --debug-level=%d " % self.DEBUG
            finalize_cmd = finalize_cmd + " --debug-level=%d " % self.DEBUG
          if not(self.INFO==False):
            job = job + " --info-level=%d " % self.INFO
            finalize_cmd = finalize_cmd + " --info-level=%d " % self.INFO
          job = job + "\n"
          job = job + """
              if [ $? -ne 0 ] ; then
                  echo "[ERROR] FAILED in Job: stopping everything..."
                  %s 
                  exit 1
              else """ % finalize_cmd
          job = job + "\n# START OF ORIGINAL USER SCRIPT  -------------------\n"

    job = job + "\n# END OF ORIGINAL USER SCRIPT  -------------------\n"
    job = job + finalize_cmd 
    job = job+ """
              fi """
    job = job + "\n"

    self.log_debug('job template created :',1)
    self.log_debug(job,3)

    return job

  #########################################################################
  # check_env
  #########################################################################
  def check_env(self):
    # chekcking python version
    try:
      subprocess.check_output(["ls"])
    except:
      print ("ERROR : Please use a more recent version of Python > 2.7.4")
      sys.exit(1)


  #########################################################################
  # save_workspace
  #########################################################################

  def save_workspace(self):
      
      self.log_debug("saving variables to file "+workspace_file)
      workspace_file = self.WORKSPACE_FILE
      f_workspace = open(workspace_file+".new", "wb" )
      # Save your data here
      #pickle.dump(self.JOB_ID    ,f_workspace)
      f_workspace.close()
      if os.path.exists(workspace_file):
        os.rename(workspace_file,workspace_file+".old")
      os.rename(workspace_file+".new",workspace_file)
      

  #########################################################################
  # load_workspace
  #########################################################################

  def load_workspace(self):

      self.log_debug("loading variables from file "+workspace_file)

      f_workspace = open( self.WORKSPACE_FILE, "rb" )
      # retrieve data here
      #self.JOB_ID    = pickle.load(f_workspace)
      f_workspace.close()

      for job_dir in self.JOB_ID.keys():
        job_id  = self.JOB_ID[job_dir]
        self.JOB_DIR[job_id] = job_dir


  #########################################################################
  # usage ...
  #########################################################################

      

  def usage(self,message = None, error_detail = "",exit=True):
      """ helping message"""
      if message:
          print "\n\tError %s:\n\t\t%s\n" % (error_detail,message)
          print "\tappend -h for the list of available options..."
      else:
        print "\n  usage: \n \t python  %s.py \
               \n\t\t[ --help ] \
               \n\t\t[ --job=<job_script file> ] \
               \n\t\t[ --restart | --scratch | --kill ]\
               \n\t\t[ --debug ] [ --debug-level=[0|1|2]  ] [ --fake ]  \
             \n"   % self.APP_NAME

      if exit:
        sys.exit(1)


  #########################################################################
  # parsing command line
  #########################################################################

  def parse(self,args=sys.argv[1:]):
      """ parse the command line and set global _flags according to it """



      self.FAKE = None
      self.JOB = None
      self.PBS = None
      self.SCRATCH = self.KILL = self.RESTART = None
      
      try:
          if " --help" in " "+" ".join(args) or " -h " in (" "+" ".join(args)+" ") :
            self.error_report("")

          opts, args = getopt.getopt(args, "h", 
                            ["help", "job=", \
                               "restart", "scratch", "kill", \
                               "debug", "debug-level=",  \
                               "fake" ])    
      except getopt.GetoptError, err:
          # print help information and exit:
          self.usage(err)

      for option, argument in opts:
        if option in ("--log-dir"):
          self.LOG_DIR = expanduser(argument)

      # initialize Logs
      self.initialize_log_files()

      # first scan opf option to get prioritary one first
      # those who sets the state of the process
      # especially those only setting flags are expected here
      for option, argument in opts:
        if option in ("-h", "--help"):
          self.usage("")
        elif option in ("--info"):
          self.INFO = 1
        elif option in ("--info-level"):
          self.INFO = int(argument)
        elif option in ("--debug"):
          self.DEBUG = 1
        elif option in ("--debug-level"):
          self.DEBUG = int(argument)
        elif option in ("--restart"):
          self.RESTART = 1
        elif option in ("--scratch"):
          self.SCRATCH = 1
        elif option in ("--kill"):
          self.KILL = 1
        elif option in ("--job"):
          self.JOB = argument
        elif option in ("--fake"):
          self.DEBUG = True
          self.FAKE = True
        elif option in ("--clean"):
           self.CLEAN = True

      if not(self.JOB):
        self.error_report(message='please set a job to launch')

      return True

  #########################################################################
  # os.system wrapped to enable Trace if needed
  #########################################################################

  def wrapped_system(self,cmd,comment="No comment",fake=False):

    self.log_debug("\tcurrently executing /%s/ :\n\t\t%s" % (comment,cmd))

    if not(fake) and not(self.FAKE):
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
    return ouput

  
if __name__ == "__main__":
    K = break_it()
    
