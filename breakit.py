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
from os.path import expanduser
import glob

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
    self.LOG_DIR = os.path.abspath("./JOBS/LOGS")
    
    self.JOB_ID = {}
    self.MY_EXEC = sys.argv[0]
    self.MY_EXEC_SAVED = self.SAVE_DIR+"/"+os.path.basename(sys.argv[0])
    self.INITIAL_DATA_DIR = "."

    if os.path.exists(self.WORKSPACE_FILE):
      self.load_workspace()

    engine.__init__(self,self.APP_NAME,self.VERSION,self.LOG_DIR)

    self.run()
      
  #########################################################################
  # run_test()
  #########################################################################
  def run(self):
    #
    self.initialize_scheduler()
    self.env_init()
    if not(self.CONTINUE):
      self.prepare_computation()
      job = self.job_submit(1,5)



  #########################################################################
  # initialize job environment
  #########################################################################

  def env_init(self):

    self.log_debug('initialize environment ')

    if self.CONTINUE:
      return
    
    for d in [self.SAVE_DIR,self.LOG_DIR]:
      if not(os.path.exists(d)):
        os.makedirs(d)
        self.log_debug("creating directory "+d,1)
    
    for f in glob.glob("*.py"):
      self.log_debug("copying file %s into SAVE directory " % f,1)
      os.system("cp ./%s  %s" % (f,self.SAVE_DIR))

    # for f in self.FILES_TO_COPY:
    #   os.system("cp %s/%s %s/" % (self.INITIAL_DATA_DIR,f,self.JOB_DIR_0))

    self.log_info('environment initialized successfully')
    

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

        

  #########################################################################
  # submit a job_array
  #########################################################################

  def job_array_submit(self,job_name, job_file, array_first, array_last, dep=""):

    cmd = [self.SCHED_SUB]
    if self.NODES_FAILED:
      cmd = cmd + ["-x",self.NODES_FAILED]

    cmd = cmd + [self.SCHED_ARR,"%d-%d" % (array_first,array_last),job_file ]

    self.log_debug("submitting : "+" ".join(cmd))

    if not self.DRY_RUN:
      output = subprocess.check_output(cmd)
      if self.PBS:
        #print output.split("\n")
        job_id = output.split("\n")[0].split(".")[0]
        #print job_id
      else:
        for l in output.split("\n"):
          self.log_debug(l,1)
          #print l.split(" ")
          if "Submitted batch job" in l:
            job_id = l.split(" ")[-1]
      self.log_debug("job submitted : %s depends on %s" % (job_id,dep),1)
    else: 
      self.log_debug("should submit job %s" % job_name,1)
      self.log_debug(" with cmd = %s " % " ".join(cmd),1)
      job_id = "self.JOB_ID_%s" % job_name

    self.JOB_ID[job_name] =  job_id

    print 'submitting job %s Job # %s_%s-%s' % (job_name,job_id,array_first,array_last)

    return job_id

  #########################################################################
  # submit a job
  #########################################################################

  def job_submit(self,array_first,array_last):

    self.log_debug('submitting job array [%s,%s]...' % (array_first,array_last),1) 

    job_template = self.create_job_template()

    job_name = "%d-%d" % (array_first,array_last)
    # job_dir = "%s/%03d" %  (self.JOB_DIR,n)
    # if os.path.exists(job_dir) and not (n+1 in self.JOBS_TO_RELAUNCH.keys()):
    #   self.error_report("Something is going weird...\ndirectory %s allready exists..." % job_dir)

    dep =  ""
    # job_name_dependent = "%03d" % (n+1)
    # if job_name_dependent in self.JOB_ID.keys():
    #   dep = dep + ":" + self.JOB_ID[job_name_dependent]
    # if (dep) :
    #   job_content = job_template.replace("__SCHEDULER_DEPENDENCY__",\
    #                                        self.SCHED_TAG+" "+self.SCHED_DEP+dep)
    # else:
    #   job_content = job_template.replace("__SCHEDULER_DEPENDENCY__","")
    job_content = job_template.replace("__SCHEDULER_DEPENDENCY__","")
    job_content = job_content.replace("__JOB_NAME__",job_name)
    job_content = job_content.replace("__self.JOB_DIR__",self.JOB_DIR)

            
    job_file = '%s/%s.job' % (self.SAVE_DIR,job_name)
    f=open(job_file,'w')
    f.write(job_content)
    f.close()

    job_id=self.job_array_submit(job_name,job_file,array_first,array_last,dep)

    self.log_debug('submitted job array [%s,%s] successfully...' % (array_first,array_last),1) 



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

          finalize_cmd = "python -u ../../SAVE/%s.py --task=$task_id --finalize --log-dir=%s" % \
                         (self.APP_NAME,self.LOG_DIR)

          job = job + self.job_header_amend()
          job = job + "mkdir -p %s/$task_id\n\n" % self.JOB_DIR
          job = job + "cd %s/$task_id \n\n" % self.JOB_DIR
          job = job + "python -u ../../SAVE/%s.py --task=$task_id --continue --log-dir=%s" % \
              (self.APP_NAME,self.LOG_DIR)

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
  # job_header_amend
  #########################################################################
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
               \n\t\t[ --exclude_nodes=<nodes where not to run> ] \
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
      self.DRY_RUN = False
      self.TASK = -1

      self.SCRATCH = self.KILL = self.RESTART = self.CONTINUE = None
      self.NODES_FAILED = None      
      try:
          if " --help" in " "+" ".join(args) or " -h " in (" "+" ".join(args)+" ") :
            self.error_report("")

          opts, args = getopt.getopt(args, "h", 
                            ["help", "job=", "exclude_nodes=","dry",\
                             "restart", "scratch", "kill", "continue", \
                             "log-dir=","task=", \
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
        elif option in ("--continue"):
          self.CONTINUE = 1
          print "continuing..."
          sys.exit(0)
        elif option in ("--job"):
          self.JOB = argument
        elif option in ("--exclude_nodes"):
          self.NODES_FAILED = argument
        elif option in ("--fake"):
          self.FAKE = True
        elif option in ("--dry"):
           self.DRY_RUN = True
        elif option in ("--task"):
          self.TASK = argument
          

      if not(self.JOB):
        self.error_report(message='please set a job to launch')

      self.log_info('starting Task %s' % self.TASK)

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
    