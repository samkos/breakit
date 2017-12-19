#
# Copyright (c) 2016 Contributors as noted in the AUTHORS file
#
#  Written by Samuel Kortas <samuel.kortas (at) kaust.edu.sa>,
#

# This file is part of breakit.

#  breakit is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.

#  breakit is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.

#  You should have received a copy of the GNU Lesser General Public License
#  along with breakit.  If not, see <http://www.gnu.org/licenses/>.


import getopt
import sys
import os

import logging
import logging.handlers

import time
import subprocess
import re
import pickle
import getpass
import shutil
from os.path import expanduser
import glob

from engine import engine
from env import greps,getDate
from ClusterShell.NodeSet import RangeSet

ERROR = -1


class breakit(engine):

  def __init__(self, engine_version=0.14, app_name='breakit'):

    self.check_python_version()
    self.CLEAN = False
    self.ALL = True
    self.APP_NAME = app_name
    self.VERSION = "0.3"
    self.ENGINE_VERSION_REQUIRED = engine_version

    self.JOB_DIR = os.path.abspath("./.breakit/RESULTS")
    self.SAVE_DIR = os.path.abspath("./.breakit/SAVE")
    self.LOG_DIR = os.path.abspath("./.breakit/LOGS")
    self.BREAKIT_DIR = os.getenv('BREAKIT_PATH')

    self.JOB_ID = {}
    self.JOB_ID_FILE = "%s/job_ids.pickle" % self.LOG_DIR
    self.LOCK_FILE = "%s/lock" % self.LOG_DIR
    self.STATS_ID_FILE = "%s/stats_ids.pickle" % self.LOG_DIR
    self.WORKSPACE_FILE = "%s/workspace.pickle" % self.SAVE_DIR

    self.MY_EXEC = sys.argv[0]
    self.MY_EXEC_SAVED = self.SAVE_DIR + "/" + os.path.basename(sys.argv[0])
    self.INITIAL_DATA_DIR = "."

    engine.__init__(self, self.APP_NAME, self.VERSION, self.LOG_DIR, self.ENGINE_VERSION_REQUIRED)
    if os.path.exists(self.WORKSPACE_FILE):
      self.load_workspace()

    self.env_init()
    self.run()

  #########################################################################
  # run_test()
  #########################################################################
  def run(self):
    #
    if not(self.CONTINUE or self.CONTINUEX):
      self.prepare_computation()
      self.log_debug("Saving Job Ids...", 1)
      pickle.dump(self.JOB_ID, open(self.JOB_ID_FILE, "wb"))
    else:
      self.manage_jobs()

  #########################################################################
  # manage_jobs()
  #########################################################################
  def manage_jobs(self):
    #
    if not(self.ARRAY[self.MY_ARRAY_CURRENT_FIRST - 1] == self.MY_TASK):
      self.log_debug(('not in charge to spawn more jobs: ' +\
                      'MY_ARRAY_CURRENT_FIRST=%s not equals MY_TASK=%s ') % \
                     (self.ARRAY[self.MY_ARRAY_CURRENT_FIRST - 1], self.MY_TASK))
      return

    self.log_info("my task is in charge of adding work...")

    range_first = self.MY_ARRAY_CURRENT_FIRST + self.CHUNK

    self.log_debug("range_first=%s, self.TO=%s, len(self.ARRAY)=%s" %
                   (range_first, self.TO, len(self.ARRAY)))

    if (range_first <= self.TO):
      self.log_info('can still submit... (%d-%d) dependent on %s' % \
                    (range_first, min(range_first + self.CHUNK/4-1, self.TO),\
                     self.MY_JOB))
      if self.CONTINUE:
        self.job_array_submit("job.template", self.JOB_FILE_PATH, range_first,
                              range_first + self.CHUNK/4-1, self.MY_JOB)
        pickle.dump(self.JOB_ID, open(self.JOB_ID_FILE, "wb"))

  #########################################################################
  # manage_jobs()
  #########################################################################
  def check_jobs(self):
    #
    print "continuing... taking lock"
    lock_file = self.take_lock(self.LOCK_FILE)
    print "got lock!"

    jobs = self.get_current_jobs_status()
    print jobs
    max = 0
    nb_jobs = 0
    for j in jobs.keys():
      status = jobs[j]
      if status == 'RUNNING' or status == 'PENDING':
        cmd = "squeue -r -u %s | grep %s " % (getpass.getuser(), j)
        qualified_jobs = self.wrapped_system(cmd)
        print qualified_jobs
        for q in qualified_jobs.split('\n'):
          q = re.sub(r"\s+", " ", q)
          q = re.sub(r"^\s+", "", q)
          q = re.sub(r"\s.*$", "", q)
          print "/%s/" % q
          if len(q):
            job_nb = int(q.split("_")[1])
            print job_nb
            nb_jobs = nb_jobs + 1
            if job_nb > max:
              max = job_nb
        print nb_jobs, 'are still queued for job ', j, 'and max index is ', max

    print nb_jobs, 'are still queued and max index is ', max, 'out of ', self.TO, 'jobs'

    if max > 0 and max < self.TO:
      print 'can still submit...'
      if self.CONTINUE:
        self.job_array_submit("xxx", self.JOB_FILE_PATH, max + 1, self.TO)
        pickle.dump(self.JOB_ID, open(self.JOB_ID_FILE, "wb"))

    time.sleep(2)
    self.release_lock(lock_file)
    print 'lock released'

  #########################################################################
  # initialize job environment
  #########################################################################

  def env_init(self):

    self.log_debug('initialize environment ', 1)

    self.initialize_scheduler()

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

    self.log_info('environment initialized successfully',1)

  #########################################################################
  # initialize_scheduler
  #########################################################################
  def initialize_scheduler(self):
    global SCHED_TYPE

    if SCHED_TYPE == "pbs" or self.PBS:
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
  # starting the process
  #########################################################################

  def prepare_computation(self):

    self.log.info("")
    self.log.info("="*60)
    self.log.info("preparing computation...")

    self.log_debug("======== STARTING BREAKIT ==========")
    self.log_debug("now : " + getDate())

    if (os.path.exists(self.JOB_DIR)):
      if not(self.SCRATCH or self.RESTART or self.KILL):
        self.error_report(error_detail="RESULT_DIR_EXISTING",\
                          message="Result directory %s already exists from a previous run " % \
                          self.JOB_DIR +\
                          "\nplease rename it, add --restart or --scratch to your command line" +\
                          "\n\t\t" + " ".join(sys.argv)+" --restart" + \
                          "\n\t\t" + " ".join(sys.argv)+" --scratch")
      if self.SCRATCH:
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
  # get current job status
  #########################################################################
  def get_current_jobs_status(self):

    self.log_debug("getting  current status of all jobs",1)

    self.initialize_scheduler()

    existing_jobs = {}
    completed_jobs = {}

    if not(os.path.exists(self.JOB_ID_FILE)):
      self.log_debug("No Job information available... Cannot kill anything...",1)
      return existing_jobs

    self.JOB_ID = pickle.load(open(self.JOB_ID_FILE, "rb"))

    if self.DRY_RUN:
      return existing_jobs

    my_username = getpass.getuser()
    my_jobs = []
    for k in self.JOB_ID.keys():
      my_jobs = my_jobs + [self.JOB_ID[k]]
    self.log_debug("checking on exiting jobs for user %s : > %s < " %\
                   (my_username," ".join(my_jobs)),2)

    cmd = [self.SCHED_Q,"-l","-u",my_username]

    output = subprocess.check_output(cmd)
    for c in output.split("\n"):
      self.log_debug(c,2)
      if c.find(my_username) > -1:
        c = re.sub(r'^\s+', "",c)
        c = re.sub(r'\s+', " ",c)
        f = c.split(" ")
        if self.PBS:
          fj = f[0].split(".")
        else:
          fj = f[0].split("_")
        job_id = fj[0]
        if len(fj) > 1:
          job_range = fj[1]
        else:
          job_range = ""
        if self.PBS:
          job_status = f[-3]
        else:
          job_status = f[4]
        if job_id in my_jobs:
          existing_jobs[job_id] = job_status
          self.log_debug("job %s %s %s " % (job_id,job_status,job_range),1)

    for job_id in self.JOB_ID.keys():
      if not(job_id in existing_jobs.keys()):
        completed_jobs[job_id] = 'COMPLETED'
        self.log_debug("job %s %s %s" % (job_id,"COMPLETED","??"))

    return existing_jobs

  #########################################################################
  # kill jobs... after asking confirmation
  #########################################################################
  def kill_jobs(self,force=False):

    self.log_debug("killing all jobs",1)

    existing_jobs = self.get_current_jobs_status()

    if len(existing_jobs):
      if force:
        self.log_debug("killed forced... not asking confirmation...")
      else:
        input_var = raw_input("Do you really want to kill all running jobs ? (yes/no) ")

        if not(input_var == "yes"):
          self.error_report("No clear confirmation... giving up!")

      for j in existing_jobs.keys():
        self.log_debug("killing job %s -> ?? " % (j),1)
        self.log_debug("   with command : /%s %s/ " % (self.SCHED_KIL,j),2)
        output = subprocess.Popen([self.SCHED_KIL,j],\
                                  stdout=subprocess.PIPE).communicate()[0].split("\n")
        self.log_debug("   with output : /%s/  " % (output),2)

      self.log.info("All jobs killed with success")
      self.log.info("="*60)
      print("All jobs killed with success")
      # self.send_mail("All job have been killed with success")
    else:
      self.log.info("No job still exists for this study...")

  #########################################################################
  # submit a job_array
  #########################################################################

  def job_array_submit(self,job_name, job_file, range_first, range_last, dep=""):

    range_last = min(range_last,self.TO)

    if range_last < range_first:
      self.log_debug('no more job need to be submitted')
      sys.exit(0)

    f = open(job_file,'r')
    job_content = "".join(f.readlines())
    f.close()

    job_content = job_content.replace("__ARRAY_CURRENT_FIRST__","%s" % range_first)

    job_file = '%s/job_template.%d-%d.job' % (self.SAVE_DIR,range_first,range_last)
    f = open(job_file,'w')
    f.write(job_content)
    f.close()

    cmd = [self.SCHED_SUB]
    if (dep):
      cmd = cmd + [self.SCHED_DEP+":%s" % dep]

    if self.NODES_FAILED:
      cmd = cmd + ["-x",self.NODES_FAILED]

    cmd = cmd + [self.SCHED_ARR,"%s" % self.ARRAY[(range_first-1):range_last],job_file]

    self.log_debug("submitting : "+" ".join(cmd))

    if not self.DRY_RUN:
      try:
        output = subprocess.check_output(cmd)
      except:
        self.error_report("Something went wrong during the submission of the job",\
                          exit=True,exception=self.DEBUG)

      if self.PBS:
        # print output.split("\n")
        job_id = output.split("\n")[0].split(".")[0]
        # print job_id
      else:
        for l in output.split("\n"):
          self.log_debug(l,1)
          # print l.split(" ")
          if "Submitted batch job" in l:
            job_id = l.split(" ")[-1]
      self.log_debug("job submitted : %s depends on %s" % (job_id,dep),1)
    else:
      self.log_debug("should submit job %s" % job_name,1)
      self.log_debug(" with cmd = %s " % " ".join(cmd),1)
      job_id = "self.JOB_ID_%s" % job_name

    self.JOB_ID[job_id] = dep
    self.save_workspace()

    print 'submitting job %s Job # %s_%s-%s' % (job_name,job_id,range_first,range_last)

    return job_id

  #########################################################################
  # submit a job
  #########################################################################

  def job_submit(self,range_first,range_last):

    self.log_debug('submitting job array [%s,%s]...' % (range_first,range_last),1)

    job_template = self.create_job_template()

    # job_name = "%d-%d-%%K" % (range_first,range_last)
    job_name = "job_template"
    # job_dir = "%s/%03d" %  (self.JOB_DIR,n)
    # if os.path.exists(job_dir) and not (n+1 in self.JOBS_TO_RELAUNCH.keys()):
    #   self.error_report("Something is going weird...\ndirectory %s allready exists..." % job_dir)

    dep = ""
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
    job_content = job_content.replace("__JOB_DIR__",self.JOB_DIR)

    job_file = '%s/%s.job' % (self.SAVE_DIR,job_name)
    f = open(job_file,'w')
    f.write(job_content)
    f.close()

    for j in range(range_first,range_first+self.CHUNK,self.CHUNK/4):
      job_id = self.job_array_submit(job_name,job_file,j,j+self.CHUNK/4-1,dep)

    self.log_debug('submitted job array [%s,%s] successfully...' % \
                   (range_first,range_last),1)

  #######################################################################
  # create a job template
  #########################################################################

  def create_job_template(self):

    self.log_debug('creating job template ',1)
    job = ""

    job_name = self.JOB
    job_file_path = '%s/job_template.job' % (self.SAVE_DIR)  # ,job_name)

    if not(os.path.exists(job_name)):
        self.error_report("Template job file %s missing..." % job_name)
    job_header = greps(self.SCHED_TAG,job_name)

    if not(job_header):
        self.error_report("Template job file %s does not contain any %s line..." % \
                          (job_name,self.SCHED_TAG))

    nb_header_lines = len(job_header)
    for l in open(job_name,"r").readlines():

      job = job + l

      if l.find(self.SCHED_TAG) >= 0:
        nb_header_lines = nb_header_lines - 1
        if (nb_header_lines) == 0:

          finalize_cmd = "echo finalizing"
          job = job + self.job_header_amend()

          job = job + ("%s -u %s  --jobid=$job_id  --taskid=$task_id " +\
                       "--range-first=__ARRAY_CURRENT_FIRST__ --continue " +\
                       "--log-dir=%s --range=%s --array=%s --chunk=%s --job_file=%s") % \
              (sys.executable,os.path.realpath(__file__),
               self.LOG_DIR,self.RANGE,self.ARRAY,self.CHUNK,job_file_path)

          if self.FAKE:
            job = job + " --fake"
            finalize_cmd = finalize_cmd + " --fake"
          if self.PBS:
            job = job + " --pbs"
            finalize_cmd = finalize_cmd + " --pbs"
          if not(self.DEBUG is False):
            job = job + " --debug-level=%d " % self.DEBUG
            finalize_cmd = finalize_cmd + " --debug-level=%d " % self.DEBUG
          if not(self.INFO is False):
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
    job = job + """
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
    # job = job + "__SCHEDULER_DEPENDENCY__\n"
    if self.PBS:
      job = job + self.SCHED_TAG+" -o %s/__JOB_NAME__.out\n" % self.SAVE_DIR
      job = job + self.SCHED_TAG+" -e %s/__JOB_NAME__.err\n" % self.SAVE_DIR
      job = job + self.SCHED_TAG+" -N __JOB_NAME__\n"
      job = job + """\ntask_id=`printf "%03d" "$PBS_ARRAYID"`\n"""
    else:
      # job = job + self.SCHED_TAG+" -o %s/__JOB_NAME__.out-%%a\n" % self.SAVE_DIR
      # job = job + self.SCHED_TAG+" -e %s/__JOB_NAME__.err-%%a\n" % self.SAVE_DIR
      # job = job + self.SCHED_TAG+" --job-name=__JOB_NAME__\n"
      job = job + """\ntask_id=`printf "%03d" "$SLURM_ARRAY_TASK_ID"`\n"""
      job = job + """\njob_id=`printf "%03d" "$SLURM_JOB_ID"`\n"""
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

      workspace_file = self.WORKSPACE_FILE
      self.log_debug("saving variables to file "+workspace_file)

      lock_file = self.take_lock(self.LOCK_FILE)
      f_workspace = open(workspace_file+".new", "wb")
      # Save your data here
      pickle.dump(self.JOB_ID, f_workspace)
      f_workspace.close()
      if os.path.exists(workspace_file):
        os.rename(workspace_file,workspace_file+".old")
      os.rename(workspace_file+".new",workspace_file)
      self.unlock(lock_file)

  #########################################################################
  # load_workspace
  #########################################################################

  def load_workspace(self):

      workspace_file = self.WORKSPACE_FILE
      self.log_debug("loading variables from file "+workspace_file)

      f_workspace = open(self.WORKSPACE_FILE, "rb")
      # retrieve data here
      self.JOB_ID = pickle.load(f_workspace)
      f_workspace.close()

  #########################################################################
  # usage ...
  #########################################################################

  def usage(self,message=None, error_detail="", exit=True):
      """ helping message"""
      if message:
          print "\n\tError %s:\n\t\t%s\n" % (error_detail,message)
          print "\tappend -h for the list of available options..."
      else:
        print ("\n  usage: \n \t python  %s.py " + \
               "\n\t\t  --job=<Slurm job_script file> " + \
               "\n\t\t  --array=<array indexes>       " + \
               "[ | --range=<total number of jobs to submit> ]" + \
               "\n\t\t  [ --chunk=<maximum number of jobs to queue simultaneously> ] " + \
               "\n\t\t  [ --help ] " + \
               " \n") % self.APP_NAME
               # \n\t\t[ --exclude_nodes=<nodes where not to run> ] \
               # \n\t\t[ --restart | --scratch | --kill ]\
               # \n\t\t[ --debug ] [ --debug-level=[0|1|2]  ] [ --fake ]  \

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
      self.RANGE = self.ARRAY = False
      self.MY_TASK = -1
      self.MY_JOB = -1
      self.CHUNK = 8
      self.MY_ARRAY_CURRENT_FIRST = -3

      self.SCRATCH = self.KILL = self.RESTART = self.CONTINUE = self.CONTINUEX = False
      self.NODES_FAILED = None
      self.JOB_FILE_PATH = None

      try:
          if " --help" in " "+" ".join(args) or " -h " in (" "+" ".join(args)+" "):
            self.error_report("")

          opts, args = getopt.getopt(args, "h",\
                                     ["help", "job=", "range=", "array=", "chunk=", \
                                      "exclude_nodes=","dry", "create-template", \
                                      "restart", "scratch", "kill", "continue", "continuex", \
                                      "log-dir=","taskid=", "jobid=", "range-first=", \
                                      "job_file_path=", \
                                      "debug", "debug-level=",  \
                                      "fake"])
      except getopt.GetoptError, err:
          # print help information and exit:
          self.usage(err)

      for option, argument in opts:
        if option in ("--log-dir"):
          self.LOG_DIR = expanduser(argument)
          self.SAVE_DIR = expanduser(argument+"/../SAVE")
          self.JOB_ID_FILE = "%s/job_ids.pickle" % self.LOG_DIR
          self.LOCK_FILE = "%s/lock" % self.LOG_DIR
          self.WORKSPACE_FILE = "%s/workspace.pickle" % self.SAVE_DIR

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
          self.DEBUG = 0
          self.log.setLevel(logging.DEBUG)
        elif option in ("--debug-level"):
          self.DEBUG = int(argument)
          self.log.setLevel(logging.DEBUG)
        elif option in ("--restart"):
          self.RESTART = 1
        elif option in ("--scratch"):
          self.SCRATCH = 1
          self.log_info("restart from scratch")
          self.log_info("killing previous jobs...")
          self.kill_jobs()
          for d in [self.JOB_DIR, self.SAVE_DIR]:
            if os.path.exists(d):
              shutil.rmtree(d)
        elif option in ("--create-template"):
           self.create_breakit_template()
        elif option in ("--kill"):
          self.KILL = 1
          self.kill_jobs()
          sys.exit(0)
        elif option in ("--job"):
          self.JOB = os.path.expanduser(argument)
        elif option in ("--continue"):
          self.CONTINUE = 1
        elif option in ("--continuex"):
          self.CONTINUEX = 1
        elif option in ("--chunk"):
          chunk = max(int(argument),8)
          chunk = (chunk/4)*4
          self.CHUNK = chunk
        elif option in ("--array"):
          self.ARRAY = RangeSet(argument)
        elif option in ("--range"):
          self.RANGE = argument
          self.TO = int(self.RANGE)
        elif option in ("--exclude_nodes"):
          self.NODES_FAILED = argument
        elif option in ("--fake"):
          self.FAKE = True
        elif option in ("--dry"):
           self.DRY_RUN = True
        elif option in ("--taskid"):
          self.MY_TASK = int(argument)
        elif option in ("--jobid"):
          self.MY_JOB = argument
        elif option in ("--range-first"):
          self.MY_ARRAY_CURRENT_FIRST = int(argument)
        elif option in ("--job_file_path"):
          self.JOB_FILE_PATH = argument

      if not(self.JOB) and not(self.CONTINUE or self.CONTINUEX):
        self.error_report(message='please set a job to launch with the option --job=<job file>')

      if (self.RANGE and not(self.ARRAY)):
        self.ARRAY = RangeSet("1-%s" % self.RANGE)

      if (self.ARRAY and not(self.RANGE)):
        self.RANGE = len(self.ARRAY)
        self.TO = int(self.RANGE)

      self.log_debug('RANGE=%s, ARRAY=%s' % (self.RANGE,self.ARRAY))

      if (self.ARRAY and self.RANGE and not(int(self.RANGE) == len(self.ARRAY))):
        self.error_report(message='mismatch between --range=%s and --array-argument=%s ' % \
                          (self.RANGE,self.ARRAY) + \
                          '\n  - range should be equal of the number of array-indexes: %s' % \
                          len(self.ARRAY))

      if not(self.RANGE) and not(self.CONTINUE or self.CONTINUEX):
        self.error_report(message='please set ' + \
                          '\n  - a range for your job with the option ' + \
                          '\n           --array=<array-indexes> ' + \
                          '\n        or ' + \
                          '\n           --range=<total number of the jobs in the array> ' + \
                          '\n         ' + \
                          '\n  - the number of jobs you want in the queue with the option' + \
                          '\n           --chunk=<maximum number of jobs to queued simultaneously>')

      self.log_info('starting Task %s' % self.MY_TASK,1)

      return True

  #########################################################################
  # os.system wrapped to enable Trace if needed
  #########################################################################

  def create_breakit_template(self):

    path = os.getenv('BREAKIT_PATH')
    if not(path):
      path = '.'
    lines = "".join(open('%s/run_tests.py' % path,"r").readlines())
    self.create_template("run_test.py__SEP2__"+lines)


if __name__ == "__main__":
    K = breakit()
