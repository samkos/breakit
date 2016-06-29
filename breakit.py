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

import argparse

from engine import *
from env import *
from ClusterShell.NodeSet import *

ERROR = -1

TASK_POSSIBLE_STATES = JOB_POSSIBLE_STATES + ('SUBMITTED','OK','NOK','WAITING','KILLED')



class breakit(engine):

  def __init__(self,engine_version=0.16,app_name='breakit'):

    
    self.APP_NAME  = app_name
    self.VERSION = "0.3"
    self.ENGINE_VERSION_REQUIRED = engine_version

    engine.__init__(self,self.APP_NAME,self.VERSION,engine_version_required=self.ENGINE_VERSION_REQUIRED)

    
    self.TASK_FILE = "./.%s/SAVE/%s.pickle" % (app_name,'tasks')
    self.BREAKIT_DIR = os.getenv('BREAKIT_PATH')
    self.TASK_LOCK_FILE = "%s/task_lock" % self.LOG_DIR
    
        

    self.MY_EXEC = sys.argv[0]
    self.MY_EXEC_SAVED = self.SAVE_DIR+"/"+os.path.basename(sys.argv[0])
    self.INITIAL_DATA_DIR = "."

    self.TASK_STATUS = {}
    self.TASK_JOB_ID = {}
    
    #self.load()
    


  def start(self):
   
    self.log_debug('[breakit:start] entering')
    engine.start(self)
    
    self.env_init()
    self.run()
      
  #########################################################################
  # check for tne option on the command line
  #########################################################################


  def initialize_parser(self):

    self.parser.add_argument("-a","--array", type=str , help="Array indexes")
    self.parser.add_argument("-c","--chunk", type=int , help="maximum number of jobs to queue simultaneously", default=8)
    self.parser.add_argument("-j","--job", type=str , help="Slurm Job script")
    
    self.parser.add_argument("--go-on", action="store_true", help=argparse.SUPPRESS)
    self.parser.add_argument("--finalize", action="store_true", help=argparse.SUPPRESS)
    self.parser.add_argument("-s","--status", action="store_true", help='checking status of jobs')
    self.parser.add_argument("--exit-code", type=int , default=0,  help=argparse.SUPPRESS)
    self.parser.add_argument("--jobid", type=int ,  help=argparse.SUPPRESS)
    self.parser.add_argument("--job-file-path", type=str , help=argparse.SUPPRESS)
    self.parser.add_argument("--taskid", type=int , default=0, help=argparse.SUPPRESS)
    self.parser.add_argument("--array-current-first", type=int , help=argparse.SUPPRESS)
    self.parser.add_argument("--attempt", type=int , default=0, help=argparse.SUPPRESS)

    engine.initialize_parser(self)
    
  #########################################################################
  # main routeur
  #########################################################################
  def run(self):
    #
    self.set_log_prefix('%s' % self.args.taskid)
    
    if self.args.status:
      self.check_jobs()
      sys.exit(0)

    
    if self.args.finalize:
      self.finalize()
      sys.exit(0)

    self.log_info('starting Task %s' % self.args.taskid)

    if self.args.job:
      self.args.job = os.path.expanduser(self.args.job)

    if self.args.chunk:
      self.args.chunk = max(self.args.chunk,8)/4*4

    if self.args.array:
      self.ARRAY = RangeSet(self.args.array)
      self.TO = len(self.ARRAY)
      self.log_debug('ARRAY=%s' % ( self.ARRAY))
    else:
      self.error(message='please set ' + \
                               '\n  - a range for your job with the option ' + \
                               '\n           --array=<array-indexes> ' + \
                               '\n         ' + \
                               '\n  - the number of jobs you want in the queue with the option' + \
                               '\n           --chunk=<maximum number of jobs to queued simultaneously>')

    if self.args.restart:
      self.kill_jobs(tasks_to_kill=map(str,self.ARRAY))
    
    lock_file = self.take_lock(self.TASK_LOCK_FILE)
    self.log_debug("got lock!",4)
    self.load()
    self.load_task_stats()

    if not(self.args.go_on):
      self.check_on_previous_computation()
      self.prepare_computation()
      job = self.job_submit(1,self.TO)
      self.log_debug("Saving Job Ids...",1)
    else:
      self.manage_jobs()

    # saving status for further use
    self.save()
    self.save_task_stats(take_lock=False)
    self.release_lock(lock_file)
    self.log_debug('lock released')
      
    if self.args.taskid:
      print self.TASK_STATUS
      if self.TASK_STATUS['%s' % self.args.taskid]=='KILLED':
        self.log_info('task %s was killed by the user. Giving up this job')
        sys.exit(1)

  #########################################################################
  # save_workspace
  #########################################################################

  def save_task_stats(self,take_lock=True):
      
    #
    if take_lock:
      lock_file = self.take_lock(self.TASK_LOCK_FILE)

    # could need to load workspace and merge it
    
    #print "saving variables to file "+workspace_file
    workspace_file = self.TASK_FILE
    f = open(workspace_file+".new", "wb" )
    pickle.dump(self.TASK_STATUS,f)
    pickle.dump(self.TASK_JOB_ID,f)
    f.close()
    if os.path.exists(workspace_file):
      os.rename(workspace_file,workspace_file+".old")
    os.rename(workspace_file+".new",workspace_file)

    if take_lock:
      self.release_lock(lock_file)


  #########################################################################
  # load_workspace
  #########################################################################

  def load_task_stats(self):

    try:
      #print "loading variables from file "+workspace_file
      if os.path.exists(self.TASK_FILE):
          f = open( self.TASK_FILE, "rb" )
          self.TASK_STATUS = pickle.load(f)
          self.TASK_JOB_ID = pickle.load(f)
          f.close()
    except:
        self.error('[load_task_stats]  problem encountered while loading task stats'
                   +'\n---->  rerun with -d to have more information',
                          exit=True, exception=True) #self.args.debug)
  #########################################################################
  # finalizing task
  #########################################################################
  def finalize(self):
    #
    self.log_info('FINALIZING... task %s -> %s ' % (self.args.taskid,self.args.exit_code))
    status = 'OK'
    if self.args.exit_code:
      status = 'NOK'
    filename = '%s/%s;%s;%s;%s' % (self.SAVE_DIR,status,self.args.taskid,\
                                   self.args.jobid,self.args.exit_code)
    self.log_debug('touching stub file %s' % filename,1)
    f = open(filename,'w')
    f.close()
    time.sleep(1)
    
      
  #########################################################################
  # manage_jobs()
  #########################################################################
  def manage_jobs(self):
    #
    if not(self.ARRAY[self.args.array_current_first-1] == self.args.taskid):
      self.log_debug('not in charge to spawn more jobs: MY_ARRAY_CURRENT_FIRST=%s not equals MY_TASK=%s ' % \
                     (self.ARRAY[self.args.array_current_first-1],self.args.taskid))
      return
    
    self.log_info("my task is in charge of adding work...")

    range_first = self.args.array_current_first+self.args.chunk

    self.log_debug("range_first=%s, self.TO=%s, len(self.ARRAY)=%s" % (range_first, self.TO, len(self.ARRAY)))

    if (range_first<=self.TO):
      self.log_info('can still submit... (%d-%d) dependent on %s' % \
        (range_first,min(range_first+self.args.chunk/4-1,self.TO),self.args.jobid))
      if self.args.go_on:
        self.job_array_submit("job.template", self.args.job_file_path,
                              range_first=range_first,
                              range_last=range_first+self.args.chunk/4-1,
                              dep=self.args.jobid,filter=True)

  #########################################################################
  # check_jobs update current job status
  #########################################################################
  def check_jobs(self,take_lock=True):
    #
    if take_lock:
      self.log_debug("continuing... taking lock",4)
      lock_file = self.take_lock(self.TASK_LOCK_FILE)
      self.log_debug("got lock!",4)

      self.load()
      self.load_task_stats()

    # gathering information on tasks just submitted butn ot scheduled
    for status in ['SUBMITTED']:
      for f in  glob.glob('%s/%s;*' % (self.SAVE_DIR,status)):
        l=(status_saved,task,job,exit_code) = os.path.basename(f).split(';')
        self.TASK_STATUS [task] = status_saved
        self.TASK_JOB_ID [task] = job
        #self.system('mv %s %s/' % (f,self.LOG_DIR),verbosity=4)
        os.unlink(f)

    # updating status of tasks already spawned by breakit
    
    additional_check = []
    for (task,status) in self.TASK_STATUS.items():
      #print task,status
      if status in ('RUNNING','SUBMITTED','PENDING'):
        self.log_debug('checking on last status of task %s of previous status /%s/' % \
                       (task,status),2)
        additional_check.append("%s_%s" % (self.TASK_JOB_ID[task],task))

    if len(additional_check):
        cmd = 'squeue -h -l -o "%.20i %.20T" -r ' + '-u %s | grep _'  % (getpass.getuser())
        output =  self.system(cmd)
        jobs = output[:-1].split("\n")
        if len(jobs):
          self.log_debug('squeue result: \n >>>%s<<' % output,2)
          for l in jobs:
            if len(l)<3:
              continue
            self.log_debug('line scanned: \n >>>%s<<' % l,2)
            l = clean_line(l)
            (id,status) = l.split(" ")
            #id = '%s' % id
            (job,task) = id.split("_")
            # print id, additional_check, (id in additional_check)
            if (id in additional_check):
              self.TASK_STATUS[task] = status
              self.log_debug('updated status for %s : %s' % (task,status))

    # tasks that completed and had the time to save their status

    for status in ['OK','NOK']:
      for f in  glob.glob('%s/%s;*' % (self.SAVE_DIR,status)):
        l=(status_saved,task,job,exit_code) = os.path.basename(f).split(';')
        self.TASK_STATUS [task] = status_saved
        self.TASK_JOB_ID [task] = job
        #self.system('mv %s %s/' % (f,self.LOG_DIR),verbosity=4)
        os.unlink(f)

    # saving status for further use
    if take_lock:
      self.save()
      self.save_task_stats(take_lock=False)
      self.release_lock(lock_file)
      self.log_debug('lock released')

    return self.display_status('o')
  
  #########################################################################
  # display_status
  #########################################################################
    
  def display_status(self,msg=''):
    
    self.TASK_STATS = {}
    #print TASK_POSSIBLE_STATES
    for status in TASK_POSSIBLE_STATES:
        self.TASK_STATS[status] = []
    #print self.TASK_STATUS
    for (task,status) in self.TASK_STATUS.items():
      self.TASK_STATS[status].append(task)

    nb_all = 0
    self.log_info( '')
    self.log_info( '--- Current status at %s --%s------' % (time.ctime(),msg))
    for (status,tasks) in self.TASK_STATS.items():
      nb = len(self.TASK_STATS[status])
      nb_all = nb_all + nb
      if nb:
        # print tasks,status
        # print ",".join(tasks)
        #print RangeSet(",".join(tasks))
        self.log_info( '%10s -> %4d jobs : %s ' % (status,nb,RangeSet(",".join(tasks))))
  
    return nb_all

  #########################################################################
  # Check for previous computation in same directory
  #########################################################################

  def check_on_previous_computation(self):

    previous_running_tasks = self.check_jobs(take_lock=False)

    if (previous_running_tasks>0) and not(self.args.restart or self.args.scratch):

      self.log_info('WARNING!  A computation already occurred in this directory')
      self.log_info("""          to enforce the run, you can use one the following options : 
                   --kill     to kill the ongoing computation 
                              and keep previous results   
                   --status   to obtain a detailed status on 
                              ongoing computation
                   --scratch  to erase previous result, kill 
                              an eventual ongoing computation
                              and start a new one from scratch
                   --restart  to recompute tasks that failed or
                              to continue or complete
                              an even still ongoing computation
                              and only erase the results that will be rerun """)
 
      sys.exit(1)
      
    pass
    
        


  #########################################################################
  # starting the process
  #########################################################################

  def prepare_computation(self):

    self.log.info("")
    self.log.info("="*60)
    self.log.info("preparing computation...")

    self.log_debug("======== STARTING BREAKIT ==========")
    self.log_debug("now : "+getDate())


    for d in [self.JOB_DIR,self.SAVE_DIR,self.LOG_DIR]:
      if not(os.path.exists(d)):
        os.makedirs(d)
        self.log_debug("creating directory %s" % d,1)


    for task in self.ARRAY:
      self.TASK_STATUS['%s' % task] = 'WAITING'


  #########################################################################
  # cleaning environment
  #########################################################################
  def clean(self,force=False):
    for d in [self.JOB_DIR,self.SAVE_DIR]:
      if os.path.exists(d):
        self.log_info("deleting directory %s..." % d)
        shutil.rmtree(d)        
      self.TASK_STATUS = {}
      self.TASK_JOB_ID = {}
        
    
  #########################################################################
  # kill jobs... after asking confirmation
  #########################################################################
  def kill_jobs(self,force=False,tasks_to_kill=[]):

    self.log_debug("killing all jobs",1)

    lock_file = self.take_lock(self.TASK_LOCK_FILE)
    self.log_debug("got lock!",4)
    self.load()
    self.load_task_stats()
    
    if self.args.array:
      tasks_to_kill = RangeSet(self.args.array)
    
    previous_running_tasks = self.check_jobs(take_lock=False)

    jobs_to_kill = []
    
    if len(tasks_to_kill):
      self.log_debug('killing specific tasks : %s' % tasks_to_kill)
      task_keys = self.TASK_STATUS.keys()
      for task in map(str,tasks_to_kill):
        if task in task_keys:
          if self.TASK_STATUS[task] in JOB_ACTIVE_STATES and not(int(task) % (self.args.chunk/4)==1):
            jobs_to_kill.append("%s_%s" % (self.TASK_JOB_ID[task],task))
          self.TASK_STATUS[task]='KILLED'
      #print self.TASK_STATUS
    else:
      for status in JOB_ACTIVE_STATES + ('WAITING',):
        for task in self.TASK_STATS[status]:
          if status in JOB_ACTIVE_STATES and not(int(task) % (self.args.chunk/4)==1):
            jobs_to_kill.append("%s_%s" % (self.TASK_JOB_ID[task],task))
          self.TASK_STATUS[task]='KILLED'
        self.TASK_STATS['KILLED'] = self.TASK_STATS[status]
        self.TASK_STATS[status] = []
        
    if len(jobs_to_kill):
      cmd = 'scancel ' + " ".join(jobs_to_kill)
      self.log_info(cmd)
      self.system(cmd)

    
    self.display_status('after-kill')

    # saving status for further use
    self.save()
    self.save_task_stats(take_lock=False)
    self.release_lock(lock_file)
    self.log_debug('lock released')

    if True:
      pass
    else:
      self.log.info("No job still exists for this study...")

  #########################################################################
  # submit a job_array
  #########################################################################

  def job_array_submit(self,job_name, job_file, range_first, range_last, dep="", filter=False):

    range_last = min(range_last,self.TO)

    if range_last < range_first:
      self.log_debug('no more job need to be submitted')
      sys.exit(0)


    f=open(job_file,'r')
    job_content = "".join(f.readlines())
    f.close()

    job_content = job_content.replace("__ARRAY_CURRENT_FIRST__","%s" % range_first)

    job_script = '%s/job_template.%d-%d.job' % \
                 (self.SAVE_DIR,range_first,range_last)
    f=open(job_script,'w')
    f.write(job_content)
    f.close()

    if filter:
      tasks = [str(self.ARRAY[(range_first-1)])]
      for task in map(str,self.ARRAY[(range_first-1):range_last]):
        if not(self.TASK_STATUS[task] == 'KILLED'):
          tasks.append(task)
      tasks = RangeSet(",".join(tasks))
    else:      
      tasks = self.ARRAY[(range_first-1):range_last]


    new_job = { 'name' : job_name,
                'comes_after': dep,
                'depends_on' : dep,
                'command' : os.path.abspath("%s" % job_script),
                'submit_dir' : os.getcwd(),
                'array' : tasks,
    }



    (job_id,cmd)  = self.submit(new_job)
    for i in map(str,tasks):
      if not(self.TASK_STATUS[i] == 'KILLED'):
        status = 'SUBMITTED'
        filename = '%s/%s;%s;%s;%s' % (self.SAVE_DIR,status,i,job_id,0)
        open(filename,"w").close()
        self.TASK_STATUS[i] = 'SUBMITTED'
      
    self.log_debug('submitting job %s Job # %s_%s-%s' % (job_name,job_id,range_first,range_last),4)

    return job_id

  #########################################################################
  # submit a job
  #########################################################################

  def job_submit(self,range_first,range_last):

    self.log_debug('submitting job array [%s,%s]...' % (range_first,range_last),1)

    job_template = self.create_job_template()

    #job_name = "%d-%d-%%K" % (range_first,range_last)
    job_name = "job_template"
    # job_dir = "%s/%03d" %  (self.JOB_DIR,n)
    # if os.path.exists(job_dir) and not (n+1 in self.JOB_TO_RELAUNCH.keys()):
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
    job_content = job_content.replace("__JOB_DIR__",self.JOB_DIR)

            
    job_file = '%s/%s.job' % (self.SAVE_DIR,job_name)
    f=open(job_file,'w')
    f.write(job_content)
    f.close()

    for j in range(range_first,range_first+self.args.chunk,self.args.chunk/4):
      job_id=self.job_array_submit(job_name,job_file,j,j+self.args.chunk/4-1,dep)


    self.log_debug('submitted job array [%s,%s] successfully...' % (range_first,range_last),1) 



  #######################################################################
  # create a job template
  #########################################################################

  def create_job_template(self):

    self.log_debug('creating job template ',1)
    job = ""
    
    job_name = self.args.job
    job_file_path = '%s/job_template.job' % (self.SAVE_DIR) #,job_name)

    if not(os.path.exists(job_name)):
        self.error("Template job file %s missing..." % job_name)
    job_header = greps(self.SCHED_TAG,job_name)

    if not(job_header):
        self.error("Template job file %s does not contain any %s line..." % (job_name,self.SCHED_TAG))

    nb_header_lines = len(job_header)
    for l in open(job_name,"r").readlines():

      job = job + l

      if l.find(self.SCHED_TAG)>=0:
        nb_header_lines = nb_header_lines - 1
        if (nb_header_lines) == 0:

          finalize_cmd = "echo finalizing"
          job = job + self.job_header_amend()

          script = "%s -u %s  --jobid=$job_id  --taskid=$task_id --array-current-first=__ARRAY_CURRENT_FIRST__ " % \
                (sys.executable,os.path.realpath(__file__)) +\
                "--go-on --log-dir=%s  --array=%s --chunk=%s --job-file-path=%s --job %s %s %s" % \
              (   self.LOG_DIR,self.ARRAY,self.args.chunk,job_file_path, self.args.job,"-d "*self.args.debug,"-i "*self.args.info)

          if self.args.fake:
            script = script + " --fake"
          if self.args.pbs:
            script = script + " --pbs"

          job = job + script
          finalize_cmd  = finalize_cmd + "\n" + script+" --finalize"
          
          job = job + "\n"
          job = job + """
              if [ $? -ne 0 ] ; then
                  echo "[ERROR] FAILED in Job: stopping everything..."
                  exit 1
              else \n """ 
          job = job + "\n# START OF ORIGINAL USER SCRIPT  -------------------\n"

    job = job + "\n# END OF ORIGINAL USER SCRIPT  -------------------\n"
    job = job + """
                result_job=$?
                if [ $result_job -ne 0 ] ; then
                   echo Job failed with a non zeroexit value
                   %s --exit-code $result_job
                   exit $result_job
                else

"""             % (finalize_cmd)
    job = job + finalize_cmd 
    job = job+ """
                fi
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
    if self.args.pbs:
      job = job + self.SCHED_TAG+" -o %s/__JOB_NAME__.out\n" % self.SAVE_DIR
      job = job + self.SCHED_TAG+" -e %s/__JOB_NAME__.err\n" % self.SAVE_DIR
      job = job + self.SCHED_TAG+" -N __JOB_NAME__\n"
      job = job + """\ntask_id=`printf "%03d" "$PBS_ARRAYID"`\n"""
    else:
      #job = job + self.SCHED_TAG+" -o %s/__JOB_NAME__.out-%%a\n" % self.SAVE_DIR
      #job = job + self.SCHED_TAG+" -e %s/__JOB_NAME__.err-%%a\n" % self.SAVE_DIR
      #job = job + self.SCHED_TAG+" --job-name=__JOB_NAME__\n"
      job = job + """\ntask_id=`printf "%03d" "$SLURM_ARRAY_TASK_ID"`\n"""
      job = job + """\njob_id=`printf "%03d" "$SLURM_JOB_ID"`\n"""
    return job


  #########################################################################
  # os.system wrapped to enable Trace if needed
  #########################################################################

  def create_template(self):

    path = os.getenv('BREAKIT_PATH')
    if not(path):
      path='.'
    engine.create_template(self,path)
  
if __name__ == "__main__":
    K = breakit()
    K.start()
