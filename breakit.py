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

from engine import engine
from env import *
from ClusterShell.NodeSet import *

ERROR = -1


class breakit(engine):

  def __init__(self,engine_version=0.15,app_name='breakit'):

    
    self.WORKSPACE_FILE = "nodecheck.pickle"

    self.APP_NAME  = app_name
    self.VERSION = "0.3"
    self.ENGINE_VERSION_REQUIRED = engine_version

    engine.__init__(self,self.APP_NAME,self.VERSION,engine_version_required=self.ENGINE_VERSION_REQUIRED)

    
    self.BREAKIT_DIR = os.getenv('BREAKIT_PATH')
    
        

    self.MY_EXEC = sys.argv[0]
    self.MY_EXEC_SAVED = self.SAVE_DIR+"/"+os.path.basename(sys.argv[0])
    self.INITIAL_DATA_DIR = "."


    
    if os.path.exists(self.WORKSPACE_FILE):
      self.load()


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
    self.parser.add_argument("--taskid", type=int ,  help=argparse.SUPPRESS)
    self.parser.add_argument("--array-current-first", type=int , help=argparse.SUPPRESS)
    self.parser.add_argument("--attempt", type=int , default=0, help=argparse.SUPPRESS)

    engine.initialize_parser(self)
    
  #########################################################################
  # main routeur
  #########################################################################
  def run(self):
    #
    if self.args.status:
      self.get_current_jobs_status()
      sys.exit(0)

    
    if self.args.finalize:
      self.finalize()
      sys.exit(0)

    self.log_info('starting Task %s' % self.args.taskid,1)

    if self.args.job:
      self.args.job = os.path.expanduser(self.args.job)

    if self.args.chunk:
      self.args.chunk = max(self.args.chunk,8)/4*4

    if self.args.array:
      self.ARRAY = RangeSet(self.args.array)
      self.TO = len(self.ARRAY)
      self.log_debug('ARRAY=%s' % ( self.ARRAY))
    else:
      self.error_report(message='please set ' + \
                               '\n  - a range for your job with the option ' + \
                               '\n           --array=<array-indexes> ' + \
                               '\n         ' + \
                               '\n  - the number of jobs you want in the queue with the option' + \
                               '\n           --chunk=<maximum number of jobs to queued simultaneously>')
    
    
    if not(self.args.go_on):
      self.prepare_computation()
      job = self.job_submit(1,self.TO)
      self.log_debug("Saving Job Ids...",1)
      self.save()
    else:
      self.manage_jobs()


  #########################################################################
  # finalizing task
  #########################################################################
  def finalize(self):
    #
    self.log_info('FINALIZING... task %s -> %s ' % (self.args.taskid,self.args.exit_code))
    status = 'OK'
    if self.args.exit_code:
      status = 'NOK'
    filename = '%s/%s.%s-%s-%s' % (self.SAVE_DIR,status,self.args.taskid,\
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
                              dep=self.args.jobid)
        self.save()

  #########################################################################
  # manage_jobs()
  #########################################################################
  def check_jobs(self):
    #
    self.log_debug("continuing... taking lock",4)
    lock_file = self.take_lock(self.LOCK_FILE)
    self.log_debug("got lock!",4)
    
    jobs = self.get_current_jobs_status()

    max=0
    nb_jobs = 0
    for j in jobs.keys():
      status = jobs[j]
      if status=='RUNNING' or status=='PENDING':
        cmd = "squeue -r -u %s | grep %s " % (getpass.getuser(),j)
        qualified_jobs = self.system(cmd)
        print qualified_jobs
        for q in qualified_jobs.split('\n'):
          q = re.sub(r"\s+"," ", q)
          q = re.sub(r"^\s+","", q)
          q = re.sub(r"\s.*$","", q)
          print "/%s/" % q
          if len(q):
            job_nb = int(q.split("_")[1])
            print job_nb
            nb_jobs = nb_jobs+1
            if job_nb>max:
              max = job_nb
        print nb_jobs,'are still queued for job ',j,'and max index is ',max
        
    print nb_jobs,'are still queued and max index is ',max,'out of ',self.TO,'jobs'

    if max>0 and max<self.TO:
      print 'can still submit...'
      if self.args.go_on:
        self.job_array_submit("xxx", self.args.job_file_path, max+1, self.TO)
        self.save()

    time.sleep(2)
    self.release_lock(lock_file)
    self.log_debug('lock released')
    


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

      self.log.info("All jobs killed with success")
      self.log.info("="*60)
      print("All jobs killed with success")
      #self.send_mail("All job have been killed with success")
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


    f=open(job_file,'r')
    job_content = "".join(f.readlines())
    f.close()

    job_content = job_content.replace("__ARRAY_CURRENT_FIRST__","%s" % range_first)

    job_script = '%s/job_template.%d-%d.job' % \
                 (self.SAVE_DIR,range_first,range_last)
    f=open(job_script,'w')
    f.write(job_content)
    f.close()

      
    new_job = { 'name' : job_name,
                'comes_after': dep,
                'depends_on' : dep,
                'command' : os.path.abspath("%s" % job_script),
                'submit_dir' : os.getcwd(),
                'array' :self.ARRAY[(range_first-1):range_last],
    }



    (job_id,cmd)  = self.submit(new_job)
          
    self.JOB_ID[job_name]  = self.JOB_ID[os.path.abspath("%s" % job_script)] =   job_id
    self.JOB_WORKDIR[job_id]  =   os.getcwd()
    self.JOB_STATUS[job_id] = 'SPAWNED'

    self.save()

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



  #########################################################################
  # submitting one job   the definitive one
  #########################################################################

  def submit(self,job):

    cmd = [self.SCHED_SUB]

    if (job['depends_on']) :
      cmd = cmd + [self.SCHED_DEP+":%s"%job['depends_on'] ]

    if self.args.exclude_nodes:
      cmd = cmd + ["-x",self.args.exclude_nodes]

    if not(job['array']):
      job['array'] = '1-1'
      
    if self.MY_MACHINE=="sam":
      job['account'] = None

    jk = job.keys()
    for param in ['partition','reservation','time','job-name',
                  'error','output','ntasks','array','account']:
      if param in jk:
        if job[param]:
          cmd = cmd + ['--%s=%s' % (param,job[param])]
        
    if self.args.attempt:
      cmd = cmd + \
            ['%s_%s'                % (job['command'], self.args.attempt) ]
      job_content_template = "".join(open(job['command'],"r").readlines())
      job_content_updated  = job_content_template.replace('__ATTEMPT__',"%s" % self.args.attempt)
      job_script_updated  = open('%s_%s' % (job['command'], self.args.attempt), "w")
      job_script_updated.write(job_content_updated)
      job_script_updated.close()
    else:
      cmd = cmd + \
            ['%s'                % (job['command']) ]

    if not self.args.dry:
      self.log_debug("submitting : "+" ".join(cmd))
      output = subprocess.check_output(cmd)
      if self.args.pbs:
        #print output.split("\n")
        job_id = output.split("\n")[0].split(".")[0]
        #print job_id
      else:
        for l in output.split("\n"):
          self.log_debug(l,1)
          #print l.split(" ")
          if "Submitted batch job" in l:
            job_id = l.split(" ")[-1]
      self.log_debug("job submitted : %s depends on %s" % (job_id,job['depends_on']),1)
    else: 
      self.log_debug("should submit job %s" % job['name'])
      self.log_debug(" with cmd = %s " % " ".join(cmd))
      job_id = "%s" % job['name']

    job['job_id'] = job_id
    job['submit_cmd'] = cmd
        

    if 'step_before' in jk:
      step_before = job['step_before']
      if step_before:
        self.JOB[step_before]['comes_before'] = self.JOB[self.JOB[step_before]['job_id']]['comes_before'] =  job_id
        self.JOB[step_before]['make_depend'] = self.JOB[self.JOB[step_before]['job_id']]['make_depend'] =  job_id

    self.JOB[job_id] = self.JOB[job['name']] = job
    
      
    self.log_info('submitting job %s --> Job # %s <-depends-on %s' % (job['name'],job_id,job['depends_on']))

    self.log_debug("Saving Job Ids...",1)
    self.save()

    return (job_id,cmd)



  #######################################################################
  # create a job template
  #########################################################################

  def create_job_template(self):

    self.log_debug('creating job template ',1)
    job = ""
    
    job_name = self.args.job
    job_file_path = '%s/job_template.job' % (self.SAVE_DIR) #,job_name)

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
                  %s 
                  exit 1
              else \n """ + finalize_cmd
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

  def create_breakit_template(self):

    path = os.getenv('BREAKIT_PATH')
    if not(path):
      path='.'
    l = "".join(open('%s/run_tests.py' % path,"r").readlines())
    self.create_template("run_test.py__SEP2__"+l)
  
if __name__ == "__main__":
    K = breakit()
    K.start()
