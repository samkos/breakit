#!/usr/bin/python
#
# Copyright (c) 2016 Contributors as noted in the AUTHORS file
#
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

    self.MAIL_COMMAND = MAIL_COMMAND
    self.SUBMIT_COMMAND = SUBMIT_COMMAND
    self.SCHED_TYPE = SCHED_TYPE
    self.DEFAULT_QUEUE = DEFAULT_QUEUE
    
    self.DEBUG=False
    self.INFO=0

    self.LOG_PREFIX=""
    
    if not(app_dir_log):
      app_dir_log = "/scratch/%s/logs/.%s" % (getpass.getuser(),self.APPLICATION_NAME)
    self.LOG_DIR = expanduser("%s" % app_dir_log)
   
    self.log = False

    self.welcome_message()

    self.check_python_version()
    self.check_engine_version(engine_version_required)
      

    if not(self.parse()):
      sys.exit(1)



  #########################################################################
  # set self.log file
  #########################################################################


  def initialize_log_files(self):

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
    if level<=self.DEBUG:
      if len(self.LOG_PREFIX):
          msg = "%s:%s" % (self.LOG_PREFIX,msg)
      self.log.debug(msg)
      if (dump_exception):
        self.dump_exception()
      #self.log.debug("%d:%d:%s"%(self.DEBUG,level,msg))

  def log_info(self,msg,level=0,dump_exception=0):
    if level<=self.INFO:
      if len(self.LOG_PREFIX):
          msg = "%s:%s" % (self.LOG_PREFIX,msg)
      self.log.info(msg)
      if (dump_exception):
        self.dump_exception()
      #self.log.debug("%d:%d:%s"%(self.DEBUG,level,msg))

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
  # usage ...
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
  # parsing command line
  #########################################################################

  def parse(self,args=sys.argv[1:]):
      """ parse the command line and set global _flags according to it """

      try:
          if " --help" in " "+" ".join(args) or " -h " in (" "+" ".join(args)+" ") :
            self.error_report("")

          opts, args = getopt.getopt(args, "h", 
                            ["help", "debug", "debug-level=", \
                                      "info", "info-level=", "log-dir=" ])    
      except getopt.GetoptError, err:
          # print help information and exit:
          self.error_report(err)


      for option, argument in opts:
        if option in ("--log-dir"):
          self.LOG_DIR = expanduser(argument)

      # initialize Logs
      self.initialize_log_files()

      self.log_info("\tprocessing ...",2)
      self.log_info("\t\t" + " ".join(sys.argv),2)

      for option, argument in opts:
        if option in ("--info"):
          self.INFO = 1
          self.log.setLevel(logging.INFO)
        elif option in ("--info-level"):
          self.INFO = int(argument)
          self.log.setLevel(logging.INFO)

      for option, argument in opts:
        if option in ("--debug"):
          self.DEBUG = 0
          self.log.setLevel(logging.DEBUG)
        elif option in ("--debug-level"):
          self.DEBUG = int(argument)
          self.log.setLevel(logging.DEBUG)


      self.log_debug('Parse successfully exited',2)
      return True


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
    return output


#########################################################################
  # send mail back to the user
  #########################################################################

  def init_mail(self):

    if not(self.MAIL):
      return False
    
    # sendmail only works from a node on shaheen 2 via an ssh connection to cdl via gateway...

    if not(self.MAIL_TO):
      self.MAIL_TO = getpass.getuser()

  def send_mail(self,title,msg,to=None):

    if not(self.MAIL):
      return False

    if not(self.MAIL_COMMAND):
      self.error_report("No mail command available on this machine")

    # sendmail only works from a node on shaheen 2 via an ssh connection to cdl via gateway...

    mail_file = os.path.abspath("./mail.txt")

    f = open(mail_file,'w')
    f.write(msg)
    f.close()

    if not(to):
        to = self.MAIL_TO
        
    cmd = (self.MAIL_COMMAND+"2> /dev/null") % (title, to, mail_file)
    self.log_debug("self.MAIL cmd : "+cmd,2)
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
