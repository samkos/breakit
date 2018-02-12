Welcome to Breakit's documentation!
====================================


NAME
       breakit - KSL many jobs automated submitter

SYNOPSIS
       breakit  --job=any-Slurm-Job
                --range=Total-number-of-jobs-to-submit
                [--chunk=Maximum-number-of-jobs-to-be-seen-in-the-queue]

DESCRIPTION
       breakit  assists  the  user  in submitting Slurm job array of important
       size on shaheen II. It has been  designed,  developped  and  officially
       maintained by the Kaust Supercomputing Laboratory.

       By  policy, on shaheen II, the maximum number of jobs authorized in the
       queue is limited per user. breakit seamlessly allows the submission  of
       job arrays whose total number can be higher than this limit.

USE
       Not  a  single  modification needs to be made to the Slurm job it self.
       Let assume 1000 occcurences of "my_job" needs to be submitted, the com-
       mand:

          breakit --job=my-job --range=1000 --chunk=20

       will  submit  the 1000 jobs to shaheen II in several chunks of 20 jobs.
       At a time, no more than 20 jobs will appear in the queue.  In  reality,
       breakit  will  use job dependency to automate the ongoing submission of
       the remaining jobs. These batches of 20 jobs will be seen as successive
       job arrays in the scheduling queue.

       If  no  option  --chunk  is  mentionned,  default number of jobs queued
       simultaneously is set to 8.

       To cancel the full set of jobs, an option --kill is  under  development
       and  testing.  In  the meantime, when choosing a different name for all
       the jobs launched through breakit the following Slurm  command  can  be
       used:

          scancel  -n=name-of-job

AUTHOR
       Written by Samuel Kortas (samuel.kortas (at) kaust.edu.sa)


REPORTING BUGS
       Report breakit bugs to help@hpc.kaust.edu.sa
       breakit home page: <https://github.com/samkos/breakit>
       KAUST Supercomputing Laboratory: <http://hpc.kaust.edu.sa/>

COPYRIGHT
       Copyright  © 2016  KAUST Supercomputing Laboratory License LGPLv3+: GNU
       LGPL version 3 or later <http://gnu.org/licenses/gpl.html>.
       This is free software: you are free  to  change  and  redistribute  it.
       There is NO WARRANTY, to the extent permitted by law.

SEE ALSO
       A comprehensive presentation of breakit has been given by Samuel Kortas
       during the KSL Workshop entitled 'Boost your  efficiency  when  dealing
       with  multiple  jobs on the Cray XC40 supercomputer Shaheen II' held at
       KAUST On Sunday June 5th 2016. The workshop slides can be freely  down-
       loaded from
       <https://www.hpc.kaust.edu.sa/sites/default/files/files/public/many_jobs.pdf>

