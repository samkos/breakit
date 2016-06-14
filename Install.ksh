python breakit.py --job=job.sh --range=20 --restart
b | awk '/_3/ { sub("_"," "); print $1}'
job_id=`b -r | awk '/_3/ { sub("_"," "); print $1}'`
echo $job_id
python -u breakit.py  --jobid=$job_id  --taskid=3 --array-first=3 --continue --log-dir=/home/kortass/BREAKIT/JOBS/LOGS --range=20  --job_file=/home/kortass/BREAKIT/JOBS/SAVE/job_template.job
b -r
job_id=`b -r | awk '/_13/ { sub("_"," "); print $1}'`
echo $job_id

python -u breakit.py  --jobid=$job_id  --taskid=13 --array-first=13 --continue --log-dir=/home/kortass/BREAKIT/JOBS/LOGS --range=20  --job_file=/home/kortass/BREAKIT/JOBS/SAVE/job_template.job
