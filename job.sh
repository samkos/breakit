#!/bin/bash
#SBATCH -A k1016
#SBATCH --partition=workq_low
#SBATCH --nodes=32
#SBATCH --time=4:00:00
#SBATCH --exclusive
#SBATCH --dependency=afterany:194969
#SBATCH --job-name=080
#SBATCH -o /lustre/project/k1016/Sam-algo/JOBS/SAVE/080-%a.out
#SBATCH -e /lustre/project/k1016/Sam-algo/JOBS/SAVE/080-%a.err
task_id=`printf "%03d" "$SLURM_ARRAY_TASK_ID"`
mkdir -p /lustre/project/k1016/Sam-algo/JOBS/RESULTS/080/$task_id
 cd /lustre/project/k1016/Sam-algo/JOBS/RESULTS/080/$task_id 
python -u ../../../SAVE/decime.py --depends --log-dir=../../../LOGS --info-level=1 

            if [ $? -ne 0 ] ; then
                echo "[ERROR] FAILED Job: stopping everything..."
                python -u ../../../SAVE/decime.py --finalize --log-dir=../../../LOGS --info-level=0  
                exit 1
            else 
# START OF ORIGINAL USER SCRIPT  -------------------
#----------------------------------------------------------#
module load vasp/5.3.5
export OMP_NUM_THREADS=1
#----------------------------------------------------------#
ulimit -s unlimited
cp ${VASP_HOME}/vasp_gamma .
srun --ntasks=1024 --hint=nomultithread --ntasks-per-node=32 --ntasks-per-socket=16 --ntasks-per-core=1 --mem_bind=v,local ./vasp_gamma
rm ./vasp_gamma

# END OF ORIGINAL USER SCRIPT  -------------------
python -u ../../../SAVE/decime.py --finalize --log-dir=../../../LOGS --info-level=0 
            fi 
