#!/bin/bash

#SBATCH --job-name=arrayJob
#SBATCH --output=arrayJob_%A_%a.out
#SBATCH --error=arrayJob_%A_%a.err
#SBATCH --time=00:01:00
#SBATCH --ntasks=1


######################
# Begin work section #
######################

# Print this sub-job's task ID
echo "My SLURM_ARRAY_TASK_ID: " $SLURM_ARRAY_TASK_ID

# Do some work based on the SLURM_ARRAY_TASK_ID
# For example: 
# ./my_process $SLURM_ARRAY_TASK_ID
# 
# where my_process is you executable

