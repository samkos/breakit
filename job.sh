#!/bin/bash
#SBATCH -A k1016
#SBATCH --partition=workq
#SBATCH --nodes=1
#SBATCH --time=0:02:00
#SBATCH --exclusive
#SBATCH --job-name=test

hostname

