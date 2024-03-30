#!/bin/bash -le
#PBS -l walltime=01:00:00
#PBS -l filesystems=home:grand
#PBS -q debug-scaling
#PBS -A SuperBERT

# - PBS -l select=10:system=polaris

# Change to working directory
cd /grand/RL-fold/jgpaul/mof-generation-at-scale/

module load conda cudatoolkit-standalone/11.8.0 kokkos

# Activate the environment
conda activate /grand/RL-fold/jgpaul/mof-generation-at-scale/env

NODES=$(< $PBS_NODEFILE wc -l)
PRIMARY_RANK=$(head -n 1 $PBS_NODEFILE)

# Start Redis
redis-server --bind 0.0.0.0 --save "" --appendonly no --logfile redis.log &
redis_pid=$!
echo launched redis on $redis_pid

# Run
python run_parallel_workflow.py \
      --node-path input-files/zn-paddle-pillar/node.json \
      --generator-path input-files/zn-paddle-pillar/geom_difflinker.ckpt \
      --ligand-templates input-files/zn-paddle-pillar/template_*.yml \
      --num-samples 1024 \
      --gen-batch-size 16 \
      --simulation-budget 256 \
      --md-timesteps 1000000 \
      --md-snapshots 10 \
      --redis-host $PRIMARY_RANK \
      --compute-config polaris \
      --ownership
echo Python done

# Shutdown services
kill $redis_pid
