#! /bin/bash

# module load conda
# module load cudatoolkit-standalone/11.8.0
# module load kokkos
# conda activate ./env

# Start Redis
# redis-server --bind 0.0.0.0 --appendonly no --logfile redis.log &
# redis_pid=$!
# echo launched redis on $redis_pid

python run_parallel_workflow.py \
      --node-path input-files/zn-paddle-pillar/node.json \
      --generator-path input-files/zn-paddle-pillar/geom_difflinker.ckpt \
      --ligand-templates input-files/zn-paddle-pillar/template_*.yml \
      --num-samples 32 \
      --simulation-budget 4 \
      --compute-config local \
      --queue proxystream

# kill $redis_pid
