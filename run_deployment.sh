#!/bin/bash
set -e # exits upon first error in any of the commands

# update the system
sudo apt-get update && sudo apt-get upgrade --yes
sudo apt clean
sudo apt autoremove

eval "$(micromamba shell hook --shell=bash)"

# activate the environment
micromamba activate zoomin

# populate DB  
cd snakemake/climate_vars
START=$(date +%s)
snakemake -c 5 -j 5
END=$(date +%s)

duration=$((END - START))
echo "Time taken: $duration seconds ===================================="

cd ../collected_vars
START=$(date +%s)
snakemake -c 5 -j 5
END=$(date +%s)

duration=$((END - START))
echo "Time taken: $duration seconds ===================================="

cd ../eucalc_vars
START=$(date +%s)
snakemake -c 5 -j 5
END=$(date +%s)

duration=$((END - START))
echo "Time taken: $duration seconds ===================================="