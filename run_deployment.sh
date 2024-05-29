#!/bin/bash
set -e # exits upon first error in any of the commands

# update the system
sudo apt-get update && sudo apt-get upgrade --yes
sudo apt clean
sudo apt autoremove

eval "$(micromamba shell hook --shell=bash)"
micromamba activate zoomin

# List of databases
DATABASES=("de" "es" "pl" "be" "el" "lt" "pt" "bg" "lu" "ro" "cz" "fr" "hu" "si" "dk" "hr" "mt" "sk" "it" "nl" "fi" "ee" "cy" "at" "se" "ie" "lv")

# Run snakemake for each database in parallel
for db_name in "${DATABASES[@]}"; do 
    export DB_NAME=$db_name
    # populate DB  
    cd snakemake/climate_vars
    START=$(date +%s)
    snakemake -c 10 -j 10
    END=$(date +%s)

    duration=$((END - START))
    echo "Time taken: $duration seconds ===================================="

    cd ../collected_vars
    START=$(date +%s)
    snakemake -c 10 -j 10
    END=$(date +%s)

    duration=$((END - START))
    echo "Time taken: $duration seconds ===================================="

    cd ../eucalc_vars
    START=$(date +%s)
    snakemake -c 10 -j 10
    END=$(date +%s)

    duration=$((END - START))
    echo "Time taken: $duration seconds ===================================="
done