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
    
    # copy and agggregate data 
    cd snakemake/copy_and_agg
    START=$(date +%s)
    snakemake -c 1 -j 1
    END=$(date +%s)

    duration=$((END - START))
    echo "Time taken to copy and aggregate data: $duration seconds ===================================="

    # Disaggregate to NUTS1
    cd ../nuts1_disagg
    START=$(date +%s)
    snakemake -c 1 -j 1
    END=$(date +%s)

    duration=$((END - START))
    echo "Time taken to disaggregate to NUTS1: $duration seconds ===================================="

    # Disaggregate to NUTS2
    cd ../nuts2_disagg
    START=$(date +%s)
    snakemake -c 1 -j 1
    END=$(date +%s)

    duration=$((END - START))
    echo "Time taken to disaggregate to NUTS2: $duration seconds ===================================="

    # Disaggregate to NUTS3
    cd ../nuts3_disagg
    START=$(date +%s)
    snakemake -c 1 -j 1
    END=$(date +%s)

    duration=$((END - START))
    echo "Time taken to disaggregate to NUTS3: $duration seconds ===================================="

    # Disaggregate to LAU
    cd ../lau_disagg
    START=$(date +%s)
    snakemake -c 1 -j 1
    END=$(date +%s)

    duration=$((END - START))
    echo "Time taken to disaggregate to LAU: $duration seconds ===================================="

    # Post disaggregation calculation 
    cd ../post_disagg_calculation
    START=$(date +%s)
    snakemake -c 1 -j 1
    END=$(date +%s)

    duration=$((END - START))
    echo "Time taken to perform post disaggregation calculation: $duration seconds ===================================="

    cd ../..
    sudo systemctl restart nginx
    sudo systemctl restart gunicorn

done