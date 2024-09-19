#!/bin/bash
set -e # exits upon first error in any of the commands

# List of databases
DATABASES=("it")

# Run snakemake for each database in parallel
for db_name in "${DATABASES[@]}"; do 
    export DB_NAME=$db_name
    # populate DB  
    cd snakemake/climate_vars
    START=$(date +%s)
    snakemake -c 1 -j 1
    END=$(date +%s)

    duration=$((END - START))
    echo "Time taken: $duration seconds ===================================="

    cd ../collected_vars
    START=$(date +%s)
    snakemake -c 1 -j 1
    END=$(date +%s)

    duration=$((END - START))
    echo "Time taken: $duration seconds ===================================="

    cd ../eucalc_vars
    START=$(date +%s)
    snakemake -c 1 -j 1
    END=$(date +%s)

    duration=$((END - START))
    echo "Time taken: $duration seconds ===================================="
done