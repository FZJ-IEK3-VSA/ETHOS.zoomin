#!/bin/bash
set -e # exits upon first error in any of the commands

# List of databases
DATABASES=("it")

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

    # # Disaggregate to NUTS1
    # cd ../nuts1_disagg
    # START=$(date +%s)
    # snakemake -c 1 -j 1
    # END=$(date +%s)

    # duration=$((END - START))
    # echo "Time taken to disaggregate to NUTS1: $duration seconds ===================================="

    # # Disaggregate to NUTS2
    # cd ../nuts2_disagg
    # START=$(date +%s)
    # snakemake -c 1 -j 1
    # END=$(date +%s)

    # duration=$((END - START))
    # echo "Time taken to disaggregate to NUTS2: $duration seconds ===================================="

    # Disaggregate to NUTS3
    cd ../nuts3_disagg
    START=$(date +%s)
    snakemake -c 1 -j 1
    END=$(date +%s)

    duration=$((END - START))
    echo "Time taken to disaggregate to NUTS3: $duration seconds ===================================="

    # # Disaggregate to LAU
    # cd ../lau_disagg
    # START=$(date +%s)
    # snakemake -c 1 -j 1
    # END=$(date +%s)

    # duration=$((END - START))
    # echo "Time taken to disaggregate to LAU: $duration seconds ===================================="

    # Post disaggregation calculation 
    cd ../post_disagg_calculation
    START=$(date +%s)
    snakemake -c 1 -j 1
    END=$(date +%s)

    duration=$((END - START))
    echo "Time taken to perform post disaggregation calculation: $duration seconds ===================================="
done