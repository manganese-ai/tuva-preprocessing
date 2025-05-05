#!/bin/bash

# training set: 2, p, M, R
# validation set: k, K
# embeddings set: n, o
# test set: Y, s
subsets=("2" "p" "M" "R" "k" "K" "n" "o" "Y" "s")

check_status() {
    if [[ $? -ne 0 ]]; then
        echo "❌ Error: $1"
        exit 1
    else
        echo "✅ Success: $2"
    fi
}

# create the cohort table in the seeds folder and save demographic info to the shared folder
python src/cohort.py
check_status "creating cohort table failed" "created cohort table"

for subset in "${subsets[@]}"; do
    # set DECI_RUN to the deci subset
    export DECI_RUN="$subset"
    echo "DECI_RUN is set to: $DECI_RUN"
    
    # copy tuva seeds
    cp tuvaseeded.duckdb "deci_${subset}.duckdb"
    check_status "Copying database for $subset failed." "Copied tuvaseeded.duckdb to deci_${subset}.duckdb"

    # add cohort eligibility seed
    dbt seed -s cohort
    check_status "dbt seed failed for $subset" "dbt seed completed for $subset"

    # run the dbt scripts we need
    dbt run -s +final claims_preprocessing core cms_hcc financial_pmpm
    check_status "dbt run failed for $subset" "dbt run completed for $subset"

    # save preprocessed data
    python src/preprocess_tuva.py
    check_status "Saving preprocessed parquet files failed for $subset" "Saved preprocessed parquet files for $subset"
    
    echo "--------------------------------"
done

echo "All subsets processed successfully."