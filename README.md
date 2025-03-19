# Tuva Preprocessing

### Set up
1. Install DuckDB
    - Download: `curl install.duckdb.org | sh`
    - Set path:
        ```
        nano ~/.bashrc
        export DUCKDB_PATH='/home/lindsaw/.duckdb/cli/latest':$PATH
        source ~/.bashrc
        ```
2. Clone repos
    - In your Michigan user folder: `git clone git@github.com:manganese-ai/tuva-preprocessing.git`
    - Locally (not in Michigan), clone the LDS connector: `git@github.com:tuva-health/medicare_lds_connector.git`
3. Seed the local LDS connector with Tuva tables (Michigan has some permissions issues, so can't do it there):
    - Make sure dbt is set up and connected to a DuckDB database called `tuvaseeded.duckdb`
        - Install DuckDB: `curl install.duckdb.org | sh`
        - Set up local environment to work with DuckDB. Assuming you have a Mac:
        ```
        nano ~/.zshrc 
        export DUCKDB_PATH='/Users/lindw/.duckdb/cli/latest':$PATH
        source ~/.zshrc
        ```
        - Set up dbt: `python -m pip install dbt-core dbt-duckdb`
        - Initialize dbt (if haven't done it before): `dbt init`
        - Add DuckDB to your dbt profile
        ```
        nano ~/.dbt/profiles.yml 
        default:
            outputs:
                duck:
                type: duckdb
                path: /Users/lindw/Desktop/medicare_lds_connector/tuvaseeded.duckdb
            target: duck
        ```
        - Get Tuva set up: `dbt deps`
        - Create the DuckDB database: `duckdb tuvaseeded.duckdb`
        - Seed the connector with the Tuva files (no claims data): `dbt seed`
    - Secure copy these Tuva seeds into your Michigan folder (change my folder path to yours): `scp tuvaseeded.duckdb lindsaw@armis2.arc-ts.umich.edu:/nfs/turbo/ihpi-cms/Wiens_ML/users/lindsay/tuva-preprocessing/`
4. Set up your Michigan tuva-preprocessing folder dbt to work with DuckDB
    ```
    python -m pip install dbt-core dbt-duckdb
    pip install dbt-duckdb[parquet]
    pip install nbstripout
    ```
5. Update your (Michigan) dbt profiles to work with DuckDB. Note that there's an environment variable for decis so we can run and save one deci at a time. I set a default to one deci (`s`), but you don't need the default.
    ```
    nano ~/.dbt/profiles.yml
    default:
    outputs:
        duck:
        type: duckdb
        path: /nfs/turbo/ihpi-cms/Wiens_ML/users/lindsay/tuva-preprocessing/{{ env_var('DECI_RUN', 's') }}.duckdb

    target: duck
    ```
6. Update your `dbt_project.yml` to work with the right dbt profile. Things you need to check:
    - dbt profile: `profile: default`
    - variables: 
        - `input_database` is the name of the subfolder in the `seeds` folder
        - `payment_year` is the payment year you want to run through
        - `years` are the years you want to cycle through
        - `patient_id_suffix` lets you choose which deci you want to run (e.g., all benes ending in `s` or `k`). It's set to an environment variable to be used in the `src/run_dbt_subsets.sh` script
        ```
        vars:
            input_database: ffs_all
            cms_hcc_payment_year: 2019
            years: 2018,2019
            patient_id_suffix: "{{ env_var('DECI_RUN', 's') }}"
        ```
7. Set up Tuva
    - Get dbt to get Tuva set up: `dbt deps`
    - Change the files in `dbt_packages/the_tuva_project` to align with the corrected scripts in the `modified_tuva_scripts` folder
8. Get the claims seeds set up: `cp -rf` the files you need (e.g., all 2018 and 2019 files) from `/nfs/turbo/ihpi-cms/Wiens_ML/parquet_data` into your seeds folder `/nfs/turbo/ihpi-cms/Wiens_ML/users/lindsay/tuva-preprocessing/seeds/ffs_all`
9. Change file locations to align with your folder
    - The `external_location` line in `models/_sources.yml` to align with your folder (keep the variables, just change the base folder part, like `users/lindsay/tuva-preprocessing`)
    - The `fp` line in `src/preprocess_tuva.py`
    - The `save_path` variable  in `src/cohort.py` 
    - The `fp` line in `src/save_duckdb_parquets.py`
10. Make the shell script executable: `chmod +x src/run_dbt_subsets.sh`

### Running
We've had good success using the slurm interactive sessions, instead of submitting a script. You can change whatever variables you need.
`salloc --account=cms_project1 --partition=largemem --nodes=1 --ntasks-per-node=1 --cpus-per-task=1 --mem-per-cpu=100GB --time=2:00:00`

Run the shell script (`src/run_dbt_subsets.sh`), which does the following:
1. Creates the cohort table in the seeds folder (`src/cohort.py`)

For each deci:
2. Copies the Tuva seeds into a deci-specific duckdb (e.g., `deci_s.duckdb`)
3. Seeds the cohort table
4. Runs the dbt models we need: 
    - claims preprocessing (`models` folder): staging, intermediate, final 
    - Tuva models (`dbt_packages/the_tuva_project/models` folder): claims_preprocessing core cms_hcc financial_pmpm
    - Processes and saves parquet files for modeling (`src/preprocess_tuva.py`)

### Saved files (in `data` fodler):
- All condition data: `condition_all_{deci}.parquet`
    - For 2018 and 2019, all columns from `core.condition`

- Modeling condition data (for Franklin): `condition_modeling_{deci}.parquet`
    - For 2018 only, unique combinations of patient id and normalized code from `core.condition`
    - Columns: patient id, normalized code, normalized description

- All procedure data: `procedure_all_{deci}.parquet`
    - For 2018 and 2019, all columns from `core.procedure`

- Modeling procedure data: `procedure_modeling_{deci}.parquet`
    - For 2018 only, unique combinations of patient id, code type (hcpcs or icd-10-pcs), and normalized code from `core.procedure`
    - Columns: patient id, code type, normalized code, description, code modifiers, practioner id (I believe this is NPI)

- Medical claim data: `medical_claim_{deci}.parquet`
    - For 2018 and 2019, the useful columns from claim data from `core.medical_claim`

- Cost demographics data: `cost_demographics_{deci}.parquet`
    - HCC v24 scores (v28 is always 0 in Tuva)
    - Bene demographics: gender, age (normalized), enrollment status, Medicaid status, dual status, original reason for entitlement, institutional status
    - Cost data for 2019: medical paid, log10 medical paid, all cost categories (e.g., outpatient, urgent care)