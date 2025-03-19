##################################################
# This script was run for each deci after completing Tuva

# These files are saved as parquets for each deci:
# 1. condition_{letter}.parquet
# 2. procedure_{letter}.parquet
# 3. medical_claim_{letter}.parquet
# 4. cost_demographics_{letter}.parquet

# Note: inpatient cost here is weird because of our medpar situation.
# See other repo for a workaround
# Also, entitled through disability aged >=65 are included here.
# Can filter out in the preprocess function if want to
##################################################
import os
import duckdb
import polars as pl
import pandas as pd
import numpy as np
import yaml

from functools import partial, reduce

fp = '/nfs/turbo/ihpi-cms/Wiens_ML/users/lindsay/tuva-preprocessing'

subset = os.getenv("DECI_RUN")
if not subset:
    raise ValueError("DECI_RUN environment variable is not set.")

conn = duckdb.connect(f"{fp}/deci_{subset}.duckdb")

with open(f"{fp}/dbt_project.yml", "r") as file:
    config = yaml.safe_load(file)

payment_year = config.get("vars", {}).get("cms_hcc_payment_year", None)
diagnosis_year = payment_year-1


def get_all_condition():
    sql = f"""
    SELECT *
    FROM core.condition
    WHERE YEAR(recorded_date) IN ({diagnosis_year},{payment_year})
    ORDER BY person_id, claim_id, recorded_date, normalized_code ASC
    ;"""
    df = pl.from_arrow(conn.execute(sql).fetch_arrow_table())
    return df.drop('patient_id').rename({'person_id': 'patient_id'})


def get_all_procedure():
    sql = f"""
    SELECT *
    FROM core.procedure
    WHERE YEAR(procedure_date) IN ({diagnosis_year},{payment_year})
    ORDER BY
        person_id, claim_id, procedure_date,
        source_code_type, normalized_code ASC
    ;"""
    df = pl.from_arrow(conn.execute(sql).fetch_arrow_table())
    return df.drop('patient_id').rename({'person_id': 'patient_id'})


def get_modeling_condition():
    sql = f"""
    SELECT
        person_id AS patient_id,
        normalized_code AS code,
        normalized_description AS desc
    FROM core.condition
    WHERE YEAR(recorded_date) = {diagnosis_year}
    AND person_id IS NOT NULL
    AND normalized_code IS NOT NULL
    ORDER BY person_id, normalized_code ASC
    ;"""
    df = pl.from_arrow(conn.execute(sql).fetch_arrow_table())
    return df.unique(subset=['patient_id', 'code'])


def get_modeling_procedure():
    sql = f"""
    SELECT
        person_id AS patient_id,
        source_code_type AS code_type,
        normalized_code AS code,
        normalized_description AS desc,
        modifier_1, modifier_2, modifier_3, modifier_4, modifier_5,
        practitioner_id
    FROM core.procedure
    WHERE YEAR(procedure_date) = {diagnosis_year}
    AND person_id IS NOT NULL
    AND normalized_code IS NOT NULL
    ORDER BY person_id, source_code_type, normalized_code ASC
    ;"""
    df = pl.from_arrow(conn.execute(sql).fetch_arrow_table())
    return df.unique(subset=['patient_id', 'code_type', 'code'])


def get_medical_claim():
    sql = f"""
    SELECT
        person_id AS patient_id,
        claim_id, claim_line_number, claim_type,
        encounter_id, encounter_type, encounter_group,
        claim_end_date, claim_line_end_date,
        service_category_1, service_category_2, service_category_3,
        admit_source_description, admit_type_description,
        discharge_disposition_description,
        ms_drg_description, apr_drg_description,
        place_of_service_code, place_of_service_description,
        bill_type_code, bill_type_description,
        revenue_center_code, revenue_center_description,
        service_unit_quantity, hcpcs_code,
        hcpcs_modifier_1, hcpcs_modifier_2, hcpcs_modifier_3,
        hcpcs_modifier_4, hcpcs_modifier_5,
        rendering_id, billing_id, billing_name, facility_id, facility_name,
        paid_amount, allowed_amount, charge_amount, coinsurance_amount,
        copayment_amount, deductible_amount, total_cost_amount,
        enrollment_flag
    FROM core.medical_claim
    WHERE YEAR(claim_end_date) IN ({diagnosis_year},{payment_year})
    ORDER BY person_id, claim_id, claim_line_number ASC
    ;"""
    return pl.from_arrow(conn.execute(sql).fetch_arrow_table())


def get_risk_scores():
    sql = f'''
    SELECT DISTINCT
        person_id AS patient_id,
        v24_risk_score, v28_risk_score,
        payment_risk_score
    FROM cms_hcc.patient_risk_scores
    WHERE payment_year = {payment_year}
    ;'''
    scores = conn.execute(sql).fetchdf()
    return scores


def get_demographic_factors():
    sql = f'''
    SELECT DISTINCT
        person_id AS patient_id,
        risk_factor_description
    FROM cms_hcc.patient_risk_factors
    WHERE factor_type = 'Demographic'
    AND payment_year = {payment_year}
    ;'''
    dem = conn.execute(sql).fetchdf()
    dem[[
        'gender', 'age', 'enrollment_status', 'medicaid_status',
        'dual_status', 'orec', 'institutional_status'
    ]] = dem['risk_factor_description'].str.split(pat=",", expand=True)
    dem['age'] = dem['age'].str.rstrip(' Years')
    dem['enrollment_status'] = (
        dem['enrollment_status'].str.replace(r' Enrollee', '')
    )
    dem['dual_status'] = dem['dual_status'].str.replace(r' Dual', '')
    dem = dem.drop(columns='risk_factor_description')
    for c in dem.columns[1:]:
        dem[c] = dem[c].str.strip()

    return dem


def get_cost():
    """
    payment_year needs to be a string
    """
    sql = f"""
    SELECT
        person_id AS patient_id,
        inpatient_paid, outpatient_paid, office_based_paid, ancillary_paid
        other_paid, pharmacy_paid, acute_inpatient_paid, ambulance_paid
        ambulatory_surgery_center_paid, dialysis_paid,
        durable_medical_equipment_paid, emergency_department_paid,
        home_health_paid, inpatient_hospice_paid, inpatient_psychiatric_paid
        inpatient_rehabilitation_paid, lab_paid, observation_paid,
        office_based_other_paid, office_based_pt_ot_st_paid,
        office_based_radiology_paid, office_based_surgery_paid,
        office_based_visit_paid, other_paid_2, outpatient_hospice_paid,
        outpatient_hospital_or_clinic_paid, outpatient_pt_ot_st_paid,
        outpatient_psychiatric_paid, outpatient_radiology_paid,
        outpatient_rehabilitation_paid, outpatient_surgery_paid,
        pharmacy_paid_2, skilled_nursing_paid, telehealth_visit_paid,
        urgent_care_paid, total_paid, medical_paid,
        total_allowed, medical_allowed
    FROM financial_pmpm.pmpm_prep
    WHERE LEFT(year_month,4) = {payment_year}
    ;"""
    tmp = (
        pl.from_arrow(conn.execute(sql).fetch_arrow_table())
    ).to_pandas()

    return (
        tmp.groupby('patient_id')
        .agg('sum')
        .reset_index(drop=False)
    )


def combine_df(df_list):
    outer_merge = partial(pd.merge, how='outer', on='patient_id')
    df = reduce(outer_merge, df_list)
    return df


def preprocess(df):
    # # drop disabled
    # df = df[df.orec == 'Aged'].reset_index(drop=True)

    df['is_female'] = (df.gender == 'Female').astype(int)

    age_dict = {
        '0-34': 0.,
        '35-44': 35.,
        '45-54': 45.,
        '55-59': 55.,
        '60-64': 60.,
        '65-69': 65.,
        '70-74': 70.,
        '75-79': 75.,
        '80-84': 80.,
        '85-89': 85.,
        '90-94': 90.,
        '>=95': 95.,
    }
    df.age = df.age.map(age_dict) / 100

    cost_cols = [c for c in df.columns if '_paid' in c or '_allowed' in c]
    for c in cost_cols:
        df.loc[df[c] < 0, c] = 0.

    df['log10_medical_paid'] = np.log(df.medical_paid + 1) / np.log(10)
    return df


def main():
    # get conditions
    cond = get_all_condition()
    cond.write_parquet(f"{fp}/data/condition_all_{subset}.parquet")
    print(f"Saved all condition for {subset}:\t\t {len(cond)} rows \t{cond['patient_id'].n_unique()} benes")  # noqa

    cond = get_modeling_condition()
    cond.write_parquet(f"{fp}/data/condition_modeling_{subset}.parquet")
    print(f"Saved modeling condition for {subset}:\t {len(cond)} rows \t{cond['patient_id'].n_unique()} benes")  # noqa

    # get procedures
    proc = get_all_procedure()
    proc.write_parquet(f"{fp}/data/procedure_all_{subset}.parquet")
    print(f"Saved all procedure for {subset}:\t\t {len(proc)} rows \t{proc['patient_id'].n_unique()} benes")  # noqa

    proc = get_modeling_procedure()
    proc.write_parquet(f"{fp}/data/procedure_modeling_{subset}.parquet")
    print(f"Saved modeling procedure for {subset}:\t {len(proc)} rows \t{proc['patient_id'].n_unique()} benes")  # noqa

    # get medical claim
    med = get_medical_claim()
    med.write_parquet(f"{fp}/data/medical_claim_{subset}.parquet")
    print(f"Saved medical claim for {subset}:\t\t {len(med)} rows \t{med['patient_id'].n_unique()} benes")  # noqa

    # get hcc scores and demographics
    scores = get_risk_scores()
    dem = get_demographic_factors()
    mid = combine_df([scores, dem])
    cost = get_cost()
    df = mid.merge(cost, how='left', on='patient_id')

    for c in [i for i in cost.columns if i != 'patient_id']:
        df[c] = df[c].fillna(0.00).astype(float)

    # basic preprocessing
    df = preprocess(df)

    p = f'{fp}/data/cost_demographics_{subset}.parquet'
    df.to_parquet(p)
    print(f"Saved cost / demographics for {subset}:\t {len(df)} rows \t{df.patient_id.nunique()} benes")  # noqa


if __name__ == '__main__':
    main()
