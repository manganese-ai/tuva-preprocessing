####################################################
# if we modify the eligibility / cohort criteria,
# re-run this py script and then re-seed with
# dbt seed -s cohort.csv

# note -- this keeps originally entitled disabled individuals
####################################################

import polars as pl
import warnings

warnings.filterwarnings("ignore")

# cohort, but including decedents and originally entitled disabled
denom18_path = "/nfs/turbo/ihpi-cms/Wiens_ML/parquet_data/den18p20.parquet/*.parquet"  # noqa
denom19_path = "/nfs/turbo/ihpi-cms/Wiens_ML/parquet_data/den19p20.parquet/*.parquet"  # noqa
seed_path = "/nfs/turbo/ihpi-cms/Wiens_ML/users/lindsay/tuva-preprocessing/seeds"      # noqa
demographic_path = "/nfs/turbo/ihpi-cms/Wiens_ML/eda/selection/data/bene_info"         # noqa

is_female_map = {"1": 0, "2": 1}

race_map = {
    "0": "Unknown",
    "1": "White",
    "2": "Black",
    "3": "Other",
    "4": "Asian",
    "5": "Hispanic",
    "6": "North American Native",
}

rti_race_map = {
    "0": "Unknown",
    "1": "Non-Hispanic White",
    "2": "Black (or African-American)",
    "3": "Other",
    "4": "Asian/Pacific Islander",
    "5": "Hispanic",
    "6": "American Indian/Alaska Native",
}

orig_entitlement_map = {
    "0": "Old age and survivor’s insurance (OASI)",
    "1": "Disability insurance benefits (DIB)",
    "2": "End-stage renal disease (ESRD)",
    "3": "Both DIB and ESRD",
}

state_map = {
    "01": "Alabama",
    "02": "Alaska",
    "03": "Arizona",
    "04": "Arkansas",
    "05": "California",
    "06": "Colorado",
    "07": "Connecticut",
    "08": "Delaware",
    "09": "District of Columbia",
    "10": "Florida",
    "11": "Georgia",
    "12": "Hawaii",
    "13": "Idaho",
    "14": "Illinois",
    "15": "Indiana",
    "16": "Iowa",
    "17": "Kansas",
    "18": "Kentucky",
    "19": "Louisiana",
    "20": "Maine",
    "21": "Maryland",
    "22": "Massachusetts",
    "23": "Michigan",
    "24": "Minnesota",
    "25": "Mississippi",
    "26": "Missouri",
    "27": "Montana",
    "28": "Nebraska",
    "29": "Nevada",
    "30": "New Hampshire",
    "31": "New Jersey",
    "32": "New Mexico",
    "33": "New York",
    "34": "North Carolina",
    "35": "North Dakota",
    "36": "Ohio",
    "37": "Oklahoma",
    "38": "Oregon",
    "39": "Pennsylvania",
    "40": "Puerto Rico",
    "41": "Rhode Island",
    "42": "South Carolina",
    "43": "South Dakota",
    "44": "Tennessee",
    "45": "Texas",
    "46": "Utah",
    "47": "Vermont",
    "48": "Virgin Islands",
    "49": "Virginia",
    "50": "Washington",
    "51": "West Virginia",
    "52": "Wisconsin",
    "53": "Wyoming",
    "54": "Africa",
    "55": "Asia",
    "56": "Canada and Islands",
    "57": "Central America and West Indies",
    "58": "Europe",
    "59": "Mexico",
    "60": "Oceania",
    "61": "Philippines",
    "62": "South America",
    "63": "U.S. Possessions",
    "64": "American Samoa",
    "65": "Guam",
    "66": "Commonwealth of the Northern Marianas Islands",
    "67": "Texas",
    "68": "Florida (eff. 10/2005)",
    "69": "Florida (eff. 10/2005)",
    "70": "Kansas (eff. 10/2005)",
    "71": "Louisiana (eff. 10/2005)",
    "72": "Ohio (eff. 10/2005)",
    "73": "Pennsylvania (eff. 10/2005)",
    "74": "Texas (eff. 10/2005)",
    "80": "Maryland (eff. 8/2000)",
    "97": "Northern Marianas",
    "98": "Guam",
    "99": "With 000 county code is American Samoa; otherwise unknown",
}

# relevant columns
cols = [
    "BENE_ID",
    "AGE_AT_END_REF_YR", "BENE_DEATH_DT",
    "ENTLMT_RSN_ORIG", "ENTLMT_RSN_CURR",
    "BENE_HI_CVRAGE_TOT_MONS", "BENE_SMI_CVRAGE_TOT_MONS",
    "BENE_HMO_CVRAGE_TOT_MONS",
    "DUAL_ELGBL_MONS"
]


def filter_2018(df):
    return (
        df.filter(
            (pl.col("BENE_HMO_CVRAGE_TOT_MONS") == 0.0) &
            (pl.col("DUAL_ELGBL_MONS") == 0.0) &
            (pl.col("BENE_SMI_CVRAGE_TOT_MONS") == 12.0) &
            (pl.col("BENE_HI_CVRAGE_TOT_MONS") == 12.0) &
            (pl.col("ENTLMT_RSN_ORIG").is_in(["0", "1"])) &
            (pl.col("ENTLMT_RSN_CURR").is_in(["0", "1"])) &
            (pl.col("AGE_AT_END_REF_YR") >= 65) &
            (pl.col("BENE_DEATH_DT").is_null())
        )
    )


def filter_2019(df):
    return (
        df.filter(
            (pl.col("BENE_HMO_CVRAGE_TOT_MONS") == 0.0) &
            (pl.col("DUAL_ELGBL_MONS") == 0.0) &
            (pl.col("ENTLMT_RSN_ORIG").is_in(["0", "1"])) &
            (pl.col("ENTLMT_RSN_CURR").is_in(["0", "1"]))
        )
    )


def save_demographic_info(save_fp, pids):
    df = (
        pl.scan_parquet(denom18_path)
        .rename({
            'AGE_AT_END_REF_YR': 'age',
            'BENE_DEATH_DT': 'death_date',
            'ENTLMT_RSN_ORIG': 'orig_entitlement',
            'SEX_IDENT_CD': 'is_female',
            'BENE_RACE_CD': 'race',
            'RTI_RACE_CD': 'rti_race',
            'STATE_CODE': 'state',
            'COUNTY_CD': 'county',
            'ZIP_CD': 'zip'
        }).select([
            'bene_id', 'age', 'death_date', 'is_female', 'orig_entitlement',
            'race', 'rti_race', 'state', 'county', 'zip'
        ]).with_columns(
            pl.col("is_female").replace(is_female_map).alias("is_female"),
            pl.col("race").replace(race_map).alias("race"),
            pl.col("rti_race").replace(rti_race_map).alias("rti_race"),
            pl.col("orig_entitlement").replace(orig_entitlement_map).alias("orig_entitlement"),         # noqa
            pl.col('state').replace(state_map).alias('state'),
            pl.when(pl.col('bene_id').str.slice(-1).is_in(["2", "p", "M", "R"])).then(pl.lit('train'))  # noqa
              .when(pl.col('bene_id').str.slice(-1).is_in(["k", "K"])).then(pl.lit('valid'))            # noqa
              .when(pl.col('bene_id').str.slice(-1).is_in(["n", "o"])).then(pl.lit('embeddings'))       # noqa
              .otherwise(pl.lit('test'))
              .alias('set')
        ).with_columns(
            pl.concat_str([pl.col('state'), pl.col('county')], separator="_").alias('state_county'),    # noqa
        ).filter(pl.col("bene_id").is_in(pids))
    ).collect()
    df.write_parquet(f'{save_fp}/ffs_bene_info_new_model.parquet')

# get cohort data
denom18 = pl.scan_parquet(denom18_path).rename({"bene_id": "BENE_ID"}).select(cols)                     # noqa
denom19 = pl.scan_parquet(denom19_path).select(cols)

benes18 = filter_2018(denom18)
benes19 = filter_2019(denom19)

benes18 = benes18.select(["BENE_ID"]).collect()
benes19 = benes19.select(["BENE_ID"]).collect()

original_cohort = list(
    set(benes18["BENE_ID"].to_list()) & set(benes19["BENE_ID"].to_list())
)

# save cohort seed
df = pl.DataFrame({"patient_id": original_cohort})
df.write_csv(f'{seed_path}/cohort.csv')

# save demographic info
save_demographic_info(demographic_path, original_cohort)
