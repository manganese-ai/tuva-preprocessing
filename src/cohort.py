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
save_path = "/nfs/turbo/ihpi-cms/Wiens_ML/users/lindsay/tuva-preprocessing/seeds"     # noqa
denom18_path = "/nfs/turbo/ihpi-cms/Wiens_ML/parquet_data/den18p20.parquet/*.parquet"  # noqa
denom19_path = "/nfs/turbo/ihpi-cms/Wiens_ML/parquet_data/den19p20.parquet/*.parquet"  # noqa

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


denom18 = (
    pl.scan_parquet(denom18_path).
    rename({"bene_id": "BENE_ID"})
    .select(cols)
)
denom19 = pl.scan_parquet(denom19_path).select(cols)

benes18 = filter_2018(denom18)
benes19 = filter_2019(denom19)

benes18 = benes18.select(["BENE_ID"]).collect()
benes19 = benes19.select(["BENE_ID"]).collect()

original_cohort = list(
    set(benes18["BENE_ID"].to_list()) & set(benes19["BENE_ID"].to_list())
)

df = pl.DataFrame({"patient_id": original_cohort})
df.write_csv(f'{save_path}/cohort.csv')
