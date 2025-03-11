import duckdb
import polars as pl

conn = duckdb.connect("tuvaduck.duckdb")

# df = conn.execute(f"select * from input_layer.medical_claim").pl()
# df.write_parquet(f'/nfs/turbo/ihpi-cms/Wiens_ML/users/lindsay/tuva-preprocessing/data/medical_claim.parquet')
# print('saved medical claim', df.shape)

for i in [
    'carrier','dme','home_health','hospice','inpatient','outpatient','snf',
]:
    df = conn.execute(f"select * from _int_input_layer.{i}_claim").pl()
    df.write_parquet(f'/nfs/turbo/ihpi-cms/Wiens_ML/users/lindsay/tuva-preprocessing/data/{i}.parquet')
    print(f'saved {i}', df.shape)