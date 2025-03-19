import duckdb
import polars as pl

conn = duckdb.connect("tuvaduck.duckdb")

fp = '/nfs/turbo/ihpi-cms/Wiens_ML/users/lindsay/tuva-preprocessing/data'

# df = pl.from_arrow(
#     conn.execute("select * from input_layer.medical_claim")
#     .fetch_arrow_table()
# )
# df.write_parquet(f'{fp}/medical_claim.parquet')
# print('saved medical claim', df.shape)

for i in [
    'carrier', 'dme', 'home_health', 'hospice', 'inpatient',
    'outpatient', 'snf',
]:
    sql = f"select * from _int_input_layer.{i}_claim"
    df = pl.from_arrow(conn.execute(sql).fetch_arrow_table())
    df.write_parquet(f'{fp}/{i}.parquet')
    print(f'saved {i}', df.shape)
