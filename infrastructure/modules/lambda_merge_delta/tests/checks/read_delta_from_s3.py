from deltalake import DeltaTable
import pandas as pd

table_path = "s3://dev-multicloud-lambda-tests/pasx/batchrecord"

df = DeltaTable(table_path).to_pandas()

pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)

print(df)