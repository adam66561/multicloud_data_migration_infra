from deltalake import DeltaTable

table_path = "s3://dev-multicloud-lambda-tests/pasx/batchrecord"

df = DeltaTable(table_path).to_pandas()

print(df)