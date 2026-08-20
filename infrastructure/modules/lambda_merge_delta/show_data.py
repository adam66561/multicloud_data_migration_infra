# from deltalake import DeltaTable
# import pandas as pd

# table_path = "s3://dev-multicloud-lambda-tests/wltuser/lsvcharge"

# df = DeltaTable(table_path).to_pandas()

# pd.set_option("display.max_columns", None)
# pd.set_option("display.width", None)
# pd.set_option("display.max_colwidth", None)

# print(df)



from pathlib import Path
from deltalake import DeltaTable


table_path = "s3://dev-multicloud-lambda-tests/cdc_delta_audit/"
output_path = Path("cdc_delta_audit.csv")

df = DeltaTable(table_path).to_pandas()

df.to_csv(
    output_path,
    index=False,
    encoding="utf-8",
)

print(f"Wrote {len(df)} rows and {len(df.columns)} columns to: {output_path.resolve()}")




# import io

# import boto3
# import pandas as pd

# BUCKET = "dev-multicloud-lambda-tests"
# KEY = "cdc/pasx/batchrecord/data-e4dbfd2c17c04d1c83aebf57de82388d.parquet"


# def main() -> None:
#     s3 = boto3.client("s3")  # Uses your configured AWS credentials and default region.
#     response = s3.get_object(Bucket=BUCKET, Key=KEY)

#     df = pd.read_parquet(io.BytesIO(response["Body"].read()))

#     print(f"Read {len(df):,} rows and {len(df.columns):,} columns from s3://{BUCKET}/{KEY}")
#     print(df.head().to_string())


# if __name__ == "__main__":
#     main()