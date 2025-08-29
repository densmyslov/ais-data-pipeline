import polars as pl

def lambda_handler(event, context):
    df = pl.DataFrame({
        "column1": [1, 2, 3],
        "column2": ["a", "b", "c"]
    })
    return df.to_dict()