import polars as pl


def model(dbt, session):
    url = "https://drive.google.com/u/0/uc?id={file_id}&export=download"
    file_id = "1oln2ri6nu1wDQfT3gQMLLNlmQ2h6B9d9"
    csv = pl.read_csv(url.format(file_id=file_id))
    data = csv.rename(
        {
            "Transaction Code": "transaction_code",
            "Value": "value",
            "Customer Code": "customer_code",
            "Online or In-Person": "online_or_in_person",
            "Transaction Date": "transaction_date",
        }
    ).cast(
        {
            "transaction_code": pl.String,
            "value": pl.Int64,
            "customer_code": pl.String,
            "online_or_in_person": pl.Int64,
            "transaction_date": pl.String,
        }
    )
    return data.to_pandas(use_pyarrow_extension_array=True)
