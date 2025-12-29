import polars as pl


def model(dbt, session):
    url = "https://drive.google.com/u/0/uc?id={file_id}&export=download"
    file_id = "14Vth2qaJPj_Iq0JyC-3aMo9-Jfa9wPXg"
    csv = pl.read_csv(url.format(file_id=file_id))
    data = csv.rename(
        {
            "Transaction ID": "transaction_id",
            "Account Number": "account_number",
            "Sort Code": "sort_code",
            "Bank": "bank",
        }
    ).cast(
        {
            "transaction_id": pl.String,
            "account_number": pl.String,
            "sort_code": pl.String,
            "bank": pl.String,
        }
    )
    return data.to_pandas(use_pyarrow_extension_array=True)
