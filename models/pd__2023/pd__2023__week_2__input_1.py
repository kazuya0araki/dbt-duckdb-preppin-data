import polars as pl


def model(dbt, session):
    url = "https://drive.google.com/u/0/uc?id={file_id}&export=download"
    file_id = "1AxOZbkyk3goruw8-y9MdjalekwEJyTrf"
    csv = pl.read_csv(url.format(file_id=file_id))
    data = csv.rename(
        {
            "Bank": "bank",
            "SWIFT code": "swift_code",
            "Check Digits": "check_digits",
        }
    ).cast(
        {
            "bank": pl.String,
            "swift_code": pl.String,
            "check_digits": pl.String,
        }
    )
    return data.to_pandas(use_pyarrow_extension_array=True)
