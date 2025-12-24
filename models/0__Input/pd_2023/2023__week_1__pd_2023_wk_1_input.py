from pandas import read_csv


def model(dbt, session):
    url = "https://drive.google.com/u/0/uc?id={file_id}&export=download"
    file_id = "1oln2ri6nu1wDQfT3gQMLLNlmQ2h6B9d9"
    csv = read_csv(url.format(file_id=file_id))
    rename_csv = csv.rename(
        columns={
            "Transaction Code": "transaction_code",
            "Value": "value",
            "Customer Code": "customer_code",
            "Online or In-Person": "online_or_in_person",
            "Transaction Date": "transaction_date",
        }
    )
    return rename_csv
