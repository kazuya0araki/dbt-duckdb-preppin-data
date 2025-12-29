from polars import read_csv


def model(dbt, session):
    url = "https://drive.google.com/u/0/uc?id={file_id}&export=download"
    file_id = "1STVYZvXzfGMuEq9Yq3yYOmCDCFq4iB0Z"
    csv = read_csv(url.format(file_id=file_id))
    rename_csv = csv.rename(
        {
            "Flight Details": "flight_details",
            "Flow Card?": "flow_card",
            "Bags Checked": "bags_checked",
            "Meal Type": "meal_type",
        }
    )
    return rename_csv.to_pandas(use_pyarrow_extension_array=True)
