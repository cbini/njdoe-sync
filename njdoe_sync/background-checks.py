import json
import os
import pathlib
from datetime import datetime

import njdoe

# import requests
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

PROJECT_PATH = pathlib.Path(__file__).parent.absolute()
NOW_TIMESTAMP = datetime.datetime.now()
NOW_DATE_ISO = NOW_TIMESTAMP.date().isoformat()


def main():
    data_path = PROJECT_PATH / "data"
    if not data_path.exists():
        data_path.mkdir(parents=True)

    gcs_client = storage.Client()
    gcs_bucket = gcs_client.bucket(GCS_BUCKET_NAME)

    njdoe_export_data = [{}]

    for p in njdoe_export_data:
        employee_number = p["Reference_Code"]
        ssn = p["SSN/SIN"] or "000-00-0000"
        birth_date = p["Birth_Date"]

        # ssn1, ssn2, ssn3 = ssn.split("-")
        # dob_clean = datetime.strptime(birth_date, "%m/%d/%Y")
        # dob_month = dob_clean.strftime("%m")
        # dob_day = dob_clean.strftime("%d")
        # dob_year = dob_clean.strftime("%Y")

        try:
            bg = njdoe.criminal_history.applicant_approval_employment_history(
                *ssn.split("-"), *birth_date.split("/")
            )
            if bg:
                bg["employee_number"] = employee_number

                data_filename = f"njdoe_backround_check_records_{employee_number}.json"
                data_filepath = data_path / data_filename
                with open(data_filepath, "w+") as f:
                    json.dump(bg, f)

                destination_blob_name = "njdoe/" + "/".join(data_filename.parts[-2:])
                blob = gcs_bucket.blob(destination_blob_name)
                blob.upload_from_filename(data_filename)
                print(f"\tUploaded to {destination_blob_name}!\n")
            else:
                print(
                    "\tBG NO MATCH -", employee_number, p["First_Name"], p["Last_Name"]
                )
        except:
            print("\tBG ERROR -", employee_number, p["First_Name"], p["Last_Name"])


if __name__ == "__main__":
    try:
        main()
    except Exception as xc:
        print(xc)
