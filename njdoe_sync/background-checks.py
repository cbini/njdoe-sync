import datetime
import json
import os
import pathlib
import sys
import traceback
from collections import deque

import njdoe
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
ADP_MODULE_PATH = os.getenv("ADP_MODULE_PATH")
ADP_CLIENT_ID = os.getenv("ADP_CLIENT_ID")
ADP_CLIENT_SECRET = os.getenv("ADP_CLIENT_SECRET")
ADP_CERT_FILEPATH = os.getenv("ADP_CERT_FILEPATH")
ADP_KEY_FILEPATH = os.getenv("ADP_KEY_FILEPATH")

PROJECT_PATH = pathlib.Path(__file__).parent.absolute()
NOW_TIMESTAMP = datetime.datetime.now()
NOW_DATE_ISO = NOW_TIMESTAMP.date().isoformat()

sys.path.insert(0, ADP_MODULE_PATH)
import adp


def main():
    file_dir = PROJECT_PATH / "data" / "background_check"
    if not file_dir.exists():
        file_dir.mkdir(parents=True)

    adp_client = adp.authorize(
        ADP_CLIENT_ID, ADP_CLIENT_SECRET, ADP_CERT_FILEPATH, ADP_KEY_FILEPATH
    )
    adp_client.headers["Accept"] = "application/json;masked=false"

    gcs_client = storage.Client()
    gcs_bucket = gcs_client.bucket(GCS_BUCKET_NAME)

    print("Downloading woker data from ADP...")
    querystring = {
        "$select": ",".join(
            [
                "worker/person/governmentIDs",
                "worker/person/birthDate",
                "worker/customFieldGroup/stringFields",
                "worker/workAssignments/homeOrganizationalUnits",
            ]
        ),
        "$filter": "workers/workAssignments/assignmentStatus/statusCode/codeValue eq 'A'",
    }
    all_staff = adp.get_all_records(adp_client, "/hr/v2/workers", querystring)

    for p in all_staff:
        home_org_units = next(
            iter([w.get("homeOrganizationalUnits") for w in p.get("workAssignments")]),
            None,
        )
        business_unit = next(
            iter(
                [
                    u.get("nameCode").get("codeValue")
                    for u in home_org_units
                    if u.get("typeCode").get("codeValue") == "Business Unit"
                ]
            ),
            None,
        )
        if not business_unit in ["KCNA", "KIPP_TAF", "TEAM"]:
            continue

        worker_id = p.get("workerID").get("idValue")
        govt_ids = p.get("person").get("governmentIDs")
        ssn = next(
            iter(
                [
                    gi.get("idValue")
                    for gi in govt_ids
                    if gi.get("nameCode").get("codeValue") == "SSN"
                ]
            ),
            None,
        )

        custom_fields_str = p.get("customFieldGroup").get("stringFields", [])
        employee_number = next(
            iter(
                [
                    sf.get("stringValue")
                    for sf in custom_fields_str
                    if sf.get("nameCode").get("codeValue") == "Employee Number"
                ]
            ),
            None,
        )

        birth_date = p.get("person").get("birthDate")
        dob = deque(birth_date.split("-"))
        dob.rotate(-1)

        if not all([employee_number, ssn, dob]):
            print(f"{worker_id}\n\tMISSING DATA")
            continue

        try:
            bg = njdoe.criminal_history.get_applicant_approval_employment_history(
                *ssn.split("-"), *dob
            )
            if bg:
                bg["worker_id"] = worker_id
                bg["employee_number"] = employee_number

                file_name = f"njdoe_backround_check_records_{employee_number}.json"
                file_path = file_dir / file_name
                with open(file_path, "w+") as f:
                    json.dump(bg, f)

                destination_blob_name = f"njdoe/{'/'.join(file_path.parts[-2:])}"
                blob = gcs_bucket.blob(destination_blob_name)
                blob.upload_from_filename(file_path)
                print(f"{worker_id}\n\tUploaded to {destination_blob_name}!")
            else:
                print(f"{worker_id}\n\tNO MATCH")
        except Exception as xc:
            print(f"{worker_id}\n\tERROR")
            print(xc)
            print(traceback.format_exc())


if __name__ == "__main__":
    try:
        main()
    except Exception as xc:
        print(xc)
        print(traceback.format_exc())
