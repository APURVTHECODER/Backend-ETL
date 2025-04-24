from fastapi import FastAPI, File, UploadFile, HTTPException
import shutil
import os
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud.pubsub_v1 import PublisherClient
from datetime import timedelta
from pydantic import BaseModel

import logging
import tempfile

from fastapi.middleware.cors import CORSMiddleware
app = FastAPI()  # 👈 This is what was missing
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
creds = service_account.Credentials.from_service_account_file("sl-etb-bot.json")
publisher = PublisherClient(credentials=creds)
GCP_PROJECT = "crafty-tracker-457215-g6"
PUBSUB_TOPIC = "mytopic78600"  # fallback if env not set
topic_path = publisher.topic_path(GCP_PROJECT, PUBSUB_TOPIC)



app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 👈 Allow your frontend origin
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods (POST, GET, etc)
    allow_headers=["*"],  # Allow all headers
)

BUCKET = "mybucket78600"  # 👈 Move your bucket name to a constant for reuse

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket using explicit service account credentials."""
    creds = service_account.Credentials.from_service_account_file("sl-etb-bot.json")
    storage_client = storage.Client(credentials=creds, project="crafty-tracker-457215-g6")
    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}."
    )

@app.get("/api/upload-url")
def get_upload_url(filename: str):
    creds = service_account.Credentials.from_service_account_file("sl-etb-bot.json")
    storage_client = storage.Client(credentials=creds, project=GCP_PROJECT)
    bucket = storage_client.bucket(BUCKET)
    blob = bucket.blob(f"uploads/{filename}")
    url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(minutes=15),
        method="PUT",
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    return {"url": url, "object_name": blob.name}


class ETLRequest(BaseModel):
    object_name: str

@app.post("/api/trigger-etl")
def trigger_etl(payload: ETLRequest):
    data = payload.object_name.encode("utf-8")
    future = publisher.publish(topic_path, data=data)
    future.result()
    return {"status": "queued", "object_name": payload.object_name}


# @app.post("/api/upload-file")
# async def upload_file(file: UploadFile = File(...)):
#     try:
#         with tempfile.NamedTemporaryFile(delete=False) as tmp:
#             shutil.copyfileobj(file.file, tmp)
#             temp_path = tmp.name

#         destination_blob_name = f"uploads/{file.filename}"
#         upload_blob(BUCKET, temp_path, destination_blob_name)

#         os.remove(temp_path)

#         return {"message": f"File '{file.filename}' uploaded successfully."}
#     except Exception as e:
#         logger.error(f"Error uploading file: {e}")
#         raise HTTPException(status_code=500, detail=str(e))


