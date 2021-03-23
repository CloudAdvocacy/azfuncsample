import json
import logging
import imageio

import azure.functions as func
import azure.storage.blob as blb
import os
from urllib.parse import urlparse

def main(event: func.EventGridEvent):
    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })

    logging.info('Python EventGrid trigger processed an event: %s', result)

    url = event.get_json()['url']
    logging.info("Reading %s",url)

    reader = imageio.get_reader(url)

    fn = os.path.splitext(os.path.basename(urlparse(url).path))[0]

    storage_conn = os.environ.get('VIDEO_STORAGE_ACCOUNT_CONN')
    blob_service_client = blb.BlobServiceClient.from_connection_string(storage_conn)
    blob = blob_service_client.get_container_client('thumbnail-container')

    n=0
    for frame_number, im in enumerate(reader):
        if frame_number%25==0:
            b = blob.get_blob_client(f"{fn}_{n}.jpg")
            b.upload_blob(imageio.imwrite("<bytes>",im,format='jpeg'))
            n+=1

    logging.info("%d frames uploaded",n)