# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


'''
This Cloud function is responsible for:
- Parsing and validating new files added to Cloud Storage.
- Checking for duplications.
- Inserting files' content into BigQuery.
- Logging the ingestion status into Cloud Firestore and Stackdriver.
- Publishing a message to either an error or success topic in Cloud Pub/Sub.
'''

import json
import logging
import os
import traceback
from datetime import datetime

from google.api_core import retry
from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import storage
import pytz



PROJECT_ID = os.getenv('cac-data-warehouse')
BQ_DATASET = 'CAC_Trainers'
BQ_TABLE = 'scheduled_load'
CS = storage.Client()
BQ = bigquery.Client()


def streaming(data, context):
    '''This function is executed whenever a file is added to Cloud Storage'''
    bucket_name = data['bucket']
    file_name = data['name']
    db_ref = DB.document(u'streaming_files/%s' % file_name)


def _insert_into_bigquery(bucket_name, file_name):
    blob = CS.get_bucket(bucket_name).blob(file_name)
    row = json.loads(blob.download_as_string())
    table = BQ.dataset(BQ_DATASET).table(BQ_TABLE)
    errors = BQ.insert_rows_json(table,
                                 json_rows=[row],
                                 row_ids=[file_name],
                                 retry=retry.Retry(deadline=30))
    if errors != []:
        raise BigQueryError(errors)


