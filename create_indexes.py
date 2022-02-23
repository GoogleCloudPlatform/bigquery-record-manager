# Copyright 2022 Google, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
from google.cloud.firestore_admin_v1.services.firestore_admin.client import FirestoreAdminClient

fs_client = FirestoreAdminClient()

def create_index_1(project_id):
    
    coll = 'projects/{project_id}/databases/(default)/collectionGroups/ondemand_policies'.format(project_id=project_id)
    
    index = {
         "fields": [
        {
          "field_path": "entity_name",
          "order": "ASCENDING"
        },
        {
          "field_path": "entity_groups",
          "order": "ASCENDING"
        },
        {
          "field_path": "storage_system",
          "order": "ASCENDING"
        }],
        "query_scope": "COLLECTION"
        }

    resp = fs_client.create_index(parent=coll, index=index)


def create_index_2(project_id):
    
    coll = 'projects/{project_id}/databases/(default)/collectionGroups/policies'.format(project_id=project_id)
    
    index = {
         "fields": [
        {
          "field_path": "grouping",
          "order": "ASCENDING"
        },
        {
          "field_path": "storage_system",
          "order": "ASCENDING"
        }],
        "query_scope": "COLLECTION"
        }

    resp = fs_client.create_index(parent=coll, index=index)
    

def create_index_3(project_id):
    
    coll = 'projects/{project_id}/databases/(default)/collectionGroups/scheduled_policies'.format(project_id=project_id)
    
    index = {
         "fields": [
        {
          "field_path": "entity_name",
          "order": "ASCENDING"
        },
        {
          "field_path": "entity_groups",
          "order": "ASCENDING"
        },
        {
          "field_path": "policy_action",
          "order": "DESCENDING"
        }],
        "query_scope": "COLLECTION"
        }

    resp = fs_client.create_index(parent=coll, index=index)

   
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="create Firestore indexes")
    parser.add_argument('project_id', help='Google Cloud Project id')
    args = parser.parse_args()
     
    print('creating indexes on project ' + args.project_id)

    create_index_1(args.project_id)
    create_index_2(args.project_id)
    create_index_3(args.project_id)
    
    