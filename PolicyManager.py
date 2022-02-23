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

from google.cloud import firestore
import uuid, datetime
import constants

db = firestore.Client()

class PolicyManager:
    
    @staticmethod
    def contains_grouping(policies):
        
        is_groups = False
        
        for policy in policies:
            if policy['grouping'] == True:
                is_groups = True
                break
                
        return is_groups
        
    @staticmethod
    def get_scheduled_policies(policy_ids=None):
      
        policies = []
        policy_ref = db.collection('scheduled_policies').order_by('entity_name').order_by('entity_groups').order_by('policy_action')
        
        results = policy_ref.stream() 
        
        for doc in results:
            policy = doc.to_dict()
            
            if policy_ids:
                if policy['policy_id'] in policy_ids:
                    policies.append(policy)
            else:
                policies.append(policy)
        
        return policies
        
    @staticmethod
    def get_ondemand_policies(policy_ids=None):
    
        policies = []
        policy_ref = db.collection('ondemand_policies').order_by('entity_name').order_by('entity_groups').order_by('storage_system')
        
        results = policy_ref.stream() 
        
        for doc in results:
            policy = doc.to_dict()
            
            if policy_ids:
                if policy['policy_id'] in policy_ids:
                    policies.append(policy)
            else:
                policies.append(policy)
        
        return policies

    @staticmethod
    def write_update_scheduled_policy(policy_id, storage_system, entity_name, entity_path, grouping, entity_groups, ts_column, 
                                      sql_filter_exp, retention_period, retention_unit, policy_action):
        
        print('enter write_update_scheduled_policy')
        
        status = constants.SUCCESS
        
        policies = db.collection('scheduled_policies')
        
        if policy_id:
            doc_ref = policies.document(policy_id)
        
        else:
            policy_id = uuid.uuid1().hex
            doc_ref = policies.document(policy_id)

        if grouping == 'true' or grouping == 'True':
            grouping = True
        else:
            grouping = False

        try:
            doc_ref.set({
                'policy_id': policy_id,
                'date_created': datetime.datetime.utcnow(),
                'storage_system': storage_system,
                'entity_name': entity_name,
                'entity_path': entity_path,
                'grouping': grouping,
                'entity_groups': entity_groups,
                'ts_column': ts_column,
                'sql_filter_exp': sql_filter_exp,
                'retention_period': int(retention_period),
                'retention_unit': retention_unit,
                'policy_action': policy_action
            })
            
            print('wrote scheduled_policy: ' + policy_id)
            
        except Exception as e:
            status = constants.ERROR
            print('failed to write scheduled policy {}'.format(e))
        
        return status
        
    
    @staticmethod
    def write_update_ondemand_policy(policy_id, storage_system, entity_name, entity_path, grouping, entity_groups, sql_filter_exp,\
                                     softdelete_period, softdelete_unit, policy_action):
        
        status = constants.SUCCESS
        
        policies = db.collection('ondemand_policies')
        
        if policy_id:
            doc_ref = policies.document(policy_id)
        else:
            policy_id = uuid.uuid1().hex
            doc_ref = policies.document(policy_id)

        if grouping == 'true' or grouping == 'True':
            grouping = True
        else:
            grouping = False

        try:
            doc_ref.set({
                'policy_id': policy_id,
                'date_created': datetime.datetime.utcnow(),
                'storage_system': storage_system,
                'entity_name': entity_name,
                'entity_path': entity_path,
                'grouping': grouping,
                'entity_groups': entity_groups,
                'sql_filter_exp': sql_filter_exp,
                'softdelete_period': softdelete_period,
                'softdelete_unit': softdelete_unit,
                'policy_action': policy_action
            })
            
        except Exception as e:
            status = constants.ERROR
            print('failed to write ondemand policy {}'.format(e))
        
        return status
            
if __name__ == '__main__':
        
    pm.write_scheduled_policy('BQ', 'customer', 'sakila_archive/customer', True, 'Customer,Sales', 'last_update', 
                              'customer_id = 1', 0, 'DAY', 'ARCHIVE')
    