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
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

import uuid
from datetime import datetime
from dateutil.relativedelta import relativedelta

db = firestore.Client()
bq = bigquery.Client()

class Utils:
    
    @staticmethod
    def get_ts_value(ts_column_type, retention_period, retention_unit):
        
         print('inside get_ts_value(), ts_column_type: ' + ts_column_type + ', retention_unit: ' + retention_unit)
         current_date = datetime.today()
         ts_value = ""
         
         if ts_column_type == 'DATE':
                  
             if retention_unit.lower() == 'year': 
                 ts_value = (current_date - relativedelta(years=retention_period)).strftime('%Y-%m-%d')
             
             if retention_unit.lower() == 'month':
                 ts_value = (current_date - relativedelta(months=retention_period)).strftime('%Y-%m-%d')
         
             if retention_unit.lower() == 'day':
                 ts_value = (current_date - relativedelta(days=retention_period)).strftime('%Y-%m-%d')
                 
          
         if ts_column_type == 'TIMESTAMP':
                  
              if retention_unit.lower() == 'year': 
                  ts_value = (current_date - relativedelta(years=retention_period)).strftime('%Y-%m-%d %H:%M:%S')
             
              if retention_unit.lower() == 'month':
                  ts_value = (current_date - relativedelta(months=retention_period)).strftime('%Y-%m-%d %H:%M:%S')
         
              if retention_unit.lower() == 'day':
                  ts_value = (current_date - relativedelta(days=retention_period)).strftime('%Y-%m-%d %H:%M:%S')
                 
             
         return ts_value
    
    @staticmethod
    def lookup_column_type(entity_path, column):
    
        #print('inside lookup_column_type(), entity_path: ' + entity_path)
        column_type = None
        
        client = bigquery.Client()
        table = client.get_table(entity_path)
        schema_fields = table.schema
        
        for field in schema_fields:
            if field.name == column:
                column_type = field.field_type
                break
        
        return column_type
    
    
    @staticmethod
    def construct_datesub(ts_column_type, retention_period, retention_unit):
        
        if ts_column_type == 'DATE':
            current_date = 'current_date'
            
        if ts_column_type == 'DATETIME':
            current_date = 'current_datetime'
            
        if ts_column_type == 'TIMESTAMP':
            current_date = 'current_timestamp'
        
        datesub_clause = ' < date_sub({}, interval '.format(current_date) + str(retention_period) + ' ' + retention_unit + ')'
        
        return datesub_clause    

    @staticmethod
    def construct_dateadd(softdelete_period, softdelete_unit):
        
        dateadd_clause = 'date_add(current_date, interval ' + str(softdelete_period) + ' ' + softdelete_unit + ')'
        
        return dateadd_clause    

    
    @staticmethod
    def create_pyspark_job_config(delete_script, entity_path, file_format, ts_column=None, ts_value=None, sql_filter_exp=None):
        
        if sql_filter_exp is None and ts_column is None and ts_value is None:
            pyspark_job_config = {'main_python_file_uri': delete_script,
                           'properties': {'spark.jars.packages': 'io.delta:delta-core_2.12:1.0.0'},
                            'args': ['--entity_path=' + entity_path, 
                                     '--file_format=' + file_format.lower()]
                                 }
                        
        elif sql_filter_exp is None:
            pyspark_job_config = {'main_python_file_uri': delete_script,
                           'properties': {'spark.jars.packages': 'io.delta:delta-core_2.12:1.0.0'},
                            'args': ['--entity_path=' + entity_path, 
                                     '--file_format=' + file_format.lower(),
                                     '--ts_column=' + ts_column, 
                                     '--ts_value=' + ts_value]
                                 }
        elif ts_column is None and ts_value is None:
            pyspark_job_config = {'main_python_file_uri': delete_script,
                           'properties': {'spark.jars.packages': 'io.delta:delta-core_2.12:1.0.0'},
                            'args': ['--entity_path=' + entity_path, 
                                     '--file_format=' + file_format.lower(),
                                     '--sql_filter_exp=' + sql_filter_exp]
                                 }    
        else:
            pyspark_job_config = {'main_python_file_uri': delete_script,
                           'properties': {'spark.jars.packages': 'io.delta:delta-core_2.12:1.0.0'},
                            'args': ['--entity_path=' + entity_path, 
                                     '--file_format=' + file_format.lower(),
                                     '--ts_column=' + ts_column, 
                                     '--ts_value=' + ts_value,
                                     '--sql_filter_exp=' + sql_filter_exp]
                                 }
                             
        return pyspark_job_config
      
        
    @staticmethod
    def get_foreign_keys():
    
        fk_records = []
        fks_ref = db.collection('foreign_keys')
    
        fks = fks_ref.stream()

        for fk in fks:
            
            fk_dict = fk.to_dict()

            fk_records.append(fk_dict)

        return fk_records
        
    
    @staticmethod
    def get_foreign_key_stmts():
    
        fks_stmts = None
        fks_ref = db.collection('foreign_keys')
    
        fks = fks_ref.stream()

        for fk in fks:
            
            fk_dict = fk.to_dict()

            fk_stmt = 'ALTER TABLE ' + fk_dict['fk_table'] + ' ADD FOREIGN KEY (' + fk_dict['fk_columns'] +\
                      ') REFERENCES ' + fk_dict['pk_table'] + '(' + fk_dict['pk_columns'] + ');\n'
            
            if fks_stmts:
                fks_stmts = fks_stmts + fk_stmt
            else:
                fks_stmts = fk_stmt

        return fks_stmts

    @staticmethod
    def write_foreign_keys(fk_stmts):
        
        fk_coll = db.collection('foreign_keys')
        fk_stmt_list = fk_stmts.split(";")
        
        for fk_stmt in fk_stmt_list:
            #print("fk_stmt: " + fk_stmt)
            
            # parse Foreign Key table
            fk_table_start = fk_stmt.lower().find("alter table ") + 12
            fk_table_end = fk_stmt.find(" ", fk_table_start)
            
            if fk_table_end == -1:
                continue
            
            fk_table = fk_stmt[fk_table_start:fk_table_end]
            
            #print("fk_table: " + str(fk_table))
            
            # parse Primary Key table
            pk_table_start = fk_stmt.lower().find("references ") + 11
            pk_table_end = fk_stmt.find("(", pk_table_start)
            pk_table = fk_stmt[pk_table_start:pk_table_end]
            
            #print("pk_table: " + str(pk_table))
            
            # parse Foreign Key column(s)
            fk_colums_start = fk_stmt.find("(") + 1
            fk_colums_end = fk_stmt.find(")")
            fk_columns = fk_stmt[fk_colums_start:fk_colums_end] 
            
            #print("fk_columns: " + str(fk_columns))
            
            # parse Primary Key column(s)
            pk_colums_start = fk_stmt.rfind("(") + 1
            pk_colums_end = fk_stmt.rfind(")")
            pk_columns = fk_stmt[pk_colums_start:pk_colums_end]
            
            #print("pk_columns: " + str(pk_columns))
            
            doc_id = fk_table + '.' + fk_columns
            doc_ref = fk_coll.document(doc_id)
            
            doc_ref.set({
                'fk_table': fk_table.strip(),
                'fk_columns': fk_columns.strip(),
                'pk_table': pk_table.strip(),
                'pk_columns': pk_columns.strip()
            })
    
        
    @staticmethod
    def table_exists(table_id):

        exists = False
        
        try:
            bq.get_table(table_id)  
            exists = True
        except NotFound:
            exits = False
        
        return exists
        
        
    @staticmethod
    def get_table_group_pairs():
        
        bq_settings = Utils.get_bq_settings()
        groups = Utils.get_groups()
        
        table_group_list = []
        tables = list(bq.list_tables(bigquery.DatasetReference(bq_settings['project'], bq_settings['native_dataset'])))
    
        for table_res in tables:

            table = table_res.table_id
            
            found_group = False
        
            for group in groups:
                
                if group['entity_name'] == table:

                    found_group = True
                    table_tuple = (table, group['entity_group'])
                    table_group_list.append(table_tuple)
                    break
            
            if found_group == False:
                table_tuple = (table, None)
                table_group_list.append(table_tuple)
        
        return table_group_list
        
        
    @staticmethod
    def get_groups():
    
        groups = []
        groups_ref = db.collection('groupings')
    
        results = groups_ref.stream()

        for doc in results:
            groups.append(doc.to_dict())
        
        return groups
    
    
    @staticmethod
    def get_tables():
        
        bq_settings = Utils.get_bq_settings()
        
        table_list = []
        tables = list(bq.list_tables(bigquery.DatasetReference(bq_settings['project'], bq_settings['native_dataset'])))
    
        for table_res in tables:

            table = table_res.table_id
            print('table: ' + table)
            
            table_list.append(table)
            
        return table_list
    
    
    @staticmethod
    def lookup_group(entity_path):    
        
        group_found = None    
        
        results = db.collection('groupings').where('entity_path', '==', entity_path).stream()
        
        for doc in results:

            group_found = doc.to_dict()['entity_group']
        
        return group_found
        
            
    @staticmethod
    def write_group(entity_name, entity_path, entity_group):
        
        mappings = db.collection('groupings')
        doc_ref = mappings.document(entity_path)
        doc_ref.set({
            'entity_name': entity_name,
            'entity_path': entity_path,
            'entity_group': entity_group
        })
        
        print('assigned ' + entity_path + ' to ' + entity_group + ' group')
        
       
    @staticmethod
    def init_bq(project, native_dataset, tombstone_dataset, temp_dataset, external_dataset):
        
        mappings = db.collection('settings')
        doc_ref = mappings.document('bq')
        doc_ref.set({
            'project': project,
            'native_dataset': native_dataset,
            'tombstone_dataset': tombstone_dataset,
            'temp_dataset': temp_dataset,
            'external_dataset': external_dataset
            
        })
        
        print('saved bq settings.')
    
    
    @staticmethod    
    def get_bq_settings():
        
        settings = {}
        
        doc_ref = db.collection('settings').document('bq')

        doc = doc_ref.get()
        
        if doc.exists:
            settings = doc.to_dict()
        
        return settings
    
    
    @staticmethod
    def init_gcs(project, archive_bucket, lake_bucket, file_format, compression):
        
        mappings = db.collection('settings')
        doc_ref = mappings.document('gcs')
        doc_ref.set({
            'project': project,
            'archive_bucket': archive_bucket,
            'lake_bucket': lake_bucket,
            'file_format': file_format,
            'compression': compression
        })
        
        print('saved gcs settings.')

    
    @staticmethod    
    def get_gcs_settings():
        
        settings = {}
        
        doc_ref = db.collection('settings').document('gcs')

        doc = doc_ref.get()
        
        if doc.exists:
            settings = doc.to_dict()
        
        return settings

    
    @staticmethod
    def init_dataproc(project, region, cluster, delete_script):
        
        mappings = db.collection('settings')
        doc_ref = mappings.document('dataproc')
        doc_ref.set({
            'project': project,
            'region': region,
            'cluster': cluster,
            'delete_script': delete_script
        })
        
        print('saved dataproc settings')
        
    @staticmethod    
    def get_dataproc_settings():
        
        settings = {}
        
        doc_ref = db.collection('settings').document('dataproc')

        doc = doc_ref.get()
        
        if doc.exists:
            settings = doc.to_dict()
        
        return settings
        
    @staticmethod
    def write_storage_mappings(policy_name, bq_path, gcs_path):
        
        mappings = db.collection('storage_mappings')
        doc_ref = mappings.document(policy_name)
        doc_ref.set({
            'bq_path': bq_path,
            'gcs_path': gcs_path
        })
        
        print('saved storage mappings.')
        
    @staticmethod    
    def map_entity_path(entity_path, destination_bucket=None):
        
        print('INFO: running map_entity_path(), entity_path: ' + entity_path)
        
        # if a destination_bucket is provided, we are mapping from BQ to GCS (to either archive or lake)
        # if no destination bucket is provided, we are mapping from GCS to BQ
        
        bq_settings = Utils.get_bq_settings()
                    
        if len(entity_path.split('.')) > 1:
            entity_name = entity_path.split('.')[1]
            return destination_bucket + '/' + entity_name
    
        if len(entity_path.split('/')) > 1:
            entity_name = entity_path.split('/')[1]
            return bq_settings['native_dataset'] + '.' + entity_name
        
        return None
                   
    
if __name__ == '__main__':
        
    # write general settings
    #write_dataproc_settings('dgtoolkit', 'us-central1', 'delta-cluster', 'gs://dgtoolkit/scripts/run_delta_deletes.py')
    column_type = Utils.lookup_column_type('sakila_dw.customer', 'last_update')
    print('column_type: ' + column_type)
     