import sys, json
import datetime, time
import requests
from google.cloud import storage
from google.cloud import datacatalog
from google.cloud.datacatalog_v1 import types
from google.cloud import bigquery
from google.cloud import logging
from google.cloud.exceptions import NotFound

class Service:

    def __init__(self):
        pass
                
                    
    def search_catalog(self):
    
        retention_records = [] # collect all the tables to be processed along with their retention details
        
        scope = datacatalog.SearchCatalogRequest.Scope()
         
        for project in self.projects_in_scope:
            print('Info: project', project, 'is in scope')
            self.logger.log_text('Info: project ' + project + ' is in scope', severity='INFO')
            
            scope.include_project_ids.append(project)
        
        request = datacatalog.SearchCatalogRequest()
        request.scope = scope
    
        query = 'tag:' + self.template_project + '.' + self.template_id + '.' + self.retention_period_field
        print('Info: using query: ' + query)
        self.logger.log_text('Info: using query: ' + query, severity='INFO')
        request.query = query
        request.page_size = 1
    
        for result in self.dc_client.search_catalog(request):

            if result.integrated_system != types.IntegratedSystem.BIGQUERY:
                continue
            
            #print('Info: found linked resource', result.linked_resource)
                
            fqt = result.linked_resource.replace('//bigquery.googleapis.com/projects/', '').replace('/datasets/', '.').replace('/tables/', '.')
            project = fqt.split('.')[0]
            dataset = fqt.split('.')[1]
            table = fqt.split('.')[2]
            
            print('Info: found tagged table:', table) 
            self.logger.log_text('Info: found tagged table: ' + table, severity='INFO')
            #print('result.linked_resource:', result.linked_resource)
                
            request = datacatalog.LookupEntryRequest()
            request.linked_resource=result.linked_resource
            entry = self.dc_client.lookup_entry(request)
            
            if entry.bigquery_table_spec.table_source_type != types.TableSourceType.BIGQUERY_TABLE:
                continue
            
            #print('entry:', entry)
            
            create_date = entry.source_system_timestamps.create_time.strftime("%Y-%m-%d")
            year = int(create_date.split('-')[0])
            month = int(create_date.split('-')[1])
            day = int(create_date.split('-')[2])
            
            tag_list = self.dc_client.list_tags(parent=entry.name, timeout=120)
        
            for tag in tag_list:
                
                #print('Info: found tag: ', tag)
                
                if tag.template == 'projects/{0}/locations/{1}/tagTemplates/{2}'.format(self.template_project, self.template_region, self.template_id):
                    
                    if self.retention_period_field in tag.fields:
                        field = tag.fields[self.retention_period_field]
                        if field.double_value:
                            retention_period_value = field.double_value
                        else:
                            retention_period_value = None
                            
                    if self.expiration_action_field in tag.fields:
                        field = tag.fields[self.expiration_action_field]
                        if field.enum_value:
                            expiration_action_value = field.enum_value.display_name
                        else:
                            expiration_action_value = None
                    
                    if retention_period_value and expiration_action_value:
                        record = {"project": project, "dataset": dataset, "table": table, "year": year, "month": month, "day": day, \
                                  "retention_period": retention_period_value, "expiration_action": expiration_action_value}
                        retention_records.append(record)
                    break
        
        return retention_records                      


    def create_snapshots(self, retention_records):
            
        for record in retention_records:

            if record['expiration_action'] != 'Purge':
                continue
        
            snapshot_name = record['dataset'] + '_' + record['table']
            snapshot_table = self.snapshot_project + '.' + self.snapshot_dataset + '.' + snapshot_name
            snapshot_expiration = record['retention_period'] + self.snapshot_retention_period
            
            create_date = datetime.datetime(record['year'], record['month'], record['day'])
            expiration = create_date + datetime.timedelta(days=snapshot_expiration)
            
            if expiration == date.today():
                ddl = ('create snapshot table ' + snapshot_table
                        + ' clone ' + record['project'] + '.' + record['dataset'] + '.' + record['table'] 
                        + ' options ('
                        + ' expiration_timestamp = timestamp "' + expiration.strftime("%Y-%m-%d") + '");') 
            
                print('Info: create snapshot table', snapshot_table, 'using DDL:', ddl)
                self.logger.log_text('Info: create snapshot table ' + snapshot_table + ' using DDL: ' + ddl, severity='INFO')
            
            
                try:

                    if self.mode == 'apply':
                    
                        self.bq_client.delete_table(snapshot_table, not_found_ok=True) 
                    
                        print('Info: deleted snapshot table', snapshot_table)
                        self.logger.log_text('Info: deleted snapshot table ' + snapshot_table, severity='INFO')
                    
                        self.bq_client.query(ddl).result()
                
                        print('Info: created snapshot table', snapshot_table)
                        self.logger.log_text('Info: created snapshot table ' + snapshot_table, severity='INFO')
                    
                except Exception as e:
                    print('Error occurred in create_snapshots. Error message:', e)
                    self.logger.log_text('Error occurred in create_snapshots. Error message: ' + str(e), severity='ERROR')  


    def expire_tables(self, retention_records):
    
        for record in retention_records:
        
            if record['expiration_action'] != 'Purge':
                continue
        
            table_id = record['project'] + '.' + record['dataset'] + '.' + record['table']
            table_ref = bigquery.Table.from_string(table_id)
            table = self.bq_client.get_table(table_ref)
        
            create_date = datetime.datetime(record['year'], record['month'], record['day'])
            expiration = create_date + datetime.timedelta(days=record['retention_period']+1)
            
            print('Info: expiration on', table_id, 'should be set to', expiration.strftime("%Y-%m-%d"))
            self.logger.log_text('Info: expiration on ' + table_id + ' should be set to ' + expiration.strftime("%Y-%m-%d"))
            
            try:
                                
                if self.mode == 'apply':
                    table.expires = expiration
                    table = self.bq_client.update_table(table, ["expires"])
                
                    print('Info: set expiration on table', table_id) 
                    self.logger.log_text('Info: set expiration on table ' + table_id, severity='INFO')
            
            except Exception as e:
                print('Error occurred when setting expiration on table', table_id, '. Error message:', e)
                self.logger.log_text('Error occurred when setting expiration on table ' + table_id + '. Error message:' + e, severity='ERROR')  
    
    
    def archive_tables(self, retention_records):
        
        for record in retention_records:
        
            if record['expiration_action'] != 'Archive':
                continue
            
            create_date = datetime.datetime(record['year'], record['month'], record['day'])
            expiration = create_date + datetime.timedelta(days=record['retention_period']+1)
            
            if expiration.date() <= datetime.date.today():
                 
                 table_id = record['project'] + '.' + record['dataset'] + '.' + record['table']
                 
                 try:
                     
                     success, export_file = self.export_table(record['project'], record['dataset'], record['table'], table_id)
                     
                     if success == False:
                         print('Error: Exporting table failed, table', table_id, ' has not been archived.')
                         return
                         
                     external_table = record['dataset'] + '_' + record['table']
                     self.create_biglake_table(record['dataset'], record['table'], external_table, export_file)
                     self.copy_tags(record['project'], record['dataset'], record['table'], self.archives_project, self.archives_dataset,\
                                    external_table)
                     
                     # uncomment the line below to delete archived table
                     #self.bq_client.delete_table(table_id, not_found_ok=True)
                     
                     print('Info: table', table_id, 'has been archived.')
                     self.logger.log_text('Info: table ' + table_id + ' has been archived.', severity='INFO')
                 
                 except Exception as e:
                     print('Error occurred while archiving table ', table_id, '. Error message:', e)
                     self.logger.log_text('Error occurred while archiving table ' + table_id + '. Error message: ' + str(e), severity='ERROR')
    
    
    def export_table(self, project, dataset, table, table_id):
        
        success = True
        config = bigquery.job.ExtractJobConfig()
        config.destination_format = self.export_format
        
        table_ref = bigquery.Table.from_string(table_id)
        
        file_path = 'gs://' + self.archives_bucket + '/' + project + '/' + dataset + '/' + table + '.' + self.export_format
        
        print('Info: export', table_id, 'to', file_path)
        self.logger.log_text('Info: export ' + table_id + ' to ' + file_path)

        try:
            
            if self.mode == 'apply':
                job = self.bq_client.extract_table(source=table_ref, destination_uris=file_path, job_config=config)
                result = job.result()

                print('Info: created external file: ' + file_path)
                self.logger.log_text('Info: created external file: ' + file_path, severity='INFO')
                
        except Exception as e:
            print('Error occurred while exporting the table: ', e)
            self.logger.log_text('Error occurred while exporting the table: ' + str(e), severity='ERROR')
            success = False
            
        return success, file_path
        
    
    def create_biglake_table(self, dataset, table, external_table, export_file):
        
        ddl = ('create or replace external table `' + self.archives_project + '.' + self.archives_dataset + '.' + external_table + '` '
              + 'with connection `' + self.remote_connection + '` '
              + 'options ( '
              + 'format = "' + self.export_format + '", '
              + 'uris = ["' + export_file + '"]);')
        
        print('Info: create the Biglake table using DDL:', ddl)
        self.logger.log_text('Info: create the Biglake table using DDL: ' + ddl, severity='INFO')
         
        try:
            
            if self.mode == 'apply':
                self.bq_client.query(ddl).result() 
                
                print('Info: created the Biglake table', external_table)
                self.logger.log_text('Info: created the Biglake table ' + external_table)
            
        except Exception as e: 
            print('Error occurred during create biglake table: ', e)
            self.logger.log_text('Error occurred while creating thearchives_ biglake table: ' + str(e), severity='ERROR')


    
    def copy_tags(self, source_project, source_dataset, source_table, target_project, target_dataset, target_table):
        
        url = self.tag_engine_endpoint + '/copy_tags'
        payload = json.dumps({'source_project': source_project, 'source_dataset': source_dataset, 'source_table': source_table, 
                              'target_project': target_project, 'target_dataset': target_dataset, 'target_table': target_table})

        print('Info: copy tags using params:', payload)
        self.logger.log_text('Info: copy tags using params: ' + str(payload), severity='INFO')
        
        try:
            
            if self.mode == 'apply':
                res = requests.post(url, data=payload).json()
                
                print('Info: copy tags response:', res)
                self.logger.log_text('Info: copy tags response: ' + str(res), severity='INFO')
        
        except Exception as e: 
            print('Error occurred during copy_tags: ', e)
            self.logger.log_text('Error occurred during copy_tags ' + str(e), severity='ERROR')    
        

    def get_param_file(self, file_path):
    
        print('file_path: ', file_path)
        file_path = file_path.replace('gs://', '')
        bucket = file_path.split('/')[0]
        file_name = file_path.replace(bucket + '/', '')
        
        print('bucket:', bucket)
        print('file_name:', file_name)
    
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket)
        blob = bucket.get_blob(file_name)
        blob_data = blob.download_as_string()
        json_input = json.loads(blob_data) 
        return json_input


    def run_service(self, file_path):
        
        json_input = s.get_param_file(file_path)
        
        if not json_input:
            print('Missing json parameters.')
            sys.exit()
        
        if 'template_id' not in json_input:
            print('Missing template_id parameter.')
            sys.exit()
        else:   
            self.template_id = json_input['template_id']
            print('template_id: ', self.template_id)
    
        if 'template_project' not in json_input:
            print('Missing template_project parameter.')
            sys.exit()
        else:   
            self.template_project = json_input['template_project']
    
        if 'template_region' not in json_input:
            print('Missing template_region parameter.')
            sys.exit()
        else:   
            self.template_region = json_input['template_region']

        if 'retention_period_field' not in json_input:
            print('Missing retention_period_field parameter.')
            sys.exit()
        else:   
            self.retention_period_field = json_input['retention_period_field']

        if 'expiration_action_field' not in json_input:
            print('Missing expiration_action_field parameter.')
            sys.exit()
        else:   
            self.expiration_action_field = json_input['expiration_action_field']

        if 'projects_in_scope' not in json_input:
            print('Missing projects_in_scope parameter.')
            sys.exit()
        else:   
            self.projects_in_scope = json_input['projects_in_scope']

        if 'bigquery_region' not in json_input:
            print('Missing bigquery_region parameter.')
            sys.exit()
        else:   
            self.bigquery_region = json_input['bigquery_region']

        if 'snapshot_project' not in json_input:
            print('Missing snapshot_project parameter.')
            sys.exit()
        else:   
            self.snapshot_project = json_input['snapshot_project']

        if 'snapshot_dataset' not in json_input:
            print('Missing snapshot_dataset parameter.')
            sys.exit()
        else:   
            self.snapshot_dataset = json_input['snapshot_dataset']

        if 'snapshot_retention_period' not in json_input:
            print('Missing snapshot_retention_period parameter.')
            sys.exit()
        else:   
            self.snapshot_retention_period = json_input['snapshot_retention_period']

        if 'archives_bucket' not in json_input:
            print('Missing archives_bucket parameter.')
            sys.exit()
        else:   
            self.archives_bucket = json_input['archives_bucket']

        if 'export_format' not in json_input:
            print('Missing export_format parameter.')
            sys.exit()
        else:   
            self.export_format = json_input['export_format']

        if 'archives_project' not in json_input:
            print('Missing archives_project parameter.')
            sys.exit()
        else:   
            self.archives_project = json_input['archives_project']
        
        if 'archives_dataset' not in json_input:
            print('Missing archives_dataset parameter.')
            sys.exit()
        else:   
            self.archives_dataset = json_input['archives_dataset']

        if 'remote_connection' not in json_input:
            print('Missing remote_connection parameter.')
            sys.exit()
        else:   
            self.remote_connection = json_input['remote_connection']
        
        if 'tag_engine_endpoint' not in json_input:
            print('Missing tag_engine_endpoint parameter.')
            sys.exit()
        else:   
            self.tag_engine_endpoint = json_input['tag_engine_endpoint']
       
        if 'mode' not in json_input:
            print('Missing mode parameter.')
            sys.exit()
        else:   
            if json_input['mode'] != 'validate' and json_input['mode'] != 'apply':
                print('Invalid mode parameter. Must be equal to "validate" or "apply"')
                sys.exit()
            self.mode = json_input['mode']

        # create clients
        log_client = logging.Client()
        self.logger = log_client.logger('Record_Manager_Service')        
        self.dc_client = datacatalog.DataCatalogClient()
        self.bq_client = bigquery.Client(location=self.bigquery_region)
        
        print('Info: running in', self.mode, 'mode.')
        self.logger.log_text('Info: running in ' + self.mode + ' mode', severity='INFO')
        
        # search catalog
        retention_records = s.search_catalog()
        print('Info: found retention records in the catalog:', retention_records)
    
        # process purge actions
        s.create_snapshots(retention_records)
        s.expire_tables(retention_records)
    
        # process archive actions
        s.archive_tables(retention_records)
        

if __name__ == '__main__':
        
    # python Service.py [PARAM_FILE]
    if len(sys.argv) != 2:
        print('python Service.py [PARAM_FILE]')
        sys.exit()
    
    param_file = sys.argv[1].strip()
    print('Info: param_file:', param_file)
   
    if not param_file:
        print('Missing param file for running job. Needs to be equal a GCS path starting with gs://')
        sys.exit()

    s = Service()
    s.run_service(param_file)
     