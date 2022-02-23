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

from datetime import datetime
import time
import os
import networkx as nx
import sys

from google.cloud import firestore
from google.cloud import bigquery
from google.cloud import dataproc_v1 as dataproc

import PolicyManager as pm
import GraphService as gs
import Utils as u

db = firestore.Client()
bq = bigquery.Client()

class Runner:
    
    def __init__(self):
        
        self.G = None
    
    def run_scheduled(self, policy_ids):

        if policy_ids:
            print('INFO: running job on scheduled policy (or policies): ' + policy_ids)
        else:
            print('INFO: running job on all scheduled policies')
        
        policies = pm.PolicyManager.get_scheduled_policies(policy_ids)
        
        if pm.PolicyManager.contains_grouping(policies):
            self.G = gs.GraphService.init_graph()
        
        for policy in policies:
            
            print('INFO: processing policy: ' + policy['policy_id'])
            
            if policy['storage_system'] == 'BQ' and policy['policy_action'] == 'delete':
                
                self.run_bq_delete_active(policy)
            
            if policy['storage_system'] == 'BQ' and policy['policy_action'] == 'archive':
                
                bq_settings = u.Utils.get_bq_settings()
                gcs_settings = u.Utils.get_gcs_settings()
                self.run_bq_migrate_archives(policy, bq_settings, gcs_settings)
                
            if policy['storage_system'] == 'GCS' and policy['policy_action'] == 'delete':
                
                bq_settings = u.Utils.get_bq_settings()
                gcs_settings = u.Utils.get_gcs_settings()
                self.run_gcs_delete_archives(policy, bq_settings, gcs_settings)
                
            # TO DO: implement an archive function for GCS objects for scheduled policies
                
            print('INFO: completed processing policy: ' + policy['policy_id'])
                
                    
    def run_bq_migrate_archives(self, policy, bq_settings, gcs_settings):
        
        print("INFO: running BQ migrate archives routine")
        #print('policy: ' + str(policy))
        
        ts_column_type = u.Utils.lookup_column_type(policy['entity_path'], policy['ts_column'])
        datesub_clause = u.Utils.construct_datesub(ts_column_type, policy['retention_period'], policy['retention_unit'])
        where_clause = 'where ' + policy['ts_column'] + datesub_clause 
        
        if policy['sql_filter_exp'] and policy['sql_filter_exp'] != "":
            where_clause = where_clause + ' and ' + policy['sql_filter_exp']
        
        # check if there are any records to process
        select_stmt = 'select count(*) as count from ' + policy['entity_path'] + ' ' + where_clause
        
        print(select_stmt)
        
        # run select stmt
        query_job = bq.query(select_stmt)  
        results = query_job.result()
        record_count = 0
        
        for result in results:
            
            record_count = result.count
        
        if record_count == 0:
            print('INFO: record_count = 0, exiting BQ archive routine')
            return
                
        # build CTAS stmt for tmp table
        ctas_stmt_source_entity = 'create or replace table ' + bq_settings['temp_dataset'] + '.' + policy['entity_name']\
                                  + ' as select * from ' + policy['entity_path'] + ' ' + where_clause
        
        print(ctas_stmt_source_entity)
        query_job = bq.query(ctas_stmt_source_entity)
        result = query_job.result()
        
        # prepare and run extract job
        file_name = self.run_extract_job(policy['entity_name'], bq_settings, gcs_settings)
        self.create_external_table(policy['entity_name'], bq_settings, gcs_settings, file_name)
        
        if policy['grouping'] == True:
            
            source_entity_path = policy['entity_path']
            entity_groups = policy['entity_groups']
            
            neighbors = gs.GraphService.get_neighbors(self.G, source_entity_path)
            
            for neighbor_entity_path in neighbors:
                
                print('neighbor_entity_path: ' + str(neighbor_entity_path))
                neighbor_entity_name = neighbor_entity_path.split('.')[1]
                print('neighbor_entity_name: ' + str(neighbor_entity_name))
                
                if self.G.nodes[neighbor_entity_path]['group'] in entity_groups:
                    
                    neighbor_id = self.G.edges[source_entity_path, neighbor_entity_path][neighbor_entity_path]
                    source_entity_id = self.G.edges[source_entity_path, neighbor_entity_path][source_entity_path]
                    
                    # construct ctas stmt based on edge attributes
                    ctas_subquery_stmt = ('create or replace table ' + bq_settings['temp_dataset'] + '.' + neighbor_entity_name +
                                         ' as select * from ' + neighbor_entity_path + ' where ' + neighbor_id + ' in (select ' + 
                                         source_entity_id + ' from ' +
                                         source_entity_path + ' ' + where_clause + ')')
                    
                    print(ctas_subquery_stmt)
                    
                    #run all ctas statements with subqueries
                    query_job = bq.query(ctas_subquery_stmt)  
                    result = query_job.result()
        
                    # prepare and run extract job
                    file_name = self.run_extract_job(neighbor_entity_name, bq_settings, gcs_settings)
                    self.create_external_table(neighbor_entity_name, bq_settings, gcs_settings, file_name)
        
        # delete records from active storage
        self.run_bq_delete_active(policy)
     
    def run_extract_job(self, entity_name, bq_settings, gcs_settings):
        
        config = bigquery.job.ExtractJobConfig()
        config.destination_format = gcs_settings['file_format']
        config.compression = gcs_settings['compression']

        # get ref to tmp table
        full_table_id = bq_settings['project'] + '.' + bq_settings['temp_dataset'] + '.' + entity_name
        table = bigquery.Table.from_string(full_table_id)
        
        print('INFO: using tmp table: ' + full_table_id)
        
        file_path = 'gs://' + gcs_settings['archive_bucket'] + '/' + entity_name
        full_file_path = file_path + '/' + datetime.today().strftime('%Y-%m-%d-%H-%M-%S') + '.' + gcs_settings['file_format'].lower()
        
        print('INFO: created external file: ' + full_file_path)
        
        job = bq.extract_table(source=table, destination_uris=full_file_path, job_config=config)
        
        while job.running():
            time.sleep(2)
            
        # delete tmp table
        bq.delete_table(full_table_id, not_found_ok=True)
        print('INFO: deleted tmp table: ' + full_table_id)
            
        return file_path
          
    
    def create_external_table(self, entity_name, bq_settings, gcs_settings, file_path):
                
        dataset_ref = bigquery.DatasetReference(bq_settings['project'], bq_settings['external_dataset'])
        
        ext_table = bigquery.Table(dataset_ref.table(entity_name), schema=None)
        ext_config = bigquery.ExternalConfig(gcs_settings['file_format'])
        ext_config.source_uris = [file_path + '/*.' + gcs_settings['file_format'].lower()]
        ext_table.external_data_configuration = ext_config
        resp = bq.create_table(ext_table, exists_ok=True)
        print('INFO: created external table: ' + bq_settings['project'] + '.' + bq_settings['external_dataset'] + '.' + entity_name)
        
    
    def run_bq_delete_active(self, policy):
        
        print("INFO: running BQ delete active routine")
        
        #print('policy: ' + str(policy))
        
        # build where clause
        ts_column_type = u.Utils.lookup_column_type(policy['entity_path'], policy['ts_column'])
        datesub_clause = u.Utils.construct_datesub(ts_column_type, policy['retention_period'], policy['retention_unit'])
        where_clause = 'where ' + policy['ts_column'] + datesub_clause 
        
        if policy['sql_filter_exp'] and policy['sql_filter_exp'] != "":
            where_clause = where_clause + ' and ' + policy['sql_filter_exp']
            
        # build the rest of the delete stmt
        del_stmt_source_entity = 'delete from ' + policy['entity_path'] + ' ' + where_clause
        
        if policy['grouping'] == True:
            
            source_entity_path = policy['entity_path']
            entity_groups = policy['entity_groups']
            
            neighbors = gs.GraphService.get_neighbors(self.G, source_entity_path)
            
            for neighbor_entity_path in neighbors:
                
                print('INFO: found neighbor: ' + str(neighbor_entity_path))
                
                if self.G.nodes[neighbor_entity_path]['group'] in entity_groups:
                    
                    neighbor_id = self.G.edges[source_entity_path, neighbor_entity_path][neighbor_entity_path]
                    source_entity_id = self.G.edges[source_entity_path, neighbor_entity_path][source_entity_path]
                    
                    # construct delete stmt based on edge attributes
                    del_stmt_subquery = ('delete from ' + neighbor_entity_path + ' where ' + neighbor_id + 
                                         ' in (select ' + source_entity_id + ' from ' +
                                         source_entity_path + ' ' + where_clause + ')')
                    
                    print(del_stmt_subquery)
                    
                    # run all deletes with subqueries
                    query_job = bq.query(del_stmt_subquery)  
                    result = query_job.result()

        # run delete statement on source entity
        print(del_stmt_source_entity)
        query_job = bq.query(del_stmt_source_entity)  
        result = query_job.result()
    
            
    def run_gcs_delete_archives(self, policy, bq_settings, gcs_settings):

        print("INFO: running GCS delete archives routine")
        
        dataproc_settings = u.Utils.get_dataproc_settings()
        
        job_client = dataproc.JobControllerClient(
                client_options={'api_endpoint': '{}-dataproc.googleapis.com:443'.format(dataproc_settings['region'])}
            )
        
        
        # map entity path from GCS to BQ because BQ's entity paths are used to navigate the graph
        source_entity_path = u.Utils.map_entity_path(policy['entity_path'])
        print('INFO: using source_entity_path: ' + source_entity_path)
        
        ts_column_type = u.Utils.lookup_column_type(source_entity_path, policy['ts_column'])
        ts_value = u.Utils.get_ts_value(ts_column_type, policy['retention_period'], policy['retention_unit'])
        print('INFO: using ts_value: ' + ts_value)
        
        datesub_clause = u.Utils.construct_datesub(ts_column_type, policy['retention_period'], policy['retention_unit'])
        print('INFO: using datesub_clause: ' + datesub_clause)
        
        # process neighbor nodes before source entity
        # format for sql_filter_expression param:
        # address_id in (1, 2, 3)
        
        if policy['grouping'] == True:
            
            entity_groups = policy['entity_groups']
            
            neighbors = gs.GraphService.get_neighbors(self.G, source_entity_path)
            
            for neighbor_entity_path in neighbors:
                
                print('INFO: found neighbor: ' + str(neighbor_entity_path))
                
                if self.G.nodes[neighbor_entity_path]['group'] in entity_groups:
                    
                    neighbor_id = self.G.edges[source_entity_path, neighbor_entity_path][neighbor_entity_path]
                    source_entity_id = self.G.edges[source_entity_path, neighbor_entity_path][source_entity_path]

                    # Delta Tables don't support subqueries in deletes, so need to run the select part on external table
                    # construct SQL stmt against external table 
                    external_dataset_entity_path = bq_settings['external_dataset'] + '.' + policy['entity_name']  
                    
                    select_stmt = 'select distinct ' + source_entity_id + ' as id from ' + external_dataset_entity_path + ' where '\
                                 + policy['ts_column'] + datesub_clause 
                    
                    if policy['sql_filter_exp'] is not None:
                        select_stmt = select_stmt + ' and ' + policy['sql_filter_exp']
                    
                    print(select_stmt)
                    
                    # run select stmt
                    query_job = bq.query(select_stmt)  
                    results = query_job.result()
                    
                    ids = None
                    record_count = 0
                
                    for result in results:
                        
                        if record_count == 0:
                            ids = str(result.id) + ','
                        else:
                            ids = ids + str(result.id) + ','
                        
                        record_count = record_count + 1
                    
                    if record_count == 0:
                        print('INFO: record_count = 0, skipping GCS delete job')
                        continue
                    
                    sql_filter_exp = neighbor_id + ' in (' + ids[0:-1] + ')'
                    print('INFO: using sql filter: ' + sql_filter_exp)
                    
                    # map neighbor's path from BQ to GCS
                    neighbor_gcs_path = u.Utils.map_entity_path(neighbor_entity_path, gcs_settings['archive_bucket'])
                    
                    # run pyspark job
                    pyspark_job = u.Utils.create_pyspark_job_config(dataproc_settings['delete_script'], neighbor_gcs_path,\
                                                        gcs_settings['file_format'], None, None, sql_filter_exp)
                                                        
                    print('INFO: created pyspark job: ' + str(pyspark_job))

                    job = {
                        'placement': {'cluster_name': dataproc_settings['cluster']},
                        'pyspark_job': pyspark_job
                    }
                    
                    try:
                        operation = job_client.submit_job_as_operation(
                            request={'project_id': dataproc_settings['project'], 'region': dataproc_settings['region'], 'job': job}
                            )
                        
                        resp = operation.result()
                        print('INFO: pyspark job response: ' + str(resp))
                    
                    except FailedPrecondition as err:
                        print("Dataproc error: {0}".format(err))
        
        # process deletes on source entity node
        
        external_dataset_entity_path = bq_settings['external_dataset'] + '.' + policy['entity_name']  
        
        select_stmt = 'select count(*) as count from ' + external_dataset_entity_path + ' where '\
                     + policy['ts_column'] + datesub_clause 
        
        if policy['sql_filter_exp'] and policy['sql_filter_exp'] != "":
            select_stmt = select_stmt + ' and ' + policy['sql_filter_exp']
        
        print(select_stmt)
        
        # run select stmt
        query_job = bq.query(select_stmt)  
        results = query_job.result()
        record_count = 0
        
        for result in results:
            
            record_count = result.count
        
        if record_count == 0:
            print('INFO: record_count = 0, skipping GCS delete job')
            return
        
        pyspark_job = u.Utils.create_pyspark_job_config(dataproc_settings['delete_script'], policy['entity_path'],\
                                            gcs_settings['file_format'], policy['ts_column'], ts_value, policy['sql_filter_exp'])
                                            
        print('INFO: created pyspark job: ' + str(pyspark_job))

        job = {
            'placement': {'cluster_name': dataproc_settings['cluster']},
            'pyspark_job': pyspark_job
        }

        try:
            operation = job_client.submit_job_as_operation(
                request={'project_id': dataproc_settings['project'], 'region': dataproc_settings['region'], 'job': job}
                )
            
            resp = operation.result()
            print('INFO: pyspark job response: ' + str(resp))
        
        except FailedPrecondition as err:
            print("Dataproc error: {0}".format(err))
                
############################################################
#### On-Demand methods ####

    def run_on_demand(self, policy_ids=None):

        if policy_ids:
            print('INFO: running job on on-demand policy (or policies): ' + policy_ids)
        else:
            print('INFO: running job on all on-demand policies')
        
        bq_settings = u.Utils.get_bq_settings()
        
        policies = pm.PolicyManager.get_ondemand_policies(policy_ids)
        
        if pm.PolicyManager.contains_grouping(policies):
            self.G = gs.GraphService.init_graph()
    
        for policy in policies:
            
            print('INFO: processing policy: ' + policy['policy_id'])

            if policy['storage_system'] == 'BQ' and policy['policy_action'] == 'delete':
                
                self.run_bq_on_demand_deletes(policy, bq_settings)
                
                # TO DO: implement a generic purge routine which routinely scans the softdelete tables and removes records which 
                # are eligible to be purged (i.e. once softdeletion period has expired) 
                
            if policy['storage_system'] == 'GCS' and policy['policy_action'] == 'delete':
                
                print('Future: GCS delete action not implemented')
                gcs_settings = u.Utils.get_gcs_settings()
                
                # TO DO: implement gcs soft-deletes and purges once ISSUE 826 has been fixed (https://github.com/delta-io/delta/issues/826). Without subquery
                # support, we really can't issue deletes in GCS over connected tables because passing large
                # inputs from source to target table is infeasible
                # self.run_gcs_delete_active(policy, bq_settings, gcs_settings)
                
            print('INFO: completed processing policy: ' + policy['policy_id'])

    
    def run_bq_on_demand_deletes(self, policy, bq_settings):
        
        print("INFO: running BQ on-demand deletes routine")
        #print('policy: ' + str(policy))
        
        source_entity_path = policy['entity_path']
        source_entity_name = policy['entity_name']
        softdelete_period = policy['softdelete_period']
        softdelete_unit = policy['softdelete_unit']
        sql_filter_exp = policy['sql_filter_exp']
        
        # check to see if any records qualify for soft-deletion
        soft_delete_check = 'select count(*) as count from ' + source_entity_path + ' where ' + sql_filter_exp
        
        print(soft_delete_check)
        query_job = bq.query(soft_delete_check)
        results = query_job.result()
        
        record_count = 0
        for result in results:
            record_count = result.count
        
        # found records to soft-delete   
        if record_count == 0:
            print('INFO: found no records to soft-delete in ' + source_entity_path + ', exiting routine')
            return
        
        if policy['grouping'] == True:
            
            entity_groups = policy['entity_groups']
            
            # keep track of visited nodes
            visited_nodes = set()
            visited_nodes.add(source_entity_name)
            
            self.search_graph(policy, entity_groups, source_entity_path, source_entity_name, visited_nodes, bq_settings)
        

        # run softdelete and purge on source entity node, neighbor attributes and origin entity path are None as they are not needed
        self.run_bq_softdelete(softdelete_period, softdelete_unit, None, sql_filter_exp, source_entity_name, source_entity_path, \
                               None, None, None, None, bq_settings)  
                
        self.run_bq_purge(source_entity_name, bq_settings)
        
    
    def search_graph(self, policy, entity_groups, source_entity_path, source_entity_name, visited_nodes, bq_settings):
                
        print('INFO: searching graph')
        
        neighbors = gs.GraphService.get_neighbors(self.G, source_entity_path)
    
        for neighbor_entity_path in neighbors:
        
            neighbor_entity_name = neighbor_entity_path.split('.')[1]
            
            print('INFO: evaluating source_entity_name: ' + source_entity_name + ' and neighbor_entity_name: ' + neighbor_entity_name)
            
            if self.G.nodes[neighbor_entity_path]['group'] in entity_groups:
            
                if neighbor_entity_name in visited_nodes:
                    print('INFO: ' + neighbor_entity_name + ' already in visited nodes, going to next loop iteration')
                    continue
        
                visited_nodes.add(neighbor_entity_name)
                print('INFO: added ' + neighbor_entity_name + ' to the ' + str(visited_nodes))
                
                neighbor_entity_id = self.G.edges[source_entity_path, neighbor_entity_path][neighbor_entity_path]
                source_entity_id = self.G.edges[source_entity_path, neighbor_entity_path][source_entity_path]
            

                sql_filter_exp = self.construct_sql_filter_exp(policy['sql_filter_exp'], policy['entity_path'],\
                                                               neighbor_entity_id, source_entity_id,\
                                                               source_entity_name, source_entity_path, bq_settings)
                print('INFO: using sql filter: ' + sql_filter_exp)
            
            
                self.run_bq_softdelete(policy['softdelete_period'], policy['softdelete_unit'], policy['entity_path'],\
                                       sql_filter_exp, source_entity_name, source_entity_path, source_entity_id,\
                                       neighbor_entity_name, neighbor_entity_path,\
                                       neighbor_entity_id, bq_settings)
            
                self.run_bq_purge(neighbor_entity_name, bq_settings)
                
                self.search_graph(policy, entity_groups, neighbor_entity_path, neighbor_entity_name, visited_nodes, bq_settings)
            
            else:
                print('INFO: ' + neighbor_entity_path + ' not in groups ' + entity_groups)
    
    
    def construct_sql_filter_exp(self, origin_sql_filter_exp, origin_entity_path, neighbor_entity_id, source_entity_id,\
                                source_entity_name, source_entity_path, bq_settings):
        
        # don't remove this condition, needed because these records are still in source entity, 
        # they are only migrated to the tombstone dataset until all other linked entities have been processed
        if origin_entity_path == source_entity_path:
            sql_filter_exp = (neighbor_entity_id + ' in (select distinct ' + source_entity_id + ' from ' + 
                              source_entity_path + ' where ' + origin_sql_filter_exp + ')')
        else:
            sql_filter_exp = (neighbor_entity_id + ' in (select distinct ' + source_entity_id + ' from ' + 
                              bq_settings['tombstone_dataset'] + '.' + source_entity_name + 
                              ' where softdelete_date = current_date)')
                
        return sql_filter_exp
        
    
    def run_bq_softdelete(self, softdelete_period, softdelete_unit, origin_entity_path, sql_filter_exp, source_entity_name,
                          source_entity_path, source_entity_id, neighbor_entity_name, neighbor_entity_path, \
                          neighbor_entity_id, bq_settings):
        
        print("INFO: running BQ soft-delete routine")
         
        where_clause = ' where ' + sql_filter_exp
              
        # check to see if any records qualify for soft-deletion
        if neighbor_entity_path:
            
            soft_delete_check = ('select count(*) as count from ' + neighbor_entity_path + where_clause)

            print(soft_delete_check)
            query_job = bq.query(soft_delete_check)
            results = query_job.result()
        
            record_count = 0
            
            for result in results:
                record_count = result.count
        
            # found records to soft-delete   
            if record_count == 0:
                print('INFO: found no records to soft-delete in ' + neighbor_entity_path + ', exiting routine')
                return
            
        # check if tombstone table exists
        if neighbor_entity_path:
            table_id = bq_settings['project'] + '.' + bq_settings['tombstone_dataset'] + '.' + neighbor_entity_name
        else:
            table_id = bq_settings['project'] + '.' + bq_settings['tombstone_dataset'] + '.' + source_entity_name
        
        tombstone_table_exists = u.Utils.table_exists(table_id)
        
        if tombstone_table_exists:
            
            if neighbor_entity_path:
                
                soft_delete_insert = ('insert into ' + bq_settings['tombstone_dataset'] + '.' + neighbor_entity_name +
                                      ' select *, current_date, ' +
                                      u.Utils.construct_dateadd(softdelete_period, softdelete_unit) + 
                                      ' from ' + neighbor_entity_path + ' where ' + sql_filter_exp)
                    
            else:
                soft_delete_insert = ('insert into ' + bq_settings['tombstone_dataset'] + '.' + source_entity_name +
                                      ' select *, current_date, ' +
                                      u.Utils.construct_dateadd(softdelete_period, softdelete_unit) + 
                                      ' from ' + source_entity_path + where_clause)
                
        else:
            if neighbor_entity_path:
                
                soft_delete_insert = ('create table ' + bq_settings['tombstone_dataset'] + '.' + neighbor_entity_name + 
                                      ' as select *, current_date as softdelete_date, ' +
                                      u.Utils.construct_dateadd(softdelete_period, softdelete_unit) +
                                      ' as purge_date from ' + neighbor_entity_path +
                                      ' where ' + sql_filter_exp) 
                                            
            else:
                soft_delete_insert = ('create table ' + bq_settings['tombstone_dataset'] + '.' + source_entity_name + 
                                      ' as select *, current_date as softdelete_date, ' +
                                      u.Utils.construct_dateadd(softdelete_period, softdelete_unit) +
                                      ' as purge_date from ' + source_entity_path + where_clause)
                                    
        print(soft_delete_insert)
        
        query_job = bq.query(soft_delete_insert)
        results = query_job.result()
        
        # remove soft-deleted records from table
        if neighbor_entity_path:
            
            soft_delete_delete = ('delete from ' + neighbor_entity_path + ' where ' + neighbor_entity_id + ' in ' + 
                                 '(select ' + neighbor_entity_id + ' from ' + bq_settings['tombstone_dataset'] + '.' +
                                  neighbor_entity_name + ' where softdelete_date = current_date)')
           
        else:
            soft_delete_delete = 'delete from ' + source_entity_path + where_clause
        
        print(soft_delete_delete)
        query_job = bq.query(soft_delete_delete)
        results = query_job.result()
            
          
    def run_bq_purge(self, entity_name, bq_settings):
        
        print("INFO: running BQ purge routine")
        #print('policy: ' + str(policy))   
       
        # check if tombstone table exists
        table_id = bq_settings['project'] + '.' + bq_settings['tombstone_dataset'] + '.' + entity_name
        tombstone_table_exists = (u.Utils.table_exists(table_id))
       
        # delete records which qualify for purging
        if tombstone_table_exists:
            
            purge_stmt = 'delete from ' + bq_settings['tombstone_dataset'] + '.' + entity_name + ' where purge_date <= current_date'
            
            print(purge_stmt)
            
            query_job = bq.query(purge_stmt)
            results = query_job.result() 

if __name__ == '__main__':
    
    mode = sys.argv[1]
    
    if not mode:
        raise ValueError('Missing mode argument for running job. Mode needs to be equal to "scheduled" (or "s") for\
                         scheduled jobs or "on-demand" (or "d") for on-demand jobs')
    
    policy_ids = None
    if len(sys.argv) > 2:
        policy_ids = sys.argv[2].strip()
    
    mode = mode.strip().lower()
        
    r = Runner();
                        
    if mode == 'scheduled' or mode == 's':
        r.run_scheduled(policy_ids)
    elif mode == 'on-demand' or mode == 'd' or mode == 'on_demand':
        r.run_on_demand(policy_ids)
    else:
        raise ValueError('Invalid mode argument for running job. Mode should be equal to "scheduled" (or "s") or "on-demand" (or "d")')
    
     
    