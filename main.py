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

from flask import Flask, render_template, request, jsonify
from google.cloud.exceptions import NotFound
import Utils as u
import PolicyManager as pm

app = Flask(__name__)


@app.route("/")
def homepage():
    
    # [END homepage]
    # [START render_template]
    return render_template(
        'index.html')

#
# Init methods
#   
@app.route("/bq_settings<int:saved>", methods=['GET'])
def bq_settings(saved):
    
    settings = u.Utils.get_bq_settings()
    
    if settings:
        return render_template('bq_settings.html',
            project=settings['project'],
            native_dataset=settings['native_dataset'],
            tombstone_dataset=settings['tombstone_dataset'],
            temp_dataset=settings['temp_dataset'],
            external_dataset=settings['external_dataset'],
            saved=saved)
    
    else:
        return render_template('bq_settings.html')
  

@app.route("/process_bq_settings", methods=['POST'])
def process_bq_settings():
    
    action = request.form['action']
    
    if action == "Submit Changes":
    
        project = request.form['project'].rstrip()
        native_dataset = request.form['native_dataset'].rstrip()
        tombstone_dataset = request.form['tombstone_dataset'].rstrip()
        temp_dataset = request.form['temp_dataset'].rstrip()
        external_dataset = request.form['external_dataset'].rstrip()
    
        if project != None and native_dataset != None:
            u.Utils.init_bq(project, native_dataset, tombstone_dataset, temp_dataset, external_dataset)
            return bq_settings(1) 
        else:
            return bq_settings(0) 
    
    if action == "Cancel Changes":
            return homepage() 
        

@app.route("/gcs_settings<int:saved>", methods=['GET'])
def gcs_settings(saved):
    
    settings = u.Utils.get_gcs_settings()
    
    if settings:
        
        return render_template('gcs_settings.html',
            project=settings['project'],
            archive_bucket=settings['archive_bucket'],
            lake_bucket=settings['lake_bucket'],
            file_format=settings['file_format'],
            compression=settings['compression'],
            saved=saved)
            
    else:
        
        return render_template('gcs_settings.html')
    

@app.route("/process_gcs_settings", methods=['POST'])
def process_gcs_settings():
    
    action = request.form['action']
    
    if action == "Submit Changes":
    
        project = request.form['project'].rstrip()
        archive_bucket = request.form['archive_bucket'].rstrip()
        lake_bucket = request.form['lake_bucket'].rstrip()
        file_format = request.form['file_format'].rstrip()
        compression = request.form['compression'].rstrip()
    
        if project != None and archive_bucket != None and lake_bucket != None:
            u.Utils.init_gcs(project, archive_bucket, lake_bucket, file_format, compression)
            return gcs_settings(1) 
        else:
            return gcs_settings(0) 
    
    if action == "Cancel Changes":
        return homepage() 
        
 
@app.route("/dataproc_settings<int:saved>", methods=['GET'])
def dataproc_settings(saved):
    
    settings = u.Utils.get_dataproc_settings()
    
    if settings:
        
        return render_template('dataproc_settings.html',
            project=settings['project'],
            region=settings['region'],
            cluster=settings['cluster'],
            delete_script=settings['delete_script'],
            saved=saved)
            
    else:
        
        return render_template('dataproc_settings.html')    


@app.route("/process_dataproc_settings", methods=['POST'])
def process_dataproc_settings():
    
    action = request.form['action']
    
    if action == "Submit Changes":
    
        project = request.form['project'].rstrip()
        region = request.form['region'].rstrip()
        cluster = request.form['cluster'].rstrip()
        delete_script = request.form['delete_script'].rstrip()
    
        if project != None and region != None and cluster != None and delete_script != None:
            u.Utils.init_dataproc(project, region, cluster, delete_script)
            return dataproc_settings(1) 
        else:
            return dataproc_settings(0) 
    
    if action == "Cancel Changes":
        return homepage() 

#
# Config methods
#   
@app.route("/create_associations<int:saved>", methods=['GET'])
def create_associations(saved):
    
    foreign_key_stmts = u.Utils.get_foreign_key_stmts()
    
    return render_template(
        'create_associations.html',
        foreign_key_stmts=foreign_key_stmts,
        saved=saved)    
        
@app.route("/process_associations", methods=['POST'])
def process_associations():
    
    action = request.form['action']
    
    if action == "Submit Changes":
        
        foreign_key_stmts = request.form['foreign_key_stmts'].rstrip()
        u.Utils.write_foreign_keys(foreign_key_stmts)
        return create_associations(1)  
    
    if action == "Cancel Changes":
        return homepage()        
        
                
@app.route("/create_groupings<int:saved>", methods=['GET'])
def create_groupings(saved):
    
    bq_settings = u.Utils.get_bq_settings()
    project = bq_settings['project']
    dataset = bq_settings['native_dataset']
    
    table_group_list = u.Utils.get_table_group_pairs()
    print('table_group_list: ' + str(table_group_list))
    
    return render_template(
        'create_groupings.html', 
        table_group_list=table_group_list,
        project=project,
        dataset=dataset, 
        saved=saved)


@app.route("/process_groupings", methods=['POST'])
def process_groupings():
    
    action = request.form['action']
    
    if action == "Submit Changes":
        table_list = u.Utils.get_tables()
        bq_settings = u.Utils.get_bq_settings()
    
        for entity_name in table_list:
            entity_group = request.form[entity_name].rstrip()
        
            if entity_group:
                entity_path = bq_settings['native_dataset'] + '.' + entity_name
                u.Utils.write_group(entity_name, entity_path, entity_group)
    
        return create_groupings(1) 
    
    if action == "Cancel Changes":
        return homepage() 



@app.route("/create_scheduled_policy", methods=['GET'])
def create_scheduled_policy():
    
    return render_template(
        'create_scheduled_policy.html')
        
        
@app.route("/create_ondemand_policy", methods=['GET'])
def create_ondemand_policy():
    
    return render_template(
        'create_ondemand_policy.html')

        
@app.route("/process_scheduled_policy", methods=['POST'])
def process_scheduled_policy():

    print('enter process_scheduled_policy')
    
    action = request.form['action']
    
    if action == "Submit Policy":
        
        if 'policy_id' in request.form:
            policy_id = request.form['policy_id']
            print('policy_id: ' + policy_id)
        else:
            policy_id = None
        
        policy_action = request.form['policy_action']
        entity_name = request.form['entity_name'].rstrip()
        entity_path = request.form['entity_path'].rstrip()
        storage_system = request.form['storage_system']
        grouping = request.form['grouping']
        
        if grouping:
            entity_groups = request.form['entity_groups'].rstrip()
        else:
            entity_groups = None
        
        ts_column = request.form['ts_column'].rstrip()
        sql_filter_exp = request.form['sql_filter_exp'].rstrip()
        retention_period = request.form['retention_period'].rstrip()
        retention_unit = request.form['retention_unit']
        
        pm.PolicyManager.write_update_scheduled_policy(policy_id, storage_system, entity_name, entity_path, grouping, entity_groups,\
                                     ts_column, sql_filter_exp, retention_period, retention_unit, policy_action)
    
        return view_scheduled_policies()
    
    if action == "Cancel Changes":
        return homepage()   
        

@app.route("/process_ondemand_policy", methods=['POST'])
def process_ondemand_policy():

    action = request.form['action']
    
    if action == "Submit Policy":
        
        if 'policy_id' in request.form:
            policy_id = request.form['policy_id']
        else:
            policy_id = None
            
        policy_action = request.form['policy_action']
        entity_name = request.form['entity_name'].rstrip()
        entity_path = request.form['entity_path'].rstrip()
        storage_system = request.form['storage_system']
        grouping = request.form['grouping']
        
        if grouping:
            entity_groups = request.form['entity_groups'].rstrip()
        else:
            entity_groups = None
        
        sql_filter_exp = request.form['sql_filter_exp'].rstrip()
        softdelete_period = request.form['softdelete_period'].rstrip()
        softdelete_unit = request.form['softdelete_unit']
        
        pm.PolicyManager.write_update_ondemand_policy(policy_id, storage_system, entity_name, entity_path, grouping, entity_groups,\
                                                      sql_filter_exp, softdelete_period, softdelete_unit, policy_action)
    
        return view_ondemand_policies()
    
    if action == "Cancel Changes":
        return homepage()  
    

@app.route("/view_scheduled_policies", methods=['GET'])
def view_scheduled_policies():
    
    policies = pm.PolicyManager.get_scheduled_policies()

    if policies:
        return render_template('view_scheduled_policies.html', 
                                policies=policies)
    else:
        return create_scheduled_policy()
    

@app.route("/view_ondemand_policies", methods=['GET'])
def view_ondemand_policies():
    
    policies = pm.PolicyManager.get_ondemand_policies()

    if policies:
        return render_template('view_ondemand_policies.html', 
                                policies=policies)
    else:
        return create_ondemand_policy()


@app.route("/update_scheduled_policy", methods=['POST'])
def update_scheduled_policy():
    
    policy_id = request.form['policy_id']
    policy_action = request.form['policy_action']
    storage_system = request.form['storage_system']
    entity_name = request.form['entity_name']
    entity_path = request.form['entity_path']
    grouping = request.form['grouping']
    entity_groups = request.form['entity_groups']
    ts_column = request.form['ts_column']
    sql_filter_exp = request.form['sql_filter_exp']
    retention_period = request.form['retention_period']
    retention_unit = request.form['retention_unit']
    
    print('sql_filter_exp: ' + sql_filter_exp)

    return render_template(
        'update_scheduled_policy.html', 
        policy_id=policy_id,
        policy_action=policy_action,
        storage_system=storage_system,
        entity_name=entity_name,
        entity_path=entity_path,
        grouping=grouping,
        entity_groups=entity_groups,
        ts_column=ts_column,
        sql_filter_exp=sql_filter_exp,
        retention_period=retention_period,
        retention_unit=retention_unit)    


@app.route("/update_ondemand_policy", methods=['POST'])
def update_ondemand_policy():
    
    policy_id = request.form['policy_id']
    policy_action = request.form['policy_action']
    storage_system = request.form['storage_system']
    entity_name = request.form['entity_name']
    entity_path = request.form['entity_path']
    grouping = request.form['grouping']
    entity_groups = request.form['entity_groups']
    sql_filter_exp = request.form['sql_filter_exp']
    softdelete_period = request.form['softdelete_period']
    softdelete_unit = request.form['softdelete_unit']
    

    return render_template(
        'update_ondemand_policy.html', 
        policy_id=policy_id,
        policy_action=policy_action,
        storage_system=storage_system,
        entity_name=entity_name,
        entity_path=entity_path,
        grouping=grouping,
        entity_groups=entity_groups,
        sql_filter_exp=sql_filter_exp,
        softdelete_period=softdelete_period,
        softdelete_unit=softdelete_unit)    


#
# TO DO: implement reporting methods  
#  

@app.route("/visualize_associations", methods=['GET'])
def visualize_associations():
    
    return render_template(
        'visualize_associations.html')
                    
@app.route("/policy_execution_history", methods=['GET'])
def policy_execution_history():
       
    return render_template(
        'policy_execution_history.html')
    
@app.route("/policy_compliance", methods=['GET'])
def policy_compliance():
       
    return render_template(
        'policy_compliance.html')


@app.errorhandler(500)
def server_error(e):
    # Log the error and stacktrace.
    #logging.exception('An error occurred during a request.')
    return 'An internal error occurred: ' + str(e), 500
# [END app]


if __name__ == "__main__":
    app.run()
