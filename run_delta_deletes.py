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
from datetime import datetime
import logging
import os
import sys

from google.cloud import storage

from delta import *
from delta.tables import *
import pyspark
from pyspark.sql.functions import *

def parse_args(argv):
    
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--entity_path',
        help='''The full path to your entity on GCS''',
        required=True,
    )
    
    parser.add_argument(
        '--file_format',
        help='''The file format that your entity is stored in (currently only parquet is supported)''',
        required=True,
    )
    
    parser.add_argument(
        '--ts_column',
        help='''The timestamp column name''',
        required=False,
    )
    
    parser.add_argument(
        '--ts_value',
        help='''The timestamp value that represents your retention threshold.''',
        required=False,
    )
    parser.add_argument(
        '--sql_filter_expression',
        help='''An additional filter expression (expressed in SQL).''',
        required=False,
    )
    
    return parser.parse_args(argv)

def delete_expired_records(args):
    
    builder = pyspark.sql.SparkSession.builder.appName("gcs_delete") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") 
        
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    entity_full_path = "gs://" + args.entity_path + "/*." + args.file_format
    tmp_dir= "gs://" + args.entity_path + "/tmp/*"

    df=spark.read.format(args.file_format).load(entity_full_path)
    
    df.show(5)
    
    df.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(tmp_dir)

    deltaTable = DeltaTable.forPath(spark, tmp_dir)
    
    del_stmt = ""
    # construct delete statement, should look like:
    # "ss_sold_ts < '2014-10-05' and ss_store_sk = 2"
    if args.ts_column and args.ts_column != "":
        del_stmt += args.ts_column + " < '" + args.ts_value + "'"
    
    if args.ts_column and args.ts_column != "" and args.sql_filter_expression and args.sql_filter_expression != "":
        del_stmt += " and "
    
    if args.sql_filter_expression and args.sql_filter_expression != "":
        del_stmt += args.sql_filter_expression
    
    print('del_stmt: ' + del_stmt)
        
    deltaTable.delete(del_stmt)
        
    deltaTable.vacuum(0)
    
    # overwriting original files doesn't work, using gsutil for this part
    #df=spark.read.format("delta").load(tmp_dir)
    #df.write.mode("overwrite").format(args.file_format).option("overwriteSchema", "true").save("gs://" + args.entity_path)
    
def overwrite_files(args):
    
    gcs = storage.Client()
    
    splits = args.entity_path.split("/", 2)
    bucket = splits[0]
    folder = splits[1]
      
    # delete original files
    for iterator in gcs.list_blobs(bucket, prefix=folder):
      
        #print(iterator.name)
        
        if "/tmp/" not in iterator.name and "part-" not in iterator.name and args.file_format in iterator.name:
            
            gcs.get_bucket(bucket).delete_blob(iterator.name)
            
            print("deleted original file: " + iterator.name)
       
    # move / rename new files
    for iterator in gcs.list_blobs(bucket, prefix=folder):
      
        #print(iterator.name)
        
        if "/tmp/" in iterator.name and "part-" in iterator.name and args.file_format in iterator.name:
            
            file_name = folder + "/" + iterator.name.split("/*/")[1]
            
            gcs.get_bucket(bucket).rename_blob(iterator, file_name)
            
            print("renamed " + file_name)
            
    # delete tmp files
    for iterator in gcs.list_blobs(bucket, prefix=folder + "/tmp"):

        gcs.get_bucket(bucket).delete_blob(iterator.name)
            
        print("deleted tmp file: " + iterator.name)
            

if __name__ == '__main__':

    args = parse_args(sys.argv[1:])
    delete_expired_records(args)
    overwrite_files(args)
