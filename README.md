### Record Manager
This repository contains the Record Manager service for BigQuery, an open-source extension that automates the purging and archiving of BigQuery tables. The service takes as input a set of BigQuery tables that are tagged with a data retention policy. Record Manager reads the tags from Data Catalog and executes the policies on the tagged tables. 

The purging procedure includes setting the expiration date on a table and creating a snapshot table. During the soft-deletion period, the table can be recovered from the snapshot table. After the soft-deletion period has passed, the snapshot table is also deleted and the source table can no longer be recovered. 

The archival procedure includes creating an external table for each BigQuery table that has passed its retention period. The external table is stored on Cloud Storage in parquet format and upgraded to Biglake. This allows the external table to have the same column-level access as the source table. Record Manager uses Tag Engine to copy the metadata tags and policy tags from the source table to the external table. The source table is dropped once it has been archived. 

#### Dependencies 

Record Manager uses Tag Engine to propagate the metadata tags and policy tags to Biglake tables. You can deploy Tag Engine by following [its deployment procedure](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/README.md). 

Note: You can also use Tag Engine to populate the data retention tags that are needed by Record Manager. The data retention tags are created using [this tag template](https://github.com/GoogleCloudPlatform/datacatalog-templates/blob/master/data_retention.yaml).  

Tag Engine can be deployed into the same project as Record Manager or into a different project. For proof-of-concepts, it is often simpler to deploy them together into the same project. 

#### update Google Cloud SDK
gcloud components update


#### Step 1: Set the required environment variables
```
export PROJECT=record-manager-service
export REGION=us-central
export SERVICE_ACCOUNT=record-manager@appspot.gserviceaccount.com
gcloud config set core/project $PROJECT
gcloud config set run/region $REGION

```

#### Step 2: Enable Google Cloud APIs
```
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable datacatalog.googleapis.com
```

#### Step 3: Clone this repository
```
git clone https://github.com/GoogleCloudPlatform/bigquery-record-manager.git
```

#### Step 4: Build the Docker image
```
gcloud builds submit --tag gcr.io/$PROJECT/record-manager
```

#### Step 4: Create the parameter file

Create the parameter file to Record Manager based on [this example](https://github.com/GoogleCloudPlatform/bigquery-record-manager/blob/main/param.json). 
Once you have created the file, upload it to a Cloud Storage bucket. Copy the full path to this file, as you will need it in the next step. 


#### Step 6: Create the Cloud Run job
```
gcloud beta run jobs create record-manager-job \
  --image=gcr.io/$PROJECT/record-manager \
  --tasks=1 \
  --parallelism=1 \
  --service-account=$SERVICE_ACCOUNT \
  --args=[PARAM_FILE]
```

For example:

```
gcloud beta run jobs create record-manager-job \
  --image=gcr.io/$PROJECT/record-manager \
  --tasks=1 \
  --parallelism=1 \
  --service-account=record-manager@appspot.gserviceaccount.com \
  --args=gs://record-manager/param.json
```

#### Step 7: Run Record Manager
```
gcloud beta run jobs execute record-manager-job --wait
```


### Troubleshooting:

* Go to the Cloud Run console and click on your job to view the logs. 
* If you encounter the error `ERROR: (gcloud.beta.run.jobs.create) User X does not have permission to access namespaces instance Y (or it may not exist): Permission 'iam.serviceaccounts.actAs' denied on service account Z (or it may not exist) when creating the Cloud Run job, please consult [this page](https://cloud.google.com/iam/docs/service-accounts-actas).
* If you encounter the error `terminated: Application failed to start: invalid status ::14: could not start container: no such file or directory` when running the Cloud Run job, you may have hit a bug in Cloud Run. You'll need to work-around it by creating the job without the args and hard-coding the path to your parameter file in the main of [Service.py](https://github.com/GoogleCloudPlatform/bigquery-record-manager/blob/main/Service.py). 

