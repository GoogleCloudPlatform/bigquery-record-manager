### Record Manager
This repository contains the Record Manager service for BigQuery, an open-source extension to BQ that automates the purging and archiving of BQ tables to Google Cloud Storage. The service takes as input a set of BQ tables that are tagged with a data retention policy. It reads the tags from Data Catalog and executes the policies specified in the tags. 

The purging procedure includes setting the expiration date on a table and creating a snapshot table. During the soft-deletion period, the table can be recovered from the snapshot table. After the soft-deletion period has passed, the snapshot table also gets purged and the source table can no longer be recovered. 

The archival procedure includes creating an external table for each BQ table that has exceeded its retention period. The external table is stored on GCS in parquet format and upgraded to Biglake. This allows the external table to have the same column-level access as the source table. Record Manager uses Tag Engine to copy the metadata tags and policy tags from the source table to the external table. The source table is dropped once it has been archived. 

You run Record Manager in either `validate` or `apply` mode. The `validate` mode lets you see what actions Record Manager would take without actually performing those actions, whereas `apply` actually performs the purging and archiving actions. Since those actions are destructive, you should run the service in `validate` mode before switching to `apply` mode. You specify the mode in the `param.json` file discussed below. 

#### Dependencies 

Record Manager requires [Tag Engine](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/) to propagate the metadata tags and policy tags to Biglake tables. It is compatible with [Tag Engine v1](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/README.md) and [Tag Engine v2](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/tree/cloud-run). 

Tag Engine can also be used to populate the data retention tags, which are required by Record Manager. The tags are created using [this tag template](https://github.com/GoogleCloudPlatform/datacatalog-templates/blob/master/data_retention.yaml).  

Tag Engine can be deployed into the same project as Record Manager or into its own project. If you are doing a POC, it is easier to deploy both into the same project. 


#### Step 0: update Google Cloud SDK

```
gcloud components update
```

#### Step 1: Set the required environment variables

```
export PROJECT_ID=my-project
gcloud config set core/project $PROJECT_ID
```

#### Step 2: Create service account and grant it required roles

```
export RECORD_MANAGER_SA=record-manager-sa@$PROJECT_ID.iam.gserviceaccount.com

gcloud iam roles create StorageBucketReader \
	--project $PROJECT_ID \
	--title StorageBucketReader \
	--description "Read GCS Bucket" \
	--permissions storage.buckets.get

gcloud projects add-iam-policy-binding $PROJECT_ID \
	--member=serviceAccount:$RECORD_MANAGER_SA \
	--role=projects/$PROJECT_ID/roles/StorageBucketReader

gcloud projects add-iam-policy-binding $PROJECT_ID \
	--member=serviceAccount:$RECORD_MANAGER_SA \
	--role=roles/storage.objectViewer

gcloud projects add-iam-policy-binding $PROJECT_ID \
	--member=serviceAccount:$RECORD_MANAGER_SA \
	--role=roles/bigquery.jobUser
	
gcloud projects add-iam-policy-binding $PROJECT_ID \
	--member=serviceAccount:$RECORD_MANAGER_SA \
	--role=roles/bigquery.dataOwner
	
gcloud projects add-iam-policy-binding $PROJECT_ID \
	--member=serviceAccount:$RECORD_MANAGER_SA \
	--role=roles/datacatalog.tagTemplateViewer

gcloud projects add-iam-policy-binding $PROJECT_ID \
	--member=serviceAccount:$RECORD_MANAGER_SA \
	--role=roles/run.invoker
	
gcloud projects add-iam-policy-binding $PROJECT_ID \
	--member=serviceAccount:$RECORD_MANAGER_SA \
	--role=roles/logging.logWriter
```

#### Step 3: Enable Google Cloud APIs

```
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable datacatalog.googleapis.com
```

#### Step 4: Clone this repository
```
git clone https://github.com/GoogleCloudPlatform/bigquery-record-manager.git
```

#### Step 5: Build the Docker image

```
gcloud builds submit --tag gcr.io/$PROJECT_ID/record-manager
```

#### Step 6: Create the parameter file

Record Manager requires a parameter file. The parameter file looks like:

```
{
	"template_id": "data_retention", 
	"template_project": "tag-engine-develop", 
	"template_region": "us-central1", 
	"retention_period_field": "retention_period",
	"expiration_action_field": "expiration_action",
	"projects_in_scope": ["tag-engine-develop"],
	"datasets_in_scope": ["crm", "oltp", "hr"],
	"bigquery_region": "us-central1",
	"snapshot_project": "tag-engine-develop",
	"snapshot_dataset": "snapshots",
	"snapshot_retention_period": 60,
	"archives_bucket": "archived_assets",
	"export_format": "parquet",
	"archives_project": "tag-engine-develop",
	"archives_dataset": "archives",
	"remote_connection": "projects/tag-engine-develop/locations/us-central1/connections/gcs_connection",
	"tag_engine_endpoint": "https://tag-engine-develop.uc.r.appspot.com",
	"mode": "apply"
}
```

At a minimum, you want to replace the values for the variables: `template_project`, `template_region`, `projects_in_scope`, `datasets_in_scope`, `bigquery_region`, `snapshot_project`, `snapshot_dataset`, `archives_bucket`, `archives_project`, `remote_connection`, `tag_engine_endpoint`. 
 
In addition, you may also want to adjust `snapshot_retention_period` which is given in days. 

The param file is available [here](https://github.com/GoogleCloudPlatform/bigquery-record-manager/blob/main/param.json). 

Once you have created the file, upload it to your GCS bucket. Copy the full path to this file, as you will need it in the next step. 

As mentioned previously, you run Record Manager in either `validate` or `apply` mode. 

#### Step 7: Create the Cloud Run job

```
gcloud run jobs create record-manager-job \
	--image gcr.io/$PROJECT_ID/record-manager \
        --tasks=1 \
        --parallelism=1 \
	--set-env-vars PARAM_FILE="gs://record-manager-param/param.json" \
	--service-account=$RECORD_MANAGER_SA
```

Note: `PARAM_FILE` in the above command is an environment variable. You need to replace `gs://record-manager-param/param.json` with the path to the `param.json` file you created in the earlier step. 

#### Step 8: Run Record Manager

```
gcloud run jobs execute record-manager-job --wait
```

### Troubleshooting:

* Go to the Cloud Run console and click on your job to view the logs. 
* If you encounter the error `ERROR: (gcloud.beta.run.jobs.create) User X does not have permission to access namespaces instance Y (or it may not exist): Permission 'iam.serviceaccounts.actAs' denied on service account Z (or it may not exist)` when creating the Cloud Run job, please consult [this page](https://cloud.google.com/iam/docs/service-accounts-actas).
