Record Manager is a solution for implementing fine-grained data retention policies on BigQuery tables. 
<br><br>
Policies can archive subsets of records from managed BQ tables to external BQ tables stored in parquet files on GCS. Policies can also delete subsets of records from managed BQ tables or external BQ tables. 
<br><br>
Policies can be executed on-demand or on a schedule. Scheduled policies use a timestamp field to determine the retention of individual records. 
<br><br>
Policies can operate on standalone tables or connected tables. With connected tables, the action performed by the policy gets propagated to the connected records as well. 
<br><br>
The tool comes with a basic UI which is used for creating and managing policies. The UI will be enhanced over time with visualization and reporting capabilities. 
<br><br>

##### Follow the steps below to set up Record Manager on Google Cloud. 

###### Step 1: (Required) Clone this repo:
```
git clone https://github.com/GoogleCloudPlatform/record-manager.git
```

###### Step 2: (Required) Deploy the Record Manager UI:
```
gcloud services enable appengine.googleapis.com
gcloud app deploy
gcloud app browse
```

###### Step 3: (Required) Create the Firestore indexes:
###### Firestore indexes must be created prior to running the UI. Creating the indexes can take a few minutes. 
1. Run the create_indexes.py script as follows:
```
cd record-manager
python create_indexes.py $PROJECT_ID
```
2. Go to the Firestore UI and click on the indexes tab. Make sure that all 3 indexes have been created before proceeding. You will see a green checkbox next to the index once it's ready.  


###### Step 4: (Required) Set initialization variables:
1. Open the Record Manager UI by running: `gcloud app browse`. 
2. In the Init Settings section, set the BQ, GCS, and Dataproc variables. 
3. In the Table Relationships section, create your table associations and groupings.  
4. In the Manage Policies section, create zero or more scheduled policies and zero or more on-demand policies.  


######Step 5: (Required) Create Cloud Run job:
###### Sign-up for the Cloud Run Job Private Preview by filling out [this form](https://docs.google.com/forms/d/e/1FAIpQLSdLqffpS3e-KtLJ25GPhkZ653W_aXg-I2UNKbg-jAn316Mj1A/viewform)
```
export PROJECT='your-project'
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
```
Build the container which will be used to run the jobs:
gcloud builds submit --tag gcr.io/$PROJECT/record-manager

```
Create a job that runs all the scheduled policies using args "scheduled":
```gcloud alpha run jobs create scheduled-policies \
  --image=gcr.io/$PROJECT/record-manager \
  --tasks=1 \
  --args="scheduled" \
  --wait-for-completion 
```

Create a job that runs all the on-demand policies using args "on-demand":
```gcloud alpha run jobs create on-demand-policies \
  --image=gcr.io/$PROJECT/record-manager \
  --tasks=1 \
  --args="on-demand" \
  --wait-for-completion 
```

To see the progress of your jobs:
```
gcloud alpha run jobs describe scheduled-policies
gcloud alpha run jobs describe on-demand-policies
```

###### Step 6: (Optional) Schedule jobs: 
To run the scheduled and on-demand policies jobs on a schedule, consult the Cloud Run Jobs Private Preview User Guide. You will be given access to this doc when you sign up for the private preview.  


###### Troubleshooting:

If you encounter an error in the Record Manager UI, consult the App Engine logs as follows:

```
gcloud app logs tail -s default
```

If you encouter an error when running the Cloud Run jobs, view the job logs as follows:

```
gcloud alpha run jobs logs read scheduled-policies
gcloud alpha run jobs logs read on-demand-policies
```
