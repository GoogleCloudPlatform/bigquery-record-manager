# ----- Dockerfile ------
FROM python:3-slim

RUN apt-get update 

# Setup env vars 
ENV APP_HOME /app
WORKDIR $APP_HOME

# Install dependencies
RUN pip3 install python-dateutil
RUN pip3 install google-cloud-firestore
RUN pip3 install google-cloud-bigquery
RUN pip3 install google-cloud-dataproc
RUN pip3 install networkx

COPY Runner.py $APP_HOME
COPY PolicyManager.py $APP_HOME
COPY GraphService.py $APP_HOME
COPY Utils.py $APP_HOME
COPY constants.py $APP_HOME
CMD ["python", "Runner.py"]