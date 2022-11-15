# ----- Dockerfile ------
FROM python:3-slim

RUN apt-get update 

# Setup env vars 
ENV APP_HOME /app
WORKDIR $APP_HOME

# Install dependencies
RUN pip3 install google-cloud-storage
RUN pip3 install google-cloud-datacatalog
RUN pip3 install google-cloud-bigquery
RUN pip3 install google-cloud-logging
RUN pip3 install pytz

COPY Service.py $APP_HOME
CMD ["python", "Service.py"]