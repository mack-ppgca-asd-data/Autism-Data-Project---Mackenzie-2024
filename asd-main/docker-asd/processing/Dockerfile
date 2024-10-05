# Use an official Python runtime as a parent image
# FROM python:3.8
FROM apache/spark-py
WORKDIR /app

#COPY Ingestion.py ./
COPY Processing.py ./
#COPY run_pipeline.sh ./

USER root

RUN pip install pymongo pyspark
#pymongo[srv]
USER $NB_UID

#EXPOSE 27017

CMD ["python3", "Processing.py"]