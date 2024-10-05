FROM python:3.8
#FROM apache/spark-py

# Set the working directory to /app
WORKDIR /app

COPY Ingestion.py ./
#COPY Processing.py ./
#COPY run_pipeline.sh ./

USER root

RUN pip install pymongo pandas requests bs4 lxml 
#pymongo[srv]

USER $NB_UID

#EXPOSE 27017

CMD ["python", "Ingestion.py"]
#CMD ["sh", "run_pipeline.sh"]