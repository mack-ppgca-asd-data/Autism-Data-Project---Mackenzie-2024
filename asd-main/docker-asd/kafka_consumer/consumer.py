import json
from kafka import KafkaConsumer
import pymongo

bootstrap_servers = ['kafka:9092']
topicName = 'faker-data'
mongo_connection_string = "mongodb://mongo:27017"  # From environment variable

consumer = KafkaConsumer(topicName, bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))

client = pymongo.MongoClient(mongo_connection_string)
db = client['cdc_db'] # Replace with your database name
collection = db['asd_data_faker']

for message in consumer:
    data = message.value
    collection.insert_one(data)
    print(f"Inserted data: {data}")
