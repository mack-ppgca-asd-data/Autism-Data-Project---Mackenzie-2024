import json
import time
from kafka import KafkaProducer
from faker import Faker
import random

fake = Faker()
bootstrap_servers = ['kafka:9092']
topicName = 'faker-data'
time.sleep(10)

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

while True:
    data = []
    fips_county = fake.random_int(min=1000, max=9999) 
    county_name = fake.city()  
    pop = fake.random_int(min=1000, max=1000000)  
    cases_asd = fake.random_int(min=0, max=pop) 
    prevalence_asd = round(cases_asd / pop, 6) 

    lower_ci = round(prevalence_asd - random.uniform(0.05, 0.15), 1)
    upper_ci = round(prevalence_asd + random.uniform(0.05, 0.15), 1)
    prevalence_asd_ci = f"{round(prevalence_asd, 1)} ({lower_ci}-{upper_ci})"

    if prevalence_asd < 0.01:
        map_category = "<1%"
    elif prevalence_asd < 0.05:
        map_category = "1-5%"
    else:
        map_category = ">5%"

    data = {
        "fips_county": fips_county,
        "county_name": county_name,
        "cases_asd": cases_asd,
        "pop": pop,
        "prevalence_asd": prevalence_asd,
        "prevalence_asd_ci": prevalence_asd_ci,
        "map_category": map_category
    }

    producer.send(topicName, value=data)
    print(f"Sent data: {data}")
    time.sleep(30)