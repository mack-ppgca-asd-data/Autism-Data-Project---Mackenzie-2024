from faker import Faker
import random
import json
from kafka import KafkaProducer

fake = Faker()

def generate_synthetic_data(num_records):
    data = []
    for _ in range(num_records):
        fips_county = fake.random_int(min=1000, max=9999)
        county_name = fake.city()
        pop = fake.random_int(min=1000, max=1000000)
        cases_asd = fake.random_int(min=0, max=pop)
        prevalence_asd = round(cases_asd / pop, 6)
        lower_ci = round(prevalence_asd - random.uniform(0.05, 0.15), 1)
        upper_ci = round(prevalence_asd + random.uniform(0.05, 0.15), 1)
        prevalence_asd_ci = f"{round(prevalence_asd, 1)} (-)"
        if prevalence_asd < 0.01:
            map_category = "<1%"
        elif prevalence_asd < 0.05:
            map_category = "1-5%"
        else:
            map_category = ">5%"

        record = {
            "fips_county": fips_county,
            "county_name": county_name,
            "cases_asd": cases_asd,
            "pop": pop,
            "prevalence_asd": prevalence_asd,
            "prevalence_asd_ci": prevalence_asd_ci,
            "map_category": map_category
        }
        data.append(record)

    return data

# Kafka Producer Configuration
bootstrap_servers = 'localhost:9092'  # Replace with your Kafka broker address
topic = 'synthetic_data'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Main Loop to send data every 3 minutes
while True:
    # Generate 100 synthetic records
    synthetic_data = generate_synthetic_data(100)
    
    for record in synthetic_data:
        # Serialize the record as a JSON string
        json_data = json.dumps(record).encode('utf-8')
        producer.send(topic, json_data)
        print(f"Sent record to Kafka: {record}")

    # Sleep for 3 minutes
    time.sleep(180)