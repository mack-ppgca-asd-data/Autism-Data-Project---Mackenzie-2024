from pymongo import MongoClient
import pandas as pd
import requests
from bs4 import BeautifulSoup


#ATLAS_URI = "mongodb://root:example@mongo:27017/"
ATLAS_URI = "mongodb://mongo:27017/"

class AtlasClient:

    def __init__(self, altas_uri, dbname):
        self.mongodb_client = MongoClient(altas_uri)
        self.database = self.mongodb_client[dbname]

    # A quick way to test if we can connect to Atlas instance
    def ping(self):
        self.mongodb_client.admin.command("ping")

    # Get the MongoDB Atlas collection to connect to
    def get_collection(self, collection_name):
        collection = self.database[collection_name]
        return collection

    # Query a MongoDB collection
    def find(self, collection_name, filter={}, limit=0):
        collection = self.database[collection_name]
        items = list(collection.find(filter=filter, limit=limit))
        return items

    # Find a single document in a MongoDB collection
    def find_one(self, collection_name, filter={}):
        collection = self.database[collection_name]
        item = collection.find_one(filter)
        return item

DB_NAME = "cdc_db"

atlas_client = AtlasClient(ATLAS_URI, DB_NAME)
atlas_client.ping()
print("Connected to Atlas instance! We are good to go!")

collectionASD = atlas_client.get_collection("asd_data")
collectionFIPS = atlas_client.get_collection("fips_data")
collectionBRASIL = atlas_client.get_collection("brasil_data")
collectionUSA = atlas_client.get_collection("usa_data")


def get_asd_data():
    url = "https://data.cdc.gov/resource/7vg3-e5u2.json"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        ASD_df = pd.DataFrame(data)
    else:
        print(f"Error: {response.status_code}")

    collectionASD.delete_many({})

    data_dict = ASD_df.to_dict("records")

    # Inserindo na coleção do MongoDB
    collectionASD.insert_many(data_dict)

    print("ASD Data has been successfully ingested into MongoDB.")

def get_fips_data():
    # URL of the text file
    url = "https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt"

    # Fetch the content of the file
    response = requests.get(url)
    content = response.text

    # Split the content into lines
    lines = content.splitlines()

    # Initialize a list to store the state-level FIPS data
    state_fips_data = []

    # Flag to indicate if we are in the state-level FIPS section
    in_state_section = False

    # Iterate through the lines to extract state-level FIPS codes
    for line in lines:
        if "state-level    place" in line:
            in_state_section = True
            continue
        if "county-level      place" in line:
            break
        if in_state_section:
            parts = line.split()
            if len(parts) >= 2:
                fips_code = parts[0]
                state_name = " ".join(parts[1:])
                state_fips_data.append((fips_code, state_name))

    # Create a DataFrame from the extracted data
    fips_df = pd.DataFrame(state_fips_data, columns=["fips_code", "state_name"])

    collectionFIPS.delete_many({})

    data_dict = fips_df.to_dict("records")

    # Inserindo na coleção do MongoDB
    collectionFIPS.insert_many(data_dict)

    print("FIPS Data has been successfully ingested into MongoDB.")

def get_brasil_data():
    # URL of the Wikipedia page
    url = "https://pt.wikipedia.org/wiki/Lista_de_unidades_federativas_do_Brasil_por_popula%C3%A7%C3%A3o"

    # Fetch the content of the page
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the table under the title "Lista"
    table = soup.find('table', {'class': 'wikitable'})

    # Read the table into a DataFrame
    brasil_df = pd.read_html(str(table))[0]

    collectionBRASIL.delete_many({})

    data_dict = brasil_df.to_dict("records")

    # Inserindo na coleção do MongoDB
    collectionBRASIL.insert_many(data_dict)

    print("Brasil Data has been successfully ingested into MongoDB.")

def get_usa_data():
    # ## Create a Collection and Documents
    # URL of the Wikipedia page
    url = "https://en.wikipedia.org/wiki/List_of_U.S._states_and_territories_by_population"

    # Fetch the content of the page
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the table under the title "State and territory rankings"
    table = soup.find('table', {'class': 'wikitable'})

    # Read the table into a DataFrame
    df_usa = pd.read_html(str(table))[0]

    # Flatten the multi-level column headers
    df_usa.columns = [' '.join(col).strip() if isinstance(col, tuple) else col for col in df_usa.columns]

    # Criamos uma coleção para os dados dos EUA chamada de 'usa_data'
    collectionUSA.delete_many({})

    # Ingestando os dados
    # Convertendo o DF em dicionário
    data_dict = df_usa.to_dict("records")

    # Inserindo na coleção do MongoDB
    collectionUSA.insert_many(data_dict)

    print("USA Data has been successfully ingested into MongoDB.")

get_asd_data()
get_fips_data()
get_brasil_data()
get_usa_data()