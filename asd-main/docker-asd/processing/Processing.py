
# Etapa 2, Transformando os Dados Ingestados
# Nessa etapa vamos transformar os dados ingestados, limpá-los e deixá-los prontos para consumo.
# 
# # 1 Trazendo os dados ingestados no MongoDB e colocando-os em data frames
# Iniciamos instalando a biblioteca "pymongo" e seguimos com a conexão ao banco.

#import pymongo
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, substring
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# MongoDB conexão URI
#mongo_uri = "mongodb://root:example@mongo:27017/"
mongo_uri = "mongodb://mongo:27017/"
# Criando um cliente Mongo
client = MongoClient(mongo_uri)

# Trazendo a base de dados
db = client.get_database('cdc_db')

#Trazendo os dados das coleções do banco

# trazendo os dados
asd_data = list(db.asd_data.find())
brasil_data = list(db.brasil_data.find())
fips_data = list(db.fips_data.find())
usa_data = list(db.usa_data.find())



# Inicializa a sessão Spark
spark = SparkSession.builder.appName("TransformData").getOrCreate()

def remove_id(document):
    """Remove the '_id' field from a document."""
    if '_id' in document:
      del document['_id']
    return document

# Remove '_id' from each document before creating DataFrames
asd_data = [remove_id(doc) for doc in asd_data]
brasil_data = [remove_id(doc) for doc in brasil_data]
fips_data = [remove_id(doc) for doc in fips_data]
usa_data = [remove_id(doc) for doc in usa_data]

# Cria DataFrames a partir das coleções MongoDB (sem o _id)
df_asd_data = spark.createDataFrame(asd_data)
df_brasil_data = spark.createDataFrame(brasil_data)
df_fips_data = spark.createDataFrame(fips_data)
df_usa_data = spark.createDataFrame(usa_data)



# Ensure the fips_county column is of string type
df_asd_data = df_asd_data.withColumn("fips_county", col("fips_county").cast("string"))

# Create the fips_state column by extracting the first 2 digits of fips_county
df_asd_data = df_asd_data.withColumn("fips_state", substring(col("fips_county"), 1, 2))

# # 2.1 Agregando dados de estados
# 
# Nessa etapa vamos cruzar os dados de estado da coluna "fips_state" e inserir os nomes dos estados que estão na tabela "FIPS Data".
# 

# Create a broadcast variable for the FIPS to state mapping
fips_to_state_broadcast = spark.sparkContext.broadcast(dict(zip(df_fips_data.select("fips_code").rdd.flatMap(lambda x: x).collect(),
                                                               df_fips_data.select("state_name").rdd.flatMap(lambda x: x).collect())))

# Use a UDF to map the fips_state to the state name


get_state_name = udf(lambda fips_code: fips_to_state_broadcast.value.get(fips_code), StringType())

# Add the state name column using the UDF
df_asd_data_with_state = df_asd_data.withColumn("state_name", get_state_name(df_asd_data["fips_state"]))

# Fizemos uma comparação da população dos estados e adicionamos mais algumas colunas contendo a população de cada estado americano e o estado brasileiro relativo a esse estado (em tamanho de população).

# Ensure the fips_county column is of string type
df_asd_data = df_asd_data.withColumn("fips_county", col("fips_county").cast("string"))

# Create the fips_state column by extracting the first 2 digits of fips_county
df_asd_data = df_asd_data.withColumn("fips_state", substring(col("fips_county"), 1, 2))

# Create a broadcast variable for the FIPS to state mapping
fips_to_state_broadcast = spark.sparkContext.broadcast(dict(zip(df_fips_data.select("fips_code").rdd.flatMap(lambda x: x).collect(),
                                                               df_fips_data.select("state_name").rdd.flatMap(lambda x: x).collect())))

# Use a UDF to map the fips_state to the state name

get_state_name = udf(lambda fips_code: fips_to_state_broadcast.value.get(fips_code), StringType())

# Add the state name column using the UDF
df_asd_data_with_state = df_asd_data.withColumn("state_name", get_state_name(df_asd_data["fips_state"]))

# Renomear a coluna de df_usa_data para facilitar a junção
df_usa_data = df_usa_data.withColumnRenamed("State or territory State or territory", "state_name") \
                         .withColumnRenamed("Census population[8][a] July 1, 2023 (est.)", "usa_state_pop")

# Converter os nomes dos estados para maiúsculas em ambos os dataframes
df_asd_data_with_state = df_asd_data_with_state.withColumn("state_name", upper(col("state_name")))
df_usa_data = df_usa_data.withColumn("state_name", upper(col("state_name")))

# Realizar a junção dos dataframes com base na coluna 'state_name'
df_asd_data_final = df_asd_data_with_state.join(df_usa_data.select("state_name", "usa_state_pop"), "state_name", "left")

# Adicionando os dados de comparação de tamanhos de estado (entre USA e BR)
# Dicionário de mapeamento dos estados dos EUA para os estados brasileiros
state_mapping = {
    "ARKANSAS": "ALAGOAS",
    "ARIZONA": "SANTA CATARINA",
    "OKLAHOMA": "AMAZONAS",
    "VERMONT": "ACRE",
    "VIRGINIA": "CEARÁ",
    "MINNESOTA": "MARANHÃO",
    "NEW JERSEY": "CEARÁ",
    "TENNESSEE": "GOIÁS",
    "UTAH": "ESPÍRITO SANTO",
    "WISCONSIN": "AMAZONAS"
}

# Criando a nova coluna 'br_state' com base no mapeamento usando UDF
map_state = udf(lambda state_name: state_mapping.get(state_name), StringType())
df_asd_data_final = df_asd_data_final.withColumn("br_state", map_state(df_asd_data_final["state_name"]))

# Renomear a coluna de df_brasil_data para facilitar a junção
df_brasil_data = df_brasil_data.withColumnRenamed("Unidade federativa", "br_state") \
                         .withColumnRenamed("População (Estimativa 2024)[2]", "br_state_pop")

# Converter os nomes dos estados para maiúsculas em ambos os dataframes
df_asd_data_final = df_asd_data_final.withColumn("br_state", upper(col("br_state")))
df_brasil_data = df_brasil_data.withColumn("br_state", upper(col("br_state")))

# Realizar a junção dos dataframes com base na coluna 'br_state'
df_asd_data_final = df_asd_data_final.join(df_brasil_data.select("br_state", "br_state_pop"), "br_state", "left")

# # Ingerindo os dados no mongodb

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


collectionFINAL = atlas_client.get_collection("final_data")
collectionFINAL.delete_many({})

# Ingestando os dados
# Convertendo o DF em dicionário
data_dict = [row.asDict() for row in df_asd_data_final.collect()]

# Inserindo na coleção do MongoDB
collectionFINAL.insert_many(data_dict)

print("Data inserted successfully!")