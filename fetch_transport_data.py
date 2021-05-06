import requests
import elasticsearch
import json


# Configuration Elastic
config = {
    'host': 'localhost',
    'port': 9200
}
es = elasticsearch.Elasticsearch([config], timeout=300)
print(es)


# URL de l'API Rennes Metropole
url = "https://data.rennesmetropole.fr/api/records/1.0/search/?dataset=etat-du-trafic-en-temps-reel&q=&facet=denomination"
# Nombre de lignes de données à récupérer
rows = 1000
# Hôte Elastic
elastic_host = "http://localhost:9200"
# Index
index = "transport_rennes_test_15"

es.indices.delete(index=index)

# Récupération des données au format JSON
response = requests.get(url, params="rows={}".format(rows))
content = response.json()


# Définition de mapping
mapping = {
    "mappings": {
        # "properties": {
        #     "fields": {
                "properties": {
                    "geo_point_2d": {
                        "type": "geo_point"
                    },

                    "geo_shape": {
                        "type": "geo_shape"
                    }
                }
            }
}


# Création d'un index Elastic
# PUT http:localhost:9200/transport_rennes_test
# requests.put(elastic_host+index)
es.indices.create(index=index,
                  body=mapping
                  )

# Add mapping to the index
# es.indices.put_mapping(
#     index=index,
#     body=mapping
# )


# Filtrage des données : récupération de celles avec un indice de confiance supérieur à 50%
# confident_data = []
for data in content["records"]:
    if data["fields"]["traveltimereliability"] >= 50:
        # print(type(data))
        # confident_data.append(data)
        es.index(index=index, body=data["fields"])

# Stockage des données sur Elastic
# for i, data in enumerate(confident_data):
#     print("Indexation donnée {}".format(i))
#     es.index(index=index, id=i, body=json.dumps(data))

print(es.indices.get_mapping(index=index))
