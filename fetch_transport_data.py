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
index = "transport_rennes_test_8"


# Récupération des données au format JSON
response = requests.get(url, params="rows={}".format(rows))
content = response.json()

# Filtrage des données : récupération de celles avec un indice de confiance supérieur à 50%
confident_data = []
for data in content["records"]:
    if data["fields"]["traveltimereliability"] >= 50:
        print(type(data))
        confident_data.append(data)

# Création d'un index Elastic
# PUT http:localhost:9200/transport_rennes_test
# requests.put(elastic_host+index)
es.indices.create(index=index)

# Stockage des données sur Elastic
for i, data in enumerate(confident_data):
    print("Indexation donnée "+i)
    es.index(index=index, id=i, body=json.dumps(data))
