import requests
import elasticsearch


# Configuration Elastic
config = {
    'host': 'http://localhost:9200'
}
es = elasticsearch.Elasticsearch([config], timeout=300)


# URL de l'API Rennes Metropole
url = "https://data.rennesmetropole.fr/api/records/1.0/search/?dataset=etat-du-trafic-en-temps-reel&q=&facet=denomination"
# Nombre de lignes de données à récupérer
rows = 10
# Hôte Elastic
elastic_host = "http://localhost:9200"
# Index
index = "/transport_rennes_test"


# Récupération des données au format JSON
response = requests.get(url, params="rows={}".format(rows))
content = response.json()

# Filtrage des données : récupération de celles avec un indice de confiance supérieur à 50%
confident_data = []
for data in content["records"]:
    if data["fields"]["traveltimereliability"] >= 50:
        confident_data.append(data)

# Création d'un index Elastic
# PUT http:localhost:9200/transport_rennes_test
# requests.put(elastic_host+index)
es.indices.create(index=index)

# Prépation


# Stockage des données sur Elastic
for i, data in enumerate(confident_data):
    # PUT http://localhost:9200/transport_rennes_test/NUMERO_DE_LA_DONNEE + données JSON
    requests.put(elastic_host+index+"/{i}", data=data)