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
index = "transport_rennes_test_10"


# Récupération des données au format JSON
response = requests.get(url, params="rows={}".format(rows))
content = response.json()

# Filtrage des données : récupération de celles avec un indice de confiance supérieur à 50%
confident_data = []
for data in content["records"]:
    if data["fields"]["traveltimereliability"] >= 50:
        print(type(data))
        confident_data.append(data)

# Définition de mapping
mapping = {
    "properties": {
            "datasetid": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "fields" : {
                "properties" : {
                    "averagevehiclespeed" : {
                        "type" : "long"
                    },
                    "datetime" : {
                        "type" : "date"
                    },
                    "denomination" : {
                        "type" : "text",
                        "fields" : {
                           "keyword" : {
                                "type" : "keyword",
                                "ignore_above" : 256
                            }   
                        }
                    },
                    "func_class" : {
                        "type" : "long"
                    },
                    "geo_point_2d" : {
                        "type" : "geo_point"
                    },
                    "geo_shape" : {
                        "properties" : {
                            "coordinates" : {
                                "type" : "geo_shape"
                            },
                            "type" : {
                                "type" : "text",
                                "fields" : {
                                    "keyword" : {
                                        "type" : "keyword",
                                        "ignore_above" : 256
                                    }
                                }
                            }
                        }
                    },
                    "id" : {
                        "type" : "long"
                    },
                    "predefinedlocationreference" : {
                        "type" : "text",
                        "fields" : {
                            "keyword" : {
                                "type" : "keyword",
                                "ignore_above" : 256
                            }
                        }
                    },
                    "trafficstatus" : {
                        "type" : "text",
                        "fields" : {
                            "keyword" : {
                                "type" : "keyword",
                                "ignore_above" : 256
                            }
                        }
                    },
                    "traveltime" : {
                        "type" : "long"
                    },
                    "traveltimereliability" : {
                        "type" : "long"
                    }
                }
            },
            "geometry" : {
                "properties" : {
                    "coordinates" : {
                        "type" : "float"
                    },
                    "type" : {
                        "type" : "text",
                        "fields" : {
                            "keyword" : {
                                "type" : "keyword",
                                "ignore_above" : 256
                            }
                        }
                    }
                }
            },   
            "record_timestamp": {
                "type": "date"
            },
            "recordid": {
                "type": "text",
                "fields": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }

        }
}



# Création d'un index Elastic
# PUT http:localhost:9200/transport_rennes_test
# requests.put(elastic_host+index)
es.indices.create(index=index,
                  #body=mapping,
                  ignore=400  # Ignore 400 already exists
                  )

# Add mapping to the index
es.indices.put_mapping(
    index=index,
    body=mapping
)

# Stockage des données sur Elastic
for i, data in enumerate(confident_data):
    print("Indexation donnée {}".format(i))
    es.index(index=index, id=i, body=json.dumps(data))
