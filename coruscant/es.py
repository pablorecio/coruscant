from elasticsearch import Elasticsearch
from elasticsearch_dsl import connections

ES_HOST = 'es01:9200'


# Connection needed for elasticsearch-dsl
connections.create_connection(hosts=[ES_HOST], timeout=20)


def get_es_client() -> Elasticsearch:
    return Elasticsearch(ES_HOST, timeout=30)
