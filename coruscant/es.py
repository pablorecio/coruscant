from elasticsearch import Elasticsearch

ES_HOST = 'es01:9200'


def get_es_client() -> Elasticsearch:
    return Elasticsearch(ES_HOST, timeout=30)
