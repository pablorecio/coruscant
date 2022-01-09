from datetime import date, datetime

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError
from flask import Flask, request, jsonify

app = Flask(__name__)

ES_HOST = 'es01:9200'


def _get_es_client():
    return Elasticsearch(ES_HOST, timeout=30)


def _get_es_indexes(_from, _to):
    current = _from
    indexes = []
    while current <= _to:
        indexes.append(current.year)
        current = current.replace(year=current.year + 1)

    return ','.join(map(lambda x: f'global_land_temperatures_by_city-{x}', indexes))


# @app.route('/api/measurement/add')
# @app.route('/api/measurement/update')
@app.route('/api/measurements')
def measurements_list():
    # Note: I am using elasticsearch-py instead of elasticsearch-dsl-py because
    # the later does not yet support the collapse operator:
    # https://github.com/elastic/elasticsearch-dsl-py/issues/1215
    # and making this query without that operator would end up into a very
    # complex pipeline of python functions that I'd rather avoid. This way,
    # the code is much cleaner IMO.
    query_parameters = request.args

    try:
        number_of_cities = int(query_parameters.get('cities', 10))
    except ValueError:
        return {"error": f"Invalid cities number: {query_parameters.get('cities')}"}, 400

    _from = request.args.get('from')
    _to = request.args.get('to')

    body = {
        'collapse': {'field': 'city'},
        'sort': [{"average_temperature": "desc"}],
        'size': number_of_cities
    }

    if _from or _to:
        date_range = {}

        if _from:
            try:
                # Adds it to the query
                date_range['gte'] = _from
                _from = datetime.strptime(_from, '%Y-%m-%d').date()
            except ValueError:
                return {"error": f"Invalid date format: {_from}"}, 400
        else:
            _from = date(1500, 1, 1)  # lowest value is ~ 1700 but this can be cleaner

        if _to:
            try:
                # Adds it to the query
                date_range['lte'] = _to
                _to = datetime.strptime(_to, '%Y-%m-%d').date()
            except ValueError:
                return {"error": f"Invalid date format: {_to}"}, 400
        else:
            _to = date.today()

        if _from > _to:
            return {"error": "'from' has to be before 'to'"}, 400

        indexes = _get_es_indexes(_from, _to)

        body['query'] = {'range': {'day': date_range}}
    else:
        indexes = 'global_land_temperatures_by_city-*'

    client = _get_es_client()

    try:
        response = client.search(index=indexes, body=body, ignore_unavailable=True)
    except ConnectionError:
        return {'error': 'ES does not seem to be reachable'}, 400

    return jsonify(
        {'cities': [hit['_source'] for hit in response['hits']['hits']]}
    )


@app.route('/')
def hello():
    return 'Hello, world!'


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
