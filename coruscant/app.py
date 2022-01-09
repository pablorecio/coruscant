from datetime import date, datetime

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError
from elasticsearch_dsl import A, Search
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
    query_parameters = request.args

    try:
        number_of_cities = int(query_parameters.get('cities', 10))
    except ValueError:
        return {"error": f"Invalid cities number: {query_parameters.get('cities')}"}, 400

    _from = request.args.get('from')
    _to = request.args.get('to')

    if _from or _to:

        if _from:
            try:
                _from = datetime.strptime(_from, '%Y-%m-%d').date()
            except ValueError:
                return {"error": f"Invalid date format: {_from}"}, 400
        else:
            _from = date(1500, 1, 1)  # lowest value is ~ 1700 but this can be cleaner

        if _to:
            try:
                _to = datetime.strptime(_to, '%Y-%m-%d').date()
            except ValueError:
                return {"error": f"Invalid date format: {_to}"}, 400
        else:
            _to = date.today()

        if _from > _to:
            return {"error": "'from' has to be before 'to'"}, 400

        indexes = _get_es_indexes(_from, _to)
    else:
        indexes = None

    client = _get_es_client()

    a_cities = A('terms', field='city', size=number_of_cities, order={'max_average_temperature': 'desc'})
    a_max_average_temperature = A('max', field='average_temperature')
    a_by_top_hit = A('top_hits', size=1, sort=[{'average_temperature': 'desc'}])

    s = Search(index=indexes).using(client)
    (
        s.aggs
        .bucket('cities', a_cities)
        .metric('max_average_temperature', a_max_average_temperature)
        .pipeline('by_top_hit', a_by_top_hit)
    )
    # This is equivalent to:
    # {
    #     "aggs": {
    #         "cities": {
    #             "terms": {
    #                 "field": "city",
    #                 "size": 5,
    #                 "order": { "max_average_temperature" : "desc" }
    #             },
    #             "aggs": { // Sub-aggregations
    #                 "by_top_hit": { "top_hits": {
    #                     "sort" : [
    #                         { "average_temperature" : "desc" }
    #                     ],
    #                     "size": 1
    #                 } },
    #                 "max_average_temperature": { "max": { "field": "average_temperature" } }
    #             }
    #         }
    #     }
    # }

    try:
        resp = s.params(ignore_unavailable=True).query().execute()
    except ConnectionError:
        return {'error': 'ES does not seem to be reachable'}, 400

    return jsonify(
        {
            'cities': [
                city['by_top_hit']['hits']['hits'][0]['_source'].to_dict()
                for city in resp.aggregations['cities']['buckets']
            ]
        }
    )


@app.route('/')
def hello():
    return 'Hello, world!'


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
