from datetime import datetime

from elasticsearch.exceptions import ConnectionError
from flask import request, jsonify

from coruscant.documents import Measurement
from coruscant.es import get_es_client


# @app.route('/api/measurement/add')
def measurement_add():
    FIELDS = [
        'average_temperature',
        'average_temperature_uncertainty',
        'city',
        'country',
        'day',
        'location',
        'location.lat',
        'location.lon'
    ]
    body = request.json
    errors = []

    # Validations
    for field in FIELDS:
        if '.' in field:
            base, _field = field.split('.')
            if base in body and _field not in body[base]:
                errors.append(f'Missing field {field}')
        else:
            if field not in body:
                errors.append(f'Missing field {field}')

        if field in body:
            # Day parsing
            if field == 'day':
                try:
                    body['day'] = datetime.strptime(body['day'], '%Y-%m-%d').date()
                except ValueError:
                    errors.append('Invalid date format for day')

            # Validates floats
            if field in ('average_temperature', 'average_temperature_uncertainty'):
                try:
                    body[field] = float(body[field])
                except ValueError:
                    errors.append(f'{field} must be a float')

    if errors:
        return {'errors': errors}, 400

    measurement = Measurement(**body)
    measurement.save()

    return measurement.to_dict(), 201


# @app.route('/api/measurement/update')

# @app.route('/api/measurements')
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

        if _to:
            try:
                # Adds it to the query
                date_range['lte'] = _to
                _to = datetime.strptime(_to, '%Y-%m-%d').date()
            except ValueError:
                return {"error": f"Invalid date format: {_to}"}, 400

        if _from and _to and _from > _to:
            return {"error": "'from' has to be before 'to'"}, 400

        indexes = Measurement.get_indexes_for_range(_from, _to)

        body['query'] = {'range': {'day': date_range}}
    else:
        indexes = 'global_land_temperatures_by_city-*'

    client = get_es_client()

    try:
        response = client.search(index=indexes, body=body, ignore_unavailable=True)
    except ConnectionError:
        return {'error': 'ES does not seem to be reachable'}, 400

    return jsonify(
        {'cities': [hit['_source'] for hit in response['hits']['hits']]}
    )
