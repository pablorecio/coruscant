from unittest.mock import patch
import urllib

import pytest
from elasticsearch.exceptions import ConnectionError

BASE_REQUEST_BODY = {
    'collapse': {'field': 'city'},
    'sort': [{"average_temperature": "desc"}],
    'size': 10
}


@pytest.mark.parametrize('obj, result, status_code', [
    (
        {
            'average_temperature': 41,
            'average_temperature_uncertainty': 0.37,
            'city': 'Jerez',
            'country': 'Spain',
            'day': '2021-08-01',
            'location': {
                'lat': 31.35,
                'lon': 49.01
            }
        },
        {
            'average_temperature': 41,
            'average_temperature_uncertainty': 0.37,
            'city': 'Jerez',
            'country': 'Spain',
            'day': 'Sun, 01 Aug 2021 00:00:00 GMT',
            'location': {
                'lat': 31.35,
                'lon': 49.01
            }
        },
        201
    ),
    (
        {},
        {'errors': [
            'Missing field average_temperature',
            'Missing field average_temperature_uncertainty',
            'Missing field city',
            'Missing field country',
            'Missing field day',
            'Missing field location'
        ]},
        400
    ),
    (
        {
            'average_temperature': 41,
            'average_temperature_uncertainty': 0.37,
            'city': 'Jerez',
            'country': 'Spain',
            'day': '2021-08-01',
            'location': {}
        },
        {'errors': [
            'Missing field location.lat',
            'Missing field location.lon'
        ]},
        400
    ),
    (
        {
            'average_temperature': 41,
            'average_temperature_uncertainty': 0.37,
            'city': 'Jerez',
            'country': 'Spain',
            'day': '202108-01',
            'location': {
                'lat': 31.35,
                'lon': 49.01
            }
        },
        {'errors': [
            'Invalid date format for day'
        ]},
        400
    ),
    (
        {
            'average_temperature': 'aa',
            'average_temperature_uncertainty': '0x.37',
            'city': 'Jerez',
            'country': 'Spain',
            'day': '2021-08-01',
            'location': {
                'lat': 31.35,
                'lon': 49.01
            }
        },
        {'errors': [
            'average_temperature must be a float',
            'average_temperature_uncertainty must be a float'
        ]},
        400
    )
])
@patch('coruscant.api.Measurement.save')
def test_add_measurement(m_measurement_save, client, obj, result, status_code):
    resp = client.post('/api/measurement/add', json=obj)

    if status_code == 201:
        m_measurement_save.assert_called_once_with()
    assert resp.status_code == status_code
    assert resp.json == result


@pytest.mark.parametrize('cities', [
    None,
    5,
    15,
    27
])
@patch('coruscant.api.get_es_client')
def test_get_measurements_size(m_es_client, client, cities):
    path = '/api/measurements'
    body = BASE_REQUEST_BODY.copy()
    if cities:
        path = f'{path}?cities={cities}'
        body['size'] = cities

    resp = client.get(path)
    assert resp.status_code == 200

    m_es_client.return_value.search.assert_called_once_with(
        index='global_land_temperatures_by_city-*',
        body=body,
        ignore_unavailable=True
    )


@pytest.mark.parametrize('cities', [
    'a',
    '121a'
])
def test_get_invalid_measurements_size(client, cities):
    path = f'/api/measurements?cities={cities}'
    resp = client.get(path)
    assert resp.status_code == 400


@pytest.mark.parametrize('from_d, to_d, indexes', [
    ('2019-01-01', '2019-05-01', ['2019']),
    ('2018-01-01', '2020-04-07', ['2018', '2019', '2020']),
    ('2018-01-01', None, ['2018', '2019', '2020', '2021', '2022']),
    (None, '2018-01-01', [str(y) for y in range(1500, 2019)]),
    (None, None, None),
])
@patch('coruscant.api.get_es_client')
def test_date_range_to_es_index(m_es_client, client, from_d, to_d, indexes):
    params = {}
    body = BASE_REQUEST_BODY.copy()
    if from_d or to_d:
        date_range = {}
    if from_d:
        params['from'] = from_d
        date_range['gte'] = from_d
    if to_d:
        params['to'] = to_d
        date_range['lte'] = to_d
    if from_d or to_d:
        body['query'] = {'range': {'day': date_range}}

    path = '/api/measurements'
    if params:
        path = f'{path}?{urllib.parse.urlencode(params)}'

    resp = client.get(path)
    assert resp.status_code == 200

    if indexes:
        final_indexes = ','.join(map(lambda x: f'global_land_temperatures_by_city-{x[:4]}', indexes))
    else:
        final_indexes = 'global_land_temperatures_by_city-*'

    m_es_client.return_value.search.assert_called_once_with(
        index=final_indexes,
        body=body,
        ignore_unavailable=True
    )


@pytest.mark.parametrize('from_d, to_d', [
    ('2019-02-30', '2019-05-01'),
    ('xxxxx', '2020-04-07'),
    ('2020-02-28', '2019-05-01')
])
def test_invalid_date_range_to_es_index(client, from_d, to_d):
    params = {'from': from_d, 'to': to_d}
    path = f'/api/measurements?{urllib.parse.urlencode(params)}'
    resp = client.get(path)
    assert resp.status_code == 400


@patch('coruscant.api.get_es_client')
def test_convert_es_response(m_es_client, client):
    m_es_client.return_value.search.return_value = {
        "took": 26,
        "timed_out": False,
        "_shards": {
            "total": 14,
            "successful": 14,
            "skipped": 10,
            "failed": 0
        },
        "hits": {
            "total": {
                "value": 10000,
                "relation": "gte"
            },
            "max_score": None,
            "hits": [
                {
                    "_index": "global_land_temperatures_by_city-2013",
                    "_type": "_doc",
                    "_id": "pIDEOn4B5Sa-aY70zJZF",
                    "_score": None,
                    "_source": {
                        "day": "2013-07-01T00:00:00",
                        "average_temperature": 39.15600000000001,
                        "average_temperature_uncertainty": 0.37,
                        "city": "Ahvaz",
                        "country": "Iran",
                        "location": {
                            "lat": 31.35,
                            "lon": 49.01
                        }
                    },
                    "fields": {
                        "city": [
                            "Ahvaz"
                        ]
                    },
                    "sort": [
                        39.156
                    ]
                },
                {
                    "_index": "global_land_temperatures_by_city-2013",
                    "_type": "_doc",
                    "_id": "zsbgOn4B5Sa-aY70b05w",
                    "_score": None,
                    "_source": {
                        "day": "2013-07-01T00:00:00",
                        "average_temperature": 39.15600000000001,
                        "average_temperature_uncertainty": 0.37,
                        "city": "Masjed E Soleyman",
                        "country": "Iran",
                        "location": {
                            "lat": 31.35,
                            "lon": 49.01
                        }
                    },
                    "fields": {
                        "city": [
                            "Masjed E Soleyman"
                        ]
                    },
                    "sort": [
                        39.156
                    ]
                },
                {
                    "_index": "global_land_temperatures_by_city-2012",
                    "_type": "_doc",
                    "_id": "j3_EOn4B5Sa-aY70Azsc",
                    "_score": None,
                    "_source": {
                        "day": "2012-07-01T00:00:00",
                        "average_temperature": 38.531,
                        "average_temperature_uncertainty": 0.431,
                        "city": "Abadan",
                        "country": "Iran",
                        "location": {
                            "lat": 29.74,
                            "lon": 48.0
                        }
                    },
                    "fields": {
                        "city": [
                            "Abadan"
                        ]
                    },
                    "sort": [
                        38.531
                    ]
                },
                {
                    "_index": "global_land_temperatures_by_city-2012",
                    "_type": "_doc",
                    "_id": "WLjbOn4B5Sa-aY70HLsL",
                    "_score": None,
                    "_source": {
                        "day": "2012-07-01T00:00:00",
                        "average_temperature": 38.531,
                        "average_temperature_uncertainty": 0.431,
                        "city": "Khorramshahr",
                        "country": "Iran",
                        "location": {
                            "lat": 29.74,
                            "lon": 48.0
                        }
                    },
                    "fields": {
                        "city": [
                            "Khorramshahr"
                        ]
                    },
                    "sort": [
                        38.531
                    ]
                },
                {
                    "_index": "global_land_temperatures_by_city-2012",
                    "_type": "_doc",
                    "_id": "qJHLOn4B5Sa-aY70rEXI",
                    "_score": None,
                    "_source": {
                        "day": "2012-07-01T00:00:00",
                        "average_temperature": 38.049,
                        "average_temperature_uncertainty": 0.6579999999999999,
                        "city": "Buraydah",
                        "country": "Saudi Arabia",
                        "location": {
                            "lat": 26.52,
                            "lon": 44.78
                        }
                    },
                    "fields": {
                        "city": [
                            "Buraydah"
                        ]
                    },
                    "sort": [
                        38.049
                    ]
                }
            ]
        }
    }

    path = '/api/measurements'
    resp = client.get(path)
    assert resp.status_code == 200
    assert resp.json == {
        "cities": [
            {
                "day": "2013-07-01T00:00:00",
                "average_temperature": 39.15600000000001,
                "average_temperature_uncertainty": 0.37,
                "city": "Ahvaz",
                "country": "Iran",
                "location": {
                    "lat": 31.35,
                    "lon": 49.01
                }
            },
            {
                "day": "2013-07-01T00:00:00",
                "average_temperature": 39.15600000000001,
                "average_temperature_uncertainty": 0.37,
                "city": "Masjed E Soleyman",
                "country": "Iran",
                "location": {
                    "lat": 31.35,
                    "lon": 49.01
                }
            },
            {
                "day": "2012-07-01T00:00:00",
                "average_temperature": 38.531,
                "average_temperature_uncertainty": 0.431,
                "city": "Abadan",
                "country": "Iran",
                "location": {
                    "lat": 29.74,
                    "lon": 48.0
                }
            },
            {
                "day": "2012-07-01T00:00:00",
                "average_temperature": 38.531,
                "average_temperature_uncertainty": 0.431,
                "city": "Khorramshahr",
                "country": "Iran",
                "location": {
                    "lat": 29.74,
                    "lon": 48.0
                }
            },
            {
                "day": "2012-07-01T00:00:00",
                "average_temperature": 38.049,
                "average_temperature_uncertainty": 0.6579999999999999,
                "city": "Buraydah",
                "country": "Saudi Arabia",
                "location": {
                    "lat": 26.52,
                    "lon": 44.78
                }
            },
        ]
    }


@patch('coruscant.api.get_es_client')
def test_es_not_responding(m_es_client, client):
    m_es_client.return_value.search.side_effect = ConnectionError
    path = '/api/measurements'
    resp = client.get(path)
    assert resp.status_code == 400
