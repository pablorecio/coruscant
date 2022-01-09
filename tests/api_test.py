from unittest.mock import patch, Mock
import urllib

import pytest
from elasticsearch.exceptions import ConnectionError


@pytest.mark.parametrize('cities', [
    None,
    5,
    15,
    27
])
@patch('coruscant.app.Search')
def test_get_measurements_size(m_search, client, cities):
    path = '/api/measurements'
    if cities:
        path = f'{path}?cities={cities}'
    resp = client.get(path)
    assert resp.status_code == 200
    assert m_search.return_value.using.return_value.aggs.bucket.call_args_list[0][0][1].size == (cities or 10)


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
@patch('coruscant.app.Search')
def test_date_range_to_es_index(m_search, client, from_d, to_d, indexes):
    params = {}
    if from_d:
        params['from'] = from_d
    if to_d:
        params['to'] = to_d
    path = '/api/measurements'
    if params:
        path = f'{path}?{urllib.parse.urlencode(params)}'

    resp = client.get(path)
    assert resp.status_code == 200

    if indexes:
        final_indexes = ','.join(map(lambda x: f'global_land_temperatures_by_city-{x[:4]}', indexes))
    else:
        final_indexes = None

    m_search.assert_called_once_with(index=final_indexes)


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


@patch('coruscant.app.Search')
def test_convert_es_response(m_search, client):

    # Very ugly hack to workaround the fact that ES library does not return
    # a dictionary, but rather its own custom dict that we need to convert
    # using .to_dict() method
    res_1_source = Mock()
    res_1_source.to_dict.return_value = {
        "day": "2013-07-01T00:00:00",
        "average_temperature": 39.15600000000001,
        "average_temperature_uncertainty": 0.37,
        "city": "Ahvaz",
        "country": "Iran",
        "location": {
            "lat": 31.35,
            "lon": 49.01
        }
    }
    res_2_source = Mock()
    res_2_source.to_dict.return_value = {
        "day": "2013-07-01T00:00:00",
        "average_temperature": 39.15600000000001,
        "average_temperature_uncertainty": 0.37,
        "city": "Masjed E Soleyman",
        "country": "Iran",
        "location": {
            "lat": 31.35,
            "lon": 49.01
        }
    }
    res_3_source = Mock()
    res_3_source.to_dict.return_value = {
        "day": "2012-07-01T00:00:00",
        "average_temperature": 38.531,
        "average_temperature_uncertainty": 0.431,
        "city": "Abadan",
        "country": "Iran",
        "location": {
            "lat": 29.74,
            "lon": 48.0
        }
    }
    res_4_source = Mock()
    res_4_source.to_dict.return_value = {
        "day": "2012-07-01T00:00:00",
        "average_temperature": 38.531,
        "average_temperature_uncertainty": 0.431,
        "city": "Khorramshahr",
        "country": "Iran",
        "location": {
            "lat": 29.74,
            "lon": 48.0
        }
    }
    res_5_source = Mock()
    res_5_source.to_dict.return_value = {
        "day": "2000-07-01T00:00:00",
        "average_temperature": 38.283,
        "average_temperature_uncertainty": 0.436,
        "city": "Baghdad",
        "country": "Iraq",
        "location": {
            "lat": 32.95,
            "lon": 45.0
        }
    }
    (
        m_search.return_value
        .using.return_value
        .params.return_value
        .query.return_value
        .execute
        .return_value.aggregations
    ) = {
        "cities": {
            "doc_count_error_upper_bound": -1,
            "sum_other_doc_count": 578382,
            "buckets": [
                {
                    "key": "Ahvaz",
                    "doc_count": 165,
                    "by_top_hit": {
                        "hits": {
                            "total": {
                                "value": 165,
                                "relation": "eq"
                            },
                            "max_score": None,
                            "hits": [
                                {
                                    "_index": "global_land_temperatures_by_city-2013",
                                    "_type": "_doc",
                                    "_id": "pIDEOn4B5Sa-aY70zJZF",
                                    "_score": None,
                                    "_source": res_1_source,
                                    "sort": [
                                        39.156
                                    ]
                                }
                            ]
                        }
                    },
                    "max_average_temperature": {
                        "value": 39.15599822998047
                    }
                },
                {
                    "key": "Masjed E Soleyman",
                    "doc_count": 165,
                    "by_top_hit": {
                        "hits": {
                            "total": {
                                "value": 165,
                                "relation": "eq"
                            },
                            "max_score": None,
                            "hits": [
                                {
                                    "_index": "global_land_temperatures_by_city-2013",
                                    "_type": "_doc",
                                    "_id": "zsbgOn4B5Sa-aY70b05w",
                                    "_score": None,
                                    "_source": res_2_source,
                                    "sort": [
                                        39.156
                                    ]
                                }
                            ]
                        }
                    },
                    "max_average_temperature": {
                        "value": 39.15599822998047
                    }
                },
                {
                    "key": "Abadan",
                    "doc_count": 165,
                    "by_top_hit": {
                        "hits": {
                            "total": {
                                "value": 165,
                                "relation": "eq"
                            },
                            "max_score": None,
                            "hits": [
                                {
                                    "_index": "global_land_temperatures_by_city-2012",
                                    "_type": "_doc",
                                    "_id": "j3_EOn4B5Sa-aY70Azsc",
                                    "_score": None,
                                    "_source": res_3_source,
                                    "sort": [
                                        38.531
                                    ]
                                }
                            ]
                        }
                    },
                    "max_average_temperature": {
                        "value": 38.53099822998047
                    }
                },
                {
                    "key": "Khorramshahr",
                    "doc_count": 165,
                    "by_top_hit": {
                        "hits": {
                            "total": {
                                "value": 165,
                                "relation": "eq"
                            },
                            "max_score": None,
                            "hits": [
                                {
                                    "_index": "global_land_temperatures_by_city-2012",
                                    "_type": "_doc",
                                    "_id": "WLjbOn4B5Sa-aY70HLsL",
                                    "_score": None,
                                    "_source": res_4_source,
                                    "sort": [
                                        38.531
                                    ]
                                }
                            ]
                        }
                    },
                    "max_average_temperature": {
                        "value": 38.53099822998047
                    }
                },
                {
                    "key": "Baghdad",
                    "doc_count": 108,
                    "by_top_hit": {
                        "hits": {
                            "total": {
                                "value": 108,
                                "relation": "eq"
                            },
                            "max_score": None,
                            "hits": [
                                {
                                    "_index": "global_land_temperatures_by_city-2000",
                                    "_type": "_doc",
                                    "_id": "pYfHOn4B5Sa-aY70LAlL",
                                    "_score": None,
                                    "_source": res_5_source,
                                    "sort": [
                                        38.283
                                    ]
                                }
                            ]
                        }
                    },
                    "max_average_temperature": {
                        "value": 38.28300094604492
                    }
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
                "average_temperature": 39.15600000000001,
                "average_temperature_uncertainty": 0.37,
                "city": "Ahvaz",
                "country": "Iran",
                "day": "2013-07-01T00:00:00",
                "location": {
                    "lat": 31.35,
                    "lon": 49.01
                }
            },
            {
                "average_temperature": 39.15600000000001,
                "average_temperature_uncertainty": 0.37,
                "city": "Masjed E Soleyman",
                "country": "Iran",
                "day": "2013-07-01T00:00:00",
                "location": {
                    "lat": 31.35,
                    "lon": 49.01
                }
            },
            {
                "average_temperature": 38.531,
                "average_temperature_uncertainty": 0.431,
                "city": "Abadan",
                "country": "Iran",
                "day": "2012-07-01T00:00:00",
                "location": {
                    "lat": 29.74,
                    "lon": 48.0
                }
            },
            {
                "average_temperature": 38.531,
                "average_temperature_uncertainty": 0.431,
                "city": "Khorramshahr",
                "country": "Iran",
                "day": "2012-07-01T00:00:00",
                "location": {
                    "lat": 29.74,
                    "lon": 48.0
                }
            },
            {
                "average_temperature": 38.283,
                "average_temperature_uncertainty": 0.436,
                "city": "Baghdad",
                "country": "Iraq",
                "day": "2000-07-01T00:00:00",
                "location": {
                    "lat": 32.95,
                    "lon": 45.0
                }
            }
        ]
    }

@patch('coruscant.app.Search')
def test_es_not_responding(m_search, client):
    (
        m_search.return_value
        .using.return_value
        .params.return_value
        .query.return_value
        .execute.side_effects
    ) = ConnectionError
    path = '/api/measurements'
    resp = client.get(path)
    assert resp.status_code == 200