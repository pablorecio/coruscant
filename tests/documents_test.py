from datetime import date
from unittest.mock import patch

from coruscant.documents import Measurement


@patch('coruscant.documents.Document.save')
def test_save_index_name(m_save):
    measurement = Measurement(
        day=date(2019, 11, 29),
        average_temperature=32.2,
        average_temperature_uncertainty=0.5,
        city='Bristol',
        country='United Kingdom',
        location={'lat': 51.45, 'lon': 2.58}
    )

    measurement.save()
    m_save.assert_called_once_with(index='global_land_temperatures_by_city-2019')
