from datetime import date
from typing import Optional

from elasticsearch_dsl import Date, Document, Float, GeoPoint, Keyword, Text


class Measurement(Document):
    day = Date()
    average_temperature = Float()
    average_temperature_uncertainty = Float()
    city = Keyword()
    country = Text()
    location = GeoPoint()

    class Index:
        name = 'global_land_temperatures_by_city-*'
        settings = {
            'number_of_shards': 1
        }

    def save(self, **kwargs):
        # override the index to go to the proper timeslot
        kwargs['index'] = self.day.strftime('global_land_temperatures_by_city-%Y')
        return super().save(**kwargs)

    @classmethod
    def get_indexes_for_range(cls, _from: Optional[date] = None, _to: Optional[date] = None) -> str:
        if not (_from or _to):  # if both are missing, we return the wildcard
            return cls.Index.name

        if not _from:
            _from = date(1500, 1, 1)  # for example, but we can be more clever than this
        if not _to:
            _to = date.today()

        current = _from
        indexes = []
        while current <= _to:
            indexes.append(current.year)
            current = current.replace(year=current.year + 1)

        return ','.join(map(lambda x: f'global_land_temperatures_by_city-{x}', indexes))
