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
