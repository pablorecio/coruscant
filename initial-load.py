from csv import DictReader
from datetime import datetime
from decimal import Decimal
from typing import Any, Iterable, Optional

from elasticsearch.helpers import bulk
from elasticsearch_dsl import connections

from coruscant.documents import Measurement

ES_HOST = 'es01:9200'
PATH_TO_FILE = 'data/GlobalLandTemperaturesByCity.csv'
CHUNK_SIZE = 10000


def generate_es_document(obj: dict[str: Any]) -> Measurement:
    """
    Converts the obj containing the CSV data into the dictionary that will
    conform the ES document
    """

    def convert_latitude_longitude(lat: str, lon: str) -> dict[str: Decimal]:
        """
        Helper method to convert latitude and longitude data into a single dictionary.
        This will help ES understand it's a geopoint data field.
        """
        location = {}
        if lat.endswith('N'):
            location['lat'] = Decimal(lat.replace('N', ''))
        else:
            location['lat'] = Decimal(f'-{lat.replace("S", "")}')
        if lon.endswith('E'):
            location['lon'] = Decimal(lon.replace('E', ''))
        else:
            location['lon'] = Decimal(f'-{lon.replace("W", "")}')

        return location

    def clean_temperature(temp: str) -> Optional[Decimal]:
        """
        To prevent python issues with float accuracy, I'm using decimal here
        """
        if temp == '':
            return None
        else:
            return Decimal(temp)

    measurement = Measurement(
        day=datetime.strptime(obj['dt'], '%Y-%m-%d'),
        average_temperature=clean_temperature(obj['AverageTemperature']),
        average_temperature_uncertainty=clean_temperature(obj['AverageTemperatureUncertainty']),
        city=obj['City'],
        country=obj['Country'],
        location=convert_latitude_longitude(obj['Latitude'], obj['Longitude'])
    )
    measurement.meta.index = measurement.day.strftime('global_land_temperatures_by_city-%Y')

    return measurement


def docs_from_csv(filename: str, chunk_size: int = CHUNK_SIZE) -> Iterable[dict[str:Any]]:
    """
    Generate our ES document based on the content of the given CSV file.

    As the file is very large (over 8M rows), I'm using iterators to chunk the processing. This way, we
    can create 10k documents in a single ES request, rather than one request per document.
    """
    with open(filename, 'r') as csv_file:
        reader = DictReader(csv_file)
        current_chunk = []
        for row in reader:
            doc = generate_es_document(row)
            current_chunk.append(doc.to_dict(True))
            if len(current_chunk) >= chunk_size:
                yield current_chunk
                current_chunk = []
        if current_chunk:
            yield current_chunk


def main() -> None:
    i = 0

    # We batch the insert for performance reasons
    for rows in docs_from_csv(PATH_TO_FILE):
        bulk(connections.get_connection(), rows)
        i += 1

        print(f'Inserted {i * CHUNK_SIZE} documents')


if __name__ == '__main__':
    # ES setup
    connections.create_connection(hosts=[ES_HOST], timeout=20)

    measurements = Measurement._index.as_template('global_land_temperatures_by_city', order=0)
    measurements.save()

    main()
