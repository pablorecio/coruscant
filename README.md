# Coruscant - Simple web service to query temperature information

## Initial considerations

### Database decision

We are only working with a single table (`Global_Land_Temperatures_By_City`) with the following fields:

- dt: date
- AverageTemperature: float
- AverageTemperatureUncertainty: float
- City: String
- Country: String
- Latitude: String (could be converted to a float)
- Longitude: String  (could be converted to a float)

And our current requirements are:

- 8,599,213 entities in a single table, with of course support for more: the latest data is from 2013, we can safely assume we should acommodate for more data since then.
- For this case, we don't have any relationships between tables as we only have that single one.
- Our database should allow for some simple queries like "group by" and aggregations, for instance, averages. Also, should of course support create/update operations.
- Write times are not critical as we'll be loading it in a batch. Even if this started to be updated daily, we don't need a very high write latency.

With this in mind, I find difficult to justify a SQL database like PostgreSQL. It has a lot of advantages like query simplicity, easy to integrate with frameworks like Django or Flask, but with a single type of entity a document-based DB seems a better fit.

But, as said before, this is just based on the assumptions that we won't be adding relationships, our queries aren't complex, and we won't need a high latency in write operations. This will be more of a read endpoint.

For the purposes of this task, I am going to use [Elasticsearch](https://www.elastic.co/). It is a technology I'm familiar with, and it's capable to cover our requirements fairly well. The only caveat are the updates, as they aren't a cheap operation, but I'm working under the assumption those operations are not going to be very frequent. While our current query needs are simple, as long as we don't introduce relationships, Elasticsearch will support a lot of complex queries, allowing for horizontal scaling if we use a proper indexing architecture.

MongoDB or DynamoDB are potential alternatives too: both are great document-based databases with great industry support, but I'm favouring ES due to my knowledge with it. I believe the experience of the development team is something to keep in mind too when making a decision. Sometimes is better to pick a good (but not necessarily the best) option if the team who will support it is well knowledgeable with it.

### Database design

Elasticsearch a document-based database, and the first thing we need is to define said documents. To this effect, I will be using:

- `dt` - [date](https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html)
- `AverageTemperature` - [float](https://www.elastic.co/guide/en/elasticsearch/reference/current/number.html)
- `AverageTemperatureUncertainty` - [float](https://www.elastic.co/guide/en/elasticsearch/reference/current/number.html)
- `City` - [text](https://www.elastic.co/guide/en/elasticsearch/reference/current/text.html)
- `Country` - [text](https://www.elastic.co/guide/en/elasticsearch/reference/current/text.html)
- `Location` - [geo_point](https://www.elastic.co/guide/en/elasticsearch/reference/current/geo-point.html)

This should cover us well enough. Next, will be defining the index and sharding strategy. In Elasticsearch, we need to decide in which index/indexes we are going to store our data, and how we are going to shard it. It may not be relevant for the amount of data, but it is good practice for horizontal scaling and query performance.

Before deciding on this, I ran some data anlysis using Pandas to count how many days worth of data we have, and how many total cities:

```
>>> import pandas as pd
>>> df = pd.read_csv('/Users/pablorecioquijano/Downloads/GlobalLandTemperaturesByCity.csv')
>>> len(df.groupby('dt'))
3239
>>> len(df.groupby(['City', 'Country']))  # to prevent issues with name clashing (London, UK or London, Canada)
3448
```

Given the requirements are specifying a query based in time (specifically year), I think it makes sense to use the same attribute to split the indexes. I wouldn't go as far as an index per-month, but an index per-year might make some sense. This is an oversimplification though: in a real-life scenario we'll have to take more things into consideration:

- What's the data distribution like? If we have a lot of data in the last, say, 80 years, those indexes will be much larger than those from 150 years ago.
- What's the query frequency in older data points? We may end up having hot and cold indexes by using this design.

In a production situation, we'd run some more queries to assess the data distribution and perhaps decide on a different indexing pattern, plus trying to get more requirements over how this data will be queried.

Lastly, for sharding I'll use the "City" as the shard key. This means that every document for a single city within the same index will live in the same shard, and it's critical for performance: when we run a query to get data from a given city, we'll only need to hit the shards containing that info.

### Web Framework

Given I did not choose a relation database, I will not gain much by using Django on this use case. Without taking advantage of the admin, ORM, forms, templates, etc... seems quite unnecessary to go full-on "batteries included" approach. So instead, I'll use Flask with some additional libraries like the Elasticsearch pyhton client.

## Docker startup

To get everything started up, we just need both `docker` and `docker-compose`. In a shell, we can run:

```
$ docker-compose build
$ docker-compose up
```

This will spin up our ES cluster (containing 2 nodes) and the Flask server.

## Data import

The data is located in `data/GlobalLandTemperaturesByCity.csv` and we can load it by running

```
$ docker-compose exec web python initial-load.py
```

while both ES and the webserver are up and running. It takes about 90 minutes in my machine due to the volume of data. This would not be a worry in a production environment as the ES cluster will be much faster than a local dev environment.

## Running tests

Using pyenv is recommended to run the tests locally:

```
$ pyenv virtualenv 3.10.0 coruscant
$ pyenv activate coruscant
$ pip install -r requirements-tests.txt
$ pytest
```

## API

The web app is serving 3 different endpoints:

### POST /api/measurement/add

Creates a new measurement with the following required fields:

JSON body:
- `average_temperature` - Float
- `average_temperature_uncertainty` - Float
- `city` - String
- `country` - String
- `day` - Date. Format YYYY-MM-DD.
- `location` - Object with both `lat` and `lon` as floats.

Example:

```
{
    "average_temperature": 36.656,
    "average_temperature_uncertainty": 0.47,
    "city": "Ahvaz",
    "country": "Iran",
    "day": "2021-12-01",
    "location": {
        "lat": 31.35,
        "lon": 49.01
    }
}
```

### PATCH /api/measurement/update?city=<city>&day=<day>

Updates a measurement by its city and day. It receives at least one of:

JSON body:
- `average_temperature` - Float
- `average_temperature_uncertainty` - Float

### GET /api/measurements?cities=<N>&from=<from>&to=<to>

Gets the N top cities with the highest monthly average in the given time range. All paramenters are optional, with `N` defaulting to 10, `from` defaulting to 1500-01-01 and `to` to today.

## Examples

- Find the entry whose city has the highest AverageTemperature since the year 2000.

```
curl --location --request GET 'http://localhost:5000/api/measurements?cities=1&from=2000-01-01'
```

```
{
    "cities": [
        {
            "average_temperature": 39.15600000000001,
            "average_temperature_uncertainty": 0.37,
            "city": "Ahvaz",
            "country": "Iran",
            "day": "2013-07-01",
            "location": {
                "lat": 31.35,
                "lon": 49.01
            }
        }
    ]
}
```

- Following above: assume the temperature observation of the city last month breaks the record. It is 0.1 degree higher with the same uncertainty. Create this entry.


```
curl --location --request POST 'http://localhost:5000/api/measurement/add' \
--header 'Content-Type: application/json' \
--data-raw '{
    "average_temperature": 39.15600000000001,
    "average_temperature_uncertainty": 0.47,
    "city": "Ahvaz",
    "country": "Iran",
    "day": "2021-12-01",
    "location": {
        "lat": 31.35,
        "lon": 49.01
    }
}'
```

```
{
    "average_temperature": 39.15600000000001,
    "average_temperature_uncertainty": 0.47,
    "city": "Ahvaz",
    "country": "Iran",
    "day": "Wed, 01 Dec 2021 00:00:00 GMT",
    "location": {
        "lat": 31.35,
        "lon": 49.01
    }
}
```

- Following question 1: assume the returned entry has been found erroneous. The actual average temperature of this entry is 2.5 degrees lower. Update this entry.

```
curl --location --request PATCH 'http://localhost:5000/api/measurement/update?city=Ahvaz&day=2021-12-01' \
--header 'Content-Type: application/json' \
--data-raw '{
    "average_temperature": 36.656
}'
```

```
{
    "average_temperature": 36.656,
    "average_temperature_uncertainty": 0.47,
    "city": "Ahvaz",
    "country": "Iran",
    "day": "Wed, 01 Dec 2021 00:00:00 GMT",
    "location": {
        "lat": 31.35,
        "lon": 49.01
    }
}
```

## Last thoughts

This little task took me around 8 hours to complete, mainly due to some issues back and forth with the Elasticsearch libraries. Initially, as added in a code comment, I was going to use elasticsearch-dsl everywhere, but I found out it did not support `collapse` queries.

I worked around it by using aggregation queries, but it ended up being overly complicated just to shoehorn that library everywhere, so I trailed back and refactored the endpoint to use the basic elasticsearch python client and turned out with the code a bit cleaner.

That said, in a real-world example I'd probably use some library like flask-restful to make the API endpoints a bit cleaner, integration with Swagger, and perhaps a better ES wrapper to make the logic a bit more transparent.

Also, spent a bit of more time being extra careful about the endpoints being well covered for multiple cases, and not raise 500s, adding some comprehensive error handling for the frontend to use.
