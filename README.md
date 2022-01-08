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
