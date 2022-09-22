# Magic EventBus

Crystal shard to capture Postgres database change events via LISTEN/NOTIFY mechanism and publish them to `EventBus::EventHandler`s for further processing. Current implementation comes with `EventBus::RedisPublisher` which publish events to Redis Cluster for publishing to subscribers.

Events are captured and published to Redis channels which are built using scheme `schema.table.cdc_events`, where `schema` and `table` referred to your database schema and table name.

e.g. If you want to subscribe to change events for table `mytable` located in `public` schema, you should subscribe to channel `public.mytable.cdc_events`


### Event payload

All change events are published to channels in *JSON* format

```json
{
  "timestamp": "Timestamp with timezone",
  "schema": "PG Schema",
  "table": "PG Table name",
  "action": "one of insert|update|delete",
  "id": "Table row ID",
  "data" : "null for delete action else JSON object representing table row data"
}
```

## Environment

These environment variables are required for configuring an instance of EventBus

```console
SG_ENV=production  # When set to test, PG_TEST_DATABASE_URL will be used to connect to test database

# Database config:
PG_DATABASE_URL=postgresql://user:password@hostname/database
REDIS_URL=redis://user:password@redis:port/database
```

## Testing

Given you have the following dependencies...

- [docker](https://www.docker.com/)
- [docker-compose](https://github.com/docker/compose)

It is simple to develop the service with docker.

### With Docker

- Run specs, tearing down the `docker-compose` environment upon completion.

```shell-session
$ ./test
```

- Run specs on changes to Crystal files within the `src` and `spec` folders.

```shell-session
$ ./test
```

### Without Docker

- To run tests

```shell-session
$ crystal spec
```

**NOTE:** The upstream dependencies specified in `docker-compose.yml` are required...

## Compiling

```shell-session
$ shards build
```
