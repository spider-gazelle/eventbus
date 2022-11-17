# Magic EventBus

Crystal shard to capture _Postgres_ database change events via _LISTEN/NOTIFY_ mechanism and publish them to `EventBus::EventHandler`s for further processing. Shard comes with **retry and watchdog** functionality, and try to re-connect automatically until it has exhausted all of the configured `retry_attempts`. To configure **Retry Mechanics** and **WatchDog** follow below configuration options.

### Retry Configuration

* `retry_attempts`: Number of attempts to try before giving up. Use `0` or less for **infinite attempts**. Default `0`
* `retry_interval`: Interval in seconds to wait for next attempt to re-connect. Default to `5` seconds.

> Set `on_error` call back if you want to receive final give-up message, along with last exception received on re-connection attempt. _If no `on_error` callback is configured, it will raise the **last exception**_

### WatchDog Configurations

Shard monitors the database connectivity in a separate connection and watch for disconnection/freeze like situations and follow the same semantics of retrying after configured interval. To configure watchdog heartbeat interval and connection time_out, configure `EventBus` with below configurations.

* `watchdog_interval` : Heart beat interval in seconds. Default to `5` seconds
* `timeout`: Interval in seconds to wait for network connectivity. Default to `5` seconds.

WatchDog will trigger at every `watchdog_interval` and wait for connection status for `timeout` seconds before timing out.

### `EventBus::EventHandler` Lifecycle  methods

Below lifecycle methods are invoked for all registered handlers

* **on_start** - invoked when EventBus is going to start. Override this method
* **on_connect** - invoked when EventBus PG listener get connected to Postgres.
* **on_event** - invoked when an event is received. Refer to `EventBus::Event` struct for structure
* **on_close** - invoked when EventBus is going to shutdown

### `EventBus::Event` structure

*  **timestamp** : `Time` - PG Timestamp when event occurred
*  **schema** : `String` - Schema name of PG
*  **table** : `String` - PG Table name where event occurred
*  **action** : `EventBus::Action` - contains one of `INSERT` | `UPDATE` | `DELETE` based on event
*  **id** : `JSON::Any` - PG Table column `id` value
*  **data** : `String` - JSON object in String representing table row data
## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     eventbus:
       github: spider-gazelle/eventbus
   ```

2. Run `shards install`

## Usage

```crystal
require "eventbus"

# Instantiate EventBus object with Postgres URI
eventbus =  EventBus.new(PG_DATABASE_URL, retry_attempts: 5, retry_interval: 5)

# Register Custom EventHandlers which will receive events

eventbus.add_handler MyLogger.new, MyRedisPub.new 

# Register Error handler which will get invoked on fatal error
eventbus.on_error ->(ex : EventBus::ErrHandlerType) {
  puts " Received Fatal error from EventBus\n"
  puts ex
  puts "\n terminating gracefully"
  eventbus.close rescue nil
}

# enable cdc mechansim on all or particular table

 eventbus.ensure_cdc_for_all_tables 
 # OR
 eventbus.ensure_cdc_for("MyTable")

 # Start Event Bus

 eventbus.start # for asycn (non-blocking mode)
 # OR
 eventbus.run # for sync (blocking mode)

 # Once done

 eventbus.close
```

## Examples

Located under `example` folder.

### Application (`application.cr`)

Complete demo application which make use of handlers under example folder and publishes PG events to Redis Cluster. To run demo application ensure you set below environment variables for it to work.

```console
# Database config:
PG_DATABASE_URL=postgresql://user:password@hostname/database
REDIS_URL=redis://user:password@redis:port/database
```

### Client Subscriber (`subscriber.cr`)

Demo client application which connects to `REDIS_URL` and subscribe to `CHANNEL` for events.

To run demo client subscriber ensure you set below environment variables for it to work.

```console
# Database config:
REDIS_URL=redis://user:password@redis:port/database
CHANNEL="name of your application published channel"
```

###  `EventBus::EventHandler` implementations.

Below sample implementations are provided which are used by demo application.
#### `EventLogger` (`example/handlers/log.cr`)

  Sample implementation which simply logs events as they are received.

#### `RedisPublisher` (`example/handlers/redis.cr`)

 Sample implementation which publish events to Redis Cluster for publishing to subscribers.

Events are captured and published to Redis channels which are built using scheme `schema.table.cdc_events`, where `schema` and `table` referred to your database schema and table name.

e.g. If you want to subscribe to change events for table `mytable` located in `public` schema, you should subscribe to channel `public.mytable.cdc_events`

##### Event payload

All change events are published to channels in *JSON* format

```json
{
  "timestamp": "Timestamp with timezone",
  "schema": "PG Schema",
  "table": "PG Table name",
  "action": "one of insert|update|delete",
  "id": "Table row ID",
  "data" : "JSON object representing table row data"
}
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
