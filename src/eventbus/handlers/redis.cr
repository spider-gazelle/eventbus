require "redis"
require "../event"

module EventBus
  @[AutoWire]
  class RedisPublisher < EventHandler
    Log = App::Log.for("RedisPublisher")

    def initialize
      @redis = Redis.new(url: App::REDIS_URL)
      Log.info { "Connected to #{App::REDIS_URL}" }
    end

    def on_event(event : Event) : Nil
      channel = "#{event.schema}.#{event.table}.cdc_events"
      Log.info { "Publishing event to redis channel: #{channel}" }
      @redis.publish(channel, event.to_json)
    end

    def on_close
      Log.info { "Closing connection to redis host" }
      @redis.close
    end
  end
end
