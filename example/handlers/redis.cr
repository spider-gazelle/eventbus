require "log"
require "redis"
require "../../src/eventbus"

class RedisPublisher < EventBus::EventHandler
  Log = ::Log.for("RedisPublisher")

  def initialize(url : String)
    @redis = Redis.new(url: url)
    Log.info { "Connected to #{url}" }
  end

  def on_event(event : EventBus::Event) : Nil
    channel = "#{event.schema}.#{event.table}.cdc_events"
    Log.info { "Publishing event to redis channel: #{channel}" }
    @redis.publish(channel, event.to_json)
  end

  def on_close
    Log.info { "Closing connection to redis host" }
    @redis.close
  end
end
