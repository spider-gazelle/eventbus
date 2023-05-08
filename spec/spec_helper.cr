require "spec"
require "../src/eventbus"

PG_DATABASE_URL = ENV["PG_DATABASE_URL"]
REDIS_URL       = ENV["REDIS_URL"]
TABLE           = "spec_test"

Spec.before_suite { run_sql("CREATE TABLE IF NOT EXISTS #{TABLE}(id int, name VARCHAR(100))") }
Spec.after_suite { run_sql("drop table if exists #{TABLE}") }
Spec.before_each { run_sql("delete from #{TABLE}") }

def run_sql(cmd)
  DB.open(PG_DATABASE_URL) do |db|
    db.exec(cmd)
  end
end

def insert_rec(id : Int)
  run_sql("insert into #{TABLE} values(#{id}, 'Testing')")
end

def update_rec(id : Int, value : String)
  run_sql("update #{TABLE} set name = '#{value}' where id = #{id}")
end

class SpecHandler < EventBus::EventHandler
  getter events : Array(String)

  def initialize(@ch : Channel(EventBus::Event)? = nil)
    @events = Array(String).new
  end

  def on_event(event : EventBus::Event) : Nil
    @events << "on_event"
    @ch.try &.send(event)
  end

  def on_start : Nil
    @events << "on_start"
  end

  def on_connect : Nil
    @events << "on_connect"
  end

  def on_close : Nil
    @events << "on_close"
  end
end

class SpecRedisPublisher < EventBus::EventHandler
  def initialize(url : String)
    @redis = Redis.new(url: url)
  end

  def on_event(event : EventBus::Event) : Nil
    channel = "#{event.schema}.#{event.table}.cdc_events"
    @redis.publish(channel, event.to_json)
  end

  def on_close
    @redis.close
  end
end
